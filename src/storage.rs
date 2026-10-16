//! Image bytes live either on the local filesystem or in one mounted S3 bucket.
//!
//! Credentials are read from the environment at startup and are not kept on
//! [`crate::config::EncodingConfig`], whose `Debug` output shows up in logs.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::prefix::PrefixStore;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

/// Where original images are read from, and where uploads are written.
#[derive(Clone)]
pub struct ImageStore {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    label: String,
    kind: StoreKind,
}

enum StoreKind {
    Local { root: PathBuf },
    Object { store: Arc<dyn ObjectStore> },
}

/// A pass-through body. The size is the object or file length, not a buffered copy.
pub struct OpenedObject {
    pub size: u64,
    pub stream: std::pin::Pin<Box<dyn futures_util::Stream<Item = std::io::Result<Bytes>> + Send>>,
}

impl std::fmt::Debug for ImageStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageStore")
            .field("backend", &self.inner.label)
            .finish()
    }
}

impl std::fmt::Display for ImageStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.inner.label)
    }
}

impl ImageStore {
    pub fn local(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let label = format!("file:{}", root.display());
        Self {
            inner: Arc::new(StoreInner {
                label,
                kind: StoreKind::Local { root },
            }),
        }
    }

    /// Mounts an S3 bucket when `IMAGE_PROXY_S3_BUCKET` is set. Otherwise the
    /// store is the local `root_path` directory.
    pub fn from_env(root_path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let root_path = root_path.into();
        match parse_mount(&env_value)? {
            MountConfig::Local => Ok(Self::local(root_path)),
            MountConfig::S3(settings) => connect_s3(settings),
        }
    }

    /// In-memory object store. `prefix` is the mount path inside the bucket,
    /// the same role as `IMAGE_PROXY_S3_PREFIX`.
    pub fn in_memory(prefix: &str) -> anyhow::Result<Self> {
        let prefix = normalize_prefix(prefix)?;
        let memory = InMemory::new();
        let store: Arc<dyn ObjectStore> = if prefix.is_empty() {
            Arc::new(memory)
        } else {
            let prefix_path = ObjectPath::parse(&prefix)?;
            Arc::new(PrefixStore::new(memory, prefix_path))
        };
        let label = if prefix.is_empty() {
            "memory://".to_string()
        } else {
            format!("memory://{prefix}")
        };
        Ok(Self {
            inner: Arc::new(StoreInner {
                label,
                kind: StoreKind::Object { store },
            }),
        })
    }

    pub fn label(&self) -> &str {
        &self.inner.label
    }

    /// Opens the object for streaming. `Ok(None)` means it is not there.
    #[tracing::instrument(level = "debug", skip(self), fields(store = %self.inner.label))]
    pub async fn open(&self, relative: &Path) -> std::io::Result<Option<OpenedObject>> {
        match &self.inner.kind {
            StoreKind::Local { root } => open_local(root, relative).await,
            StoreKind::Object { store } => open_object(store.as_ref(), relative).await,
        }
    }

    /// Reads the whole object, enforcing `max` when it is set.
    #[tracing::instrument(level = "debug", skip(self), fields(store = %self.inner.label))]
    pub async fn load(&self, relative: &Path, max: Option<usize>) -> std::io::Result<Bytes> {
        match &self.inner.kind {
            StoreKind::Local { root } => {
                let path = local_path(root, relative)?;
                let bytes = crate::utils::load_bytes_from_disk(&path, max).await?;
                Ok(Bytes::from(bytes))
            }
            StoreKind::Object { store } => load_object(store.as_ref(), relative, max).await,
        }
    }

    /// Writes `bytes` at `relative`, replacing any object already stored there.
    #[tracing::instrument(level = "debug", skip(self, bytes), fields(store = %self.inner.label, bytes = bytes.len()))]
    pub async fn put(&self, relative: &Path, bytes: Bytes) -> std::io::Result<()> {
        match &self.inner.kind {
            StoreKind::Local { root } => put_local(root, relative, &bytes).await,
            StoreKind::Object { store } => put_object(store.as_ref(), relative, bytes).await,
        }
    }
}

enum MountConfig {
    Local,
    S3(S3Settings),
}

struct S3Settings {
    bucket: String,
    region: String,
    prefix: String,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    security_token: Option<String>,
    path_style: bool,
}

fn env_value(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_mount(get: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<MountConfig> {
    let Some(bucket) = get("IMAGE_PROXY_S3_BUCKET") else {
        return Ok(MountConfig::Local);
    };
    validate_bucket(&bucket)?;

    let region = get("IMAGE_PROXY_S3_REGION")
        .or_else(|| get("AWS_REGION"))
        .or_else(|| get("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|| "us-east-1".to_string());
    let prefix = normalize_prefix(&get("IMAGE_PROXY_S3_PREFIX").unwrap_or_default())?;
    let endpoint = get("IMAGE_PROXY_S3_ENDPOINT").or_else(|| get("AWS_ENDPOINT_URL"));
    if let Some(endpoint) = &endpoint
        && !(endpoint.starts_with("https://") || endpoint.starts_with("http://"))
    {
        anyhow::bail!("IMAGE_PROXY_S3_ENDPOINT must start with https:// or http://");
    }

    let explicit_key = get("IMAGE_PROXY_S3_ACCESS_KEY_ID");
    let explicit_secret = get("IMAGE_PROXY_S3_SECRET_ACCESS_KEY");
    let explicit_token =
        get("IMAGE_PROXY_S3_SECURITY_TOKEN").or_else(|| get("IMAGE_PROXY_S3_SESSION_TOKEN"));
    let using_explicit =
        explicit_key.is_some() || explicit_secret.is_some() || explicit_token.is_some();

    let (access_key_id, secret_access_key, security_token) = if using_explicit {
        match (explicit_key, explicit_secret) {
            (Some(key), Some(secret)) => (Some(key), Some(secret), explicit_token),
            _ => {
                anyhow::bail!(
                    "S3 static credentials need both IMAGE_PROXY_S3_ACCESS_KEY_ID and IMAGE_PROXY_S3_SECRET_ACCESS_KEY; the security token is optional"
                );
            }
        }
    } else {
        // No IMAGE_PROXY_* keys. Leave the pair empty so AmazonS3Builder::from_env
        // can use an IAM role, a web identity token, or AWS_ACCESS_KEY_ID /
        // AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN together.
        (None, None, None)
    };

    let path_style = match get("IMAGE_PROXY_S3_PATH_STYLE") {
        Some(value) => value
            .parse::<bool>()
            .map_err(|_| anyhow::anyhow!("IMAGE_PROXY_S3_PATH_STYLE must be true or false"))?,
        // MinIO, LocalStack, and most custom endpoints expect path-style URLs.
        None => endpoint.is_some(),
    };

    Ok(MountConfig::S3(S3Settings {
        bucket,
        region,
        prefix,
        endpoint,
        access_key_id,
        secret_access_key,
        security_token,
        path_style,
    }))
}

fn validate_bucket(bucket: &str) -> anyhow::Result<()> {
    if bucket.is_empty()
        || bucket.contains('/')
        || bucket.contains('\\')
        || bucket.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        anyhow::bail!("IMAGE_PROXY_S3_BUCKET is not a valid bucket name");
    }
    Ok(())
}

fn normalize_prefix(raw: &str) -> anyhow::Result<String> {
    let trimmed = raw.trim().trim_matches('/');
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    for segment in trimmed.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            anyhow::bail!(
                "IMAGE_PROXY_S3_PREFIX must be a relative prefix without '.' or '..' segments"
            );
        }
    }
    Ok(trimmed.to_string())
}

fn s3_label(settings: &S3Settings) -> String {
    let mut label = format!("s3://{}", settings.bucket);
    if !settings.prefix.is_empty() {
        label.push('/');
        label.push_str(&settings.prefix);
    }
    label.push_str(" region=");
    label.push_str(&settings.region);
    if settings.path_style {
        label.push_str(" path-style");
    }
    if settings.endpoint.is_some() {
        label.push_str(" custom-endpoint");
    }
    if settings.security_token.is_some() {
        label.push_str(" security-token");
    }
    label
}

fn connect_s3(settings: S3Settings) -> anyhow::Result<ImageStore> {
    let label = s3_label(&settings);
    let mut builder = match (settings.access_key_id, settings.secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => {
            let mut builder = AmazonS3Builder::new()
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key);
            if let Some(token) = settings.security_token.clone() {
                builder = builder.with_token(token);
            }
            builder
        }
        (None, None) => {
            tracing::info!(
                bucket = %settings.bucket,
                "no static S3 credentials configured; using the default AWS credential chain"
            );
            AmazonS3Builder::from_env()
        }
        _ => {
            anyhow::bail!(
                "S3 static credentials need both an access key id and a secret access key"
            );
        }
    };

    builder = builder
        .with_bucket_name(&settings.bucket)
        .with_region(&settings.region)
        .with_virtual_hosted_style_request(!settings.path_style);

    if let Some(endpoint) = &settings.endpoint {
        builder = builder.with_endpoint(endpoint);
        if endpoint.starts_with("http://") {
            tracing::warn!(
                "S3 endpoint uses HTTP; access keys and security tokens are visible on the network"
            );
            builder = builder.with_allow_http(true);
        }
    }

    let s3 = builder.build()?;
    let store: Arc<dyn ObjectStore> = if settings.prefix.is_empty() {
        Arc::new(s3)
    } else {
        let prefix = ObjectPath::parse(&settings.prefix)?;
        Arc::new(PrefixStore::new(s3, prefix))
    };

    Ok(ImageStore {
        inner: Arc::new(StoreInner {
            label,
            kind: StoreKind::Object { store },
        }),
    })
}

fn ensure_relative(relative: &Path) -> std::io::Result<()> {
    if relative.as_os_str().is_empty() || relative.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "image key must be a relative path",
        ));
    }
    for component in relative.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "image key must not contain parent or current directory segments",
            ));
        }
    }
    Ok(())
}

fn local_path(root: &Path, relative: &Path) -> std::io::Result<PathBuf> {
    ensure_relative(relative)?;
    Ok(root.join(relative))
}

fn object_key(relative: &Path) -> std::io::Result<ObjectPath> {
    ensure_relative(relative)?;
    let mut key = String::new();
    let mut first = true;
    for component in relative.components() {
        let Component::Normal(segment) = component else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "image key must be a relative path",
            ));
        };
        let Some(segment) = segment.to_str() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "image key is not valid unicode",
            ));
        };
        if segment.is_empty()
            || segment == "."
            || segment == ".."
            || segment.contains('/')
            || segment.contains('\\')
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "image key contains an invalid segment",
            ));
        }
        if !first {
            key.push('/');
        }
        first = false;
        key.push_str(segment);
    }
    ObjectPath::parse(&key).map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid image key: {err}"),
        )
    })
}

fn map_store_err(err: object_store::Error) -> std::io::Error {
    match err {
        object_store::Error::NotFound { .. } => {
            std::io::Error::new(std::io::ErrorKind::NotFound, "object not found")
        }
        other => std::io::Error::other(other),
    }
}

fn too_large(max: usize) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::FileTooLarge,
        format!("source file exceeds the configured maximum of {max} bytes"),
    )
}

fn exceeds(size: u64, max: usize) -> bool {
    match u64::try_from(max) {
        Ok(max) => size > max,
        Err(_) => false,
    }
}

async fn open_local(root: &Path, relative: &Path) -> std::io::Result<Option<OpenedObject>> {
    let path = local_path(root, relative)?;
    let file = match tokio::fs::File::open(&path).await {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let size = file.metadata().await?.len();
    let stream = tokio_util::io::ReaderStream::new(file);
    Ok(Some(OpenedObject {
        size,
        stream: Box::pin(stream),
    }))
}

async fn open_object(
    store: &dyn ObjectStore,
    relative: &Path,
) -> std::io::Result<Option<OpenedObject>> {
    use futures_util::StreamExt;

    let key = object_key(relative)?;
    let result = match store.get(&key).await {
        Ok(result) => result,
        Err(err) => {
            let err = map_store_err(err);
            if err.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(err);
        }
    };
    let size = result.meta.size;
    let stream = result
        .into_stream()
        .map(|chunk| chunk.map_err(std::io::Error::other));
    Ok(Some(OpenedObject {
        size,
        stream: Box::pin(stream),
    }))
}

async fn load_object(
    store: &dyn ObjectStore,
    relative: &Path,
    max: Option<usize>,
) -> std::io::Result<Bytes> {
    let key = object_key(relative)?;
    let result = store.get(&key).await.map_err(map_store_err)?;
    if let Some(max) = max
        && exceeds(result.meta.size, max)
    {
        return Err(too_large(max));
    }
    let bytes = result.bytes().await.map_err(map_store_err)?;
    if let Some(max) = max
        && bytes.len() > max
    {
        return Err(too_large(max));
    }
    Ok(bytes)
}

async fn put_local(root: &Path, relative: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let path = local_path(root, relative)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, bytes).await
}

async fn put_object(store: &dyn ObjectStore, relative: &Path, bytes: Bytes) -> std::io::Result<()> {
    let key = object_key(relative)?;
    store
        .put(&key, PutPayload::from(bytes))
        .await
        .map_err(map_store_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup<'a>(vars: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            vars.iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).to_string())
        }
    }

    fn s3(config: &MountConfig) -> &S3Settings {
        match config {
            MountConfig::S3(settings) => Some(settings),
            MountConfig::Local => None,
        }
        .unwrap()
    }

    #[test]
    fn unset_bucket_stays_on_local_disk() {
        let config = parse_mount(&lookup(&[])).unwrap();
        assert!(matches!(config, MountConfig::Local));
    }

    #[test]
    fn security_token_and_session_token_alias_are_accepted() {
        let by_security = parse_mount(&lookup(&[
            ("IMAGE_PROXY_S3_BUCKET", "photos"),
            ("IMAGE_PROXY_S3_ACCESS_KEY_ID", "AKIA_EXAMPLE"),
            ("IMAGE_PROXY_S3_SECRET_ACCESS_KEY", "secret-value"),
            ("IMAGE_PROXY_S3_SECURITY_TOKEN", "security-token-value"),
            ("IMAGE_PROXY_S3_PREFIX", "/albums/2024/"),
        ]))
        .unwrap();
        let settings = s3(&by_security);
        assert_eq!(
            settings.security_token.as_deref(),
            Some("security-token-value")
        );
        assert_eq!(settings.prefix, "albums/2024");
        assert!(!settings.path_style);

        let by_session = parse_mount(&lookup(&[
            ("IMAGE_PROXY_S3_BUCKET", "photos"),
            ("IMAGE_PROXY_S3_ACCESS_KEY_ID", "AKIA_EXAMPLE"),
            ("IMAGE_PROXY_S3_SECRET_ACCESS_KEY", "secret-value"),
            ("IMAGE_PROXY_S3_SESSION_TOKEN", "session-token-value"),
        ]))
        .unwrap();
        assert_eq!(
            s3(&by_session).security_token.as_deref(),
            Some("session-token-value")
        );
    }

    #[test]
    fn security_token_name_wins_over_session_token_alias() {
        let config = parse_mount(&lookup(&[
            ("IMAGE_PROXY_S3_BUCKET", "photos"),
            ("IMAGE_PROXY_S3_ACCESS_KEY_ID", "AKIA_EXAMPLE"),
            ("IMAGE_PROXY_S3_SECRET_ACCESS_KEY", "secret-value"),
            ("IMAGE_PROXY_S3_SECURITY_TOKEN", "from-security"),
            ("IMAGE_PROXY_S3_SESSION_TOKEN", "from-session"),
        ]))
        .unwrap();
        assert_eq!(s3(&config).security_token.as_deref(), Some("from-security"));
    }

    #[test]
    fn token_without_key_pair_is_rejected() {
        let message = parse_mount(&lookup(&[
            ("IMAGE_PROXY_S3_BUCKET", "photos"),
            ("IMAGE_PROXY_S3_SECURITY_TOKEN", "only-a-token"),
        ]))
        .err()
        .unwrap()
        .to_string();
        assert!(message.contains("IMAGE_PROXY_S3_ACCESS_KEY_ID"));
        assert!(!message.contains("only-a-token"));
    }

    #[test]
    fn partial_key_pair_is_rejected() {
        let message = parse_mount(&lookup(&[
            ("IMAGE_PROXY_S3_BUCKET", "photos"),
            ("IMAGE_PROXY_S3_ACCESS_KEY_ID", "AKIA_EXAMPLE"),
        ]))
        .err()
        .unwrap()
        .to_string();
        assert!(message.contains("SECRET_ACCESS_KEY"));
    }

    #[test]
    fn custom_endpoint_defaults_to_path_style_and_can_be_overridden() {
        let implied = parse_mount(&lookup(&[
            ("IMAGE_PROXY_S3_BUCKET", "photos"),
            ("IMAGE_PROXY_S3_ENDPOINT", "http://minio:9000"),
            ("IMAGE_PROXY_S3_ACCESS_KEY_ID", "minio"),
            ("IMAGE_PROXY_S3_SECRET_ACCESS_KEY", "minio-secret"),
        ]))
        .unwrap();
        assert!(s3(&implied).path_style);

        let forced = parse_mount(&lookup(&[
            ("IMAGE_PROXY_S3_BUCKET", "photos"),
            (
                "IMAGE_PROXY_S3_ENDPOINT",
                "https://acct.r2.cloudflarestorage.com",
            ),
            ("IMAGE_PROXY_S3_PATH_STYLE", "false"),
            ("IMAGE_PROXY_S3_ACCESS_KEY_ID", "key"),
            ("IMAGE_PROXY_S3_SECRET_ACCESS_KEY", "secret"),
        ]))
        .unwrap();
        assert!(!s3(&forced).path_style);
    }

    #[test]
    fn prefix_rejects_parent_segments() {
        let message = parse_mount(&lookup(&[
            ("IMAGE_PROXY_S3_BUCKET", "photos"),
            ("IMAGE_PROXY_S3_PREFIX", "albums/../secret"),
        ]))
        .err()
        .unwrap()
        .to_string();
        assert!(message.contains("IMAGE_PROXY_S3_PREFIX"));
    }

    #[test]
    fn parent_segments_never_become_object_keys() {
        let err = object_key(Path::new("../secret.jpg")).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn s3_debug_output_omits_secrets() {
        let store = connect_s3(S3Settings {
            bucket: "photos".to_string(),
            region: "eu-west-1".to_string(),
            prefix: "albums".to_string(),
            endpoint: None,
            access_key_id: Some("AKIA_EXAMPLE".to_string()),
            secret_access_key: Some("wJalrXUtnFEMI/SECRETKEY".to_string()),
            security_token: Some("SUPERSECRETTOKEN".to_string()),
            path_style: false,
        })
        .unwrap();
        let rendered = format!("{store:?} {store}");
        assert!(rendered.contains("s3://photos/albums"));
        assert!(rendered.contains("security-token"));
        assert!(!rendered.contains("SUPERSECRETTOKEN"));
        assert!(!rendered.contains("SECRETKEY"));
        assert!(!rendered.contains("AKIA_EXAMPLE"));
    }

    fn mount_memory(shared: Arc<InMemory>, prefix: &str) -> ImageStore {
        let store: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(shared, prefix));
        ImageStore {
            inner: Arc::new(StoreInner {
                label: format!("memory://{prefix}"),
                kind: StoreKind::Object { store },
            }),
        }
    }

    #[tokio::test]
    async fn prefix_keeps_two_mounts_apart() {
        let shared = Arc::new(InMemory::new());
        let left = mount_memory(Arc::clone(&shared), "left");
        let right = mount_memory(shared, "right");
        left.put(Path::new("a.jpg"), Bytes::from_static(b"left"))
            .await
            .unwrap();
        let missing = right.load(Path::new("a.jpg"), None).await.unwrap_err();
        assert_eq!(missing.kind(), std::io::ErrorKind::NotFound);
        let found = left.load(Path::new("a.jpg"), None).await.unwrap();
        assert_eq!(found.as_ref(), b"left");
    }

    #[tokio::test]
    async fn load_rejects_objects_over_the_size_limit() {
        let store = ImageStore::in_memory("").unwrap();
        store
            .put(Path::new("a.jpg"), Bytes::from_static(b"0123456789"))
            .await
            .unwrap();
        let err = store.load(Path::new("a.jpg"), Some(4)).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::FileTooLarge);
    }

    #[test]
    fn label_for_static_credentials_does_not_require_a_live_bucket() {
        let config = parse_mount(&lookup(&[
            ("IMAGE_PROXY_S3_BUCKET", "photos"),
            ("IMAGE_PROXY_S3_REGION", "eu-west-1"),
            ("IMAGE_PROXY_S3_ACCESS_KEY_ID", "AKIA_EXAMPLE"),
            ("IMAGE_PROXY_S3_SECRET_ACCESS_KEY", "secret-value"),
            ("IMAGE_PROXY_S3_SECURITY_TOKEN", "security-token-value"),
        ]))
        .unwrap();
        let label = s3_label(s3(&config));
        assert_eq!(label, "s3://photos region=eu-west-1 security-token");
    }
}
