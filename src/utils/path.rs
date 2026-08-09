use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum PathValidationError {
    MissingExtension,
    UnsupportedFormat(String),
}

pub fn sanitize_and_validate_path(
    filename: &str,
    strip_path: Option<&str>,
    root_path: &str,
    supported_formats: &[&str],
) -> Result<(PathBuf, PathBuf, String), PathValidationError> {
    // Strip the specified path from the filename if configured
    let filename = if let Some(strip_path) = strip_path {
        Path::new(&filename)
            .strip_prefix(strip_path)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| filename.to_string())
    } else {
        filename.to_string()
    };

    // Sanitize the path to prevent directory traversal
    let sanitized_path = Path::new(&filename)
        .components()
        .filter(|comp| matches!(comp, std::path::Component::Normal(_)))
        .fold(Path::new("").to_path_buf(), |acc, comp| {
            acc.join(comp.as_os_str())
        });

    // Join the sanitized path with the root path to get the final file path
    let sanitized_disk_path = Path::new(root_path).join(&sanitized_path);

    // Check if the extension is valid and supported
    let file_ext = sanitized_disk_path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_lowercase());
    let Some(ext) = file_ext else {
        return Err(PathValidationError::MissingExtension);
    };
    if !supported_formats.contains(&ext.as_str()) {
        return Err(PathValidationError::UnsupportedFormat(ext));
    }

    Ok((sanitized_path, sanitized_disk_path, ext))
}

/// Loads file bytes from disk, optionally enforcing a maximum file size.
#[tracing::instrument(level = "debug", skip(max_source_size))]
pub async fn load_bytes_from_disk(
    path: &Path,
    max_source_size: Option<usize>,
) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;

    let mut file = tokio::fs::File::open(path).await?;

    let bytes = match max_source_size {
        // Read at most `max + 1` bytes; reject if we read more than `max`.
        // `take` + `read_to_end` grows the buffer incrementally and loops
        // internally, so memory is bounded by the real file size and short
        // reads can't let an oversize file slip through.
        Some(max) => {
            let cap = (max as u64)
                .checked_add(1)
                .expect("max_source_size + 1 overflow");
            let mut buf = Vec::new();
            file.take(cap).read_to_end(&mut buf).await?;
            if buf.len() > max {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::FileTooLarge,
                    format!(
                        "source file exceeds the configured maximum of {} bytes",
                        max
                    ),
                ));
            }
            buf
        }
        None => {
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await?;
            buf
        }
    };

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FORMATS: &[&str] = &["avif", "jpeg", "jpg", "png", "webp"];

    #[test]
    fn sanitize_valid_jpeg_path() {
        let (sanitized, disk, ext) =
            sanitize_and_validate_path("photo.jpeg", None, "/data", FORMATS).unwrap();
        assert_eq!(sanitized, PathBuf::from("photo.jpeg"));
        assert_eq!(disk, PathBuf::from("/data/photo.jpeg"));
        assert_eq!(ext, "jpeg");
    }

    #[test]
    fn sanitize_strips_prefix() {
        let (sanitized, disk, ext) =
            sanitize_and_validate_path("prefix/photo.png", Some("prefix/"), "/data", FORMATS)
                .unwrap();
        assert_eq!(sanitized, PathBuf::from("photo.png"));
        assert_eq!(disk, PathBuf::from("/data/photo.png"));
        assert_eq!(ext, "png");
    }

    #[test]
    fn sanitize_strips_prefix_without_trailing_slash() {
        // `strip_path` is path-component-aware, so "prefix" (no trailing slash)
        // should strip the leading `prefix` component, not the literal bytes.
        let (sanitized, disk, ext) =
            sanitize_and_validate_path("prefix/photo.png", Some("prefix"), "/data", FORMATS)
                .unwrap();
        assert_eq!(sanitized, PathBuf::from("photo.png"));
        assert_eq!(disk, PathBuf::from("/data/photo.png"));
        assert_eq!(ext, "png");
    }

    #[test]
    fn sanitize_strip_prefix_does_not_match_partial_component() {
        // A strip_path of "prefix" must not match the leading bytes of
        // "prefixfoo/photo.png" — only whole path components match.
        let (sanitized, _, _) =
            sanitize_and_validate_path("prefixfoo/photo.png", Some("prefix"), "/data", FORMATS)
                .unwrap();
        assert_eq!(sanitized, PathBuf::from("prefixfoo/photo.png"));
    }

    #[test]
    fn sanitize_strip_prefix_nested() {
        // Multi-component strip_path should also be path-aware.
        let (sanitized, disk, ext) = sanitize_and_validate_path(
            "media/photos/img.png",
            Some("media/photos"),
            "/data",
            FORMATS,
        )
        .unwrap();
        assert_eq!(sanitized, PathBuf::from("img.png"));
        assert_eq!(disk, PathBuf::from("/data/img.png"));
        assert_eq!(ext, "png");
    }

    #[test]
    fn sanitize_prevents_directory_traversal() {
        let (sanitized, disk, _) =
            sanitize_and_validate_path("../secret.jpeg", None, "/data", FORMATS).unwrap();
        // Traversal component stripped
        assert_eq!(sanitized, PathBuf::from("secret.jpeg"));
        assert_eq!(disk, PathBuf::from("/data/secret.jpeg"));
    }

    #[test]
    fn sanitize_missing_extension_error() {
        let result = sanitize_and_validate_path("noext", None, "/data", FORMATS);
        assert!(matches!(result, Err(PathValidationError::MissingExtension)));
    }

    #[test]
    fn sanitize_unsupported_format_error() {
        let result = sanitize_and_validate_path("file.bmp", None, "/data", FORMATS);
        assert!(matches!(
            result,
            Err(PathValidationError::UnsupportedFormat(ref e)) if e == "bmp"
        ));
    }

    #[test]
    fn sanitize_nested_path() {
        let (sanitized, disk, ext) =
            sanitize_and_validate_path("a/b/c/photo.webp", None, "/root", FORMATS).unwrap();
        assert_eq!(sanitized, PathBuf::from("a/b/c/photo.webp"));
        assert_eq!(disk, PathBuf::from("/root/a/b/c/photo.webp"));
        assert_eq!(ext, "webp");
    }

    #[test]
    fn sanitize_case_insensitive_extension() {
        // Extension is lowercased, so "JPG" becomes "jpg" which is in FORMATS
        let (_, _, ext) = sanitize_and_validate_path("photo.JPG", None, "/data", FORMATS).unwrap();
        assert_eq!(ext, "jpg");
    }

    #[tokio::test]
    async fn load_bytes_from_disk_existing_file() {
        let dir = std::env::temp_dir().join("image_proxy_test_load");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.bin");
        std::fs::write(&path, b"hello").unwrap();

        let bytes = load_bytes_from_disk(&path, None).await.unwrap();
        assert_eq!(bytes, b"hello");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn load_bytes_from_disk_missing_file() {
        let result =
            load_bytes_from_disk(Path::new("/tmp/nonexistent_image_proxy_test"), None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn load_bytes_from_disk_rejects_oversized_file() {
        let dir = std::env::temp_dir().join("image_proxy_test_oversize");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("big.bin");
        // 10 bytes on disk, but cap at 5.
        std::fs::write(&path, b"0123456789").unwrap();

        let result = load_bytes_from_disk(&path, Some(5)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::FileTooLarge);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn load_bytes_from_disk_allows_file_at_limit() {
        let dir = std::env::temp_dir().join("image_proxy_test_at_limit");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("exact.bin");
        std::fs::write(&path, b"hello").unwrap();

        // File is exactly 5 bytes; cap of 5 should be allowed (not strictly greater).
        let bytes = load_bytes_from_disk(&path, Some(5)).await.unwrap();
        assert_eq!(bytes, b"hello");

        std::fs::remove_dir_all(&dir).ok();
    }
}
