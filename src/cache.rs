use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
    SieveConfig,
};
use mixtrics::registry::prometheus::PrometheusMetricsRegistry;

pub async fn setup_cache(
    config: &crate::config::EncodingConfig,
    prometheus_registry: &prometheus::Registry,
) -> anyhow::Result<Option<foyer::HybridCache<String, Bytes>>> {
    if !config.enable_cache {
        tracing::debug!("caching is disabled, skipping cache setup");
        return Ok(None);
    }

    build_hybrid_cache(
        config,
        prometheus_registry,
        Admission::MaxItem(config.cache_memory_max_item_size),
    )
    .await
    .map(Some)
}

/// Foyer cache used as the image store when `IMAGE_PROXY_STORAGE=foyer`.
///
/// The disk tier is included when `enable_disk_cache` is set. When the response
/// cache is also on, this same instance stores transformed responses and still
/// drops those larger than `cache_memory_max_item_size`. Origin keys (`origin/`)
/// are kept either way.
pub async fn setup_origin_cache(
    config: &crate::config::EncodingConfig,
    prometheus_registry: &prometheus::Registry,
) -> anyhow::Result<foyer::HybridCache<String, Bytes>> {
    let admission = if config.enable_cache {
        Admission::OriginAndMaxItem(config.cache_memory_max_item_size)
    } else {
        Admission::Everything
    };
    build_hybrid_cache(config, prometheus_registry, admission).await
}

/// Largest object the foyer image store accepts. Each object has to fit in the
/// memory tier; the disk tier holds more of those objects.
pub fn origin_capacity(config: &crate::config::EncodingConfig) -> usize {
    config.cache_memory_size
}

enum Admission {
    MaxItem(usize),
    OriginAndMaxItem(usize),
    Everything,
}

async fn build_hybrid_cache(
    config: &crate::config::EncodingConfig,
    prometheus_registry: &prometheus::Registry,
    admission: Admission,
) -> anyhow::Result<foyer::HybridCache<String, Bytes>> {
    let memory = HybridCacheBuilder::new()
        .with_metrics_registry(Box::new(PrometheusMetricsRegistry::new(
            prometheus_registry.clone(),
        )))
        .memory(config.cache_memory_size)
        .with_eviction_config(SieveConfig {})
        .with_weighter(|key: &String, value: &Bytes| key.len().saturating_add(value.len()));

    let memory = match admission {
        Admission::MaxItem(max_item) => {
            memory.with_filter(move |_: &String, value: &Bytes| value.len() <= max_item)
        }
        Admission::OriginAndMaxItem(max_item) => {
            memory.with_filter(move |key: &String, value: &Bytes| {
                key.starts_with("origin/") || value.len() <= max_item
            })
        }
        Admission::Everything => memory,
    };

    if config.enable_disk_cache {
        tracing::debug!(
            "setting up hybrid cache with memory size {} bytes and disk size {} bytes at path {}",
            config.cache_memory_size,
            config.cache_disk_size,
            config.cache_disk_path
        );
        let dev = FsDeviceBuilder::new(&config.cache_disk_path)
            .with_capacity(config.cache_disk_size)
            .build()?;
        memory
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::default())
            .with_engine_config(BlockEngineConfig::new(dev))
            .build()
            .await
            .map_err(Into::into)
    } else {
        tracing::debug!(
            "setting up memory-only cache with size {} bytes",
            config.cache_memory_size
        );
        memory.storage().build().await.map_err(Into::into)
    }
}
