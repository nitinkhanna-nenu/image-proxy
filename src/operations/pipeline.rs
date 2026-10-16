use image::DynamicImage;
use prometheus::HistogramVec;

use crate::{
    config::EncodingConfig,
    metrics::{PipelineStep, start_step},
    operations::{
        format::convert_image_format,
        grayscale::apply_bw,
        resize::{ResizeAlgorithm, resize_image},
    },
};

#[tracing::instrument(level = "debug", skip_all)]
#[hotpath::measure]
pub fn image_pipeline(
    image: DynamicImage,
    size: Option<u32>,
    format: &str,
    config: &EncodingConfig,
    resize_algorithm: Option<ResizeAlgorithm>,
    black_and_white: bool,
    pipeline_duration: Option<&HistogramVec>,
) -> anyhow::Result<Vec<u8>> {
    // Labels: step, format. Format is meaningful for encode (and kept consistent for all steps).
    let image = {
        let _timer = pipeline_duration.map(|h| start_step(h, PipelineStep::Resize, format));
        resize_image(image, size, resize_algorithm, config)
    };

    let image = if black_and_white {
        let _timer = pipeline_duration.map(|h| start_step(h, PipelineStep::Bw, format));
        apply_bw(image, true)
    } else {
        image
    };

    let _timer = pipeline_duration.map(|h| start_step(h, PipelineStep::Encode, format));
    convert_image_format(image, Some(format), config)
}
