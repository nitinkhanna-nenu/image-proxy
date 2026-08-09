use std::io::Cursor;

use image::{DynamicImage, ImageReaderOptions, Limits};

use crate::config::EncodingConfig;

pub fn limits_from_config(config: &EncodingConfig) -> Limits {
    let mut limits = Limits::default();

    if let Some(max_dim) = config.max_dimension {
        limits.max_image_width = Some(max_dim);
        limits.max_image_height = Some(max_dim);
    }

    limits
}

#[tracing::instrument(level = "debug", skip_all, fields(len = bytes.len()))]
#[hotpath::measure]
pub fn decode_image(bytes: &[u8], config: &EncodingConfig) -> anyhow::Result<DynamicImage> {
    let limits = limits_from_config(config);
    let mut reader = ImageReaderOptions::new(Cursor::new(bytes));
    reader.limits(limits);
    Ok(reader.with_guessed_format()?.decode()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_limits(max_dimension: Option<u32>) -> EncodingConfig {
        EncodingConfig {
            max_dimension,
            ..EncodingConfig::default()
        }
    }

    #[test]
    fn decode_valid_jpeg() {
        // Synthesize a small valid JPEG using the image crate
        let img = DynamicImage::new_rgb8(4, 4);
        let mut buf = Vec::new();
        let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut buf, 80);
        image::ImageEncoder::write_image(
            encoder,
            img.as_bytes(),
            img.width(),
            img.height(),
            image::ExtendedColorType::Rgb8,
        )
        .expect("Failed to encode image");

        let config = config_with_limits(Some(8192));
        let decoded = decode_image(&buf, &config).expect("Failed to decode image");
        assert_eq!(decoded.width(), 4);
        assert_eq!(decoded.height(), 4);
    }

    #[test]
    fn decode_invalid_data_errors() {
        let config = config_with_limits(Some(8192));
        let result = decode_image(b"not an image", &config);
        assert!(result.is_err());
    }

    #[test]
    fn decode_rejects_oversized_dimensions() {
        // A valid 4×4 JPEG, but the configured dimension limit is 2px.
        let img = DynamicImage::new_rgb8(4, 4);
        let mut buf = Vec::new();
        let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut buf, 80);
        image::ImageEncoder::write_image(
            encoder,
            img.as_bytes(),
            img.width(),
            img.height(),
            image::ExtendedColorType::Rgb8,
        )
        .expect("Failed to encode image");

        let config = config_with_limits(Some(2));
        let result = decode_image(&buf, &config);
        assert!(
            result.is_err(),
            "expected decode to fail due to dimension limit"
        );
    }

    #[test]
    fn decode_succeeds_with_no_limits() {
        let img = DynamicImage::new_rgb8(4, 4);
        let mut buf = Vec::new();
        let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut buf, 80);
        image::ImageEncoder::write_image(
            encoder,
            img.as_bytes(),
            img.width(),
            img.height(),
            image::ExtendedColorType::Rgb8,
        )
        .expect("Failed to encode image");

        // No limits configured — should decode fine.
        let config = config_with_limits(None);
        let decoded = decode_image(&buf, &config).expect("Failed to decode image");
        assert_eq!(decoded.width(), 4);
        assert_eq!(decoded.height(), 4);
    }
}
