// Strict clippy lints enforced on production code only. Test crates (tests/*)
// and examples (examples/*) are separate compilation units without these
// attributes, so they are exempt — clippy.toml additionally relaxes
// unwrap/indexing lints inside #[test] functions here in src/.
#![deny(clippy::unwrap_used)]
#![deny(clippy::indexing_slicing)]
#![warn(clippy::arithmetic_side_effects)]
#![deny(clippy::unreachable)]
#![deny(clippy::unimplemented)]
#![deny(clippy::unchecked_time_subtraction)]
#![warn(clippy::todo)]
#![deny(clippy::string_slice)]
#![deny(clippy::panic_in_result_fn)]
#![deny(clippy::panic)]
#![deny(clippy::exit)]

pub mod api;
pub mod cache;
pub mod config;
pub mod logs;
pub mod metrics;
pub mod operations;
pub mod preferred_formats;
pub mod storage;
pub mod utils;
