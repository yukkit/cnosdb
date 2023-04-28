pub mod func_manager;

mod aggregate_function;
pub mod expr_fn;
pub mod expr_rewriter;
pub mod expr_utils;
mod function_utils;
mod scalar_function;
mod selector_function;
mod window;

use datafusion::arrow::datatypes::{DataType, IntervalUnit};
pub use scalar_function::{INTERPOLATE, LOCF, TIME_WINDOW_GAPFILL};
pub use selector_function::{BOTTOM, TOPK};
use spi::query::function::FunctionMetadataManager;
use spi::Result;
pub use window::{TIME_WINDOW, WINDOW_COL_NAME, WINDOW_END, WINDOW_START};

pub static INTERVALS: &[DataType] = &[
    DataType::Interval(IntervalUnit::YearMonth),
    DataType::Interval(IntervalUnit::MonthDayNano),
    DataType::Interval(IntervalUnit::DayTime),
];

/// load all cnosdb's built-in function
pub fn load_all_functions(func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    scalar_function::register_udfs(func_manager)?;
    aggregate_function::register_udafs(func_manager)?;
    selector_function::register_selector_udfs(func_manager)?;
    window::register_window_udfs(func_manager)?;
    Ok(())
}
