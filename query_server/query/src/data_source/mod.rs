use datafusion::parquet::errors::ParquetError;
use models::define_result;
use snafu::Snafu;

use crate::utils::point_util::PointUtilError;

pub mod parquet_sink;
pub mod sink;
pub mod tskv_sink;

define_result!(DataSourceError);

#[derive(Debug, Snafu)]
pub enum DataSourceError {
    #[snafu(display("Point util error, err: {}", source))]
    PointUtil { source: PointUtilError },

    #[snafu(display("Tskv operator, err: {}", source))]
    Tskv { source: tskv::Error },

    #[snafu(display("Parquet operator, err: {}", source))]
    ParquetError { source: ParquetError },
}
