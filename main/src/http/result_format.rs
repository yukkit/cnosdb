use super::Error as HttpError;
use datafusion::arrow::csv::writer::WriterBuilder;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::json::{ArrayWriter, LineDelimitedWriter};
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use spi::query::execution::Output;
use spi::service::protocol::QueryHandle;
use std::str::FromStr;
use warp::reply::Response;

use crate::http::response::ResponseBuilder;

use super::header::{
    APPLICATION_CSV, APPLICATION_JSON, APPLICATION_NDJSON, APPLICATION_PREFIX, APPLICATION_STAR,
    APPLICATION_TSV, CONTENT_TYPE, STAR_STAR,
};
use super::status_code::OK;

macro_rules! batches_to_json {
    ($WRITER: ident, $batches: expr) => {{
        let mut bytes = vec![];
        {
            let mut writer = $WRITER::new(&mut bytes);
            writer.write_batches($batches)?;
            writer.finish()?;
        }
        Ok(bytes)
    }};
}

fn batches_with_sep(batches: &[RecordBatch], delimiter: u8) -> ArrowResult<Vec<u8>> {
    let mut bytes = vec![];
    {
        let builder = WriterBuilder::new()
            .has_headers(true)
            .with_delimiter(delimiter);
        let mut writer = builder.build(&mut bytes);
        for batch in batches {
            writer.write(batch)?;
        }
    }
    Ok(bytes)
}

/// Allow records to be printed in different formats
#[derive(Debug, PartialEq, Eq, clap::ArgEnum, Clone)]
pub enum ResultFormat {
    Csv,
    Tsv,
    Json,
    NdJson,
}

impl ResultFormat {
    fn get_http_content_type(&self) -> &'static str {
        match self {
            Self::Csv => APPLICATION_CSV,
            Self::Tsv => APPLICATION_TSV,
            Self::Json => APPLICATION_JSON,
            Self::NdJson => APPLICATION_NDJSON,
        }
    }

    pub fn format_batches(&self, batches: &[RecordBatch]) -> ArrowResult<Vec<u8>> {
        match self {
            Self::Csv => batches_with_sep(batches, b','),
            Self::Tsv => batches_with_sep(batches, b'\t'),
            Self::Json => batches_to_json!(ArrayWriter, batches),
            Self::NdJson => {
                batches_to_json!(LineDelimitedWriter, batches)
            }
        }
    }

    pub fn wrap_batches_to_response(&self, batches: &[RecordBatch]) -> Result<Response, HttpError> {
        let result = self
            .format_batches(batches)
            .map_err(|e| HttpError::FetchResult {
                reason: format!("{}", e),
            })?;

        let resp = ResponseBuilder::new(OK)
            .insert_header((CONTENT_TYPE, self.get_http_content_type()))
            .build(result);

        Ok(resp)
    }
}

impl TryFrom<&str> for ResultFormat {
    type Error = HttpError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.is_empty() || s == APPLICATION_STAR || s == STAR_STAR {
            return Ok(ResultFormat::Csv);
        }

        if let Some(fmt) = s.strip_prefix(APPLICATION_PREFIX) {
            return ResultFormat::from_str(fmt)
                .map_err(|reason| HttpError::InvalidHeader { reason });
        }

        Err(HttpError::InvalidHeader {
            reason: format!("accept type not support: {}", s),
        })
    }
}

impl FromStr for ResultFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        clap::ArgEnum::from_str(s, true)
    }
}

pub async fn fetch_record_batches(res: &mut QueryHandle) -> ArrowResult<Vec<RecordBatch>> {
    let mut actual = vec![];

    for ele in res.result().iter_mut() {
        match ele {
            Output::StreamData(stream) => {
                let mut result = stream.try_collect::<Vec<RecordBatch>>().await?;
                actual.append(&mut result);
            }
            Output::Nil(_) => {}
        }
    }

    Ok(actual)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::from_slice::FromSlice;
    use std::sync::Arc;

    #[test]
    fn test_format_batches_with_sep() {
        let batches = vec![];
        assert_eq!("".as_bytes(), batches_with_sep(&batches, b',').unwrap());

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_slice(&[1, 2, 3])),
                Arc::new(Int32Array::from_slice(&[4, 5, 6])),
                Arc::new(Int32Array::from_slice(&[7, 8, 9])),
            ],
        )
        .unwrap();

        let batches = vec![batch];
        let r = batches_with_sep(&batches, b',').unwrap();
        assert_eq!("a,b,c\n1,4,7\n2,5,8\n3,6,9\n".as_bytes(), r);
    }

    #[test]
    fn test_format_batches_to_json_empty() -> ArrowResult<()> {
        let batches = vec![];
        let r: ArrowResult<Vec<u8>> = batches_to_json!(ArrayWriter, &batches);
        assert_eq!("".as_bytes(), r?);

        let r: ArrowResult<Vec<u8>> = batches_to_json!(LineDelimitedWriter, &batches);
        assert_eq!("".as_bytes(), r?);

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_slice(&[1, 2, 3])),
                Arc::new(Int32Array::from_slice(&[4, 5, 6])),
                Arc::new(Int32Array::from_slice(&[7, 8, 9])),
            ],
        )
        .unwrap();

        let batches = vec![batch];
        let r: ArrowResult<Vec<u8>> = batches_to_json!(ArrayWriter, &batches);
        assert_eq!(
            "[{\"a\":1,\"b\":4,\"c\":7},{\"a\":2,\"b\":5,\"c\":8},{\"a\":3,\"b\":6,\"c\":9}]"
                .as_bytes(),
            r?
        );

        let r: ArrowResult<Vec<u8>> = batches_to_json!(LineDelimitedWriter, &batches);
        assert_eq!(
            "{\"a\":1,\"b\":4,\"c\":7}\n{\"a\":2,\"b\":5,\"c\":8}\n{\"a\":3,\"b\":6,\"c\":9}\n"
                .as_bytes(),
            r?
        );
        Ok(())
    }
}
