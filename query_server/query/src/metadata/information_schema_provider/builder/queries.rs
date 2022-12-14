use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Float64Builder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    error::DataFusionError,
};

use lazy_static::lazy_static;

lazy_static! {
    static ref SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("query_text", DataType::Utf8, false),
        Field::new("user_id", DataType::Utf8, false),
        Field::new("user_name", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("tenant_name", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("duration", DataType::Float64, false),
    ]));
}

/// Builds the `information_schema.Queries` table row by row
pub struct InformationSchemaQueriesBuilder {
    query_ids: StringBuilder,
    query_texts: StringBuilder,
    user_ids: StringBuilder,
    user_names: StringBuilder,
    tenant_ids: StringBuilder,
    tenant_names: StringBuilder,
    states: StringBuilder,
    durations: Float64Builder,
}

impl Default for InformationSchemaQueriesBuilder {
    fn default() -> Self {
        Self {
            query_ids: StringBuilder::new(),
            query_texts: StringBuilder::new(),
            user_ids: StringBuilder::new(),
            user_names: StringBuilder::new(),
            tenant_ids: StringBuilder::new(),
            tenant_names: StringBuilder::new(),
            states: StringBuilder::new(),
            durations: Float64Builder::new(),
        }
    }
}

impl InformationSchemaQueriesBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn append_row(
        &mut self,
        query_id: impl AsRef<str>,
        query_text: impl AsRef<str>,
        user_id: impl AsRef<str>,
        user_name: impl AsRef<str>,
        tenant_id: impl AsRef<str>,
        tenant_name: impl AsRef<str>,
        state: impl AsRef<str>,
        duration: f64,
    ) {
        // Note: append_value is actually infallable.
        self.query_ids.append_value(query_id.as_ref());
        self.query_texts.append_value(query_text.as_ref());
        self.user_ids.append_value(user_id.as_ref());
        self.user_names.append_value(user_name.as_ref());
        self.tenant_ids.append_value(tenant_id.as_ref());
        self.tenant_names.append_value(tenant_name.as_ref());
        self.states.append_value(state.as_ref());
        self.durations.append_value(duration);
    }
}

impl TryFrom<InformationSchemaQueriesBuilder> for MemTable {
    type Error = DataFusionError;

    fn try_from(value: InformationSchemaQueriesBuilder) -> Result<Self, Self::Error> {
        let InformationSchemaQueriesBuilder {
            mut query_ids,
            mut query_texts,
            mut user_ids,
            mut user_names,
            mut tenant_ids,
            mut tenant_names,
            mut states,
            mut durations,
        } = value;

        let batch = RecordBatch::try_new(
            SCHEMA.clone(),
            vec![
                Arc::new(query_ids.finish()),
                Arc::new(query_texts.finish()),
                Arc::new(user_ids.finish()),
                Arc::new(user_names.finish()),
                Arc::new(tenant_ids.finish()),
                Arc::new(tenant_names.finish()),
                Arc::new(states.finish()),
                Arc::new(durations.finish()),
            ],
        )?;

        MemTable::try_new(SCHEMA.clone(), vec![vec![batch]])
    }
}
