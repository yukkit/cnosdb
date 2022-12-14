use std::sync::Arc;

use datafusion::{
    arrow::{
        array::StringBuilder,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    error::DataFusionError,
};

use lazy_static::lazy_static;

lazy_static! {
    static ref SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("user_name", DataType::Utf8, false),
        Field::new("role_name", DataType::Utf8, false),
    ]));
}

/// Builds the `information_schema.Members` table row by row
pub struct InformationSchemaMembersBuilder {
    user_names: StringBuilder,
    role_names: StringBuilder,
}

impl Default for InformationSchemaMembersBuilder {
    fn default() -> Self {
        Self {
            user_names: StringBuilder::new(),
            role_names: StringBuilder::new(),
        }
    }
}

impl InformationSchemaMembersBuilder {
    pub fn append_row(&mut self, user_name: impl AsRef<str>, role_name: impl AsRef<str>) {
        // Note: append_value is actually infallable.
        self.user_names.append_value(user_name.as_ref());
        self.role_names.append_value(role_name.as_ref());
    }
}

impl TryFrom<InformationSchemaMembersBuilder> for MemTable {
    type Error = DataFusionError;

    fn try_from(value: InformationSchemaMembersBuilder) -> Result<Self, Self::Error> {
        let InformationSchemaMembersBuilder {
            mut user_names,
            mut role_names,
        } = value;

        let batch = RecordBatch::try_new(
            SCHEMA.clone(),
            vec![Arc::new(user_names.finish()), Arc::new(role_names.finish())],
        )?;

        MemTable::try_new(SCHEMA.clone(), vec![vec![batch]])
    }
}
