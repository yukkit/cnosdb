use std::sync::Arc;

use datafusion::{
    arrow::{
        array::StringBuilder,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    error::DataFusionError,
    logical_expr::TableType,
};

use lazy_static::lazy_static;

lazy_static! {
    static ref SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("table_tenant", DataType::Utf8, false),
        Field::new("table_database", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
        Field::new("table_engine", DataType::Utf8, false),
        Field::new("table_options", DataType::Utf8, false),
    ]));
}

/// Builds the `information_schema.TABLES` table row by row
pub struct InformationSchemaTablesBuilder {
    tenant_names: StringBuilder,
    database_names: StringBuilder,
    table_names: StringBuilder,
    table_types: StringBuilder,
    table_engines: StringBuilder,
    table_options: StringBuilder,
}

impl Default for InformationSchemaTablesBuilder {
    fn default() -> Self {
        Self {
            tenant_names: StringBuilder::new(),
            database_names: StringBuilder::new(),
            table_names: StringBuilder::new(),
            table_types: StringBuilder::new(),
            table_engines: StringBuilder::new(),
            table_options: StringBuilder::new(),
        }
    }
}

impl InformationSchemaTablesBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn append_row(
        &mut self,
        tenant_name: impl AsRef<str>,
        database_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        table_type: TableType,
        table_engine: impl AsRef<str>,
        table_option: impl AsRef<str>,
    ) {
        // Note: append_value is actually infallable.
        self.tenant_names.append_value(tenant_name.as_ref());
        self.database_names.append_value(database_name.as_ref());
        self.table_names.append_value(table_name.as_ref());
        self.table_types.append_value(match table_type {
            TableType::Base => "BASE TABLE",
            TableType::View => "VIEW",
            TableType::Temporary => "LOCAL TEMPORARY",
        });
        self.table_engines.append_value(table_engine.as_ref());
        self.table_options.append_value(table_option.as_ref());
    }
}

impl TryFrom<InformationSchemaTablesBuilder> for MemTable {
    type Error = DataFusionError;

    fn try_from(value: InformationSchemaTablesBuilder) -> Result<Self, Self::Error> {
        let InformationSchemaTablesBuilder {
            mut tenant_names,
            mut database_names,
            mut table_names,
            mut table_types,
            mut table_engines,
            mut table_options,
        } = value;

        let batch = RecordBatch::try_new(
            SCHEMA.clone(),
            vec![
                Arc::new(tenant_names.finish()),
                Arc::new(database_names.finish()),
                Arc::new(table_names.finish()),
                Arc::new(table_types.finish()),
                Arc::new(table_engines.finish()),
                Arc::new(table_options.finish()),
            ],
        )?;

        MemTable::try_new(SCHEMA.clone(), vec![vec![batch]])
    }
}
