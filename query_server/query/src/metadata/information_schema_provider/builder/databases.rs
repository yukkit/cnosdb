use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{StringBuilder, UInt64Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    error::DataFusionError,
};

use lazy_static::lazy_static;

lazy_static! {
    static ref SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("tenant_name", DataType::Utf8, false),
        Field::new("database_name", DataType::Utf8, false),
        Field::new("ttl", DataType::Utf8, false),
        Field::new("shard", DataType::UInt64, false),
        Field::new("vnode_duration", DataType::Utf8, false),
        Field::new("replica", DataType::UInt64, false),
        Field::new("percision", DataType::Utf8, false),
    ]));
}

/// Builds the `information_schema.DATABASES` table row by row
pub struct InformationSchemaDatabasesBuilder {
    tenant_names: StringBuilder,
    database_names: StringBuilder,
    option_ttls: StringBuilder,
    option_shards: UInt64Builder,
    option_vnode_durations: StringBuilder,
    option_replicas: UInt64Builder,
    option_percisions: StringBuilder,
}

impl Default for InformationSchemaDatabasesBuilder {
    fn default() -> Self {
        Self {
            tenant_names: StringBuilder::new(),
            database_names: StringBuilder::new(),
            option_ttls: StringBuilder::new(),
            option_shards: UInt64Builder::new(),
            option_vnode_durations: StringBuilder::new(),
            option_replicas: UInt64Builder::new(),
            option_percisions: StringBuilder::new(),
        }
    }
}

impl InformationSchemaDatabasesBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn append_row(
        &mut self,
        tenant_name: impl AsRef<str>,
        database_name: impl AsRef<str>,
        option_ttl: impl AsRef<str>,
        option_shard: u64,
        option_vnode_duration: impl AsRef<str>,
        option_replica: u64,
        option_percision: impl AsRef<str>,
    ) {
        // Note: append_value is actually infallable.
        self.tenant_names.append_value(tenant_name.as_ref());
        self.database_names.append_value(database_name.as_ref());
        self.option_ttls.append_value(option_ttl.as_ref());
        self.option_shards.append_value(option_shard);
        self.option_vnode_durations
            .append_value(option_vnode_duration.as_ref());
        self.option_replicas.append_value(option_replica);
        self.option_percisions.append_value(option_percision);
    }
}

impl TryFrom<InformationSchemaDatabasesBuilder> for MemTable {
    type Error = DataFusionError;

    fn try_from(value: InformationSchemaDatabasesBuilder) -> Result<Self, Self::Error> {
        let InformationSchemaDatabasesBuilder {
            mut tenant_names,
            mut database_names,
            mut option_ttls,
            mut option_shards,
            mut option_vnode_durations,
            mut option_replicas,
            mut option_percisions,
        } = value;

        let batch = RecordBatch::try_new(
            SCHEMA.clone(),
            vec![
                Arc::new(tenant_names.finish()),
                Arc::new(database_names.finish()),
                Arc::new(option_ttls.finish()),
                Arc::new(option_shards.finish()),
                Arc::new(option_vnode_durations.finish()),
                Arc::new(option_replicas.finish()),
                Arc::new(option_percisions.finish()),
            ],
        )?;

        MemTable::try_new(SCHEMA.clone(), vec![vec![batch]])
    }
}
