//! CatalogProvider:            ---> namespace
//! - SchemeProvider #1         ---> db
//!     - dyn tableProvider #1  ---> table
//!         - field #1
//!         - Column #2
//!     - dyn TableProvider #2
//!         - Column #3
//!         - Column #4

use std::collections::HashMap;
use std::fmt::{self, Display};
use std::{collections::BTreeMap, sync::Arc};

use std::mem::size_of_val;
use std::str::FromStr;

use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use arrow_schema::Schema;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, SchemaRef, TimeUnit,
};
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::{DataFusionError, Result as DataFusionResult};

use crate::codec::Encoding;
use crate::oid::{Identifier, Oid};
use crate::{ColumnId, SchemaId, ValueType};

pub type TableSchemaRef = Arc<TskvTableSchema>;

pub const TIME_FIELD_NAME: &str = "time";

pub const FIELD_ID: &str = "_field_id";
pub const TAG: &str = "_tag";
pub const TIME_FIELD: &str = "time";

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TableSchema {
    TsKvTableSchema(TskvTableSchema),
    ExternalTableSchema(ExternalTableSchema),
}

impl TableSchema {
    pub fn name(&self) -> String {
        match self {
            TableSchema::TsKvTableSchema(schema) => schema.name.clone(),
            TableSchema::ExternalTableSchema(schema) => schema.name.clone(),
        }
    }

    pub fn db(&self) -> String {
        match self {
            TableSchema::TsKvTableSchema(schema) => schema.db.clone(),
            TableSchema::ExternalTableSchema(schema) => schema.db.clone(),
        }
    }

    pub fn engine_name(&self) -> &str {
        match self {
            TableSchema::TsKvTableSchema(_) => "TSKV",
            TableSchema::ExternalTableSchema(_) => "EXTERNAL",
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalTableSchema {
    pub tenant: String,
    pub db: String,
    pub name: String,
    pub file_compression_type: String,
    pub file_type: String,
    pub location: String,
    pub target_partitions: usize,
    pub table_partition_cols: Vec<String>,
    pub has_header: bool,
    pub delimiter: u8,
    pub schema: Schema,
}

impl ExternalTableSchema {
    pub fn table_options(&self) -> DataFusionResult<ListingOptions> {
        let file_format: Arc<dyn FileFormat> = match FileType::from_str(&self.file_type)? {
            FileType::CSV => Arc::new(
                CsvFormat::default()
                    .with_has_header(self.has_header)
                    .with_delimiter(self.delimiter)
                    .with_file_compression_type(
                        FileCompressionType::from_str(&self.file_compression_type).map_err(
                            |_| {
                                DataFusionError::Execution(
                                    "Only known FileCompressionTypes can be ListingTables!"
                                        .to_string(),
                                )
                            },
                        )?,
                    ),
            ),
            FileType::PARQUET => Arc::new(ParquetFormat::default()),
            FileType::AVRO => Arc::new(AvroFormat::default()),
            FileType::JSON => Arc::new(JsonFormat::default().with_file_compression_type(
                FileCompressionType::from_str(&self.file_compression_type)?,
            )),
        };

        Ok(ListingOptions {
            format: file_format,
            collect_stat: false,
            file_extension: FileType::from_str(&self.file_type)?
                .get_ext_with_compression(self.file_compression_type.to_owned().parse()?)?,
            target_partitions: self.target_partitions,
            table_partition_cols: self.table_partition_cols.clone(),
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TskvTableSchema {
    pub tenant: String,
    pub db: String,
    pub name: String,
    pub schema_id: SchemaId,
    next_column_id: ColumnId,

    columns: Vec<TableColumn>,
    //ColumnName -> ColumnsIndex
    columns_index: HashMap<String, usize>,
}

impl Default for TskvTableSchema {
    fn default() -> Self {
        Self {
            tenant: "cnosdb".to_string(),
            db: "public".to_string(),
            name: "".to_string(),
            schema_id: 0,
            next_column_id: 0,
            columns: Default::default(),
            columns_index: Default::default(),
        }
    }
}

impl TskvTableSchema {
    pub fn to_arrow_schema(&self) -> SchemaRef {
        let fields: Vec<ArrowField> = self.columns.iter().map(|field| field.into()).collect();

        Arc::new(Schema::new(fields))
    }

    pub fn new(tenant: String, db: String, name: String, columns: Vec<TableColumn>) -> Self {
        let columns_index = columns
            .iter()
            .enumerate()
            .map(|(idx, e)| (e.name.clone(), idx))
            .collect();

        Self {
            tenant,
            db,
            name,
            schema_id: 0,
            next_column_id: columns.len() as ColumnId,
            columns,
            columns_index,
        }
    }

    /// add column
    /// not add if exists
    pub fn add_column(&mut self, col: TableColumn) {
        self.columns_index
            .entry(col.name.clone())
            .or_insert_with(|| {
                self.columns.push(col);
                self.columns.len() - 1
            });
        self.next_column_id += 1;
    }

    /// drop column if exists
    pub fn drop_column(&mut self, col_name: &str) {
        if let Some(id) = self.columns_index.get(col_name) {
            self.columns.remove(*id);
        }
        let columns_index = self
            .columns
            .iter()
            .enumerate()
            .map(|(idx, e)| (e.name.clone(), idx))
            .collect();
        self.columns_index = columns_index;
    }

    pub fn change_column(&mut self, col_name: &str, new_column: TableColumn) {
        let id = match self.columns_index.get(col_name) {
            None => return,
            Some(id) => *id,
        };
        self.columns_index.insert(new_column.name.clone(), id);
        self.columns[id] = new_column;
    }

    /// Get the metadata of the column according to the column name
    pub fn column(&self, name: &str) -> Option<&TableColumn> {
        self.columns_index
            .get(name)
            .map(|idx| unsafe { self.columns.get_unchecked(*idx) })
    }

    /// Get the index of the column
    pub fn column_index(&self, name: &str) -> Option<&usize> {
        self.columns_index.get(name)
    }

    pub fn column_name(&self, id: ColumnId) -> Option<&str> {
        for column in self.columns.iter() {
            if column.id == id {
                return Some(&column.name);
            }
        }
        None
    }

    /// Get the metadata of the column according to the column index
    pub fn column_by_index(&self, idx: usize) -> Option<&TableColumn> {
        self.columns.get(idx)
    }

    pub fn columns(&self) -> &[TableColumn] {
        &self.columns
    }

    pub fn fields(&self) -> Vec<TableColumn> {
        self.columns
            .iter()
            .filter(|column| column.column_type.is_field())
            .cloned()
            .collect()
    }

    /// Number of columns of ColumnType is Field
    pub fn field_num(&self) -> usize {
        self.columns
            .iter()
            .filter(|column| column.column_type.is_field())
            .count()
    }

    pub fn tag_num(&self) -> usize {
        self.columns
            .iter()
            .filter(|column| column.column_type.is_tag())
            .count()
    }

    // return (table_field_id, index), index mean field location which column
    pub fn fields_id(&self) -> HashMap<ColumnId, usize> {
        let mut ans = vec![];
        for i in self.columns.iter() {
            if i.column_type != ColumnType::Tag && i.column_type != ColumnType::Time {
                ans.push(i.id);
            }
        }
        ans.sort();
        let mut map = HashMap::new();
        for (i, id) in ans.iter().enumerate() {
            map.insert(*id, i);
        }
        map
    }

    pub fn next_column_id(&mut self) -> ColumnId {
        let ans = self.next_column_id;
        self.next_column_id += 1;
        ans
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        for i in self.columns.iter() {
            size += size_of_val(&i);
        }
        size += size_of_val(&self);
        size
    }

    pub fn contains_column(&self, column_name: &str) -> bool {
        self.columns_index.contains_key(column_name)
    }
}

pub fn is_time_column(field: &ArrowField) -> bool {
    TIME_FIELD_NAME == field.name()
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableColumn {
    pub id: ColumnId,
    pub name: String,
    pub column_type: ColumnType,
    pub encoding: Encoding,
}

impl From<&TableColumn> for ArrowField {
    fn from(column: &TableColumn) -> Self {
        let mut f = ArrowField::new(&column.name, column.column_type.into(), column.nullable());
        let mut map = BTreeMap::new();
        map.insert(FIELD_ID.to_string(), column.id.to_string());
        map.insert(TAG.to_string(), column.column_type.is_tag().to_string());
        f.set_metadata(Some(map));
        f
    }
}

impl From<TableColumn> for ArrowField {
    fn from(field: TableColumn) -> Self {
        (&field).into()
    }
}

impl TableColumn {
    pub fn new(id: ColumnId, name: String, column_type: ColumnType, encoding: Encoding) -> Self {
        Self {
            id,
            name,
            column_type,
            encoding,
        }
    }
    pub fn new_with_default(name: String, column_type: ColumnType) -> Self {
        Self {
            id: 0,
            name,
            column_type,
            encoding: Encoding::Default,
        }
    }

    pub fn new_time_column(id: ColumnId) -> TableColumn {
        TableColumn {
            id,
            name: TIME_FIELD_NAME.to_string(),
            column_type: ColumnType::Time,
            encoding: Encoding::Default,
        }
    }

    pub fn new_tag_column(id: ColumnId, name: String) -> TableColumn {
        TableColumn {
            id,
            name,
            column_type: ColumnType::Tag,
            encoding: Encoding::Default,
        }
    }

    pub fn nullable(&self) -> bool {
        // The time column cannot be empty
        !matches!(self.column_type, ColumnType::Time)
    }
}

impl From<ColumnType> for ArrowDataType {
    fn from(t: ColumnType) -> Self {
        match t {
            ColumnType::Tag => Self::Utf8,
            ColumnType::Time => Self::Timestamp(TimeUnit::Nanosecond, None),
            ColumnType::Field(ValueType::Float) => Self::Float64,
            ColumnType::Field(ValueType::Integer) => Self::Int64,
            ColumnType::Field(ValueType::Unsigned) => Self::UInt64,
            ColumnType::Field(ValueType::String) => Self::Utf8,
            ColumnType::Field(ValueType::Boolean) => Self::Boolean,
            _ => Self::Null,
        }
    }
}

impl TryFrom<ArrowDataType> for ColumnType {
    type Error = &'static str;

    fn try_from(value: ArrowDataType) -> Result<Self, Self::Error> {
        match value {
            ArrowDataType::Float64 => Ok(Self::Field(ValueType::Float)),
            ArrowDataType::Int64 => Ok(Self::Field(ValueType::Integer)),
            ArrowDataType::UInt64 => Ok(Self::Field(ValueType::Unsigned)),
            ArrowDataType::Utf8 => Ok(Self::Field(ValueType::String)),
            ArrowDataType::Boolean => Ok(Self::Field(ValueType::Boolean)),
            _ => Err("Error field type not supported"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ColumnType {
    Tag,
    Time,
    Field(ValueType),
}

impl ColumnType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tag => "tag",
            Self::Time => "time",
            Self::Field(ValueType::Integer) => "i64",
            Self::Field(ValueType::Unsigned) => "u64",
            Self::Field(ValueType::Float) => "f64",
            Self::Field(ValueType::Boolean) => "bool",
            Self::Field(ValueType::String) => "string",
            _ => "Error filed type not supported",
        }
    }

    pub fn as_column_type_str(&self) -> &'static str {
        match self {
            Self::Tag => "TAG",
            Self::Field(_) => "FIELD",
            Self::Time => "TIME",
        }
    }

    pub fn field_type(&self) -> u8 {
        match self {
            Self::Field(ValueType::Float) => 0,
            Self::Field(ValueType::Integer) => 1,
            Self::Field(ValueType::Unsigned) => 2,
            Self::Field(ValueType::Boolean) => 3,
            Self::Field(ValueType::String) => 4,
            _ => 0,
        }
    }

    pub fn from_i32(field_type: i32) -> Self {
        match field_type {
            0 => Self::Field(ValueType::Float),
            1 => Self::Field(ValueType::Integer),
            2 => Self::Field(ValueType::Unsigned),
            3 => Self::Field(ValueType::Boolean),
            4 => Self::Field(ValueType::String),
            5 => Self::Time,
            _ => Self::Field(ValueType::Unknown),
        }
    }

    pub fn to_sql_type_str(&self) -> &'static str {
        match self {
            Self::Tag => "STRING",
            Self::Time => "TIMESTAMP",
            Self::Field(value_type) => match value_type {
                ValueType::String => "STRING",
                ValueType::Integer => "BIGINT",
                ValueType::Unsigned => "BIGINT UNSIGNED",
                ValueType::Float => "DOUBLE",
                ValueType::Boolean => "BOOLEAN",
                ValueType::Unknown => "UNKNOWN",
            },
        }
    }
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.as_str();
        write!(f, "{}", s)
    }
}

impl ColumnType {
    pub fn is_tag(&self) -> bool {
        matches!(self, ColumnType::Tag)
    }

    pub fn is_time(&self) -> bool {
        matches!(self, ColumnType::Time)
    }

    pub fn is_field(&self) -> bool {
        matches!(self, ColumnType::Field(_))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct DatabaseSchema {
    tenant: String,
    database: String,
    pub config: DatabaseOptions,
}

impl DatabaseSchema {
    pub fn new(tenant_name: &str, database_name: &str) -> Self {
        DatabaseSchema {
            tenant: tenant_name.to_string(),
            database: database_name.to_string(),
            config: DatabaseOptions::default(),
        }
    }

    pub fn database_name(&self) -> &str {
        &self.database
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant
    }

    pub fn owner(&self) -> String {
        format!("{}.{}", self.tenant, self.database)
    }

    pub fn is_empty(&self) -> bool {
        if self.tenant.is_empty() && self.database.is_empty() {
            return true;
        }

        false
    }

    pub fn options(&self) -> &DatabaseOptions {
        &self.config
    }
}

pub fn make_owner(tenant_name: &str, database_name: &str) -> String {
    format!("{}.{}", tenant_name, database_name)
}

pub fn split_owner(owner: &str) -> (&str, &str) {
    owner
        .find('.')
        .map(|index| {
            (index < owner.len())
                .then(|| (&owner[..index], &owner[(index + 1)..]))
                .unwrap_or((owner, ""))
        })
        .unwrap_or_default()
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseOptions {
    // data keep time
    ttl: Option<Duration>,

    shard_num: Option<u64>,
    // shard coverage time range
    vnode_duration: Option<Duration>,

    replica: Option<u64>,
    // timestamp percision
    precision: Option<Precision>,
}

impl DatabaseOptions {
    pub const DEFAULT_TTL: Duration = Duration {
        time_num: 365,
        unit: DurationUnit::Day,
    };
    pub const DEFAULT_SHARD_NUM: u64 = 1;
    pub const DEFAULT_REPLICA: u64 = 1;
    pub const DEFAULT_VNODE_DURATION: Duration = Duration {
        time_num: 365,
        unit: DurationUnit::Day,
    };
    pub const DEFAULT_PRECISION: Precision = Precision::NS;

    pub fn ttl(&self) -> &Option<Duration> {
        &self.ttl
    }

    pub fn ttl_or_default(&self) -> &Duration {
        self.ttl.as_ref().unwrap_or(&DatabaseOptions::DEFAULT_TTL)
    }

    pub fn shard_num(&self) -> &Option<u64> {
        &self.shard_num
    }

    pub fn shard_num_or_default(&self) -> u64 {
        self.shard_num.unwrap_or(DatabaseOptions::DEFAULT_SHARD_NUM)
    }

    pub fn vnode_duration(&self) -> &Option<Duration> {
        &self.vnode_duration
    }

    pub fn vnode_duration_or_default(&self) -> &Duration {
        self.vnode_duration
            .as_ref()
            .unwrap_or(&DatabaseOptions::DEFAULT_VNODE_DURATION)
    }

    pub fn replica(&self) -> &Option<u64> {
        &self.replica
    }

    pub fn replica_or_default(&self) -> u64 {
        self.replica.unwrap_or(DatabaseOptions::DEFAULT_REPLICA)
    }

    pub fn precision(&self) -> &Option<Precision> {
        &self.precision
    }

    pub fn precision_or_default(&self) -> &Precision {
        self.precision
            .as_ref()
            .unwrap_or(&DatabaseOptions::DEFAULT_PRECISION)
    }

    pub fn with_ttl(&mut self, ttl: Duration) {
        self.ttl = Some(ttl);
    }

    pub fn with_shard_num(&mut self, shard_num: u64) {
        self.shard_num = Some(shard_num);
    }

    pub fn with_vnode_duration(&mut self, vnode_duration: Duration) {
        self.vnode_duration = Some(vnode_duration);
    }

    pub fn with_replica(&mut self, replica: u64) {
        self.replica = Some(replica);
    }

    pub fn with_precision(&mut self, precision: Precision) {
        self.precision = Some(precision)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum Precision {
    MS,
    US,
    NS,
}

impl Precision {
    pub fn new(text: &str) -> Option<Self> {
        match text.to_uppercase().as_str() {
            "MS" => Some(Precision::MS),
            "US" => Some(Precision::US),
            "NS" => Some(Precision::NS),
            _ => None,
        }
    }
}

impl fmt::Display for Precision {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Precision::MS => f.write_str("MS"),
            Precision::US => f.write_str("US"),
            Precision::NS => f.write_str("NS"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum DurationUnit {
    Minutes,
    Hour,
    Day,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Duration {
    pub time_num: u64,
    pub unit: DurationUnit,
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.unit {
            DurationUnit::Minutes => write!(f, "{} Minutes", self.time_num),
            DurationUnit::Hour => write!(f, "{} Hours", self.time_num),
            DurationUnit::Day => write!(f, "{} Days", self.time_num),
        }
    }
}

impl Duration {
    // with default DurationUnit day
    pub fn new(text: &str) -> Option<Self> {
        if text.is_empty() {
            return None;
        }
        let len = text.len();
        if let Ok(v) = text.parse::<u64>() {
            return Some(Duration {
                time_num: v,
                unit: DurationUnit::Day,
            });
        };

        let time = &text[..len - 1];
        let unit = &text[len - 1..];
        let time_num = match time.parse::<u64>() {
            Ok(v) => v,
            Err(_) => {
                return None;
            }
        };
        let time_unit = match unit.to_uppercase().as_str() {
            "D" => DurationUnit::Day,
            "H" => DurationUnit::Hour,
            "M" => DurationUnit::Minutes,
            _ => return None,
        };
        Some(Duration {
            time_num,
            unit: time_unit,
        })
    }

    pub fn new_inf() -> Self {
        Self {
            time_num: 100000,
            unit: DurationUnit::Day,
        }
    }

    pub fn time_stamp(&self) -> i64 {
        match self.unit {
            DurationUnit::Minutes => self.time_num as i64 * 60 * 1000000000,
            DurationUnit::Hour => self.time_num as i64 * 3600 * 1000000000,
            DurationUnit::Day => self.time_num as i64 * 24 * 3600 * 1000000000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    id: Oid,
    name: String,
    options: TenantOptions,
}

impl Identifier<Oid> for Tenant {
    fn id(&self) -> &Oid {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Tenant {
    pub fn new(id: Oid, name: String, options: TenantOptions) -> Self {
        Self { id, name, options }
    }

    pub fn options(&self) -> &TenantOptions {
        &self.options
    }
}

#[derive(Debug, Default, Clone, Builder, Serialize, Deserialize)]
#[builder(setter(into, strip_option), default)]
pub struct TenantOptions {
    pub comment: Option<String>,
}

impl TenantOptions {
    pub fn merge(self, other: Self) -> Self {
        Self {
            comment: self.comment.or(other.comment),
        }
    }
}

impl Display for TenantOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref e) = self.comment {
            write!(f, "comment={},", e)?;
        }

        Ok(())
    }
}
