use std::fmt;

use datafusion::sql::sqlparser::ast::{
    AnalyzeFormat, DataType, Expr, Ident, ObjectName, Offset, OrderByExpr, Value,
};
use datafusion::sql::sqlparser::ast::{SqlOption, TableFactor};
use datafusion::sql::{parser::CreateExternalTable, sqlparser::ast::Statement};
use models::codec::Encoding;

use super::logical_planner::{DatabaseObjectType, GlobalObjectType, TenantObjectType};

/// Statement representations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtStatement {
    /// ANSI SQL AST node
    SqlStatement(Box<Statement>),

    // bulk load/unload
    Copy(Copy),

    CreateExternalTable(CreateExternalTable),
    CreateTable(CreateTable),
    CreateDatabase(CreateDatabase),
    CreateTenant(CreateTenant),
    CreateUser(CreateUser),
    CreateRole(CreateRole),

    DropDatabaseObject(DropDatabaseObject),
    DropTenantObject(DropTenantObject),
    DropGlobalObject(DropGlobalObject),

    GrantRevoke(GrantRevoke),

    DescribeTable(DescribeTable),
    DescribeDatabase(DescribeDatabase),
    ShowDatabases(),
    ShowTables(Option<ObjectName>),
    ShowSeries(Box<ShowSeries>),
    ShowTagValues(Box<ShowTagValues>),
    Explain(Explain),
    //todo:  insert/update/alter

    // system cmd
    ShowQueries,
    AlterDatabase(AlterDatabase),
    AlterTable(AlterTable),
    AlterTenant(AlterTenant),
    AlterUser(AlterUser),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copy {
    pub copy_target: CopyTarget,
    pub file_format_options: Vec<SqlOption>,
    pub copy_options: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyTarget {
    IntoTable(CopyIntoTable),
    IntoLocation(CopyIntoLocation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyIntoTable {
    pub location: UriLocation,
    pub table_name: ObjectName,
    pub columns: Vec<Ident>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyIntoLocation {
    pub from: TableFactor,
    pub location: UriLocation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UriLocation {
    pub path: String,
    pub connection_options: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTable {
    pub table_name: ObjectName,
    pub alter_action: AlterTableAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Explain {
    pub analyze: bool,
    pub verbose: bool,
    pub ext_statement: Box<ExtStatement>,
    pub format: Option<AnalyzeFormat>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTableAction {
    AddColumn {
        column: ColumnOption,
    },
    AlterColumnEncoding {
        column_name: Ident,
        encoding: Encoding,
    },
    DropColumn {
        column_name: Ident,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterDatabase {
    pub name: ObjectName,
    pub options: DatabaseOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTenant {
    /// tenant name
    pub name: Ident,
    pub operation: AlterTenantOperation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTenantOperation {
    // Ident: user_name, Ident: role_name
    AddUser(Ident, Ident),
    // Ident: user_name, Ident: role_name
    SetUser(Ident, Ident),
    RemoveUser(Ident),
    Set(SqlOption),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropDatabaseObject {
    pub object_name: ObjectName,
    pub if_exist: bool,
    pub obj_type: DatabaseObjectType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTenantObject {
    pub object_name: Ident,
    pub if_exist: bool,
    pub obj_type: TenantObjectType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropGlobalObject {
    pub object_name: Ident,
    pub if_exist: bool,
    pub obj_type: GlobalObjectType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeObject {
    pub object_name: ObjectName,
    pub obj_type: ObjectType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateUser {
    pub if_not_exists: bool,
    /// User name
    pub name: Ident,
    pub with_options: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUser {
    /// User name
    pub name: Ident,
    pub operation: AlterUserOperation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterUserOperation {
    RenameTo(Ident),
    Set(SqlOption),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantRevoke {
    pub is_grant: bool,
    pub privileges: Vec<Privilege>,
    pub role_name: Ident,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Privilege {
    pub action: Action,
    pub database: Ident,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Read,
    Write,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRole {
    pub if_not_exists: bool,
    /// Role name
    pub name: Ident,
    pub inherit: Option<Ident>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTenant {
    pub name: Ident,
    pub if_not_exists: bool,
    pub with_options: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDatabase {
    pub name: ObjectName,
    pub if_not_exists: bool,
    pub options: DatabaseOptions,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    pub name: ObjectName,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnOption {
    pub name: Ident,
    pub is_tag: bool,
    pub data_type: DataType,
    pub encoding: Option<Encoding>,
}

impl ColumnOption {
    pub fn new_field(name: Ident, data_type: DataType, encoding: Option<Encoding>) -> Self {
        Self {
            name,
            is_tag: false,
            data_type,
            encoding,
        }
    }

    pub fn new_tag(name: Ident) -> Self {
        Self {
            name,
            is_tag: true,
            data_type: DataType::String,
            encoding: None,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct DatabaseOptions {
    // data keep time
    pub ttl: Option<String>,

    pub shard_num: Option<u64>,
    // shard coverage time range
    pub vnode_duration: Option<String>,

    pub replica: Option<u64>,
    // timestamp percision
    pub precision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTable {
    pub table_name: ObjectName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeDatabase {
    pub database_name: ObjectName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowTables {
    pub database_name: ObjectName,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowTagBody {
    // on db
    pub database_name: Option<ObjectName>,
    // from
    pub table: ObjectName,
    // where
    pub selection: Option<Expr>,
    // order by
    pub order_by: Vec<OrderByExpr>,
    // limit
    pub limit: Option<Expr>,
    // offset
    pub offset: Option<Offset>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowSeries {
    pub body: ShowTagBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum With {
    Equal(Ident),
    UnEqual(Ident),
    In(Vec<Ident>),
    NotIn(Vec<Ident>),
    Match(Value),
    UnMatch(Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowTagValues {
    pub body: ShowTagBody,
    pub with: With,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ObjectType {
    Table,
    Database,
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            ObjectType::Table => "TABLE",
            ObjectType::Database => "DATABASE",
        })
    }
}

pub fn parse_bool_value(value: Value) -> std::result::Result<bool, String> {
    match value {
        Value::Boolean(s) => Ok(s),
        _ => Err(format!("expected boolean value, but found : {}", value)),
    }
}

pub fn parse_string_value(value: Value) -> std::result::Result<String, String> {
    match value {
        Value::SingleQuotedString(s) => Ok(s),
        _ => Err(format!("expected string value, but found : {}", value)),
    }
}

pub fn parse_char_value(value: Value) -> std::result::Result<char, String> {
    let token = parse_string_value(value)?;
    match token.len() {
        1 => Ok(token.chars().next().unwrap()),
        _ => Err("Delimiter must be a single char".to_string()),
    }
}
