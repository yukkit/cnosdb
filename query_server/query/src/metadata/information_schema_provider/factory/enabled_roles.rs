use std::sync::Arc;

use datafusion::datasource::MemTable;
use meta::meta_client::{MetaClientRef, MetaError};
use models::{auth::user::User, oid::Identifier};

use crate::{
    dispatcher::query_tracker::QueryTracker,
    metadata::information_schema_provider::{
        builder::enabled_roles::InformationSchemaEnabledRolesBuilder, InformationSchemaTableFactory,
    },
};

const INFORMATION_SCHEMA_ENABLED_ROLES: &str = "ENABLED_ROLES";

/// This view displays the role information of the current user under the current tenant.
pub struct EnabledRolesFactory {}

impl InformationSchemaTableFactory for EnabledRolesFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_ENABLED_ROLES
    }

    fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        let mut builder = InformationSchemaEnabledRolesBuilder::default();

        if let Some(role) = metadata.member_role(user.desc().id())? {
            builder.append_row(role.name());
        }

        let mem_table = MemTable::try_from(builder)
            .map_err(|e| MetaError::CommonError { msg: e.to_string() })?;
        Ok(Arc::new(mem_table))
    }
}
