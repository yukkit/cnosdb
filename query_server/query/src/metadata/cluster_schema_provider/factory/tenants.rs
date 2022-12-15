use std::sync::Arc;

use datafusion::datasource::MemTable;
use meta::meta_client::{MetaError, MetaRef};
use models::{auth::user::User, oid::Identifier};

use crate::metadata::cluster_schema_provider::{
    builder::tenants::ClusterSchemaTenantsBuilder, ClusterSchemaTableFactory,
};

const INFORMATION_SCHEMA_TENANTS: &str = "TENANTS";

pub struct ClusterSchemaTenantsFactory {}

impl ClusterSchemaTableFactory for ClusterSchemaTenantsFactory {
    fn table_name(&self) -> &str {
        INFORMATION_SCHEMA_TENANTS
    }

    fn create(
        &self,
        user: &User,
        metadata: MetaRef,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        let mut builder = ClusterSchemaTenantsBuilder::default();

        // Only visible to admin
        if user.desc().is_admin() {
            for tenant in metadata.tenant_manager().tenants()? {
                let options_str = serde_json::to_string(tenant.options())
                    .map_err(|e| MetaError::CommonError { msg: e.to_string() })?;

                builder.append_row(tenant.name(), options_str);
            }
        }

        let mem_table = MemTable::try_from(builder)
            .map_err(|e| MetaError::CommonError { msg: e.to_string() })?;
        Ok(Arc::new(mem_table))
    }
}
