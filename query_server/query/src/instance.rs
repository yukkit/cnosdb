use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::scheduler::Scheduler;
use derive_builder::Builder;
use models::{
    auth::{
        role::{SystemTenantRole, TenantRoleIdentifier},
        user::{User, UserInfo, UserOptionsBuilder, ROOT},
    },
    oid::Identifier,
    schema::TenantOptionsBuilder,
};
use spi::{
    query::{
        auth::AccessControlRef, dispatcher::QueryDispatcher, session::IsiphoSessionCtxFactory,
        DEFAULT_CATALOG,
    },
    server::BuildSnafu,
    server::Result,
    server::{dbms::DatabaseManagerSystem, MetaDataSnafu},
    server::{AuthSnafu, QuerySnafu},
    service::protocol::{Query, QueryHandle, QueryId},
};

use tokio::runtime::Runtime;
use trace::{debug, info};
use tskv::kv_option::Options;

use crate::auth::auth_control::{AccessControlImpl, AccessControlNoCheck};
use crate::dispatcher::manager::SimpleQueryDispatcherBuilder;
use crate::sql::optimizer::CascadeOptimizerBuilder;
use crate::sql::parser::DefaultParser;
use meta::error::MetaError;
use models::schema::DatabaseSchema;
use snafu::ResultExt;
use spi::query::DEFAULT_DATABASE;
use spi::server::ServerError;
use tskv::engine::EngineRef;

#[derive(Builder)]
pub struct Cnosdbms<D> {
    // TODO access control
    access_control: AccessControlRef,
    // query dispatcher & query execution
    query_dispatcher: D,
}

#[async_trait]
impl<D> DatabaseManagerSystem for Cnosdbms<D>
where
    D: QueryDispatcher,
{
    fn authenticate(&self, user_info: &UserInfo, tenant_name: Option<&str>) -> Result<User> {
        self.access_control
            .access_check(user_info, tenant_name)
            .context(AuthSnafu)
    }

    async fn execute(&self, query: &Query) -> Result<QueryHandle> {
        let query_id = self.query_dispatcher.create_query_id();

        let tenant_id = self
            .access_control
            .tenant_id(query.context().tenant())
            .context(AuthSnafu)?;

        let result = self
            .query_dispatcher
            .execute_query(tenant_id, query_id, query)
            .await
            .context(QuerySnafu)?;

        Ok(QueryHandle::new(query_id, query.clone(), result))
    }

    fn metrics(&self) -> String {
        let infos = self.query_dispatcher.running_query_infos();
        let status = self.query_dispatcher.running_query_status();

        format!(
            "infos: {}\nstatus: {}\n",
            infos
                .iter()
                .map(|e| format!("{:?}", e))
                .collect::<Vec<_>>()
                .join(","),
            status
                .iter()
                .map(|e| format!("{:?}", e))
                .collect::<Vec<_>>()
                .join(",")
        )
    }

    fn cancel(&self, query_id: &QueryId) {
        self.query_dispatcher.cancel_query(query_id);
    }
}

#[derive(Debug)]
pub struct QueryEngineRuntimes {
    pub history_runtime: Arc<Runtime>,
}

pub fn make_cnosdbms(
    _engine: EngineRef,
    coord: CoordinatorRef,
    options: Options,
    runtimes: QueryEngineRuntimes,
) -> Result<impl DatabaseManagerSystem> {
    let QueryEngineRuntimes { history_runtime } = runtimes;

    // TODO session config need load global system config
    let session_factory = Arc::new(IsiphoSessionCtxFactory::default());
    let parser = Arc::new(DefaultParser::default());
    let optimizer = Arc::new(CascadeOptimizerBuilder::default().build());
    // TODO wrap, and num_threads configurable
    let scheduler = Arc::new(Scheduler::new(num_cpus::get() * 2));

    let queries_limit = options.query.max_server_connections;

    init_metadata(coord.clone())?;

    let meta_manager = coord.meta_manager();

    let query_dispatcher = SimpleQueryDispatcherBuilder::default()
        .with_history_runtime(history_runtime)
        .with_coord(coord)
        .with_session_factory(session_factory)
        .with_parser(parser)
        .with_optimizer(optimizer)
        .with_scheduler(scheduler)
        .with_queries_limit(queries_limit)
        .build()
        .context(BuildSnafu)?;

    let mut builder = CnosdbmsBuilder::default();

    let access_control_no_check = AccessControlNoCheck::new(meta_manager);
    if options.query.auth_enabled {
        debug!("build access control");
        builder.access_control(Arc::new(AccessControlImpl::new(access_control_no_check)))
    } else {
        debug!("build access control without check");
        builder.access_control(Arc::new(access_control_no_check))
    };

    let db_server = builder
        .query_dispatcher(query_dispatcher)
        .build()
        .expect("build db server");

    Ok(db_server)
}

fn init_metadata(coord: CoordinatorRef) -> Result<()> {
    // init admin
    let user_manager = coord.meta_manager().user_manager();
    debug!("Check if system user {} exist", ROOT);
    if user_manager.user(ROOT).context(MetaDataSnafu)?.is_none() {
        info!("Initialize the system user {}", ROOT);

        let options = UserOptionsBuilder::default()
            .must_change_password(true)
            .comment("system admin")
            .build()
            .expect("failed to init admin user.");
        let res = user_manager.create_user(ROOT.to_string(), options, true);
        if let Err(err) = res {
            match err {
                MetaError::UserAlreadyExists { .. } => {}
                _ => return Err(ServerError::MetaData { source: err }),
            }
        }
    }

    // init system tenant
    let tenant_manager = coord.meta_manager().tenant_manager();
    debug!("Check if system tenant {} exist", DEFAULT_CATALOG);
    if tenant_manager
        .tenant(DEFAULT_CATALOG)
        .context(MetaDataSnafu)?
        .is_none()
    {
        info!("Initialize the system tenant {}", DEFAULT_CATALOG);

        let options = TenantOptionsBuilder::default()
            .comment("system tenant")
            .build()
            .expect("failed to init admin user.");
        let res = tenant_manager.create_tenant(DEFAULT_CATALOG.to_string(), options);
        if let Err(err) = res {
            match err {
                MetaError::TenantAlreadyExists { .. } => {}
                _ => return Err(ServerError::MetaData { source: err }),
            }
        }

        debug!("Add root to the system tenant as owner");
        if let Some(root) = user_manager.user(ROOT).context(MetaDataSnafu)? {
            if let Some(client) = tenant_manager.tenant_meta(DEFAULT_CATALOG) {
                let role = TenantRoleIdentifier::System(SystemTenantRole::Owner);
                if let Err(err) = client.add_member_with_role(*root.id(), role) {
                    match err {
                        MetaError::UserAlreadyExists { .. }
                        | MetaError::MemberAlreadyExists { .. } => {}
                        _ => return Err(ServerError::MetaData { source: err }),
                    }
                }
            }
        }

        debug!("Initialize the system database {}", DEFAULT_DATABASE);

        let client = tenant_manager
            .tenant_meta(DEFAULT_CATALOG)
            .ok_or(MetaError::TenantNotFound {
                tenant: DEFAULT_CATALOG.to_string(),
            })
            .context(MetaDataSnafu)?;
        let res = client.create_db(&DatabaseSchema::new(DEFAULT_CATALOG, DEFAULT_DATABASE));
        if let Err(err) = res {
            match err {
                MetaError::DatabaseAlreadyExists { .. } => {}
                _ => return Err(ServerError::MetaData { source: err }),
            }
        }
    }

    Ok(())
}
