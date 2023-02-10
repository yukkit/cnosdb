use std::ops::Deref;
use std::sync::Arc;

use datafusion::execution::context;
use datafusion::prelude::{SessionConfig, SessionContext};
use models::auth::user::User;
use models::oid::Oid;
use tskv::kv_option::QueryOptions;

use crate::service::protocol::Context;

// custom options
// TODO
pub struct SupportsAggregatePushdown(pub bool);

// datafusion optionis
pub const SKIP_FAILED_RULES: &str = "datafusion.optimizer.skip_failed_rules";

#[derive(Clone)]
pub struct IsiphoSessionCtx {
    // todo
    // ...
    user: User,

    tenant_id: Oid,
    tenant: String,
    default_database: String,

    inner: SessionContext,
}

impl IsiphoSessionCtx {
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    pub fn tenant_id(&self) -> &Oid {
        &self.tenant_id
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn default_database(&self) -> &str {
        &self.default_database
    }

    pub fn user(&self) -> &User {
        &self.user
    }
}

pub struct IsiphoSessionCtxFactory {
    // global config
    options: Arc<QueryOptions>,
}

impl IsiphoSessionCtxFactory {
    pub fn new(options: Arc<QueryOptions>) -> Self {
        Self { options }
    }
}

impl IsiphoSessionCtxFactory {
    pub fn create_isipho_session_ctx(&self, context: Context, tenant_id: Oid) -> IsiphoSessionCtx {
        // Use global configuration as the default configuration for session.
        // Low priority, will be overwritten by client options
        let QueryOptions {
            supports_aggregate_pushdown,
            ..
        } = self.options.deref();

        let mut session_config = SessionConfig::default()
            .set_bool(SKIP_FAILED_RULES, false)
            .with_extension(Arc::new(SupportsAggregatePushdown(
                *supports_aggregate_pushdown,
            )));

        // options passed by the client, highest priority
        let isipho_ctx = context.session_config().to_owned();
        session_config = if let Some(n) = isipho_ctx.target_partitions() {
            session_config.with_target_partitions(n)
        } else {
            session_config
        };

        let df_session_state = context::default_session_builder(session_config);
        let df_session_ctx = SessionContext::with_state(df_session_state);

        IsiphoSessionCtx {
            user: context.user_info().to_owned(),
            tenant_id,
            tenant: context.tenant().to_owned(),
            default_database: context.database().to_owned(),
            inner: df_session_ctx,
        }
    }

    /// only for test!!!
    pub fn create_test_isipho_session_ctx(context: Context, tenant_id: Oid) -> IsiphoSessionCtx {
        IsiphoSessionCtx {
            user: context.user_info().to_owned(),
            tenant_id,
            tenant: context.tenant().to_owned(),
            default_database: context.database().to_owned(),
            inner: SessionContext::default(),
        }
    }
}

#[derive(Clone, Default)]
pub struct IsiphoSessionConfig {
    target_partitions: Option<usize>,
}

impl IsiphoSessionConfig {
    pub fn target_partitions(&self) -> Option<usize> {
        self.target_partitions
    }

    /// Customize target_partitions
    /// partition count must be greater than zero
    pub fn with_target_partitions(mut self, n: usize) -> Self {
        self.target_partitions = Some(n);
        self
    }
}
