use std::time::Duration;

use crate::query::execution::Output;
use crate::service::protocol::{Query, QueryId};
use async_trait::async_trait;
use models::auth::user::UserDesc;
use models::oid::{Identifier, Oid};

use super::execution::QueryState;
use super::Result;

#[async_trait]
pub trait QueryDispatcher: Send + Sync {
    fn start(&self);

    fn stop(&self);

    fn create_query_id(&self) -> QueryId;

    fn query_info(&self, id: &QueryId);

    async fn execute_query(&self, tenant_id: Oid, id: QueryId, query: &Query) -> Result<Output>;

    fn running_query_infos(&self) -> Vec<QueryInfo>;

    fn running_query_status(&self) -> Vec<QueryStatus>;

    fn cancel_query(&self, id: &QueryId);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryInfo {
    query_id: QueryId,
    query: String,

    tenant_id: Oid,
    tenant_name: String,
    user: UserDesc,
}

impl QueryInfo {
    pub fn new(
        query_id: QueryId,
        query: String,
        tenant_id: Oid,
        tenant_name: String,
        user: UserDesc,
    ) -> Self {
        Self {
            query_id,
            query,
            tenant_id,
            tenant_name,
            user,
        }
    }

    pub fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn tenant_id(&self) -> Oid {
        self.tenant_id
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant_name
    }

    pub fn user_id(&self) -> Oid {
        *self.user.id()
    }

    pub fn user_name(&self) -> &str {
        self.user.name()
    }
}

#[derive(Debug)]
pub struct QueryStatus {
    state: QueryState,
    duration: Duration,
}

impl QueryStatus {
    pub fn new(state: QueryState, duration: Duration) -> Self {
        Self { state, duration }
    }

    pub fn query_state(&self) -> &QueryState {
        &self.state
    }

    pub fn duration(&self) -> &Duration {
        &self.duration
    }
}
