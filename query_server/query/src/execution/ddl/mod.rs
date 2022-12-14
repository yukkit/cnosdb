use async_trait::async_trait;

use spi::query::dispatcher::{QueryInfo, QueryStatus};
use spi::query::execution::{Output, QueryExecution, QueryStateMachineRef};
use spi::query::logical_planner::DDLPlan;
use spi::query::{self, QueryError};

use spi::query::execution::ExecutionError;

use self::alter_tenant::AlterTenantTask;
use self::alter_user::AlterUserTask;
use self::create_role::CreateRoleTask;
use self::create_table::CreateTableTask;
use self::create_tenant::CreateTenantTask;
use self::create_user::CreateUserTask;
use self::drop_global_object::DropGlobalObjectTask;
use self::drop_tenant_object::DropTenantObjectTask;
use self::grant_revoke::GrantRevokeTask;
use crate::execution::ddl::alter_database::AlterDatabaseTask;
use crate::execution::ddl::alter_table::AlterTableTask;
use crate::execution::ddl::create_database::CreateDatabaseTask;
use crate::execution::ddl::describe_database::DescribeDatabaseTask;
use crate::execution::ddl::describe_table::DescribeTableTask;
use crate::execution::ddl::show_database::ShowDatabasesTask;
use crate::execution::ddl::show_table::ShowTablesTask;
use snafu::ResultExt;

use self::create_external_table::CreateExternalTableTask;
use self::drop_database_object::DropDatabaseObjectTask;

mod alter_database;
mod alter_table;
mod alter_tenant;
mod alter_user;
mod create_database;
mod create_external_table;
mod create_role;
mod create_table;
mod create_tenant;
mod create_user;
mod describe_database;
mod describe_table;
mod drop_database_object;
mod drop_global_object;
mod drop_tenant_object;
mod grant_revoke;
mod show_database;
mod show_table;

/// Traits that DDL tasks should implement
#[async_trait]
trait DDLDefinitionTask: Send + Sync {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError>;
}

pub struct DDLExecution {
    task_factory: DDLDefinitionTaskFactory,
    query_state_machine: QueryStateMachineRef,
}

impl DDLExecution {
    pub fn new(query_state_machine: QueryStateMachineRef, plan: DDLPlan) -> Self {
        Self {
            task_factory: DDLDefinitionTaskFactory { plan },
            query_state_machine,
        }
    }
}

#[async_trait]
impl QueryExecution for DDLExecution {
    // execute ddl task
    // This logic usually does not change
    async fn start(&self) -> Result<Output, QueryError> {
        let query_state_machine = self.query_state_machine.clone();

        query_state_machine.begin_schedule();

        let result = self
            .task_factory
            .create_task()
            .execute(query_state_machine.clone())
            .await
            .context(query::ExecutionSnafu);

        query_state_machine.end_schedule();

        result
    }

    fn cancel(&self) -> query::Result<()> {
        // ddl ignore
        Ok(())
    }

    fn info(&self) -> QueryInfo {
        let qsm = &self.query_state_machine;
        QueryInfo::new(
            qsm.query_id,
            qsm.query.content().to_string(),
            *qsm.session.tenant_id(),
            qsm.session.tenant().to_string(),
            qsm.query.context().user_info().desc().clone(),
        )
    }

    fn status(&self) -> QueryStatus {
        QueryStatus::new(
            self.query_state_machine.state().clone(),
            self.query_state_machine.duration(),
        )
    }
}

struct DDLDefinitionTaskFactory {
    plan: DDLPlan,
}

impl DDLDefinitionTaskFactory {
    // According to different statement types, construct the corresponding task
    // If you add ddl operations, you usually need to modify here
    fn create_task(&self) -> Box<dyn DDLDefinitionTask> {
        match &self.plan {
            DDLPlan::CreateExternalTable(sub_plan) => {
                Box::new(CreateExternalTableTask::new(sub_plan.clone()))
            }
            DDLPlan::DropDatabaseObject(sub_plan) => {
                Box::new(DropDatabaseObjectTask::new(sub_plan.clone()))
            }
            DDLPlan::DropTenantObject(sub_plan) => {
                Box::new(DropTenantObjectTask::new(sub_plan.clone()))
            }
            DDLPlan::DropGlobalObject(sub_plan) => {
                Box::new(DropGlobalObjectTask::new(sub_plan.clone()))
            }
            DDLPlan::CreateTable(sub_plan) => Box::new(CreateTableTask::new(sub_plan.clone())),
            DDLPlan::CreateDatabase(sub_plan) => {
                Box::new(CreateDatabaseTask::new(sub_plan.clone()))
            }
            DDLPlan::CreateTenant(sub_plan) => Box::new(CreateTenantTask::new(sub_plan.clone())),
            DDLPlan::CreateUser(sub_plan) => Box::new(CreateUserTask::new(sub_plan.clone())),
            DDLPlan::CreateRole(sub_plan) => Box::new(CreateRoleTask::new(sub_plan.clone())),
            DDLPlan::DescribeDatabase(sub_plan) => {
                Box::new(DescribeDatabaseTask::new(sub_plan.clone()))
            }
            DDLPlan::DescribeTable(sub_plan) => Box::new(DescribeTableTask::new(sub_plan.clone())),
            DDLPlan::ShowTables(sub_plan) => Box::new(ShowTablesTask::new(sub_plan.clone())),
            DDLPlan::ShowDatabases() => Box::new(ShowDatabasesTask::new()),
            DDLPlan::AlterDatabase(sub_plan) => Box::new(AlterDatabaseTask::new(sub_plan.clone())),
            DDLPlan::AlterTable(sub_plan) => Box::new(AlterTableTask::new(sub_plan.clone())),
            DDLPlan::AlterTenant(sub_plan) => Box::new(AlterTenantTask::new(sub_plan.clone())),
            DDLPlan::AlterUser(sub_plan) => Box::new(AlterUserTask::new(sub_plan.clone())),
            DDLPlan::GrantRevoke(sub_plan) => Box::new(GrantRevokeTask::new(sub_plan.clone())),
        }
    }
}
