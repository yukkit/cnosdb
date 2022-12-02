use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use models::auth::user::UserDesc;
use snafu::ResultExt;
use spi::catalog::MetadataError;
use spi::query::execution;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateUser;
use trace::debug;

pub struct CreateUserTask {
    stmt: CreateUser,
}

impl CreateUserTask {
    pub fn new(stmt: CreateUser) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CreateUserTask {
    async fn execute(
        &self,
        _query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let CreateUser {
            ref name,
            ref if_not_exists,
            ref options,
        } = self.stmt;

        // TODO 元数据接口查询用户是否存在
        // fn user(
        //     &self,
        //     name: &str
        // ) -> Result<Option<UserDesc>>;

        let user: Option<UserDesc> = None;

        match (if_not_exists, user) {
            // do not create if exists
            (true, Some(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Some(_)) => Err(MetadataError::UserAlreadyExists {
                user_name: name.clone(),
            })
            .context(execution::MetadataSnafu),
            // does not exist, create
            (_, None) => {
                // TODO 创建用户
                // name: String
                // options: UserOptions
                // fn create_user(
                //     &mut self,
                //     name: String,
                //     options: UserOptions
                // ) -> Result<&UserDesc>;

                debug!("Create user {} with options [{}]", name, options);

                Ok(Output::Nil(()))
            }
        }
    }
}