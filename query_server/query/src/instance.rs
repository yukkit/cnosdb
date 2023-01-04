use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
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

use trace::{debug, info};
use tskv::kv_option::Options;

use crate::dispatcher::manager::SimpleQueryDispatcherBuilder;
use crate::sql::optimizer::CascadeOptimizerBuilder;
use crate::sql::parser::DefaultParser;
use crate::{
    auth::auth_control::{AccessControlImpl, AccessControlNoCheck},
    execution::scheduler::LocalScheduler,
};
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

pub fn make_cnosdbms(
    _engine: EngineRef,
    coord: CoordinatorRef,
    options: Options,
) -> Result<impl DatabaseManagerSystem> {
    // TODO session config need load global system config
    let session_factory = Arc::new(IsiphoSessionCtxFactory::default());
    let parser = Arc::new(DefaultParser::default());
    let optimizer = Arc::new(CascadeOptimizerBuilder::default().build());
    // TODO wrap, and num_threads configurable
    let scheduler = Arc::new(LocalScheduler {});

    let queries_limit = options.query.max_server_connections;

    init_metadata(coord.clone())?;

    let meta_manager = coord.meta_manager();

    let query_dispatcher = SimpleQueryDispatcherBuilder::default()
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
        let res = client.create_db(DatabaseSchema::new(DEFAULT_CATALOG, DEFAULT_DATABASE));
        if let Err(err) = res {
            match err {
                MetaError::DatabaseAlreadyExists { .. } => {}
                _ => return Err(ServerError::MetaData { source: err }),
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use config::get_config;
    use coordinator::service::MockCoordinator;
    use models::auth::user::UserInfo;
    use std::ops::DerefMut;
    use trace::debug;

    use super::*;
    use datafusion::arrow::{record_batch::RecordBatch, util::pretty::pretty_format_batches};
    use spi::{query::DEFAULT_CATALOG, service::protocol::ContextBuilder};
    use tskv::engine::MockEngine;

    #[macro_export]
    macro_rules! assert_batches_eq {
        ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
            let expected_lines: Vec<String> = $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

            let formatted = pretty_format_batches($CHUNKS).unwrap().to_string();

            let actual_lines: Vec<&str> = formatted.trim().lines().collect();

            assert_eq!(
                expected_lines, actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    async fn exec_sql(db: &impl DatabaseManagerSystem, sql: &str) -> Vec<RecordBatch> {
        let user = UserInfo {
            user: DEFAULT_CATALOG.to_string(),
            password: "todo".to_string(),
            private_key: None,
        };

        let user = db
            .authenticate(&user, Some(DEFAULT_CATALOG))
            .expect("authenticate");

        let query = Query::new(ContextBuilder::new(user).build(), sql.to_string());

        let result = db.execute(&query).await.unwrap();

        result.result().chunk_result().to_vec()
    }

    #[tokio::test]
    #[ignore]
    async fn test_simple_sql() {
        let config = get_config("../../config/config.toml");
        let opt = Options::from(&config);
        let db = make_cnosdbms(
            Arc::new(MockEngine::default()),
            Arc::new(MockCoordinator::default()),
            opt,
        )
        .unwrap();

        let mut result = exec_sql(&db, "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter) order by num").await;

        let expected = vec![
            "+-----+--------+",
            "| num | letter |",
            "+-----+--------+",
            "| 1   | one    |",
            "| 2   | two    |",
            "| 3   | three  |",
            "+-----+--------+",
        ];

        // let formatted = pretty_format_batches(result.deref_mut())
        //     .unwrap()
        //     .to_string();

        // println!("{}", formatted);

        assert_batches_eq!(expected, result.deref_mut());
    }

    fn generate_data(n: usize) -> String {
        // let mut random = rand::thread_rng();

        // random.gen_range();
        debug!("start generate data.");
        let rows: Vec<String> = (0..n)
            .into_iter()
            .map(|i| {
                format!(
                    "({}, '{}----xxxxxx=====3333444hhhhhhxx324r9cc')",
                    i % 1000,
                    i % 100
                )
            })
            .collect();
        // .reduce(|l, r| {
        //     format!("{}, {}", l, r)
        // }).unwrap();

        let result = rows.join(",");

        debug!("end generate data.");

        result
    }

    #[tokio::test]
    #[ignore]
    async fn test_topk_sql() {
        // trace::init_default_global_tracing("/tmp", "test_rust.log", "debug");
        let config = get_config("../../config/config.toml");
        let opt = Options::from(&config);
        let db = make_cnosdbms(
            Arc::new(MockEngine::default()),
            Arc::new(MockCoordinator::default()),
            opt,
        )
        .unwrap();

        let sql = format!(
            "SELECT * FROM
        (VALUES  {}) AS t (num,letter)
        order by num limit 20",
            generate_data(1_000_000)
        );

        let start = Utc::now();

        let mut result = exec_sql(&db, &sql).await;

        let end = Utc::now();

        println!("used time: {}", (start - end).num_milliseconds());

        let expected = vec![
            "+-----+--------+",
            "| num | letter |",
            "+-----+--------+",
            "| 1   | one    |",
            "| 2   | two    |",
            "+-----+--------+",
        ];

        assert_batches_eq!(expected, result.deref_mut());
    }

    #[tokio::test]
    #[ignore]
    async fn test_topk_desc_sql() {
        // trace::init_default_global_tracing("/tmp", "test_rust.log", "debug");
        let config = get_config("../../config/config.toml");
        let opt = Options::from(&config);
        let db = make_cnosdbms(
            Arc::new(MockEngine::default()),
            Arc::new(MockCoordinator::default()),
            opt,
        )
        .unwrap();

        let mut result = exec_sql(
            &db,
            "
        SELECT * FROM
        (VALUES  (9, 'nine'),(2, 'two'), (1, 'one'), (3, 'three')) AS t (num,letter)
        order by num desc limit 2",
        )
        .await;

        let expected = vec![
            "+-----+--------+",
            "| num | letter |",
            "+-----+--------+",
            "| 9   | nine   |",
            "| 3   | three  |",
            "+-----+--------+",
        ];

        // let formatted = pretty_format_batches(result.deref_mut())
        //     .unwrap()
        //     .to_string();

        // println!("{}", formatted);

        assert_batches_eq!(expected, result.deref_mut());
    }

    #[tokio::test]
    #[ignore]
    async fn test_create_external_csv_table() {
        let config = get_config("../../config/config.toml");
        let opt = Options::from(&config);
        let db = make_cnosdbms(
            Arc::new(MockEngine::default()),
            Arc::new(MockCoordinator::default()),
            opt,
        )
        .unwrap();

        assert_batches_eq!(
            vec!["++", "++", "++",],
            exec_sql(
                &db,
                "CREATE EXTERNAL TABLE decimal_simple (
                    c1  DECIMAL(10,6) NOT NULL,
                    c2  DOUBLE NOT NULL,
                    c3  BIGINT NOT NULL,
                    c4  BOOLEAN NOT NULL,
                    c5  DECIMAL(12,7) NOT NULL
                )
                STORED AS CSV
                WITH HEADER ROW
                LOCATION 'tests/data/csv/decimal_data.csv'",
            )
            .await
            .deref_mut()
        );

        assert_batches_eq!(
            vec![
                "+----------+----------------+-----+-------+-----------+",
                "| c1       | c2             | c3  | c4    | c5        |",
                "+----------+----------------+-----+-------+-----------+",
                "| 0.000010 | 0.000000000001 | 1   | true  | 0.0000140 |",
                "| 0.000020 | 0.000000000002 | 2   | true  | 0.0000250 |",
                "| 0.000020 | 0.000000000002 | 3   | false | 0.0000190 |",
                "| 0.000030 | 0.000000000003 | 4   | true  | 0.0000320 |",
                "| 0.000030 | 0.000000000003 | 5   | false | 0.0000350 |",
                "| 0.000030 | 0.000000000003 | 5   | true  | 0.0000110 |",
                "| 0.000040 | 0.000000000004 | 5   | true  | 0.0000440 |",
                "| 0.000040 | 0.000000000004 | 8   | false | 0.0000440 |",
                "| 0.000040 | 0.000000000004 | 12  | false | 0.0000400 |",
                "| 0.000040 | 0.000000000004 | 14  | true  | 0.0000400 |",
                "| 0.000050 | 0.000000000005 | 1   | false | 0.0001000 |",
                "| 0.000050 | 0.000000000005 | 4   | true  | 0.0000780 |",
                "| 0.000050 | 0.000000000005 | 8   | false | 0.0000330 |",
                "| 0.000050 | 0.000000000005 | 9   | true  | 0.0000520 |",
                "| 0.000050 | 0.000000000005 | 100 | true  | 0.0000680 |",
                "+----------+----------------+-----+-------+-----------+",
            ],
            exec_sql(&db, "select * from decimal_simple order by c1,c2,c3,c4,c5;",)
                .await
                .deref_mut()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_create_external_parquet_table() {
        let config = get_config("../../config/config.toml");
        let opt = Options::from(&config);
        let db = make_cnosdbms(
            Arc::new(MockEngine::default()),
            Arc::new(MockCoordinator::default()),
            opt,
        )
        .unwrap();

        assert_batches_eq!(
            vec!["++", "++", "++",],
            exec_sql(
                &db,
                "
            CREATE EXTERNAL TABLE rep
            STORED AS PARQUET
            LOCATION 'tests/data/parquet/userdata1.parquet';",
            )
            .await
            .deref_mut()
        );

        assert_batches_eq!(vec![
            "+---------------------+----+---------+---------+--------------------------+--------+----------------+------------------+------------------------+------------+-----------+--------------------------+-------+",
            "| COUNT(UInt8(1))     |    |         |         |                          |        |                |                  |                        |            |           |                          |       |",
            "+---------------------+----+---------+---------+--------------------------+--------+----------------+------------------+------------------------+------------+-----------+--------------------------+-------+",
            "| 1000                |    |         |         |                          |        |                |                  |                        |            |           |                          |       |",
            "| 2016-02-03 07:55:29 | 1  | Amanda  | Jordan  | ajordan0@com.com         | Female | 1.197.201.2    | 6759521864920116 | Indonesia              | 3/8/1971   | 49756.53  | Internal Auditor         | 1E+02 |",
            "| 2016-02-03 17:04:03 | 2  | Albert  | Freeman | afreeman1@is.gd          | Male   | 218.111.175.34 |                  | Canada                 | 1/16/1968  | 150280.17 | Accountant IV            |       |",
            "| 2016-02-03 01:09:31 | 3  | Evelyn  | Morgan  | emorgan2@altervista.org  | Female | 7.161.136.94   | 6767119071901597 | Russia                 | 2/1/1960   | 144972.51 | Structural Engineer      |       |",
            "| 2016-02-03 00:36:21 | 4  | Denise  | Riley   | driley3@gmpg.org         | Female | 140.35.109.83  | 3576031598965625 | China                  | 4/8/1997   | 90263.05  | Senior Cost Accountant   |       |",
            "| 2016-02-03 05:05:31 | 5  | Carlos  | Burns   | cburns4@miitbeian.gov.cn |        | 169.113.235.40 | 5602256255204850 | South Africa           |            |           |                          |       |",
            "| 2016-02-03 07:22:34 | 6  | Kathryn | White   | kwhite5@google.com       | Female | 195.131.81.179 | 3583136326049310 | Indonesia              | 2/25/1983  | 69227.11  | Account Executive        |       |",
            "| 2016-02-03 08:33:08 | 7  | Samuel  | Holmes  | sholmes6@foxnews.com     | Male   | 232.234.81.197 | 3582641366974690 | Portugal               | 12/18/1987 | 14247.62  | Senior Financial Analyst |       |",
            "| 2016-02-03 06:47:06 | 8  | Harry   | Howell  | hhowell7@eepurl.com      | Male   | 91.235.51.73   |                  | Bosnia and Herzegovina | 3/1/1962   | 186469.43 | Web Developer IV         |       |",
            "| 2016-02-03 03:52:53 | 9  | Jose    | Foster  | jfoster8@yelp.com        | Male   | 132.31.53.61   |                  | South Korea            | 3/27/1992  | 231067.84 | Software Test Engineer I | 1E+02 |",
            "| 2016-02-03 18:29:47 | 10 | Emily   | Stewart | estewart9@opensource.org | Female | 143.28.251.245 | 3574254110301671 | Nigeria                | 1/28/1997  | 27234.28  | Health Coach IV          |       |",
            "+---------------------+----+---------+---------+--------------------------+--------+----------------+------------------+------------------------+------------+-----------+--------------------------+-------+",
        ], exec_sql(
            &db,
            "
            select count(1) from rep;
            select * from rep limit 10;
            ",
        )
        .await.deref_mut());
    }

    #[tokio::test]
    #[ignore]
    async fn test_create_external_json_table() {
        let config = get_config("../config/config.toml");
        let opt = Options::from(&config);
        let db = make_cnosdbms(
            Arc::new(MockEngine::default()),
            Arc::new(MockCoordinator::default()),
            opt,
        )
        .unwrap();

        assert_batches_eq!(
            vec!["++", "++", "++",],
            exec_sql(
                &db,
                "
            CREATE EXTERNAL TABLE rep
            STORED AS NDJSON
            LOCATION 'tests/data/json/schema_infer_limit.json';",
            )
            .await
            .deref_mut()
        );

        assert_batches_eq!(
            vec![
                "+-----+------+-------+---+",
                "| a   | b    | c     | d |",
                "+-----+------+-------+---+",
                "| 1   |      |       |   |",
                "| -10 | -3.5 |       |   |",
                "| 2   | 0.6  | false |   |",
                "| 1   | 2    | false | 4 |",
                "| 4   |      |       |   |",
                "+-----+------+-------+---+",
            ],
            exec_sql(
                &db,
                "
            select * from rep limit 10;
            select count(1) from rep;
            ",
            )
            .await
            .deref_mut()
        );
    }
}
