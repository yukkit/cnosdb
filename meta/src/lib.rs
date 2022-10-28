use std::sync::Arc;
use std::time::Duration;

use actix_web::middleware;
use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::App;
use actix_web::HttpServer;
use openraft::Config;
use openraft::Raft;
use openraft::SnapshotPolicy;
use crate::meta_app::MetaApp;
use crate::service::api;
use crate::service::raft_api;
use crate::service::connection::Connections;
use crate::store::KvReq;
use crate::store::KvResp;
use crate::store::Store;
use crate::store::Restore;

pub mod meta_app;
pub mod client;
pub mod service;
pub mod store;
pub type NodeId = u64;

openraft::declare_raft_types!(
    pub ExampleTypeConfig: D = KvReq, R = KvResp, NodeId = NodeId
);

pub type ExampleRaft = Raft<ExampleTypeConfig, Connections, Arc<Store>>;

pub async fn start_raft_node(
    node_id: NodeId,
    http_addr: String,
) -> std::io::Result<()> {
    let mut config = Config::default().validate().unwrap();
    config.snapshot_policy = SnapshotPolicy::LogsSinceLast(500);
    config.max_applied_log_to_keep = 20000;
    config.install_snapshot_timeout = 400;

    let config = Arc::new(config);
    let es = Store::open_create(node_id);
    let mut store = Arc::new(es);

    store.restore().await;
    let network = Connections::new();
    let raft = Raft::new(node_id, config.clone(), network, store.clone());
    let app = Data::new(MetaApp {
        id: node_id,
        addr: http_addr.clone(),
        raft,
        store,
        config,
    });

    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(app.clone())
            .service(raft_api::append)
            .service(raft_api::snapshot)
            .service(raft_api::vote)
            .service(raft_api::init)
            .service(raft_api::add_learner)
            .service(raft_api::change_membership)
            .service(raft_api::metrics)
            .service(api::write)
            .service(api::read)
            .service(api::consistent_read)
    })
    .keep_alive(Duration::from_secs(5));

    let x = server.bind(http_addr)?;

    x.run().await
}