use std::sync::Arc;
use std::sync::Mutex;

use openraft::error::ClientWriteError;
use openraft::error::ForwardToLeader;

use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RemoteError;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::meta_client::MetaError;
use crate::meta_client::MetaResult;
use crate::store::command::*;
use crate::store::state_machine::CommandResp;
use crate::ExampleTypeConfig;
use crate::NodeId;

pub type WriteError = RPCError<ExampleTypeConfig, ClientWriteError<NodeId>>;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Empty {}

#[derive(Debug)]
pub struct MetaHttpClient {
    //inner: reqwest::Client,
    addrs: Vec<String>,

    leader: Arc<Mutex<String>>,
}

impl MetaHttpClient {
    pub fn new(addr: String) -> Self {
        let mut addrs = vec![];
        let list: Vec<&str> = addr.split(';').collect();
        for item in list.iter() {
            addrs.push(item.to_string());
        }
        addrs.sort();
        let leader_addr = addrs[0].clone();

        Self {
            //inner: reqwest::Client::new(),
            addrs,
            leader: Arc::new(Mutex::new(leader_addr)),
        }
    }

    pub fn read<T>(&self, req: &ReadCommand) -> MetaResult<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let rsp = self.do_request("read", Some(req))?;

        let rsp = serde_json::from_str::<T>(&rsp).map_err(|err| MetaError::MetaClientErr {
            msg: err.to_string(),
        })?;

        Ok(rsp)
    }

    pub fn write<T>(&self, req: &WriteCommand) -> MetaResult<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let rsp = self.do_request("write", Some(req))?;

        let rsp = serde_json::from_str::<T>(&rsp).map_err(|err| MetaError::MetaClientErr {
            msg: err.to_string(),
        })?;

        Ok(rsp)
    }

    pub fn watch_tenant<T>(&self, req: &(String, String, String, u64)) -> MetaResult<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let rsp = self.do_request("watch_tenant", Some(req))?;

        let rsp = serde_json::from_str::<T>(&rsp).map_err(|err| MetaError::MetaClientErr {
            msg: err.to_string(),
        })?;

        Ok(rsp)
    }

    fn do_request<Req>(&self, uri: &str, req: Option<&Req>) -> Result<CommandResp, WriteError>
    where
        Req: Serialize + 'static,
    {
        self.send_rpc_to_leader(uri, req)
    }

    //////////////////////////////////////////////////

    fn switch_leader(&self) {
        let mut t = self.leader.lock().unwrap();

        if let Ok(index) = self.addrs.binary_search(&t) {
            let index = (index + 1) % self.addrs.len();
            *t = self.addrs[index].clone();
        } else {
            *t = self.addrs[0].clone();
        }
    }

    fn send_rpc_to_leader<Req, Resp, Err>(
        &self,
        uri: &str,
        req: Option<&Req>,
    ) -> Result<Resp, RPCError<ExampleTypeConfig, Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error
            + Serialize
            + DeserializeOwned
            + TryInto<ForwardToLeader<NodeId>>
            + Clone,
    {
        let mut n_retry = 3;

        loop {
            let res: Result<Resp, RPCError<ExampleTypeConfig, Err>> =
                self.do_send_rpc_to_leader(uri, req);

            let rpc_err = match res {
                Ok(x) => return Ok(x),
                Err(rpc_err) => rpc_err,
            };

            if let RPCError::RemoteError(remote_err) = &rpc_err {
                let forward_err_res =
                    <Err as TryInto<ForwardToLeader<NodeId>>>::try_into(remote_err.source.clone());

                if let Ok(ForwardToLeader {
                    leader_id: Some(_),
                    leader_node: Some(leader_node),
                    ..
                }) = forward_err_res
                {
                    {
                        let mut t = self.leader.lock().unwrap();
                        *t = leader_node.addr;
                    }

                    n_retry -= 1;
                    if n_retry > 0 {
                        continue;
                    }
                } else {
                    self.switch_leader();
                }
            } else {
                self.switch_leader();
            }

            return Err(rpc_err);
        }
    }

    fn do_send_rpc_to_leader<Req, Resp, Err>(
        &self,
        uri: &str,
        req: Option<&Req>,
    ) -> Result<Resp, RPCError<ExampleTypeConfig, Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        let url = format!("http://{}/{}", self.leader.lock().unwrap(), uri);
        let resp = if let Some(r) = req {
            ureq::post(&url).send_json(r)
        } else {
            ureq::get(&url).call()
        }
        .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res: Result<Resp, Err> = resp
            .into_json()
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(0, e)))
    }
}

#[cfg(test)]
mod test {
    use crate::{client::MetaHttpClient, store::command};
    use std::{thread, time};

    use models::{meta_data::NodeInfo, schema::DatabaseSchema};

    pub async fn watch_tenant(cluster: &str, tenant: &str) {
        let client = MetaHttpClient::new("127.0.0.1:21001".to_string());
        let mut version = 0;
        let mut cmd = (
            "client_id".to_string(),
            cluster.to_string(),
            tenant.to_string(),
            version,
        );

        loop {
            cmd.3 = version;

            println!("=== watch ...");
            let result = client.watch_tenant::<command::TenantMetaDataDelta>(&cmd);
            println!("=== watch: {:#?}", result);

            if let Ok(val) = result {
                version = val.ver_range.1;
                if val.full_load {
                    version = val.update.version;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_watch_tenant() {
        watch_tenant("cluster_xxx", "tenant_test").await;
    }

    #[tokio::test]
    async fn test_meta_client() {
        let cluster = "cluster_xxx".to_string();
        let tenant = "tenant_test".to_string();

        //let hand = tokio::spawn(watch_tenant("cluster_xxx", "tenant_test"));

        let client = MetaHttpClient::new("127.0.0.1:21001".to_string());

        let node = NodeInfo {
            id: 111,
            tcp_addr: "".to_string(),
            http_addr: "127.0.0.1:8888".to_string(),
            status: 0,
        };

        let req = command::WriteCommand::AddDataNode(cluster.clone(), node);
        let rsp = client.write::<command::StatusResponse>(&req);
        println!("=== add data: {:?}", rsp);
        thread::sleep(time::Duration::from_secs(3));

        let req = command::WriteCommand::CreateDB(
            cluster.clone(),
            tenant.clone(),
            DatabaseSchema::new(&tenant, "test_db"),
        );
        let rsp = client.write::<command::TenaneMetaDataResp>(&req);
        println!("=== create db: {:?}", rsp);
        thread::sleep(time::Duration::from_secs(3));

        let req = command::WriteCommand::CreateBucket(
            cluster.clone(),
            tenant.clone(),
            "test_db".to_string(),
            1667456711000000000,
        );
        let rsp = client.write::<command::TenaneMetaDataResp>(&req);
        println!("=== create bucket: {:?}", rsp);
        thread::sleep(time::Duration::from_secs(3));

        let req = command::WriteCommand::CreateDB(
            cluster,
            tenant.clone(),
            DatabaseSchema::new(&tenant, "test_db2"),
        );
        let rsp = client.write::<command::TenaneMetaDataResp>(&req);
        println!("=== create db2: {:?}", rsp);
        thread::sleep(time::Duration::from_secs(3));

        thread::sleep(time::Duration::from_secs(300));
    }
}
