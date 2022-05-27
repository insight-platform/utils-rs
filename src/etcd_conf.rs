/**
 * Copyright 2022 BWSoft Management, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use etcd_rs::*;
use futures::StreamExt;

use crate::errors::ConfigError;
use log::{info, warn};

const WATCH_WAIT_TTL: u64 = 1;

#[async_trait]
pub trait WatchResult {
    async fn notify(&mut self, res: Vec<(String, String)>) -> Result<()>;
}

#[async_trait]
pub trait KVOperator {
    async fn ops(&mut self) -> Result<Vec<Operation>>;
}

pub struct ConfClient {
    path: String,
    client: Client,
    lease_timeout: u64,
    lease_id: Option<u64>,
}

pub enum Operation {
    Set {
        key: String,
        value: String,
        with_lease: bool,
    },
    DelKey {
        key: String,
    },
    DelPrefix {
        prefix: String,
    },
}

#[derive(Debug)]
pub enum VarPathSpec {
    SingleVar(String),
    Prefix(String),
}

impl VarPathSpec {
    pub fn new_var(p: &str, var: &str) -> VarPathSpec {
        VarPathSpec::SingleVar(
            Path::new(p)
                .join(Path::new(var))
                .to_str()
                .unwrap()
                .to_string(),
        )
    }

    pub fn new_prefix(prefix: &str, dir: &str) -> VarPathSpec {
        VarPathSpec::Prefix(
            Path::new(prefix)
                .join(Path::new(dir))
                .to_str()
                .unwrap()
                .into(),
        )
    }

    async fn get(&self, client: &Client) -> Result<(String, String)> {
        match self {
            VarPathSpec::SingleVar(key) => {
                let v = client
                    .kv()
                    .range(RangeRequest::new(KeyRange::key(key.as_str())))
                    .await;
                match v {
                    Ok(mut res) if res.count() == 1 => {
                        let kv = res.take_kvs();
                        info!(
                            "Etcd Get: Key={}, Value={}",
                            &key,
                            kv.first().unwrap().value_str()
                        );
                        Ok((key.clone(), kv.first().unwrap().value_str().into()))
                    }
                    r => {
                        warn!("Error occurred: {:?}", r);
                        Err(ConfigError::KeyDoesNotExist(key.clone()).into())
                    }
                }
            }
            _ => panic!("get method is only defined for SingleVar"),
        }
    }

    async fn get_prefix(&self, client: &Client) -> Result<Vec<(String, String)>> {
        match self {
            VarPathSpec::Prefix(key) => {
                let v = client
                    .kv()
                    .range(RangeRequest::new(KeyRange::prefix(key.as_str())))
                    .await;
                match v {
                    Ok(mut res) => {
                        let kv = res.take_kvs();
                        let res: Vec<(String, String)> = kv
                            .iter()
                            .map(|e| {
                                info!(
                                    "Etcd Get Prefix: Key={}, Value={}",
                                    e.key_str(),
                                    e.value_str()
                                );
                                (e.key_str().into(), e.value_str().into())
                            })
                            .collect();
                        Ok(res)
                    }
                    Err(e) => {
                        warn!("Error occurred: {:?}", e);
                        Err(ConfigError::KeyDoesNotExist(key.clone()).into())
                    }
                }
            }
            _ => panic!("get_prefix method is only defined for Prefix"),
        }
    }
}

impl ConfClient {
    pub async fn new(
        uris: Vec<String>,
        credentials: Option<(String, String)>,
        path: String,
        lease_timeout: u64,
    ) -> Result<ConfClient> {
        info!("Connecting to {:?} etcd server", &uris);
        let client = Client::connect(ClientConfig {
            endpoints: uris,
            auth: credentials,
            tls: None,
        })
        .await?;

        Ok(ConfClient {
            path,
            client,
            lease_timeout,
            lease_id: None,
        })
    }

    async fn fetch_vars(&self, var_spec: &Vec<VarPathSpec>) -> Result<Vec<(String, String)>> {
        let mut res = Vec::default();
        for v in var_spec {
            match v {
                VarPathSpec::SingleVar(_) => {
                    let value_pair = v.get(&self.client).await?;
                    res.push(value_pair);
                }
                VarPathSpec::Prefix(_) => {
                    let mut value_pairs = v.get_prefix(&self.client).await?;
                    res.append(&mut value_pairs);
                }
            }
        }
        Ok(res)
    }

    pub async fn kv_operations(&mut self, ops: Vec<Operation>) -> Result<()> {
        if self.lease_id.is_none() {
            let lease = self
                .client
                .lease()
                .grant(LeaseGrantRequest::new(Duration::from_secs(
                    self.lease_timeout,
                )))
                .await?;

            self.lease_id = Some(lease.id());
        }

        for op in ops {
            match op {
                Operation::Set {
                    key,
                    value,
                    with_lease: use_lease,
                } => {
                    self.client
                        .kv()
                        .put({
                            let mut req = PutRequest::new(key, value);
                            if use_lease {
                                req.set_lease(self.lease_id.unwrap());
                            }
                            req
                        })
                        .await?;
                }
                Operation::DelKey { key } => {
                    self.client
                        .kv()
                        .delete(DeleteRequest::new(KeyRange::key(key)))
                        .await?;
                }
                Operation::DelPrefix { prefix } => {
                    self.client
                        .kv()
                        .delete(DeleteRequest::new(KeyRange::prefix(prefix)))
                        .await?;
                }
            }
        }
        Ok(())
    }

    pub async fn monitor(
        &mut self,
        var_spec: Vec<VarPathSpec>,
        watch_result: &mut dyn WatchResult,
        kv_operator: &mut dyn KVOperator,
    ) -> Result<()> {
        info!("Starting watching for changes on {}", self.path);

        let res = self.fetch_vars(&var_spec).await?;
        watch_result.notify(res).await?;

        let watch_path = self.path.clone();

        info!("Watching for {} for configuration changes", &watch_path);
        let mut inbound = self
            .client
            .watch(KeyRange::prefix(watch_path))
            .await
            .unwrap();

        if self.lease_id.is_none() {
            let lease = self
                .client
                .lease()
                .grant(LeaseGrantRequest::new(Duration::from_secs(
                    self.lease_timeout,
                )))
                .await?;

            self.lease_id = Some(lease.id());
        }

        loop {
            self.client
                .lease()
                .keep_alive(LeaseKeepAliveRequest::new(self.lease_id.unwrap()))
                .await?;

            let res =
                tokio::time::timeout(Duration::from_secs(WATCH_WAIT_TTL), inbound.next()).await;

            if let Ok(_) = res {
                let res = self.fetch_vars(&var_spec).await?;
                watch_result.notify(res).await?;
            }

            let ops = kv_operator.ops().await?;
            self.kv_operations(ops).await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::etcd_conf::{ConfClient, KVOperator, Operation, VarPathSpec, WatchResult};
    use anyhow::Result;
    use async_trait::async_trait;
    use std::time::Duration;

    #[tokio::test]
    async fn test_monitor() -> Result<()> {
        let mut client = ConfClient::new(
            vec!["http://10.0.0.1:22379".into()],
            None,
            "local/node".into(),
            5,
        )
        .await?;

        client
            .kv_operations(vec![
                Operation::Set {
                    key: "local/node".into(),
                    value: "value".into(),
                    with_lease: false,
                },
                Operation::Set {
                    key: "local/node/leased".into(),
                    value: "leased_value".into(),
                    with_lease: true,
                },
            ])
            .await?;

        let res = client
            .fetch_vars(&vec![VarPathSpec::SingleVar("local/node/leased".into())])
            .await?;

        assert_eq!(
            res,
            vec![("local/node/leased".into(), "leased_value".into())]
        );

        let res = client
            .fetch_vars(&vec![
                VarPathSpec::Prefix("local/node".into()),
                VarPathSpec::SingleVar("local/node/leased".into()),
            ])
            .await?;

        assert_eq!(
            res,
            vec![
                ("local/node".into(), "value".into()),
                ("local/node/leased".into(), "leased_value".into()),
                ("local/node/leased".into(), "leased_value".into())
            ]
        );

        #[derive(Default)]
        struct Watcher {
            new_leased: String,
        }

        #[async_trait]
        impl WatchResult for Watcher {
            async fn notify(&mut self, res: Vec<(String, String)>) -> Result<()> {
                self.new_leased = res.first().unwrap().1.clone();
                Ok(())
            }
        }

        struct Operator;

        #[async_trait]
        impl KVOperator for Operator {
            async fn ops(&mut self) -> Result<Vec<Operation>> {
                Ok(vec![Operation::Set {
                    key: "local/node/leased".into(),
                    value: "new_leased".into(),
                    with_lease: true,
                }])
            }
        }

        let mut w = Watcher::default();
        let mut o = Operator {};

        match tokio::time::timeout(
            Duration::from_secs(5),
            client.monitor(
                vec![VarPathSpec::SingleVar("local/node/leased".into())],
                &mut w,
                &mut o,
            ),
        )
        .await
        {
            Ok(res) => {
                panic!("Unexpected termination occurred: {:?}", res);
            }
            Err(_) => {
                assert_eq!(w.new_leased, "new_leased".to_string());
            }
        }

        Ok(())
    }
}
