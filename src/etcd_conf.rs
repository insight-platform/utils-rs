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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use etcd_client::*;

use crate::errors::ConfigError;
use log::{info, warn};

const WATCH_WAIT_TTL: u64 = 1;

#[async_trait]
pub trait WatchResult {
    async fn notify(&mut self, res: Operation) -> Result<()>;
}

#[async_trait]
pub trait KVOperator {
    async fn ops(&mut self) -> Result<Vec<Operation>>;
}

pub struct ConfClient {
    client: Client,
    watcher: (Watcher, WatchStream),
    lease_timeout: i64,
    lease_id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
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
    Nope,
}

impl Default for Operation {
    fn default() -> Self {
        Operation::Nope
    }
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

    pub async fn get(&self, client: &mut Client) -> Result<(String, String)> {
        match self {
            VarPathSpec::SingleVar(key) => {
                let resp = client.get(key.as_bytes(), None).await?;
                match resp.kvs().first() {
                    Some(res) => {
                        info!(
                            "Etcd Get: Key={}, Value={}",
                            res.key_str()?,
                            res.value_str()?
                        );
                        Ok((res.key_str()?.to_string(), res.value_str()?.to_string()))
                    }
                    None => {
                        warn!("No value found for key: {:?}", key);
                        Err(ConfigError::KeyDoesNotExist(key.clone()).into())
                    }
                }
            }
            _ => panic!("get method is only defined for SingleVar"),
        }
    }

    pub async fn get_prefix(&self, client: &mut Client) -> Result<Vec<(String, String)>> {
        match self {
            VarPathSpec::Prefix(key) => {
                let resp = client
                    .get(key.as_bytes(), Some(GetOptions::new().with_prefix()))
                    .await?;
                let mut result = Vec::default();
                for kv in resp.kvs() {
                    info!(
                        "Etcd Get Prefix: Key={}, Value={}",
                        kv.key_str()?,
                        kv.value_str()?
                    );
                    result.push((kv.key_str()?.to_string(), kv.value_str()?.to_string()));
                }
                Ok(result)
            }
            _ => panic!("get_prefix method is only defined for Prefix"),
        }
    }
}

impl ConfClient {
    pub fn get_lease_id(&self) -> Option<i64> {
        self.lease_id.clone()
    }

    pub async fn new(
        uris: Vec<String>,
        credentials: Option<(String, String)>,
        path: String,
        lease_timeout: i64,
        connect_timeout: u64,
    ) -> Result<ConfClient> {
        info!("Connecting to {:?} etcd server", &uris);
        let mut client = Client::connect(
            uris,
            Some({
                let mut opts = ConnectOptions::new();
                if let Some((user, password)) = credentials {
                    opts = opts.with_user(user, password);
                }
                opts.with_timeout(Duration::from_secs(connect_timeout))
            }),
        )
        .await?;

        let watch_path = path.clone();
        info!("Watching for {} for configuration changes", &watch_path);
        let (watcher, watch_stream) = client
            .watch(watch_path, Some(WatchOptions::new().with_prefix()))
            .await?;

        Ok(ConfClient {
            client,
            watcher: (watcher, watch_stream),
            lease_timeout,
            lease_id: None,
        })
    }

    pub async fn fetch_vars(
        &mut self,
        var_spec: &Vec<VarPathSpec>,
    ) -> Result<Vec<(String, String)>> {
        let mut res = Vec::default();
        for v in var_spec {
            match v {
                VarPathSpec::SingleVar(_) => {
                    let value_pair = v.get(&mut self.client).await?;
                    res.push(value_pair);
                }
                VarPathSpec::Prefix(_) => {
                    let mut value_pairs = v.get_prefix(&mut self.client).await?;
                    res.append(&mut value_pairs);
                }
            }
        }
        Ok(res)
    }

    pub async fn kv_operations(&mut self, ops: Vec<Operation>) -> Result<()> {
        if self.lease_id.is_none() {
            let lease = self.client.lease_grant(self.lease_timeout, None).await?;
            self.lease_id = Some(lease.id());
        }

        for op in ops {
            match op {
                Operation::Set {
                    key,
                    value,
                    with_lease,
                } => {
                    self.client
                        .put(
                            key,
                            value,
                            Some({
                                let mut opts = PutOptions::new();
                                if with_lease {
                                    opts = opts.with_lease(self.lease_id.unwrap());
                                }
                                opts
                            }),
                        )
                        .await?;
                }
                Operation::DelKey { key } => {
                    self.client.delete(key, None).await?;
                }
                Operation::DelPrefix { prefix } => {
                    self.client
                        .delete(prefix, Some(DeleteOptions::new().with_prefix()))
                        .await?;
                }
                Operation::Nope => (),
            }
        }
        Ok(())
    }

    pub async fn monitor(
        &mut self,
        watch_result: Arc<Mutex<dyn WatchResult>>,
        kv_operator: Arc<Mutex<dyn KVOperator>>,
    ) -> Result<()> {
        info!("Starting watching for changes on {:?}", self.watcher);

        if self.lease_id.is_none() {
            let lease = self.client.lease_grant(self.lease_timeout, None).await?;
            self.lease_id = Some(lease.id());
        }

        loop {
            self.client.lease_keep_alive(self.lease_id.unwrap()).await?;

            let res = tokio::time::timeout(
                Duration::from_secs(WATCH_WAIT_TTL),
                self.watcher.1.message(),
            )
            .await;

            if let Ok(res) = res {
                if let Some(resp) = res? {
                    if resp.canceled() {
                        return Ok(());
                    } else if resp.created() {
                        info!("Etcd datcher was successfully deployed.");
                    }

                    for event in resp.events() {
                        if EventType::Delete == event.event_type() {
                            if let Some(kv) = event.kv() {
                                watch_result
                                    .lock()
                                    .unwrap()
                                    .notify(Operation::DelKey {
                                        key: kv.key_str()?.into(),
                                    })
                                    .await?;
                            }
                        }

                        if EventType::Put == event.event_type() {
                            if let Some(kv) = event.kv() {
                                watch_result
                                    .lock()
                                    .unwrap()
                                    .notify(Operation::Set {
                                        key: kv.key_str()?.to_string(),
                                        value: kv.value_str()?.to_string(),
                                        with_lease: kv.lease() != 0,
                                    })
                                    .await?;
                            }
                        }
                    }
                } else {
                    return Ok(());
                }
            }

            let ops = kv_operator.lock().unwrap().ops().await?;
            self.kv_operations(ops).await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::etcd_conf::{ConfClient, KVOperator, Operation, VarPathSpec, WatchResult};
    use anyhow::Result;
    use async_trait::async_trait;
    use log::info;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[tokio::test]
    async fn test_monitor() -> Result<()> {
        let mut client = ConfClient::new(
            vec!["10.0.0.1:2379".into()],
            Some(("root".to_string(), "secret".to_string())),
            "local/node".into(),
            5,
            10,
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
            counter: i32,
            watch_result: Operation,
        }

        #[async_trait]
        impl WatchResult for Watcher {
            async fn notify(&mut self, res: Operation) -> Result<()> {
                info!("Operation: {:?}", &res);
                self.counter += 1;
                self.watch_result = res;
                Ok(())
            }
        }

        struct Operator {
            operation: Option<Operation>,
        }

        #[async_trait]
        impl KVOperator for Operator {
            async fn ops(&mut self) -> Result<Vec<Operation>> {
                let op: Vec<_> = self.operation.take().into_iter().collect();
                Ok(op)
            }
        }

        let w = Arc::new(Mutex::new(Watcher::default()));
        let o = Arc::new(Mutex::new(Operator {
            operation: Some(Operation::Set {
                key: "local/node/leased".into(),
                value: "new_leased".into(),
                with_lease: true,
            }),
        }));

        // vec![VarPathSpec::SingleVar("local/node/leased".into())]

        match tokio::time::timeout(Duration::from_secs(5), client.monitor(w.clone(), o.clone()))
            .await
        {
            Ok(res) => {
                panic!("Unexpected termination occurred: {:?}", res);
            }
            Err(_) => {
                assert_eq!(
                    w.lock().unwrap().watch_result,
                    Operation::Set {
                        key: "local/node/leased".into(),
                        value: "new_leased".into(),
                        with_lease: true,
                    }
                );
                assert_eq!(w.lock().unwrap().counter, 3);
            }
        }

        Ok(())
    }
}
