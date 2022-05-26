/**
 * Copyright 2020 Confluent Inc.
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
use std::env::current_dir;
use std::path::Path;

use crate::errors::ConfigLoadErrors;
use anyhow::Result;
use hocon::{Hocon, HoconLoader};
use log::{debug, info};

pub struct HoconClient {
    hocon: Hocon,
}

impl HoconClient {
    pub fn load(path: &Path) -> Result<HoconClient> {
        debug!("CWD is: {:?}", current_dir().unwrap());
        info!("Loading config from the path {:?}", &path);
        let load = HoconLoader::new().load_file(path);
        match load {
            Ok(loader) => match loader.hocon() {
                Ok(hocon) => Ok(HoconClient { hocon }),
                Err(e) => Err(ConfigLoadErrors::HoconLoadError(e).into()),
            },
            Err(e) => Err(ConfigLoadErrors::HoconLoadError(e).into()),
        }
    }

    pub fn fetch_value_by_path(&self, path: &str) -> Hocon {
        let split = path.split('/');
        let mut start = &self.hocon;
        for p in split {
            debug!("Hocon path is {:?}", start);
            start = &start[p];
        }
        start.clone()
    }

    pub fn fetch_string(&self, path: &'static str) -> Result<String> {
        self.fetch_value_by_path(path)
            .as_string()
            .ok_or_else(|| ConfigLoadErrors::ValueCastError(path, "String").into())
    }

    pub fn fetch_i64(&self, path: &'static str) -> Result<i64> {
        self.fetch_value_by_path(path)
            .as_i64()
            .ok_or_else(|| ConfigLoadErrors::ValueCastError(path, "i64").into())
    }

    pub fn fetch_u64(&self, path: &'static str) -> Result<u64> {
        u64::try_from(self.fetch_i64(path)?)
            .or_else(|_| Err(ConfigLoadErrors::ValueCastError(path, "i64->u64").into()))
    }
}

#[cfg(test)]
mod tests {
    use crate::hocon_config::HoconClient;
    use anyhow::Result;
    use std::path::Path;

    #[test]
    fn test_config_load() -> Result<()> {
        let c = HoconClient::load(Path::new("./assets/test_hocon.conf"))?;

        #[allow(dead_code)]
        struct Conf {
            connection_timeout: u64,
            random_val: i64,
            server: String,
        }

        impl Conf {
            pub fn new(conf: &HoconClient) -> Result<Self> {
                Ok(Conf {
                    connection_timeout: conf.fetch_u64("section/connection_timeout")?,
                    random_val: conf.fetch_i64("section/random_val")?,
                    server: conf.fetch_string("section/server")?,
                })
            }
        }

        Conf::new(&c)?;

        drop(c);
        //let vars = c.config_vars()?;
        //drop(vars);
        Ok(())
    }
}
