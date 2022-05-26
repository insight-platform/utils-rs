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
use crate::errors::ConfigLoadErrors;
use anyhow::Result;

use rdkafka::config::ClientConfig;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;

pub fn load_kafka_config(path: &str) -> Result<ClientConfig> {
    let mut kafka_config = ClientConfig::new();

    let file = File::open(path)?;
    for line in BufReader::new(&file).lines() {
        let cur_line: String = line?.trim().to_string();
        if cur_line.starts_with('#') || cur_line.is_empty() {
            continue;
        }
        let key_value: Vec<_> = cur_line.split('=').collect();
        let key = key_value
            .get(0)
            .ok_or_else(|| ConfigLoadErrors::KeySplitError(cur_line.clone()))?;
        let value = key_value
            .get(1)
            .ok_or_else(|| ConfigLoadErrors::KeySplitError(cur_line.clone()))?;
        kafka_config.set(*key, *value);
    }

    Ok(kafka_config)
}

#[cfg(test)]
mod tests {
    use crate::kafka_config::load_kafka_config;
    use anyhow::Result;

    #[test]
    fn test_config_load() -> Result<()> {
        let conf = load_kafka_config("assets/test_kafka.conf")?;
        drop(conf);
        Ok(())
    }
}
