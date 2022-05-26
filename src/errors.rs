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
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigLoadErrors {
    #[error("Key `{0}` cannot be split to key and value!")]
    KeySplitError(String),
    #[error("Value of key `{0}` cannot be casted to requested type `{1}`")]
    ValueCastError(&'static str, &'static str),
    #[error("Unable to load HOCON configuration. Error is: ${0}")]
    HoconLoadError(hocon::Error),
}
