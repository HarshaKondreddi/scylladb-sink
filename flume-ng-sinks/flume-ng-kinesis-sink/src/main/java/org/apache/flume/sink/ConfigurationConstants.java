/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.sink;

/**
 * Contains configuration constants used by Kinesis and Firehose sources/sinks
 */
final class ConfigurationConstants {
  static final int DEFAULT_BATCH_SIZE = 100;
  //Kinesis limits
  static final int MAX_BATCH_SIZE = 500;
  static final int MAX_BATCH_BYTE_SIZE = 5000000;
  static final int MAX_EVENT_SIZE = 1000000;

  static final int DEFAULT_MAX_ATTEMPTS = 100;

  static final boolean DEFAULT_ROLLBACK_AFTER_MAX_ATTEMPTS = false;

  static final long BACKOFF_TIME_IN_MILLIS = 100L;

  static final boolean DEFAULT_PARTITION_KEY_FROM_EVENT = false;

  static final String DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-east-1.amazonaws.com";

  static final String DEFAULT_FIREHOSE_ENDPOINT = "https://firehose.us-east-1.amazonaws.com";

  static final String DEFAULT_REGION = "us-east-1";

  private ConfigurationConstants() {
    // Disable object creation
  }
}
