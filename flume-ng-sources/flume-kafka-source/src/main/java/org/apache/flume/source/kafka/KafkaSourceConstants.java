/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaSourceConstants {

  public static final String KAFKA_PREFIX = "kafka.";
  public static final String DEFAULT_KEY_DESERIALIZER = StringDeserializer.class.getName();
  public static final String DEFAULT_VALUE_DESERIALIZER = StringDeserializer.class.getName();
  public static final String BOOTSTRAP_SERVERS = KAFKA_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
  public static final String GROUP_ID = KAFKA_PREFIX + ConsumerConfig.GROUP_ID_CONFIG;
  public static final String TOPIC_CONSUMER_PREFIX = KAFKA_PREFIX + "topics.";
  public static final String TOPICS = KAFKA_PREFIX + "topics";
  public static final String TOPICS_REGEX = TOPICS + "." + "regex";
  public static final String DEFAULT_AUTO_COMMIT = "false";
  public static final String RESET_OFFSET = "resetOffset";
  public static final String BATCH_SIZE = "batchSize";
  public static final String BATCH_DURATION_MS = "batchDurationMillis";
  public static final String BACKOFF_INTERVAL_MS = "backoffDurationMillis";
  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final int DEFAULT_BATCH_DURATION = 1000;
  public static final String DEFAULT_GROUP_ID = "flume";
  public static final String CONSUMER_GROUP_SIZE = "consumerGroupSize";
  public static final String TOPIC_HEADER_KEY = "topicHeaderKey";
  public static final String PARTITION_ID_HEADER_KEY = "partitionIdHeaderKey";
  public static final String KEY_HEADER_KEY = "keyHeaderKey";
  public static final String TIMESTAMP_HEADER_KEY = "timestampHeaderKey";
  public static final String PERIOD = ".";
  public static final Integer DEFAULT_BACKOFF_INTERVAL_MS = 1000;
  public static final Integer DEFAULT_SHUTDOWN_WAIT_TIME_MS = 2000;
  public static final String SOURCE_LOCK_DIR = "sourceLockDir";

  // flume event headers
  public static final String DEFAULT_TOPIC_HEADER = "topic";
  public static final String DEFAULT_KEY_HEADER = "key";
  public static final String DEFAULT_TIMESTAMP_HEADER = "timestamp";
  public static final String DEFAULT_PARTITION_HEADER = "partition";

  public static final String CONSUMER_TIMEOUT_MS = "consumer.timeout.ms";

  public static final String MIGRATE_ZOOKEEPER_OFFSETS = "migrateZookeeperOffsets";
  public static final boolean DEFAULT_MIGRATE_ZOOKEEPER_OFFSETS = true;

  public static final String AVRO_EVENT = "useFlumeEventFormat";
  public static final boolean DEFAULT_AVRO_EVENT = false;

  /* Old Properties */
  public static final String ZOOKEEPER_CONNECT_FLUME_KEY = "zookeeperConnect";

  // flume event headers
  public static final String KEY_HEADER = "key";
  public static final String TIMESTAMP_HEADER = "timestamp";
  public static final String PARTITION_HEADER = "partition";

  public static final String SET_TOPIC_HEADER = "setTopicHeader";
  public static final boolean DEFAULT_SET_TOPIC_HEADER = true;
  public static final String TOPIC_HEADER = "topicHeader";
}
