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
package org.apache.flume.channel.rmq;

public interface AMQPChannelConfiguration {

  String AMQP_PREFIX = "amqp.";
  String QUEUE_CONFIG = AMQP_PREFIX + "queue";
  String XCHANGE_CONFIG = AMQP_PREFIX + "exchange";
  String XCHANGE_ROUTING_CONFIG = AMQP_PREFIX + "exchange.routing";
  String HOST_CONFIG = AMQP_PREFIX + "host";
  String PORT_CONFIG = AMQP_PREFIX + "port";
  String USERNAME = AMQP_PREFIX + "username";
  String PASSORD = AMQP_PREFIX + "password";

  String INFLIGHT_COUNT = "in_flight";
  String PREFETCH_COUNT = "prefetch";
  String CONSUMERS_COUNT = "consumers";
  String WAIT_TIME_IN_MILLIS = "wait_in_millis";
  String IN_MEMORY_MAX_RETRY_COUNT = "in_memory.max.retries";

  String IS_DEAD_LETTERED_ENABLED = "dead_letter_exchange.enabled";

  String OFFSET_ID = "key";
  String CHANNEL_ID = "CHANNEL_ID";
  String IN_MEMORY_RETRY_COUNT = "IN_MEMORY_RETRY_COUNT";
  String IS_DEAD_LETTERED = "IS_DEAD_LETTERED";
  String IS_PROCESSED_SUCCESSFULLY = "IS_PROCESSED_SUCCESSFULLY";
}
