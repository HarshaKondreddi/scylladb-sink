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
package org.apache.flume.testutils;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class EmbeddedServers implements AutoCloseable {

  private boolean enableRMQ;
  private boolean enableZK;
  private boolean enableKafka;

  private Service zookeeper;
  private Service kafka;
  private Service rmq;

  private int zkPort;
  private int rmqPort;
  private int kafkaPort;

  public EmbeddedServers start() throws Exception {
    if (enableRMQ()) {
      rmq = new EmbeddedRMQ().port(rmqPort).start();
    }
    if (enableZK()) {
      zookeeper = new EmbeddedZookeeper(zkPort).start();
    }
    if (enableKafka()) {
      kafka = new EmbeddedKafka(zkPort, new int[]{kafkaPort}).start();
    }
    return this;
  }

  public void close() throws Exception {
    if (null != rmq) {
      rmq.close();
    }
    if (null != zookeeper) {
      zookeeper.close();
    }
    if (null != kafka) {
//      kafka.close();
    }
  }
}
