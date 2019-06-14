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

import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

@Slf4j
@Setter
@Getter
@Accessors(fluent = true)
public class TopicHelper {

  private String zkServerString;

  public TopicHelper(String zkServerString) {
    this.zkServerString = zkServerString;
  }

  public void createTopic(String name, Integer partitions, Integer replicationFactor) {
    ZkClient zkClient = new ZkClient(zkServerString, Integer.MAX_VALUE, 10000, new EmbeddedKafka.EmbedZkSerializer());
    ZkConnection zkConnection = new ZkConnection(zkServerString, 10000);
    ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
    if (!AdminUtils.topicExists(zkUtils, name)) {
      AdminUtils.createTopic(zkUtils, name, partitions, replicationFactor, new Properties(),
          new RackAwareMode() {
            @Override
            public int hashCode() {
              return super.hashCode();
            }
          });
    }
    zkClient.close();
  }

  public boolean isTopicExists(String name) {
    ZkClient zkClient = new ZkClient(zkServerString, Integer.MAX_VALUE, 10000, new EmbeddedKafka.EmbedZkSerializer());
    ZkConnection zkConnection = new ZkConnection(zkServerString, 10000);
    ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
    return AdminUtils.topicExists(zkUtils, name);
  }
}
