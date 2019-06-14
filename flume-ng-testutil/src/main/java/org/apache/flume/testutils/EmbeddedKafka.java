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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.cluster.Broker;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import scala.collection.Seq;

@Slf4j
@Data
public class EmbeddedKafka implements Service {

  public static final ThreadLocal<Boolean> IS_ZK_DIRECT_BYTES = ThreadLocal.withInitial(() -> false);

  @Accessors(fluent = true)
  final int zkPort;
  @Accessors(fluent = true)
  final int[] kafkaPort;
  @Accessors(fluent = true)
  final String path;
  private final EmbedZkSerializer zkSerializer = new EmbedZkSerializer();
  private TemporaryFolder temporaryFolder;
  private List<KafkaServerStartable> kafkaServers = new ArrayList<>();

  public EmbeddedKafka(final int zkPort, final int[] kafkaPort) throws IOException {
    this.zkPort = zkPort;
    this.kafkaPort = kafkaPort.clone();
    this.temporaryFolder = new TemporaryFolder();
    this.path = temporaryFolder.newFolder("kafka").getAbsolutePath();
  }

  @Override
  public String name() {
    return "EmbeddedKafka";
  }

  /**
   * Start the embed kafka cluster with the giver ports
   */
  @Override
  public Service start() throws Exception {
    for (int i = 0; i < kafkaPort.length; i++) {
      KafkaServerStartable kafkaServer = new KafkaServerStartable(new KafkaConfig(
          createBrokerConfig(zkPort, kafkaPort[i], i, kafkaPort.length,
              path)));
      kafkaServer.startup();
      waitForKafkaServerup(zkPort, kafkaPort.length, TimeUnit.SECONDS.toSeconds(10));
      kafkaServers.add(kafkaServer);
    }
    return this;
  }

  @Override
  public int[] port() {
    return kafkaPort;
  }

  /**
   * Shutdown the kafka cluster
   */
  @Override
  public void close() throws Exception {
    if (null != kafkaServers) {
      kafkaServers.forEach(KafkaServerStartable::shutdown);
    }
    temporaryFolder.delete();
  }

  public int[] kafkaPort() {
    return Arrays.copyOf(kafkaPort, kafkaPort.length);
  }

  private Properties createBrokerConfig(int zkPort, int kafkaPort, int kafkaId, int defPartitions, String path) {
    try {
      Properties props = new Properties();
      props.setProperty("host.name", InetAddress.getLoopbackAddress().getHostAddress());
      props.setProperty("log.dir", path + "/" + kafkaId);
      props.setProperty("port", String.valueOf(kafkaPort));
      props.setProperty("broker.id", String.valueOf(kafkaId));
      props.setProperty("zookeeper.connect", InetAddress.getLoopbackAddress().getHostAddress() + ":" + zkPort);
      props.setProperty("zookeeper.connection.timeout.ms", "1000000");
      props.setProperty("controlled.shutdown.enable", Boolean.TRUE.toString());
      props.setProperty("num.partitions", String.valueOf(defPartitions));
      props.setProperty("delete.topic.enable", "true");
      props.setProperty("auto.create.topics.enable", "true");
      props.setProperty("log.segment.bytes", "10240000");
      props.setProperty("offsets.topic.replication.factor", String.valueOf(defPartitions));
      return props;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean waitForKafkaServerup(int zkPort, int clusterSize, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {

      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        log.error("Error sleeping", e);
      }

      try {
        if (clusterSize == getClusterSize(InetAddress.getLoopbackAddress().getHostAddress() + ":" + zkPort)) {
          return true;
        }
      } catch (Exception e) {
        log.error("Error getClusterSize", e);
      }

      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
    }
    return false;
  }

  private int getClusterSize(String zkString) {
    ZkClient zkClient = new ZkClient(zkString, 30000, 60000, zkSerializer);
    ZkUtils zkUtils = new ZkUtils(zkClient,
        new ZkConnection(InetAddress.getLoopbackAddress().getHostAddress() + ":" + zkPort), false);
    Seq<Broker> allBrokersInCluster = zkUtils.getAllBrokersInCluster();
    zkClient.close();
    return allBrokersInCluster.size();
  }

  public static class EmbedZkSerializer implements ZkSerializer {
    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
      return ZKStringSerializer.serialize(data);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
      if (IS_ZK_DIRECT_BYTES.get()) {
        return bytes;
      } else {
        return ZKStringSerializer.deserialize(bytes);
      }
    }
  }
}
