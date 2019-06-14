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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.arivera.oss.embedded.rabbitmq.EmbeddedRabbitMq;
import io.arivera.oss.embedded.rabbitmq.EmbeddedRabbitMqConfig;
import io.arivera.oss.embedded.rabbitmq.OfficialArtifactRepository;
import io.arivera.oss.embedded.rabbitmq.PredefinedVersion;
import io.arivera.oss.embedded.rabbitmq.RabbitMqEnvVar;
import io.arivera.oss.embedded.rabbitmq.bin.RabbitMqPlugins;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Accessors(fluent = true)
public class EmbeddedRMQ implements Service {

  private int port;
  private EmbeddedRabbitMq rabbitMq;
  private EmbeddedRabbitMqConfig config;
  private TemporaryFolder temporaryFolder;

  public EmbeddedRMQ() throws IOException {

    port = TestUtils.generateRandomPort();
    temporaryFolder = new TemporaryFolder();
    File configFile = temporaryFolder.newFile("rabbitmq.config");
    FileOutputStream fileOutputStream = new FileOutputStream(configFile);
    fileOutputStream.write("[{rabbit,[{log_levels,[{connection,debug},{channel,debug}]}]}].".getBytes("utf-8"));
    fileOutputStream.close();
    config = new EmbeddedRabbitMqConfig.Builder()
        .version(PredefinedVersion.V3_6_9)
//        .port(port)
        .downloadFrom(OfficialArtifactRepository.RABBITMQ)
        .envVar(RabbitMqEnvVar.CONFIG_FILE, configFile.toString().replace(".config", ""))
        .extractionFolder(temporaryFolder.newFolder("extracted"))
        .rabbitMqServerInitializationTimeoutInMillis(TimeUnit.SECONDS.toMillis(120))
        .defaultRabbitMqCtlTimeoutInMillis(TimeUnit.SECONDS.toMillis(20))
        .erlangCheckTimeoutInMillis(TimeUnit.SECONDS.toMillis(10))
        .useCachedDownload(true)
        .build();
    rabbitMq = new EmbeddedRabbitMq(config);
  }

  @Override
  public String name() {
    return "EmbeddedRMQ";
  }

  @Override
  public Service start() throws Exception {
    rabbitMq.start();
    RabbitMqPlugins rabbitMqPlugins = new RabbitMqPlugins(config);
    rabbitMqPlugins.enable("rabbitmq_management");
    rabbitMqPlugins.enable("amqp_client");
    rabbitMqPlugins.enable("cowboy");
//    rabbitMqPlugins.enable("rabbitmq_auth");
//    rabbitMqPlugins.enable("rabbitmq_consistent");
//    rabbitMqPlugins.enable("rabbitmq_event");
//    rabbitMqPlugins.enable("rabbitmq_federation");
//    rabbitMqPlugins.enable("rabbitmq_jms");
//    rabbitMqPlugins.enable("rabbitmq_recent");
//    rabbitMqPlugins.enable("rabbitmq_shovel");
//    rabbitMqPlugins.enable("rabbitmq_stomp");
//    rabbitMqPlugins.enable("rabbitmq_top");
//    rabbitMqPlugins.enable("rabbitmq_tracing");
//    rabbitMqPlugins.enable("rabbitmq_trust");
//    rabbitMqPlugins.enable("rabbitmq_web");
//    rabbitMqPlugins.enable("ranch");
//    rabbitMqPlugins.enable("sockjs");
    return this;
  }

  @Override
  public int[] port() {
    return new int[]{port};
  }

  @Override
  public void close() throws Exception {
    rabbitMq.stop();
    temporaryFolder.delete();
  }

  public EmbeddedRabbitMq getRabbitMq() {
    return rabbitMq;
  }

  public EmbeddedRabbitMqConfig getConfig() {
    return config;
  }
}
