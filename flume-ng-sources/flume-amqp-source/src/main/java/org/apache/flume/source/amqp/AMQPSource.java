/*
  * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.amqp;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQPSource extends AbstractSource implements Configurable, EventDrivenSource {

  private static final Logger logger = LoggerFactory.getLogger(AMQPSource.class);
  private static final String HOST_KEY = "host";
  private static final String PORT_KEY = "port";
  private static final String SSL_KEY = "ssl";
  private static final String VHOST_KEY = "virtual-host";
  private static final String USER_KEY = "username";
  private static final String PASSWORD_KEY = "password";
  private static final String QUEUE_KEY = "queue";
  private static final String AUTOACK_KEY = "auto-ack";
  private static final String PREFETCH_COUNT_KEY = "prefetch-count";
  private static final String TIMEOUT_KEY = "timeout";
  private static final String THREAD_COUNT_KEY = "threads";
  private static final String REQUEUING = "requeuing";
  private static final String DURABLE = "durable";
  private SourceCounter sourceCounter;
  private ConnectionFactory factory;
  private CounterGroup counterGroup;
  private String hostname;
  private int port;
  private boolean enableSSL = false;
  private String virtualHost;
  private String username;
  private String password;
  private String queue;
  private boolean autoAck = false;
  private boolean requeuing = false;
  private boolean durable = false;
  private int prefetchCount = 0;
  private int timeout = -1;
  private int consumerThreads = 1;

  private List<Consumer> consumers;

  public AMQPSource() {
    this(new ConnectionFactory());
  }

  public AMQPSource(ConnectionFactory factory) {
    consumers = new LinkedList<>();
    this.factory = factory;
  }

  @Override
  public void configure(Context context) {
    // Only the queue name does not have a default value
    Configurables.ensureRequiredNonNull(context, QUEUE_KEY);

    // Assign all of the configured values
    hostname = context.getString(HOST_KEY, ConnectionFactory.DEFAULT_HOST);
    port = context.getInteger(PORT_KEY, ConnectionFactory.DEFAULT_AMQP_PORT);
    enableSSL = context.getBoolean(SSL_KEY, false);
    virtualHost = context.getString(VHOST_KEY, ConnectionFactory.DEFAULT_VHOST);
    username = context.getString(USER_KEY, ConnectionFactory.DEFAULT_USER);
    password = context.getString(PASSWORD_KEY, ConnectionFactory.DEFAULT_PASS);
    queue = context.getString(QUEUE_KEY, null);
    autoAck = context.getBoolean(AUTOACK_KEY, false);
    requeuing = context.getBoolean(REQUEUING, false);
    durable = context.getBoolean(DURABLE, false);
    prefetchCount = context.getInteger(PREFETCH_COUNT_KEY, 0);
    timeout = context.getInteger(TIMEOUT_KEY, -1);
    consumerThreads = context.getInteger(THREAD_COUNT_KEY, 1);

    // Ensure that Flume can connect to RabbitMQ
    testRabbitMQConnection();

    // Create and configure the counters
    sourceCounter = new SourceCounter(getName());
    counterGroup = new CounterGroup();
    counterGroup.setName(getName());
  }

  @Override
  public synchronized void start() {
    logger.info("Starting {} with {} thread(s)", getName(), consumerThreads);
    sourceCounter.start();
    for (int i = 0; i < consumerThreads; i++) {
      Consumer consumer = new Consumer("RabbitMQ Consumer #" + String.valueOf(i), logger)
          .setHostname(hostname)
          .setPort(port)
          .setSSLEnabled(enableSSL)
          .setVirtualHost(virtualHost)
          .setUsername(username)
          .setPassword(password)
          .setQueue(queue)
          .setPrefetchCount(prefetchCount)
          .setTimeout(timeout)
          .setAutoAck(autoAck)
          .setRequeing(requeuing)
          .setDurable(durable)
          .setChannelProcessor(getChannelProcessor())
          .setSourceCounter(sourceCounter)
          .setCounterGroup(counterGroup);
      consumer.doStart();
      consumers.add(consumer);
    }
    super.start();
  }

  @Override
  public synchronized void stop() {
    logger.info("Stopping {}", getName());
    for (int i = 0; i < consumerThreads; i++) {
      consumers.get(i).doStop();
    }
    for (int i = 0; i < consumerThreads; i++) {
      consumers.get(i).awaitTermination();
    }
    sourceCounter.stop();
    super.stop();
  }

  private void testRabbitMQConnection() {
    Connection connection;

    factory.setPort(port);
    factory.setHost(hostname);
    factory.setVirtualHost(virtualHost);
    factory.setUsername(username);
    factory.setPassword(password);
    if (enableSSL) {
      try {
        factory.useSslProtocol();
      } catch (NoSuchAlgorithmException | KeyManagementException ex) {
        throw new IllegalArgumentException("Could not Enable SSL: ", ex);
      }
    }
    try {
      connection = factory.newConnection();
      connection.close();
    } catch (Exception ex) {
      throw new IllegalArgumentException("Could not connect to RabbitMQ: ", ex);
    }
  }
}
