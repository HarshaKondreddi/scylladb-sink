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

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicies;
import net.jodah.lyra.config.RetryPolicy;
import net.jodah.lyra.util.Duration;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.slf4j.Logger;

public class Consumer extends AbstractWorker {

  private static final String COUNTER_ACK = "rabbitmq.ack";
  private static final String COUNTER_EXCEPTION = "rabbitmq.exception";
  private static final String COUNTER_REJECT = "rabbitmq.reject";

  private Connection connection;
  private Channel channel;
  private ChannelProcessor channelProcessor;
  private CounterGroup counterGroup;
  private SourceCounter sourceCounter;

  private String hostname;
  private int port;
  private boolean sslEnabled = false;
  private String virtualHost = "/";
  private String username;
  private String password;
  private String queue;
  private boolean autoAck = false;
  private boolean requeuing = false;
  private boolean durable = false;
  private int prefetchCount = 0;
  private int timeout = -1;

  static AtomicLong counter = new AtomicLong(0);
  private CountDownLatch waiter = new CountDownLatch(1);

  public Consumer(String name, Logger logger) {
    super(name, logger);
  }

  public Consumer setHostname(String hostname) {
    this.hostname = hostname;
    return this;
  }

  public Consumer setPort(int port) {
    this.port = port;
    return this;
  }

  public Consumer setSSLEnabled(boolean sslEnabled) {
    this.sslEnabled = sslEnabled;
    return this;
  }

  public Consumer setDurable(boolean durable) {
    this.durable = durable;
    return this;
  }

  public Consumer setChannelProcessor(ChannelProcessor channelProcessor) {
    this.channelProcessor = channelProcessor;
    return this;
  }

  public Consumer setCounterGroup(CounterGroup counterGroup) {
    this.counterGroup = counterGroup;
    return this;
  }

  public Consumer setSourceCounter(SourceCounter sourceCounter) {
    this.sourceCounter = sourceCounter;
    return this;
  }

  public Consumer setVirtualHost(String virtualHost) {
    this.virtualHost = virtualHost;
    return this;
  }

  public Consumer setUsername(String username) {
    this.username = username;
    return this;
  }

  public Consumer setPassword(String password) {
    this.password = password;
    return this;
  }

  public Consumer setQueue(String queue) {
    this.queue = queue;
    return this;
  }

  public Consumer setAutoAck(boolean autoAck) {
    this.autoAck = autoAck;
    return this;
  }

  public Consumer setRequeing(boolean requeuing) {
    this.requeuing = requeuing;
    return this;
  }

  public Consumer setPrefetchCount(int prefetchCount) {
    this.prefetchCount = prefetchCount;
    return this;
  }

  public Consumer setTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  @Override
  public void run() {
    DefaultConsumer consumer;
    Config config = new Config();
    // Connect to RabbitMQ
    try {
      connection = createAMQPConnection(config);
    } catch (IOException ex) {
      logger.error("Error creating RabbitMQ connection: {}", ex);
      return;
    }

    // Keep track of how many connections were opened
    sourceCounter.setOpenConnectionCount(sourceCounter.getOpenConnectionCount() + 1);
    // Open the channel
    try {
      channel = connection.createChannel();
//      channel.queueDeclare(queue, durable, false, false, null);
    } catch (IOException ex) {
      logger.error("Error creating RabbitMQ channel: {}", ex);
      return;
    }

    // Set QoS Prefetching if enabled, exiting if it fails
    if (prefetchCount > 0) {
      if (!setQoS()) {
        this.close();
        return;
      }
    }
    // Create the new consumer and set the consumer tag
    consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag,
                                 Envelope envelope,
                                 AMQP.BasicProperties properties,
                                 byte[] body) throws IOException {
        sourceCounter.incrementEventReceivedCount();
        try {
          channelProcessor.processEvent(parseMessage(envelope, properties, body));
          sourceCounter.incrementEventAcceptedCount();
          ackMessage(envelope.getDeliveryTag());
        } catch (Exception ex) {
          logger.error("Error writing to channel for {}, message rejected {}", this, ex);
          rejectMessage(envelope.getDeliveryTag());
        }
      }
    };
    try {
      channel.basicConsume(queue, autoAck, "flumeConsumer", consumer);
    } catch (Exception ex) {
      logger.error("Error starting consumer: {}", ex);
      counterGroup.incrementAndGet(COUNTER_EXCEPTION);
      this.close();
    }
    try {
      waiter.await();
    } catch (InterruptedException ex) {
      //no op
    } catch (Exception ex) {
      logger.error("Stopping consumer: {}", ex);
    }
  }

  public void doStop() {
    super.doStop();
    waiter.countDown();
    // Tell RabbitMQ that the consumer is stopping
    cancelConsumer("flumeConsumer");
    // Cancel consumer
    this.close();
  }

  private void cancelConsumer(String consumerTag) {
    try {
      channel.basicCancel(consumerTag);
    } catch (IOException ex) {
      logger.error("Error cancelling consumer for {}: {}", this, ex);
      counterGroup.incrementAndGet(COUNTER_EXCEPTION);
    }
  }

  private void ackMessage(long deliveryTag) {
    try {
      channel.basicAck(deliveryTag, false);
    } catch (IOException ex) {
      logger.error("Error acknowledging message from {}: {}", this, ex);
      counterGroup.incrementAndGet(COUNTER_EXCEPTION);
    }
    counterGroup.incrementAndGet(COUNTER_ACK);
  }

  private void rejectMessage(long deliveryTag) {
    try {
      channel.basicReject(deliveryTag, requeuing);
    } catch (IOException ex) {
      logger.error("Error rejecting message from {}: {}", this, ex);
      counterGroup.incrementAndGet(COUNTER_EXCEPTION);
    }
    counterGroup.incrementAndGet(COUNTER_REJECT);
  }

  private Event parseMessage(Envelope envelope, AMQP.BasicProperties props, byte[] body) {
    // Create the event passing in the body
    Event event = EventBuilder.withBody(body);

    // Get the headers from properties, exchange, and routing-key
    Map<String, String> headers = buildHeaders(props);

    String exchange = envelope.getExchange();
    if (exchange != null && !exchange.isEmpty()) {
      headers.put("exchange", exchange);
    }

    String routingKey = envelope.getRoutingKey();
    if (routingKey != null && !routingKey.isEmpty()) {
      headers.put("routing-key", routingKey);
    }

    event.setHeaders(headers);
    return event;
  }

  private Map<String, String> buildHeaders(AMQP.BasicProperties props) {
    Map<String, String> headers = new HashMap<>();

    String appId = props.getAppId();
    String contentEncoding = props.getContentEncoding();
    String contentType = props.getContentType();
    String correlationId = props.getCorrelationId();
    Integer deliveryMode = props.getDeliveryMode();
    String expiration = props.getExpiration();
    String messageId = props.getMessageId();
    Integer priority = props.getPriority();
    String replyTo = props.getReplyTo();
    Date timestamp = props.getTimestamp();
    String type = props.getType();
    String userId = props.getUserId();

    if (appId != null && !appId.isEmpty()) {
      headers.put("app-id", appId);
    }
    if (contentEncoding != null && !contentEncoding.isEmpty()) {
      headers.put("content-encoding", contentEncoding);
    }
    if (contentType != null && !contentType.isEmpty()) {
      headers.put("content-type", contentType);
    }
    if (correlationId != null && !correlationId.isEmpty()) {
      headers.put("correlation-id", correlationId);
    }
    if (deliveryMode != null) {
      headers.put("delivery-mode", String.valueOf(deliveryMode));
    }
    if (expiration != null && !expiration.isEmpty()) {
      headers.put("expiration", expiration);
    }
    if (messageId != null && !messageId.isEmpty()) {
      headers.put("message-id", messageId);
    }
    if (priority != null) {
      headers.put("priority", String.valueOf(priority));
    }
    if (replyTo != null && !replyTo.isEmpty()) {
      headers.put("replyTo", replyTo);
    }
    if (timestamp != null) {
      headers.put("timestamp", String.valueOf(timestamp.getTime()));
    }
    if (type != null && !type.isEmpty()) {
      headers.put("type", type);
    }
    if (userId != null && !userId.isEmpty()) {
      headers.put("user-id", userId);
    }

    Map<String, Object> userHeaders = props.getHeaders();

    if (userHeaders != null && userHeaders.size() > 0) {
      for (String key : userHeaders.keySet()) {
        Object value = userHeaders.get(key);
        if (value != null) {
          headers.put(key, userHeaders.get(key).toString());
        } else {
          // Keep the header just in case has to be used as a flag.
          headers.put(key, "");
        }
      }
    }

    return headers;
  }

  private boolean setQoS() {
    try {
      channel.basicQos(prefetchCount);
    } catch (IOException ex) {
      logger.error("Error setting QoS prefetching: {}", ex);
      return false;
    }
    return true;
  }

  private void close() {
    try {
      channel.close();
      connection.close();
    } catch (Exception ex) {
      logger.error("Error cleanly closing RabbitMQ connection: {}", ex);
    }
  }

  private Connection createAMQPConnection(Config config) throws IOException {
    logger.debug("Connecting to RabbitMQ from {}", this);
    config = config.withRecoveryPolicy(RecoveryPolicies.recoverAlways())
        .withRetryPolicy(new RetryPolicy()
            .withMaxAttempts(200)
            .withInterval(Duration.seconds(1))
            .withMaxDuration(Duration.minutes(5)));

    ConnectionOptions options = new ConnectionOptions()
        .withHost(hostname)
        .withPort(port)
        .withVirtualHost(virtualHost)
        .withUsername(username)
        .withPassword(password);
    if (sslEnabled) {
      try {
        options = options.withSsl();
      } catch (NoSuchAlgorithmException | KeyManagementException e) {
        logger.error("Could not enable SSL: {}", e);
      }
    }
    try {
      return Connections.create(options, config);
    } catch (java.util.concurrent.TimeoutException e) {
      logger.error("Timeout connecting to RabbitMQ: {}", e);
      throw new IOException();
    }
  }

  @Override
  public void configure(Context context) {

  }
}
