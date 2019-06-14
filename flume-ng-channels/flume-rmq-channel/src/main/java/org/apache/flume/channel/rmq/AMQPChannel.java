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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.rabbitmq.client.MessageProperties.PERSISTENT_TEXT_PLAIN;
import static java.util.Collections.EMPTY_MAP;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.amqp.AMQPChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQPChannel extends BasicChannelSemantics implements AMQPChannelConfiguration {

  private String queue;
  private String host;
  private String userName;
  private String password;
  private Integer port;
  private Integer queueSize;

  private Long waitTimeMillis;
  private Integer prefetchCount = 10;
  private Integer consumerCount = 1;
  private Integer inMemoryMaxRetries;
  private Boolean isDeadLetterEnabled = Boolean.FALSE;

  private String xchange = null;
  private String xchangeRouting = null;

  private Map<String, Channel> channels;
  private Map<String, Connection> connections;

  private static final Logger logger = LoggerFactory.getLogger(AMQPChannel.class);

  private AMQPChannelCounter counter;
  private BlockingQueue<Event> recordIterator = null;
  private final List<Event> failedEvents = Collections.synchronizedList(new ArrayList<>());

  @Override
  public void configure(Context ctx) {
    logger.info("Configuring AMQP Channel: {} with {}", getName(), ctx);

    //initializing queue configurations
    queue = ctx.getString(QUEUE_CONFIG);
    host = ctx.getString(HOST_CONFIG);
    port = ctx.getInteger(PORT_CONFIG);
    userName = ctx.getString(USERNAME);
    password = ctx.getString(PASSORD);
    queueSize = ctx.getInteger(INFLIGHT_COUNT, 100);

    //initialising prefetch count, consumer count and wait time in milliseconds
    prefetchCount = ctx.getInteger(PREFETCH_COUNT, 25);
    consumerCount = ctx.getInteger(CONSUMERS_COUNT, 1);
    waitTimeMillis = ctx.getLong(WAIT_TIME_IN_MILLIS, 100L);
    inMemoryMaxRetries = ctx.getInteger(IN_MEMORY_MAX_RETRY_COUNT, 5);
    isDeadLetterEnabled = ctx.getBoolean(IS_DEAD_LETTERED_ENABLED, Boolean.FALSE);

    xchange = ctx.getString(XCHANGE_CONFIG, null);
    xchangeRouting = ctx.getString(XCHANGE_ROUTING_CONFIG, "");

    //initialising channel attributes counter
    if (counter == null) {
      counter = new AMQPChannelCounter(getName());
    }

    //initializing channels, consumers and recordIterator
    channels = new HashMap<>(consumerCount);
    connections = new HashMap<>(consumerCount);
    recordIterator = new ArrayBlockingQueue<>(queueSize);

    logger.info("configured AMQP sink {} => {}", getName(), toString());
  }

  @Override
  public void start() {
    try {
      logger.debug("Starting AMQP Channel: {}", getName());
      ConnectionFactory connectionFactory = new ConnectionFactory();
      connectionFactory.setHost(host);
      if (null != port) {
        connectionFactory.setPort(port);
      }
      if (null != userName) {
        connectionFactory.setUsername(userName);
      }
      if (null != password) {
        connectionFactory.setPassword(password);
      }

      //Initialising channels for consumer
      for (int i = 1; i <= consumerCount; i++) {
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.basicQos(prefetchCount);
        channels.put(String.valueOf(i), channel);
        connections.put(String.valueOf(i), connection);
        createConsumer(String.valueOf(i), channel);
      }

      counter.start();
      super.start();
    } catch (Throwable t) {
      counter.addToChannelErrorCount(1);
      logger.error("Error creating amqp connection", t);
      throw new FlumeException("Error creating amqp connection", t);
    }
  }

  @Override
  public void stop() {
    logger.info("Closing AMQP Channel: {}", getName());
    try {
      for (Channel channel : channels.values()) {
        channel.close();
      }
      for (Map.Entry<String, Connection> next : connections.entrySet()) {
        next.getValue().close();
      }
      counter.stop();
      super.stop();
    } catch (Throwable throwable) {
      logger.error("Error closing amqp connection", throwable);
    }
  }

  private synchronized void createConsumer(String channelId, Channel channel) {
    new Consumer(channelId, channel);
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new AMQPTransaction();
  }

  private enum TransactionType {
    PUT,
    TAKE,
    NONE
  }

  private class AMQPTransaction extends BasicTransactionSemantics {

    private TransactionType type = TransactionType.NONE;
    private LinkedList<Event> inflightEvents = new LinkedList<>();

    @Override
    protected void doPut(Event event) {
      throw new FlumeException("AMQPChannel#TransactionType#doPut not implemented");
    }

    @Override
    protected Event doTake() {
      logger.debug("taking event from amqp {}", getName());
      type = TransactionType.TAKE;
      long startTime = System.nanoTime();
      Event event = null;
      try {
        event = poll(waitTimeMillis);
      } catch (InterruptedException ex) {
        logger.warn("InterruptedException while getting inflight events from amqp buffer {}", getName(), ex);
      } catch (Throwable ex) {
        counter.addToChannelErrorCount(1);
        logger.warn("Error while getting inflight events from amqp buffer {}", getName(), ex);
        throw new ChannelException("Error while getting inflight events from amqp buffer", ex);
      } finally {
        long endTime = System.nanoTime();
        counter.addToEventGetTimer((endTime - startTime) / (1000 * 1000));
      }
      if (null == event) {
        logger.warn("retrieved an empty event from source {}", getName());
        return null;
      }
      inflightEvents.add(event);
      return event;
    }

    @Override
    public void begin() {
      super.begin();
    }

    @Override
    protected void doCommit() {
      logger.debug("Starting amqp commit {}", getName());
      long startTime = System.currentTimeMillis();
      if (type.equals(TransactionType.NONE)) {
        return;
      } else if (type.equals(TransactionType.PUT)) {
        return;
      }
      commitEvents(inflightEvents);
      counter.addToEventTakeSuccessCount(inflightEvents.size());
      inflightEvents.clear();
      counter.addToCommitTimer(System.currentTimeMillis() - startTime);
    }

    @Override
    //We are doing rollback of entire inflight events.....
    protected void doRollback() {
      logger.debug("Rolling back amqp commit {}", getName());
      long startTime = System.currentTimeMillis();
      if (type.equals(TransactionType.NONE)) {
        return;
      } else if (type.equals(TransactionType.PUT)) {
        return;
      }
      try {
        rollbackEvents(inflightEvents);
      } catch (Throwable t) {
        counter.addToChannelErrorCount(1);
        logger.error("Error occurred while rolling back events to the channel", t);
        return;
      }
      counter.addToRollbackCount(inflightEvents.size());
      inflightEvents.clear();
      counter.addToRollbackTimer(System.currentTimeMillis() - startTime);
    }

    @Override
    public void addToFailedEvents(List<Event> events) {
      failedEvents.addAll(events);
    }
  }

  private Event poll(Long waitTimeInMillis) throws InterruptedException {
    Event poll = null;
    if (!failedEvents.isEmpty()) {
      logger.info("Getting from failed events");
      poll = failedEvents.remove(0);
    }
    if (null == poll) {
      logger.debug("Getting from record iterator");
      poll = recordIterator.poll(waitTimeInMillis, TimeUnit.MILLISECONDS);
    }
    return poll;
  }

  private void commitEvents(LinkedList<Event> events) {
    for (Event event : events) {
      if (null == event) {
        continue;
      }
      long b4 = System.currentTimeMillis();
      String channelId = event.getHeaders().get(CHANNEL_ID);
      Long eventOffsetId = Long.parseLong(event.getHeaders().get(OFFSET_ID));
      try {
        channels.get(channelId).basicAck(eventOffsetId,false);
        logger.debug("committing offset for {} with {} in {}",
                channelId, eventOffsetId, (System.currentTimeMillis() - b4));
      } catch (Throwable io) {
        counter.addToChannelErrorCount(1);
        logger.error("Error acking event {} => {} ,{}", getName(), channelId, eventOffsetId, io);
      }
    }
  }

  private void rollbackEvents(LinkedList<Event> events) throws IOException {
    if (events == null || events.size() <= 0) {
      return;
    }
    logger.error("rolling back ", events.size(), " events");
    for (Event event : events) {
      if (null == event) {
        continue;
      }
      int count = Integer.valueOf(event.getHeaders().get(IN_MEMORY_RETRY_COUNT));
      if(Boolean.valueOf(event.getHeaders().get(IS_PROCESSED_SUCCESSFULLY))) {
        logger.debug("Acking this event in the rolled back events as this event is processed successfully. ",
                new String(event.getBody()));
        channels.get(event.getHeaders().get(CHANNEL_ID))
                .basicAck(Long.valueOf(event.getHeaders().get(OFFSET_ID)), false);
        continue;
      }
      if (count < inMemoryMaxRetries) {
        count++;
        event.getHeaders().put(IN_MEMORY_RETRY_COUNT, String.valueOf(count));
        failedEvents.add(event);
        counter.incrementRetryCount();
        logger.warn("retrying message {}", new String(event.getBody()));
      } else if (isDeadLetterEnabled && !Boolean.valueOf(event.getHeaders().get(IS_DEAD_LETTERED))) {
        counter.incrementDeadLetterCount();
        logger.warn("rejecting message after retry limits {}", new String(event.getBody()));
        channels.get(event.getHeaders().get(CHANNEL_ID))
            .basicReject(Long.valueOf(event.getHeaders().get(OFFSET_ID)), false);
      } else {
        counter.incrementDroppedCount();
        logger.warn("dropping message after retry limits {}", new String(event.getBody()));
        channels.get(event.getHeaders().get(CHANNEL_ID))
            .basicAck(Long.valueOf(event.getHeaders().get(OFFSET_ID)), false);
      }
    }
  }

  private class Producer {
    private String channelId;
    private Channel channel;

    public Producer(String channelId, Channel channel) {
      this.channelId = channelId;
      this.channel = channel;
      logger.info("amqp producer created {}-{}", channelId, queue);
    }

    protected void publish(Event event) {
      try {
        channel.basicPublish(xchange, xchangeRouting, true, true, PERSISTENT_TEXT_PLAIN, event.getBody());
      } catch (Throwable e) {
        counter.addToChannelErrorCount(1);
        logger.error("Producer creation error for channel {}.", getName(), e);
      }
    }

    protected void close() throws IOException, TimeoutException {
      channel.close();
      logger.info("amqp producer closed {}-{}", channelId, queue);
    }
  }

  private class Consumer {
    Consumer(String channelId, Channel channel) {
      logger.info("amqp consumer created {}-{}", channelId, queue);
      try {
        channel.basicConsume(queue, false, new DefaultConsumer(channel) {
          @Override
          public void handleDelivery(String consumerTag, Envelope envelope,
                                     AMQP.BasicProperties properties, byte[] body) {
            String message = new String(body, StandardCharsets.UTF_8);
            logger.debug(" {} Received by {}", envelope.getDeliveryTag(), channelId);
            try {
              counter.incrementEventTakeAttemptCount();
              Event event = EventBuilder.withBody(message.getBytes(), EMPTY_MAP);
              event.getHeaders().put(CHANNEL_ID, channelId);
              event.getHeaders().put(IN_MEMORY_RETRY_COUNT, String.valueOf(0));
              event.getHeaders().put(OFFSET_ID, String.valueOf(envelope.getDeliveryTag()));
              event.getHeaders().put(IS_PROCESSED_SUCCESSFULLY, String.valueOf(Boolean.FALSE));
              if (isDeadLetterEnabled) {
                if (properties.getHeaders() == null || properties.getHeaders().get("x-death") == null) {
                  event.getHeaders().put(IS_DEAD_LETTERED, String.valueOf(Boolean.FALSE));
                } else {
                  event.getHeaders().put(IS_DEAD_LETTERED, String.valueOf(Boolean.TRUE));
                }
              }
              recordIterator.put(event);
              counter.increamentReadCount();
            } catch (InterruptedException e) {
              logger.error("amqp interrupted", e);
            }
          }
        });
      } catch (Throwable e) {
        counter.addToChannelErrorCount(1);
        logger.error("Consumer creation error for channel {}.", getName(), e);
      }
    }
  }

  @Override
  public String toString() {
    return "AMQPChannel{" +
        "queue='" + queue + '\'' +
        "xchange='" + xchange + '\'' +
        ", host='" + host + '\'' +
        ", userName='" + userName + '\'' +
        ", password='" + password + '\'' +
        ", port=" + port +
        ", queueSize=" + queueSize +
        ", waitTimeMillis=" + waitTimeMillis +
        ", prefetchCount=" + prefetchCount +
        ", consumerCount=" + consumerCount +
        ", inMemoryMaxRetries=" + inMemoryMaxRetries +
        ", isDeadLetterEnabled=" + isDeadLetterEnabled +
        '}';
  }
}
//
//  private void commitEventsByBatch(LinkedList<Event> events) {
//    Map<String, Long> offsetMap = new HashMap<>();
//    for (Event event : events) {
//      if (null == event) {
//        continue;
//      }
//      try {
//        String channelId = event.getHeaders().get(CHANNEL_ID);
//        Long eventOffsetId = Long.parseLong(event.getHeaders().get(OFFSET_ID));
//        Long oldOffsetId = offsetMap.computeIfAbsent(channelId, key -> eventOffsetId);
//        if (eventOffsetId > oldOffsetId) {
//          offsetMap.put(channelId, eventOffsetId);
//        }
//      } catch (Throwable t) {
//        logger.warn("Error acking event {} => {}", getName(), event, t);
//      }
//    }
//    for (Map.Entry<String, Long> next : offsetMap.entrySet()) {
//      try {
//        long b4 = System.currentTimeMillis();
//        channels.get(next.getKey()).basicAck(next.getValue(), true);
//        logger.debug("committing offset for {} with {} in {}",
//                next.getKey(), next.getValue(), (System.currentTimeMillis() - b4));
//      } catch (Throwable io) {
//        counter.addToChannelErrorCount(1);
//        logger.error("Error acking event {} => {} ,{}", getName(), next.getKey(), next.getValue(), io);
//      }
//    }
//  }

