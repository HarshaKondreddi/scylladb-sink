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
package org.apache.flume.sink.redis;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flume.sink.redis.RedisSinkConfigurationConstant.HOST;
import static org.apache.flume.sink.redis.RedisSinkConfigurationConstant.IS_PROCESSED_SUCCESSFULLY;
import static org.apache.flume.sink.redis.RedisSinkConfigurationConstant.POISONED_EVENT;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.instrumentation.redis.RedisSinkCounter;
import org.apache.flume.processor.SimpleEventProcessor;
import org.apache.flume.serialization.SimpleEventDeserializer;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.redis.connection.JedisPoolFactory;
import org.apache.flume.sink.redis.connection.JedisPoolFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(RedisSink.class);

  /**
   * Configuration attributes
   */
  private String host = null;
  private Integer port = null;
  private Integer timeout = null;
  private String password = null;
  private Integer database = null;
  private Integer batchSize = null;
  private SimpleEventDeserializer<SimpleEvent> deserializer = null;
  private SimpleEventProcessor<Jedis, SimpleEvent> eventProcessor = null;

  private RedisSinkCounter redisSinkCounter;

  private final JedisPoolFactory jedisPoolFactory;
  private JedisPool jedisPool = null;

  public RedisSink() {
    jedisPoolFactory = new JedisPoolFactoryImpl();
  }

  @Override
  public synchronized void start() {
    logger.info("Starting Redis Sink");
    if (jedisPool != null) {
      jedisPool.destroy();
    }
    jedisPool = jedisPoolFactory.create(new JedisPoolConfig(), host, port, timeout, password, database);
    redisSinkCounter.start();
    super.start();
  }

  @Override
  public synchronized void stop() {
    logger.info("Stopping Redis Sink");
    if (jedisPool != null) {
      jedisPool.destroy();
    }
    super.stop();
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;

    if (jedisPool == null) {
      throw new EventDeliveryException("Redis connection not established. Please verify your configuration");
    }

    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();
    txn.begin();

    long b4 = System.currentTimeMillis();
    List<Event> channelEvents = new ArrayList<>();

    for (int i = 0; i < batchSize && status != Status.BACKOFF; i++) {
      Event event = channel.take();
      if (null != event) {
        channelEvents.add(event);
      } else {
        status = Status.BACKOFF;
      }
    }

    if (0 >= channelEvents.size()) {
      status = Status.BACKOFF;
      redisSinkCounter.incrementBatchEmptyCount();
      logger.warn("retrieved an empty event in sink {}", getName());
      txn.commit();
      txn.close();
      return status;
    } else if (batchSize > channelEvents.size()) {
      redisSinkCounter.incrementBatchUnderflowCount();
    } else {
      redisSinkCounter.incrementBatchCompleteCount();
    }

    int droppedEvents = 0;
    redisSinkCounter.addToEventDrainAttemptCount(channelEvents.size());
    try (Jedis jedis = jedisPool.getResource()) {

      boolean poisonedEventPresent = false;
      for (Event event : channelEvents) {
        SimpleEvent redisEvent;
        try {
          redisEvent = deserializer.transform(event.getBody()).get(0);
        } catch (Throwable e) {
          droppedEvents++;
          logger.error("Could not de-serialize , dropping event {}", event, e);
          redisSinkCounter.incrementDeserializationErrorCount();
          continue;
        }

        try {
          eventProcessor.processEvent(jedis, redisEvent);
          event.getHeaders().put(IS_PROCESSED_SUCCESSFULLY, String.valueOf(Boolean.TRUE));
        } catch (Throwable ex) {
          //TODO we need more granular level of exception handling to differentiate retriable vs non-retriable,
          //TODO for now we are retrying entire batch for "n" number times.
          poisonedEventPresent = true;
          logger.error("Error while processing the event: {}", new String(event.getBody()));
          redisSinkCounter.incrementProcessingErrorCount();
          event.getHeaders().put(POISONED_EVENT, String.valueOf(true));
          //TODO increment sink errors
        }
      }
      redisSinkCounter.incrementTxTimeSpent(System.currentTimeMillis() - b4);
      if (poisonedEventPresent) {
        logger.error("Poisoned events found, hence rolling back the events");
        txn.rollback();
        redisSinkCounter.incrementTxErrorCount();
      } else {
        txn.commit();
        redisSinkCounter.incrementTxSuccessCount();
        redisSinkCounter.addToEventDrainSuccessCount(channelEvents.size() - droppedEvents);
      }
    } catch (JedisConnectionException e) {
      txn.rollback();
      redisSinkCounter.incrementTxErrorCount();
      logger.error("Error while shipping events to redis {}", getName(), e);
      status = Status.BACKOFF;
    } catch (Throwable t) {
      logger.error("Unexpected error {}", getName(), t);
      txn.rollback();
      redisSinkCounter.incrementTxErrorCount();
      status = Status.BACKOFF;
    } finally {
      txn.close();
    }
    return status;
  }

  @Override
  public void configure(Context context) {

    logger.info("Configuring Redis sink {} ...", getName());
    this.host = context.getString(HOST);
    Preconditions.checkState(StringUtils.isNotBlank(host),
        "host cannot be empty, please specify in configuration file");

    this.port = context.getInteger(RedisSinkConfigurationConstant.PORT, Protocol.DEFAULT_PORT);
    this.timeout = context.getInteger(RedisSinkConfigurationConstant.TIMEOUT, Protocol.DEFAULT_TIMEOUT);
    this.database = context.getInteger(RedisSinkConfigurationConstant.DATABASE, Protocol.DEFAULT_DATABASE);
    this.password = context.getString(RedisSinkConfigurationConstant.PASSWORD);
    this.batchSize = context.getInteger(RedisSinkConfigurationConstant.BATCH_SIZE,
        RedisSinkConfigurationConstant.DEFAULT_BATCH_SIZE);
    Preconditions.checkState(batchSize > 0, RedisSinkConfigurationConstant.BATCH_SIZE
        + " parameter Added must be greater than 1");
    String deserializerClassName = context.getString(RedisSinkConfigurationConstant.DESERIALZER);
    String eventProcessorClassName = context.getString(RedisSinkConfigurationConstant.EVENT_PROCESSOR);

    try {

      Class<? extends SimpleEventDeserializer> deserializerClazz = (Class<? extends SimpleEventDeserializer>) Class
          .forName(deserializerClassName);
      deserializer = deserializerClazz.newInstance();
      Class<? extends SimpleEventProcessor> eventProcessorClazz = (Class<? extends SimpleEventProcessor>) Class
          .forName(eventProcessorClassName);
      eventProcessor = eventProcessorClazz.newInstance();
    } catch (ClassNotFoundException e) {
      logger.error("Could not instantiate the class : ", e);
      Throwables.propagate(e);
    } catch (InstantiationException | IllegalAccessException e) {
      logger.error("Could not instantiate the class: ", e);
      Throwables.propagate(e);
    }
    this.redisSinkCounter = new RedisSinkCounter(this.getName());
  }

  public RedisSinkCounter getRedisSinkCounter() {
    return redisSinkCounter;
  }
}
