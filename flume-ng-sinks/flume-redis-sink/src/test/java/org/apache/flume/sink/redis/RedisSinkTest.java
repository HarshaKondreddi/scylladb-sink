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
package org.apache.flume.sink.redis;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.redis.RedisSinkCounter;
import org.apache.flume.testutils.EmbeddedRedis;
import org.apache.flume.testutils.Service;
import org.fest.assertions.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RedisSinkTest extends RedisSinkConfigurationConstant {

  private static Service redisServer;

  @BeforeClass
  public static void initialize() throws Exception {
    redisServer = new EmbeddedRedis().start();
  }

  @Test
  public void testCompleteBatch() throws EventDeliveryException {
    Context context = createContext();
    Channel channel = createChannel(context);
    RedisSink redisSink = createSink(channel);

    createTransaction(channel, Arrays.asList(
        EventBuilder.withBody("1".getBytes()),
        EventBuilder.withBody("2".getBytes()),
        EventBuilder.withBody("3".getBytes()),
        EventBuilder.withBody("4".getBytes()),
        EventBuilder.withBody("5".getBytes()),
        EventBuilder.withBody("6".getBytes()),
        EventBuilder.withBody("7".getBytes()),
        EventBuilder.withBody("8".getBytes()),
        EventBuilder.withBody("9".getBytes()),
        EventBuilder.withBody("10".getBytes())));
    processAndCloseSink(context, channel, redisSink);

    validateCounters(redisSink.getRedisSinkCounter(), 0, 0, 0, 1, 0, 0, 1, 0, 10, 10);
  }

  @Test
  public void testEmptyBatch() throws EventDeliveryException {
    Context context = createContext();
    Channel channel = createChannel(context);
    RedisSink redisSink = createSink(channel);

    createTransaction(channel, Collections.emptyList());
    processAndCloseSink(context, channel, redisSink);

    validateCounters(redisSink.getRedisSinkCounter(), 0, 0, -1, 0, 0, 0, 0, 1, 0, 0);
  }

  @Test
  public void testTXSuccess() throws EventDeliveryException {

    Context context = createContext();
    Channel channel = createChannel(context);
    RedisSink redisSink = createSink(channel);

    createTransaction(channel, Collections.singletonList(
        EventBuilder.withBody("TestEvent".getBytes())));
    processAndCloseSink(context, channel, redisSink);

    validateCounters(redisSink.getRedisSinkCounter(), 0, 0, 0, 1, 0, 1, 0, 0, 1, 1);
  }

  @Test
  public void testDeserializationError() throws EventDeliveryException {

    Context context = createContext();
    Channel channel = createChannel(context);
    RedisSink redisSink = createSink(channel);

    createTransaction(channel, Arrays.asList(
        EventBuilder.withBody("TestEvent".getBytes()),
        EventBuilder.withBody("DeserializationError".getBytes())));
    processAndCloseSink(context, channel, redisSink);

    validateCounters(redisSink.getRedisSinkCounter(), 1, 0, 0, 1, 0, 1, 0, 0, 2, 1);
  }

  @Test
  public void testProcessingError() throws EventDeliveryException {

    Context context = createContext();
    Channel channel = createChannel(context);
    RedisSink redisSink = createSink(channel);

    createTransaction(channel, Arrays.asList(
        EventBuilder.withBody("TestEvent".getBytes()),
        EventBuilder.withBody("ProcessingError".getBytes())));
    processAndCloseSink(context, channel, redisSink);

    validateCounters(redisSink.getRedisSinkCounter(), 0, 1, 0, 0, 1, 1, 0, 0, 2, 0);
  }

  private void validateCounters(RedisSinkCounter counter,
                                int deserializationErrorCount,
                                int processingErrorCount,
                                int txTimeSpent,
                                int txSuccessCount,
                                int txErrorCount,
                                int batchUnderflowCount,
                                int batchCompleteCount,
                                int batchEmptyCount,
                                int eventDrainAttemptCount,
                                int eventDrainSuccessCount) {
    Assertions.assertThat(counter.getDeserializationErrorCount()).isEqualTo(deserializationErrorCount);
    Assertions.assertThat(counter.getProcessingErrorCount()).isEqualTo(processingErrorCount);
    if (-1 != txTimeSpent) {
      Assertions.assertThat(counter.getTxTimeSpent()).isGreaterThan(txTimeSpent);
    } else {
      Assertions.assertThat(counter.getTxTimeSpent()).isEqualTo(0);
    }
    Assertions.assertThat(counter.getTxSuccessCount()).isEqualTo(txSuccessCount);
    Assertions.assertThat(counter.getTxErrorCount()).isEqualTo(txErrorCount);

    Assertions.assertThat(counter.getBatchUnderflowCount()).isEqualTo(batchUnderflowCount);
    Assertions.assertThat(counter.getBatchCompleteCount()).isEqualTo(batchCompleteCount);
    Assertions.assertThat(counter.getBatchEmptyCount()).isEqualTo(batchEmptyCount);

    Assertions.assertThat(counter.getEventDrainAttemptCount()).isEqualTo(eventDrainAttemptCount);
    Assertions.assertThat(counter.getEventDrainSuccessCount()).isEqualTo(eventDrainSuccessCount);
  }

  private RedisSink createSink(Channel channel) {
    RedisSink redisSink = new RedisSink();
    redisSink.setChannel(channel);
    return redisSink;
  }

  private void createTransaction(Channel channel, List<Event> events) {
    Transaction tx = channel.getTransaction();
    tx.begin();
    for (Event event : events) {
      channel.put(event);
    }
    tx.commit();
    tx.close();
  }

  private Context createContext() {

    Context context = new Context();
    context.put("capacity", "100");
    context.put("transactionCapacity", "20");
    context.put(HOST, "localhost");
    context.put(PORT, String.valueOf(redisServer.port()[0]));
    context.put(BATCH_SIZE, "10");
    context.put(DESERIALZER, TestEventDeserializer.class.getName());
    context.put(EVENT_PROCESSOR, TestEventProcessor.class.getName());
    return context;
  }

  private void processAndCloseSink(Context context, Channel channel, RedisSink redisSink) throws EventDeliveryException {
    redisSink.configure(context);
    redisSink.start();
    redisSink.process();
    redisSink.stop();
    channel.stop();
  }

  private Channel createChannel(Context context) {
    Channel channel = new MemoryChannel();
    channel.setName("junitChannel");
    Configurables.configure(channel, context);
    return channel;
  }

  @AfterClass
  public static void shutdown() throws Exception {
    redisServer.close();
  }
}
