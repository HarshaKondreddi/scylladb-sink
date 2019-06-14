/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * limitations under the License.
 */
package org.apache.flume.sink.kafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.base.Throwables;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import org.apache.flume.Channel;
import org.apache.flume.ChannelFullException;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerWorker extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerWorker.class);

  private static final int DEFAULT_FAIL_EVENT_RETRY_INTERVAL = 500;
  private final PooledKafkaSink sink;
  private Channel channel;
  private volatile boolean running;
  private KafkaProducer<String, byte[]> producer;

  public KafkaProducerWorker(int id, PooledKafkaSink sink) {
    this.sink = sink;
    setName("kafka-producer-worker-" + this.sink.getName() + "-" + id);
  }

  public void doStart() {
    LOG.info("{}:: starting", this.getName());
    this.running = true;
    // instantiate the producer
    this.producer = new KafkaProducer<>(sink.kafkaProps);
    this.channel = sink.getChannel();
    this.start();
  }

  public void doStop() {
    LOG.info("{}:: initiating shutdown.", getName());
    running = Boolean.FALSE;
    this.interrupt();
  }

  public void awaitTermination() {
    try {
      this.join();
      producer.close();
    } catch (Exception e) {
      LOG.warn("error occurred while waiting to stop: {}", this.getName(), e);
    }

    LOG.info("stopped::{}", this.getName());
  }

  public void run() {
    Transaction transaction = null, rollbackTx = null;
    Event event = null;
    String eventTopic = null;
    String eventKey = null;
    int retryCnt = 0;
    boolean putSuccess = false;
    Map<Future<RecordMetadata>, Event> kafkaFutures;
    kafkaFutures = Maps.newHashMap();
    List<Event> failedEvents;
    Multiset<String> failureCounts;
    failedEvents = Lists.newArrayListWithCapacity(sink.batchSize);
    failureCounts = ConcurrentHashMultiset.create();
    while (running) {
      try {
        long processedEvents = 0;

        transaction = channel.getTransaction();
        transaction.begin();

        kafkaFutures.clear();
        failedEvents.clear();
        failureCounts.clear();

        long batchStartTime = System.nanoTime();
        for (; processedEvents < sink.batchSize; processedEvents += 1) {
          event = channel.take();

          if (event == null) {
            // no events available in channel
            if (processedEvents == 0) {
              sink.counter.incrementBatchEmptyCount();
            } else {
              sink.counter.incrementBatchUnderflowCount();
            }
            break;
          }

          byte[] eventBody = event.getBody();
          Map<String, String> headers = event.getHeaders();

          eventTopic = headers.get(sink.topicHeader);
          if (eventTopic == null) {
            eventTopic = sink.topic;
          }
          eventKey = headers.get(sink.topicKeyHeader);

          if (LOG.isDebugEnabled()) {
            LOG.debug("{Event} " + eventTopic + " : " + eventKey + " : "
                + new String(eventBody, "UTF-8"));
            LOG.debug("event #{}", processedEvents);
          }

          // create a message and add to buffer
          long startTime = System.currentTimeMillis();
          ProducerRecord<String, byte[]> record = new ProducerRecord<>(eventTopic, eventKey, eventBody);
          kafkaFutures.put(producer.send(record,
              new KafkaEventCallback(startTime, eventTopic, failureCounts)), event);
        }

        //Prevent linger.ms from holding the batch
        // producer.flush();

        // publish batch and commit.
        if (processedEvents > 0) {
          int successCount = 0;
          for (Map.Entry<Future<RecordMetadata>, Event> entry : kafkaFutures.entrySet()) {
            try {
              entry.getKey().get();
              successCount++;
            } catch (Exception e) {
              failedEvents.add(entry.getValue());
            }
          }
          long endTime = System.nanoTime();
          sink.counter.addToKafkaEventSendTimer((endTime - batchStartTime) / (1000 * 1000));
          sink.counter.addToEventDrainSuccessCount(successCount);
        }

        transaction.commit();
        transaction.close();
        transaction = null;

        if (failedEvents.size() > 0) {
          LOG.warn("failed to send some events in this batch. topic-wise failure counts: {}",
              failureCounts.toString());
          try {
            rollbackTx = null;
            rollbackTx = channel.getTransaction();
            rollbackTx.begin();

            for (Event failedEvent : failedEvents) {
              retryCnt = 0;
              putSuccess = false;
              while (!putSuccess) {
                try {
                  channel.put(failedEvent);
                  putSuccess = true;
                } catch (ChannelFullException ce) {
                  LOG.warn("failed to put back failed events in queue. will retry after {} ms. retry#: {}, error: {}",
                      DEFAULT_FAIL_EVENT_RETRY_INTERVAL, retryCnt, ce);
                  retryCnt++;
                  Thread.sleep(DEFAULT_FAIL_EVENT_RETRY_INTERVAL);
                }
              }
            }

            rollbackTx.commit();
            rollbackTx.close();
            rollbackTx = null;
            sink.counter.addToRollbackEventCount(failedEvents.size());
            sink.counter.incrementRollbackBatchCount();
          } finally {
            if (rollbackTx != null) {
              rollbackTx.rollback();
              rollbackTx.close();
            }
          }
        }
      } catch (Exception ex) {
        LOG.error("Failed to publish events", ex);
        if (transaction != null) {
          try {
            kafkaFutures.clear();
            transaction.rollback();
            sink.counter.incrementRollbackBatchCount();
          } catch (Exception e) {
            LOG.error("Transaction rollback failed", e);
            throw Throwables.propagate(e);
          }
        }
        //   throw new EventDeliveryException(errorMsg, ex);
      } finally {
        if (transaction != null) {
          transaction.close();
        }
      }
    }
  }

  class KafkaEventCallback implements Callback {
    private final Logger logger = LoggerFactory.getLogger(SinkCallback.class);
    private long startTime;
    private String eventTopic;
    private Multiset<String> failureCounts;

    public KafkaEventCallback(long startTime, String eventTopic, Multiset<String> failureCounts) {
      this.startTime = startTime;
      this.eventTopic = eventTopic;
      this.failureCounts = failureCounts;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception != null) {
        failureCounts.add(eventTopic);
        logger.debug("Error sending message to Kafka. topic: {}, error: {}, ", eventTopic, exception.getMessage());
      }

      if (logger.isDebugEnabled()) {
        long eventElapsedTime = System.currentTimeMillis() - startTime;
        logger.debug("Acked message partition:{} offset:{}", metadata.partition(), metadata.offset());
        logger.debug("Elapsed time for send: {}", eventElapsedTime);
      }
    }
  }
}
