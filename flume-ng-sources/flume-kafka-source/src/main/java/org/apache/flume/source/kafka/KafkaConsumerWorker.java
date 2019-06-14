/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.kafka;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.DEFAULT_SHUTDOWN_WAIT_TIME_MS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.RESET_OFFSET;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaSourceCounter;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerWorker extends Thread implements Configurable {
  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerWorker.class);
  private final MultiKafkaSource source;
  private ChannelProcessor channelProcessor;

  private String topic;
  private String name;
  private volatile boolean running;
  private Map<String, String> headers;

  private Properties kafkaProps;
  private Context context;
  private List<Event> events;
  private Map<Integer, Long> partitionToOffset = Maps.newHashMap();
  private org.apache.kafka.clients.consumer.Consumer<String, String> consumer;
  private boolean seekToBegining = false;

  public KafkaConsumerWorker(MultiKafkaSource source, String topic, Properties kafkaProps) {
    this.source = source;
    this.topic = topic;
    this.kafkaProps = kafkaProps;
    this.name = source.groupId + ":" + topic;
    this.seekToBegining = Boolean.valueOf(kafkaProps.get(RESET_OFFSET).toString());
    setName(this.name);
  }

  public void doStart() {
    log.info("{}:: starting", this.getName());
    this.running = true;
    this.channelProcessor = source.getChannelProcessor();
    this.start();
  }

  public void run() {
    log.info("{}:: starting topic consumer worker for topic: {} ", getName(), this.topic);

    while (running || !events.isEmpty()) {
      try {
        // fetch next batch only when there are no more pending to be sent
        if (events.isEmpty()) {
          final long getBatchStartTime = System.nanoTime();
          fetchNextBatch(events);
          final long getBatchEndTime = System.nanoTime();
          getCounter().addToKafkaEventGetTimer(NANOSECONDS.toMillis(getBatchEndTime - getBatchStartTime));
          getCounter().addToEventReceivedCount(events.size());

          if (events.isEmpty()) {
            log.debug("{}:: fetched empty batch. backing off for {} millis.",
                getName(), source.backoffIntervalMillis);
            getCounter().incrementKafkaEmptyCount();
            MILLISECONDS.sleep(source.backoffIntervalMillis);
            continue;
          }
        }

        channelProcessor.processEventBatch(events);
        getCounter().addToEventAcceptedCount(events.size());

        long commitStartTime = System.nanoTime();
        consumer.commitSync();
        long commitEndTime = System.nanoTime();
        getCounter().incrementKafkaCommitsCount();
        getCounter().addToKafkaCommitTimer(NANOSECONDS.toMillis(commitEndTime - commitStartTime));

        events.clear();
      } catch (InterruptedException ie) {
        //no-op. expected during shutdown.
        log.warn("{}:: interrupted.", getName());
      } catch (Throwable e) {
        log.error("{}:: exception occurred in kafka source worker." +
            " backing off for {} millis", getName(), source.backoffIntervalMillis, e);
        try {
          MILLISECONDS.sleep(source.backoffIntervalMillis);
        } catch (InterruptedException ie) {
          //no-op. expected during shutdown.
          log.warn("{}:: interrupted while backing off.", getName());
        }
      }
    }
  }

  private void fetchNextBatch(List<Event> events) {
    log.debug("{}:: fetching next batch from kafka.", getName());

    long fetchStartTime = System.currentTimeMillis();
    long maxBatchEndTime = fetchStartTime + source.maxBatchDurationMillis;

    while (events.size() < source.maxBatchSize &&
        System.currentTimeMillis() < maxBatchEndTime) {
      try {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        if (records.isEmpty()) {
          break;
        }
        records.forEach(message -> {
          Long prevOffset = partitionToOffset.computeIfAbsent(message.partition(), pid -> -1L);
          if (0 < prevOffset && prevOffset >= message.offset()) {
            getCounter().incrementKafkaDirtyReadCount();
          }
          headers.clear();
          headers.put(source.topicHeader, message.topic());
          headers.put(source.partitionHeader, String.valueOf(message.partition()));
          partitionToOffset.put(message.partition(), message.offset());
          // TODO: key and timestamp
          events.add(EventBuilder.withBody(message.value(), headers));
        });
      } catch (Exception cte) {
        // no-op expected
        log.debug("{}:: consumer exception.", getName(), cte);
      }
    }

    log.debug("{}:: fetched {} events from kafka in {} millis",
        getName(), events.size(), System.currentTimeMillis() - fetchStartTime);
  }

  public void doStop() {
    log.info("{}:: initiating shutdown.", getName());
    running = Boolean.FALSE;
  }

  public void awaitTermination() {
    try {
      log.info("{}:: waiting for shutdown to complete.", getName());
      this.join(DEFAULT_SHUTDOWN_WAIT_TIME_MS);
      this.interrupt();
      this.join();
      consumer.close();
    } catch (Exception e) {
      log.warn("{}:: error occurred while waiting to stop.", this.getName(), e);
    }
    log.info("{}:: stopped", this.getName());
  }

  @Override
  public void configure(Context ctx) {
    this.context = ctx;
    this.headers = Maps.newHashMap();
    this.events = Lists.newArrayList();
    this.consumer = new KafkaConsumer<>(kafkaProps);
    log.info("{}:: starting topic consumer worker for topic: {} with config => {}", getName(), this.topic, kafkaProps);
    this.consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("{} : PartitionsRevoked : {}", getName(), partitions);
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("{} : PartitionsAssigned : {}", getName(), partitions);
        if (seekToBegining) {
          log.info("{} : seekToBeginning : {}", getName(), partitions);
          consumer.seekToBeginning(partitions);
        }
      }
    });
  }

  public KafkaSourceCounter getCounter() {
    return source.counter;
  }
}