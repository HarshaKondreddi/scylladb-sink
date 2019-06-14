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

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.flume.source.kafka.KafkaSourceConstants.BACKOFF_INTERVAL_MS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.BATCH_DURATION_MS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.BATCH_SIZE;
import static org.apache.flume.source.kafka.KafkaSourceConstants.BOOTSTRAP_SERVERS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.CONSUMER_GROUP_SIZE;
import static org.apache.flume.source.kafka.KafkaSourceConstants.CONSUMER_TIMEOUT_MS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.DEFAULT_AUTO_COMMIT;
import static org.apache.flume.source.kafka.KafkaSourceConstants.DEFAULT_BACKOFF_INTERVAL_MS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.DEFAULT_BATCH_DURATION;
import static org.apache.flume.source.kafka.KafkaSourceConstants.DEFAULT_BATCH_SIZE;
import static org.apache.flume.source.kafka.KafkaSourceConstants.DEFAULT_KEY_DESERIALIZER;
import static org.apache.flume.source.kafka.KafkaSourceConstants.DEFAULT_KEY_HEADER;
import static org.apache.flume.source.kafka.KafkaSourceConstants.DEFAULT_PARTITION_HEADER;
import static org.apache.flume.source.kafka.KafkaSourceConstants.DEFAULT_TIMESTAMP_HEADER;
import static org.apache.flume.source.kafka.KafkaSourceConstants.DEFAULT_TOPIC_HEADER;
import static org.apache.flume.source.kafka.KafkaSourceConstants.DEFAULT_VALUE_DESERIALIZER;
import static org.apache.flume.source.kafka.KafkaSourceConstants.GROUP_ID;
import static org.apache.flume.source.kafka.KafkaSourceConstants.KEY_HEADER_KEY;
import static org.apache.flume.source.kafka.KafkaSourceConstants.PARTITION_ID_HEADER_KEY;
import static org.apache.flume.source.kafka.KafkaSourceConstants.PERIOD;
import static org.apache.flume.source.kafka.KafkaSourceConstants.RESET_OFFSET;
import static org.apache.flume.source.kafka.KafkaSourceConstants.SOURCE_LOCK_DIR;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TIMESTAMP_HEADER_KEY;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TOPICS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TOPIC_CONSUMER_PREFIX;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TOPIC_HEADER_KEY;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.kafka.KafkaSourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Source for Kafka which reads messages from kafka topics.
 * <p>
 * <tt>kafka.bootstrap.servers: </tt> A comma separated list of host:port pairs
 * to use for establishing the initial connection to the Kafka cluster.
 * For example host1:port1,host2:port2,...
 * <b>Required</b> for kafka.
 * <p>
 * <tt>kafka.consumer.group.id: </tt> the group ID of consumer group. <b>Required</b>
 * <p>
 * <tt>kafka.topics: </tt> the topic list separated by commas to consume messages from.
 * <b>Required</b>
 * <p>
 * <tt>maxBatchSize: </tt> Maximum number of messages written to Channel in one
 * batch. Default: 1000
 * <p>
 * <tt>maxBatchDurationMillis: </tt> Maximum number of milliseconds before a
 * batch (of any size) will be written to a channel. Default: 1000
 * <p>
 * <tt>kafka.consumer.*: </tt> Any property starting with "kafka.consumer" will be
 * passed to the kafka consumer So you can use any configuration supported by Kafka 0.9.0.X
 * <p>
 */
public class MultiKafkaSource extends AbstractSource implements
    EventDrivenSource, Configurable {

  private static final Logger log = LoggerFactory.getLogger(MultiKafkaSource.class);

  private Context context;
  private Multimap<String, KafkaConsumerWorker> consumers;
  KafkaSourceCounter counter;

  int maxBatchSize;
  int maxBatchDurationMillis;
  int backoffIntervalMillis;
  String topicHeader;
  String partitionHeader;
  String keyHeader;
  String timestampHeader;
  String groupId;

  private Set<HostAndPort> bootstrapServers;
  private Set<String> topics;
  private Integer consumerGroupSize;
  private Properties fixedProps;
  private Properties kafkaProps;

  private Timer suspendChecker;
  private File suspendFile;

  @Override
  public void configure(Context ctx) {
    this.context = ctx;
    this.fixedProps = new Properties();
    this.kafkaProps = new Properties();
    this.consumers = ArrayListMultimap.create();
    this.bootstrapServers = Sets.newHashSet();

    String bootstrapServersStr = context.getString(BOOTSTRAP_SERVERS);
    if (bootstrapServersStr == null || bootstrapServersStr.trim().isEmpty()) {
      throw new ConfigurationException(BOOTSTRAP_SERVERS + " is not configured.");
    }

    try {
      for (String bootstrapServer : Splitter.on(",").split(bootstrapServersStr)) {
        HostAndPort hostport = HostAndPort.fromString(bootstrapServer);
        bootstrapServers.add(hostport);
      }
    } catch (Exception e) {
      throw new ConfigurationException(BOOTSTRAP_SERVERS +
          " is incorrectly configured." +
          " expected format: host1:port1,[host2:port2,...]" +
          " found: " + bootstrapServersStr, e);
    }

    this.groupId = ctx.getString(GROUP_ID);
    if ((groupId == null || groupId.trim().isEmpty())) {
      throw new ConfigurationException(GROUP_ID + " is not configured.");
    }

    this.topicHeader = context.getString(TOPIC_HEADER_KEY, DEFAULT_TOPIC_HEADER);
    this.partitionHeader = context.getString(PARTITION_ID_HEADER_KEY,
        DEFAULT_PARTITION_HEADER);
    this.keyHeader = context.getString(KEY_HEADER_KEY, DEFAULT_KEY_HEADER);
    this.timestampHeader = context.getString(TIMESTAMP_HEADER_KEY,
        DEFAULT_TIMESTAMP_HEADER);

    this.maxBatchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    this.maxBatchDurationMillis = context.getInteger(BATCH_DURATION_MS,
        DEFAULT_BATCH_DURATION);
    this.backoffIntervalMillis = context.getInteger(BACKOFF_INTERVAL_MS,
        DEFAULT_BACKOFF_INTERVAL_MS);

    topics = Sets.newHashSet(Splitter.on(",").omitEmptyStrings()
        .trimResults().split(context.getString(TOPICS, "")));

    if (topics.size() == 0) {
      throw new ConfigurationException(TOPICS + " is not configured.");
    }

    consumerGroupSize = context.getInteger(CONSUMER_GROUP_SIZE);
    if (consumerGroupSize == null || consumerGroupSize <= 0) {
      throw new ConfigurationException(CONSUMER_GROUP_SIZE + " is incorrect or" +
          " not configured. should be configured to a positive integer.");
    }

    suspendChecker = new Timer("suspend-checker for kafka-source " + getName());

    String sourceLockDir = context.getString(SOURCE_LOCK_DIR, "");
    if ((sourceLockDir != null && !sourceLockDir.trim().isEmpty())) {
      suspendFile = new File(sourceLockDir, getName() + ".lock");
      log.info("source lock directory configured: {}, will check for file {}",
          sourceLockDir, suspendFile.getAbsolutePath());
    }

    setConsumerProps(context, bootstrapServersStr);

    configureWorkers();

    if (counter == null) {
      counter = new KafkaSourceCounter(getName());
    }
  }

  private void setConsumerProps(Context ctx, String bootstrapServersStr) {
    kafkaProps.put(CONSUMER_TIMEOUT_MS, Integer.toString(maxBatchDurationMillis));
    kafkaProps.put(RESET_OFFSET, ctx.getBoolean(RESET_OFFSET, false));
    kafkaProps.put(FETCH_MIN_BYTES_CONFIG, 1024 * 50);
//    kafkaProps.put(REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
    kafkaProps.put(AUTO_OFFSET_RESET_CONFIG, "latest");
    kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
    kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIALIZER);

    //These always take precedence over config and are always fixed for all topics
    fixedProps.put(GROUP_ID_CONFIG, groupId);
    fixedProps.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServersStr);
    fixedProps.put(ENABLE_AUTO_COMMIT_CONFIG, DEFAULT_AUTO_COMMIT);

    log.info("kafka consumer fixed configuration: {}", fixedProps.toString());
    log.info("kafka consumer global configuration: {}", kafkaProps.toString());
  }

  private Properties getTopicKafkaProperties(String topic, int fetcherCount) {
    Properties topicConsumerProps = new Properties();
    topicConsumerProps.putAll(kafkaProps);
    topicConsumerProps.putAll(context.getSubProperties(TOPIC_CONSUMER_PREFIX + topic + PERIOD));
    topicConsumerProps.putAll(fixedProps);
    log.info("topicConsumerProps = {}", topicConsumerProps);
    return topicConsumerProps;
  }

  @Override
  public void start() {
    counter.start();
    startWorkers();
    if (suspendFile != null) {
      suspendChecker.schedule(new SourceLockCheckTask(suspendFile), 0, 5000L);
    }
  }

  @Override
  public void stop() {
    suspendChecker.cancel();
    suspendChecker.purge();
    stopWorkers();
    counter.stop();
  }

  public synchronized void configureWorkers() {
    this.consumers.clear();

    SimpleConsumer consumer = null;
    TopicMetadataResponse response = null;

    for (HostAndPort hostport : bootstrapServers) {
      try {
        consumer = new SimpleConsumer(hostport.getHostText(), hostport.getPort(),
            30000, 64 * 1024, "flume-kafka-source-metadata-fetcher");

        TopicMetadataRequest request = new TopicMetadataRequest(Lists.newArrayList(topics));
        response = consumer.send(request);

        if (response != null) {
          break;
        }
      } catch (Exception e) {
        log.error("error while fetching kafka topic metadata from broker {}." +
            "will retry with next available broker", hostport, e);
      } finally {
        if (consumer != null) {
          consumer.close();
        }
      }
    }

    if (response == null) {
      throw new ConfigurationException("failed to fetch metadata from brokers" +
          " configured: " + bootstrapServers);
    }

    for (String topic : topics) {
      boolean metadataFound = false;

      for (TopicMetadata metadata : response.topicsMetadata()) {
        if (topic.equals(metadata.topic())) {
          int fetcherCount = (int) Math.ceil(
              metadata.partitionsMetadata().size() / (double) consumerGroupSize
          );

          log.info("starting consumer worker for topic: {}, fetcher count: {} ", topic, fetcherCount);
          try {
            Properties topicConsumerProperties = getTopicKafkaProperties(topic, fetcherCount);
            KafkaConsumerWorker worker = new KafkaConsumerWorker(this, topic, topicConsumerProperties);
            worker.configure(context);
            consumers.put(topic, worker);
          } catch (Exception e) {
            log.error("Failed to configure worker for topic: {}", topic);
            throw new ConfigurationException("failed to configure worker for topic: " + topic, e);
          }
          metadataFound = true;
        }
      }

      if (!metadataFound) {
        throw new ConfigurationException("failed to fetch metadata for topic: " + topic);
      }
    }
  }

  private synchronized void startWorkers() {
    for (Map.Entry<String, KafkaConsumerWorker> topicWorker : consumers.entries()) {
      topicWorker.getValue().doStart();
    }
  }

  private synchronized void stopWorkers() {
    for (Map.Entry<String, KafkaConsumerWorker> topicWorker : consumers.entries()) {
      topicWorker.getValue().doStop();
    }

    for (Map.Entry<String, KafkaConsumerWorker> topicWorker : consumers.entries()) {
      topicWorker.getValue().awaitTermination();
    }
  }

  class SourceLockCheckTask extends TimerTask {
    private File lockFile;
    private volatile boolean stopped;

    SourceLockCheckTask(File lockFile) {
      this.lockFile = lockFile;
    }

    @Override
    public void run() {
      try {
        if (lockFile.exists()) {
          if (!stopped) {
            log.info("{}:: lock file found. stopping workers.", getName());
            stopWorkers();
            stopped = true;
          } else {
            log.debug("{}:: lock file found. workers already stopped.", getName());
          }
        } else {
          if (stopped) {
            log.info("{}:: lock file not found. starting workers.", getName());
            configureWorkers();
            startWorkers();
            stopped = false;
          } else {
            log.debug("{}:: lock file not found. workers already started.", getName());
          }
        }
      } catch (Throwable t) {
        log.warn("{}:: error while checking for lock file: {}", getName(), lockFile.getAbsolutePath());
      }
    }
  }
}