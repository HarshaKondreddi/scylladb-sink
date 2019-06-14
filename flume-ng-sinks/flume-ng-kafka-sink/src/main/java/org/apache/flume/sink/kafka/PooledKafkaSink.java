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
import java.util.Properties;

import static org.apache.flume.sink.kafka.KafkaSinkConstants.BATCH_SIZE;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_ACKS;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_BATCH_SIZE;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_KEY_SERIALIZER;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_TOPIC;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_TOPIC_HDR;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_TOPIC_KEY_HDR;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_VALUE_SERIAIZER;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_WORKER_THREADS;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.KAFKA_PRODUCER_PREFIX;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.TOPIC_CONFIG;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.TOPIC_HDR;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.TOPIC_KEY_HDR;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.WORKER_THREADS;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PooledKafkaSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);

  final Properties kafkaProps = new Properties();

  protected String topic;
  protected String topicHeader;
  protected String topicKeyHeader;
  protected int batchSize;
  private List<KafkaProducerWorker> workers;
  protected KafkaSinkCounter counter;
  private int workerThreads;

  //For testing
  public String getTopic() {
    return topic;
  }

  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public Status process() throws EventDeliveryException {
    //Return Backoff as all the work is done in start method by PooledKafKaSink
    return Status.BACKOFF;
  }

  @Override
  public synchronized void start() {
    super.start();
    counter.start();

    for (KafkaProducerWorker worker : workers) {
      worker.doStart();
    }
  }

  @Override
  public synchronized void stop() {
    logger.info("Stopping KafkaSink");
    for (KafkaProducerWorker worker : workers) {
      worker.doStop();
    }
    for (KafkaProducerWorker worker : workers) {
      worker.awaitTermination();
    }
    logger.info("Kafka Sink {} stopped.", getName());
    counter.stop();
    super.stop();
  }

  /**
   * We configure the sink and generate properties for the Kafka Producer
   *
   * Kafka producer properties is generated as follows:
   * 1. We generate a properties object with some static defaults that
   * can be overridden by Sink configuration
   * 2. We add the configuration users added for Kafka (parameters starting
   * with .kafka. and must be valid Kafka Producer properties
   * 3. We add the sink's documented parameters which can override other
   * properties
   *
   * @param context
   */
  @Override
  public void configure(Context context) {
    String topicStr = context.getString(TOPIC_CONFIG);

    if (topicStr == null || topicStr.isEmpty()) {
      topicStr = DEFAULT_TOPIC;
      logger.warn("Topic was not specified. Using {} as the topic.", topicStr);
    } else {
      logger.info("Using the static topic {}. This may be overridden by event headers", topicStr);
    }
    topic = topicStr;
    topicHeader = context.getString(TOPIC_HDR, DEFAULT_TOPIC_HDR);
    topicKeyHeader = context.getString(TOPIC_KEY_HDR, DEFAULT_TOPIC_KEY_HDR);
    logger.info("Using the topic header {} and topic key header {}", topicHeader, topicKeyHeader);

    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);

    if (logger.isDebugEnabled()) {
      logger.debug("Using batch size: {}", batchSize);
    }

    String bootStrapServers = context.getString(BOOTSTRAP_SERVERS_CONFIG);
    if (bootStrapServers == null || bootStrapServers.isEmpty()) {
      throw new ConfigurationException("Bootstrap Servers must be specified");
    }

    setProducerProps(context, bootStrapServers);

    if (logger.isDebugEnabled()) {
      logger.debug("Kafka producer properties: {}", kafkaProps);
    }

    if (counter == null) {
      counter = new KafkaSinkCounter(getName());
    }

    workerThreads = context.getInteger(WORKER_THREADS, DEFAULT_WORKER_THREADS);
    logger.info("workerThreads: {}", workerThreads);

    workers = Lists.newArrayList();
    // instantiate the producer
    for (int i = 1; i <= workerThreads; i++) {
      workers.add(new KafkaProducerWorker(i, this));
    }
  }

  private void setProducerProps(Context context, String bootStrapServers) {
    kafkaProps.put(ProducerConfig.ACKS_CONFIG, DEFAULT_ACKS);
    //Defaults overridden based on config
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIAIZER);
    kafkaProps.putAll(context.getSubProperties(KAFKA_PRODUCER_PREFIX));
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    logger.info("Producer properties: {}", kafkaProps.toString());
  }
}



