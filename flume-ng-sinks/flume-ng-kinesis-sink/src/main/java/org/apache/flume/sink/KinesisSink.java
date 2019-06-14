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
package org.apache.flume.sink;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.kinesis.KinesisSinkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(KinesisSink.class);

  private KinesisSinkCounter sinkCounter;

  private static AmazonKinesis kinesisClient;
  private String streamName;
  private String endpoint;
  private int batchSize;
  private int maximumBatchSizeInBytes;
  private int maximumEventSizeInBytes;
  private int maxAttempts;
  private boolean rollbackAfterMaxAttempts;
  private boolean partitionKeyFromEvent;
  private AWSCredentialsProvider credentialsProvider;
  private KinesisSinkBatchBuilder batchBuilder;

  @Override
  public void configure(Context context) {

    this.endpoint = context.getString("endpoint", ConfigurationConstants.DEFAULT_KINESIS_ENDPOINT);
    String accessKeyId = context.getString("accessKeyId");
    String secretAccessKey = context.getString("secretAccessKey");
    if (accessKeyId != null && secretAccessKey != null) {
      credentialsProvider =
          new MyAwsCredentials(accessKeyId, secretAccessKey);
    } else {
      credentialsProvider = new DefaultAWSCredentialsProviderChain();
    }
    this.streamName = Preconditions.checkNotNull(context.getString("streamName"), "streamName is required");
    String errMsgTemplate = "%s is %s, it must be between 1 and %s";
    this.batchSize = context.getInteger("batchSize", ConfigurationConstants.DEFAULT_BATCH_SIZE);
    Preconditions.checkArgument(
        batchSize > 0 && batchSize <= ConfigurationConstants.MAX_BATCH_SIZE,
        String.format(
            errMsgTemplate, "batchSize", batchSize, ConfigurationConstants.MAX_BATCH_SIZE));

    this.maximumBatchSizeInBytes =
        context.getInteger("maximumBatchSizeInBytes", ConfigurationConstants.MAX_BATCH_BYTE_SIZE);
    Preconditions.checkArgument(
        maximumBatchSizeInBytes > 0
            && maximumBatchSizeInBytes <= ConfigurationConstants.MAX_BATCH_BYTE_SIZE,
        String.format(
            errMsgTemplate,
            "maximumBatchSizeInBytes",
            maximumBatchSizeInBytes,
            ConfigurationConstants.MAX_BATCH_BYTE_SIZE));

    this.maximumEventSizeInBytes =
        context.getInteger("maximumEventSizeInBytes", ConfigurationConstants.MAX_EVENT_SIZE);
    Preconditions.checkArgument(
        maximumEventSizeInBytes > 0
            && maximumEventSizeInBytes <= ConfigurationConstants.MAX_EVENT_SIZE,
        String.format(
            errMsgTemplate,
            "maximumEventSizeInBytes",
            maximumEventSizeInBytes,
            ConfigurationConstants.MAX_EVENT_SIZE));

    this.maxAttempts =
        context.getInteger("maxAttempts", ConfigurationConstants.DEFAULT_MAX_ATTEMPTS);
    Preconditions.checkArgument(maxAttempts > 0, "maxAttempts must be greater than 0");

    this.rollbackAfterMaxAttempts =
        context.getBoolean("rollbackAfterMaxAttempts", ConfigurationConstants.DEFAULT_ROLLBACK_AFTER_MAX_ATTEMPTS);

    // If true, we will check each event's header for a key named... "key", if present, use this as
    // the kinesis
    // partitionKey, rather than randomly generating a partitionKey.
    this.partitionKeyFromEvent =
        context.getBoolean("partitionKeyFromEvent", ConfigurationConstants.DEFAULT_PARTITION_KEY_FROM_EVENT);

    if (sinkCounter == null) {
      sinkCounter = new KinesisSinkCounter(getName());
    }
    logger.info("started Kinesis sink {} => {}", getName(), toString());
  }

  @Override
  public void start() {
    kinesisClient = new AmazonKinesisClient(credentialsProvider);
    kinesisClient.setEndpoint(this.endpoint);
    batchBuilder = new KinesisSinkBatchBuilder(
        batchSize, maximumBatchSizeInBytes, maximumEventSizeInBytes, partitionKeyFromEvent);
    sinkCounter.start();
    logger.info("started kinesis client {}", getName());
  }

  @Override
  public void stop() {
    sinkCounter.stop();
    logger.info("stopped kinesis client {}", getName());
  }

  @Override
  public Status process() throws EventDeliveryException {

    long b4 = System.currentTimeMillis();
    Status status;
    // Get the channel associated with this Sink
    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();
    // Start the transaction
    txn.begin();
    try {

      KinesisRecordsBatch recordsBatch = batchBuilder.buildBatch(ch);
      List<PutRecordsRequestEntry> takeRecords = recordsBatch.getBatch();
      sinkCounter.addToEventDrainAttemptCount(takeRecords.size());
      if (recordsBatch.hasSpillOverEvent()) {
        sinkCounter.addToEventSpilloverCount(1);
      }
      int failedTxnEventCount = batchSend(takeRecords);

      /*
       * A spill over event is an event which was taken out of the flume channel, but
       * did not fit within the batch due to batch size (in bytes) limitation.
       * Attempt sending this single event within the same transaction at the cost of performance.
       * To avoid sending single event, set both batchSize & maximumBatchSizeInBytes, and set them
       * such that you don't hit maximumBatchSizeInBytes limit too often.
       */
      if (recordsBatch.hasSpillOverEvent() && failedTxnEventCount == 0) {
        List<PutRecordsRequestEntry> singleEvent = new ArrayList<>(1);
        singleEvent.add(recordsBatch.getSpillOverRecord());
        failedTxnEventCount += batchSend(singleEvent);
        recordsBatch.unSetSpillOverEvent();
      }
      if (0 >= failedTxnEventCount) {
        sinkCounter.addToEventDrainSuccessCount(takeRecords.size());
      }

      if (failedTxnEventCount > 0 && this.rollbackAfterMaxAttempts) {
        txn.rollback();
        logger.error("Failed to commit transaction after max attempts reached. Transaction rolled back.");
        status = Status.BACKOFF;
      } else {
        txn.commit();
        sinkCounter.incrementTxSuccessCount();
        boolean noEvents = takeRecords.size() == 0;
        status = noEvents ? Status.BACKOFF : Status.READY;
        if (noEvents) {
          sinkCounter.incrementBatchEmptyCount();
        }
      }
    } catch (Throwable t) {
      sinkCounter.incrementTxErrorCount();
      sinkCounter.incrementConnectionFailedCount();
      status = Status.BACKOFF;
      txn.rollback();
      logger.error("Failed to commit transaction. Transaction rolled back.", t);
      if (t instanceof Error) {
        throw (Error) t;
      } else {
        throw new EventDeliveryException(t);
      }
    } finally {
      sinkCounter.incrementTxTimeSpent(System.currentTimeMillis() - b4);
      txn.close();
    }
    return status;
  }

  /**
   * Sends batch of events to Kinesis
   *
   * @param putRecordsRequestEntryList list of PutRecordsRequestEntry
   * @return count of events that could not be sent
   */
  private int batchSend(List<PutRecordsRequestEntry> putRecordsRequestEntryList) {

    int attemptCount = 1;
    int failedTxnEventCount = 0;

    int takeEventCount = putRecordsRequestEntryList.size();
    if (takeEventCount > 0) {
      if (takeEventCount == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      } else {
        sinkCounter.incrementBatchUnderflowCount();
      }

      PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
      putRecordsRequest.setStreamName(this.streamName);
      putRecordsRequest.setRecords(putRecordsRequestEntryList);

      sinkCounter.addToEventDrainAttemptCount(putRecordsRequest.getRecords().size());
      long b4 = System.currentTimeMillis();
      PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
      sinkCounter.incrementKinesisTxTimeSpent(System.currentTimeMillis() - b4);

      while (putRecordsResult.getFailedRecordCount() > 0 && attemptCount < maxAttempts) {
        logger.warn("Failed to sink {} records on attempt of ",
            maxAttempts, putRecordsResult.getFailedRecordCount(), attemptCount);
        try {
          Thread.sleep(ConfigurationConstants.BACKOFF_TIME_IN_MILLIS);
        } catch (InterruptedException e) {
          logger.debug("Interrupted sleep", e);
        }

        List<PutRecordsRequestEntry> failedRecordsList =
            getFailedRecordsFromResult(putRecordsResult, putRecordsRequestEntryList);
        putRecordsRequest.setRecords(failedRecordsList);
        b4 = System.currentTimeMillis();
        putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
        sinkCounter.incrementKinesisTxTimeSpent(System.currentTimeMillis() - b4);
        attemptCount++;
      }

      if (putRecordsResult.getFailedRecordCount() > 0) {
        logger.error("Failed to sink  records after out of attempt(s)",
            putRecordsResult.getFailedRecordCount(), attemptCount, maxAttempts);
        sinkCounter.incrementConnectionFailedCount();
      }

      failedTxnEventCount = putRecordsResult.getFailedRecordCount();
      int successfulTxnEventCount = takeEventCount - failedTxnEventCount;
      sinkCounter.addToEventDrainSuccessCount(successfulTxnEventCount);
    } else {
      sinkCounter.incrementBatchEmptyCount();
    }
    return failedTxnEventCount;
  }

  private List<PutRecordsRequestEntry> getFailedRecordsFromResult(
      PutRecordsResult putRecordsResult, List<PutRecordsRequestEntry> putRecordsRequestEntryList) {
    List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
    List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();

    for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
      PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList.get(i);
      PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
      if (putRecordsResultEntry.getErrorCode() != null) {
        failedRecordsList.add(putRecordRequestEntry);
      }
    }

    return failedRecordsList;
  }

  @Override
  public String toString() {
    return "KinesisSink{" +
        "streamName='" + streamName + '\'' +
        ", endpoint='" + endpoint + '\'' +
        ", batchSize=" + batchSize +
        ", maximumBatchSizeInBytes=" + maximumBatchSizeInBytes +
        ", maximumEventSizeInBytes=" + maximumEventSizeInBytes +
        ", maxAttempts=" + maxAttempts +
        ", rollbackAfterMaxAttempts=" + rollbackAfterMaxAttempts +
        ", partitionKeyFromEvent=" + partitionKeyFromEvent +
        '}';
  }
}
