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

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.SimpleEvent;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FlumeSourceRecordProcessor implements IRecordProcessor {
  private static final Log LOG = LogFactory.getLog(FlumeSourceRecordProcessor.class);
  private String kinesisShardId;

  // Backoff and retry settings
  private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
  private static final int NUM_RETRIES = 10;

  // Checkpoint about once a minute
  private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
  private long nextCheckpointTimeInMillis;
    
  private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    
  private ChannelProcessor chProcessor;
  FlumeSourceRecordProcessor(ChannelProcessor chProcessor) {
    super();
    this.chProcessor = chProcessor;
  }
       
  @Override
  public void initialize(String shardId) {
    LOG.info("Initializing record processor for shard: " + shardId);
    this.kinesisShardId = shardId;
  }

  @Override
  public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
    LOG.info("Processing " + records.size() + " records from " + kinesisShardId);
      
    // Process records and perform all exception handling.
    processRecordsWithRetries(records);
    // Checkpoint once every checkpoint interval.
    if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
      checkpoint(checkpointer);
      nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
    }
  }

  /** Process records performing retries as needed. Skip "poison pill" records.
   * @param records list of records to process
   */
  private void processRecordsWithRetries(List<Record> records) {
    
    Event event;
    Map<String, String> headers;
 
    ArrayList<Event> eventList=new ArrayList<>();
        
    for (Record record : records) {
      boolean processedSuccessfully = false;
        
      for (int i = 0; i < NUM_RETRIES; i++) {
        try {
          event = new SimpleEvent();
          headers = new HashMap<>();
          headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
          String data = decoder.decode(record.getData()).toString();
          LOG.info(record.getSequenceNumber() + ", " + record.getPartitionKey()+":"+data);
          event.setBody(data.getBytes());
          event.setHeaders(headers);
          eventList.add(event);
                   
          processedSuccessfully = true;
          break;
              
        } catch (Throwable t) {
          LOG.warn("Caught throwable while processing record " + record, t);
        }
                
        // backoff if we encounter an exception.
        try {
          Thread.sleep(BACKOFF_TIME_IN_MILLIS);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted sleep", e);
        }
      }

      if (!processedSuccessfully) {
        LOG.error("Couldn't process record " + record + ". Skipping the record.");
      }
       
    }
    LOG.info("event size after: " + eventList.size());
    chProcessor.processEventBatch(eventList);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
    LOG.info("Shutting down record processor for shard: " + kinesisShardId);
    // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
    if (reason == ShutdownReason.TERMINATE) {
      checkpoint(checkpointer);
    }
  }

  /** Checkpoint with retries.
   * @param checkpointer user implementation to handle checkpointing location in stream
   */
  private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
    LOG.info("Checkpointing shard " + kinesisShardId);
    for (int i = 0; i < NUM_RETRIES; i++) {
      try {
        checkpointer.checkpoint();
        break;
      } catch (ShutdownException se) {
        // Ignore checkpoint if the processor instance has been shutdown (fail over).
        LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        break;
      } catch (ThrottlingException e) {
        // Backoff and re-attempt checkpoint upon transient failures
        if (i >= (NUM_RETRIES - 1)) {
          LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
          break;
        } else {
          LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
            + NUM_RETRIES, e);
        }
      } catch (InvalidStateException e) {
        // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
        LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        break;
      }

      try {
        Thread.sleep(BACKOFF_TIME_IN_MILLIS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted sleep", e);
      }
    }
  }
}
