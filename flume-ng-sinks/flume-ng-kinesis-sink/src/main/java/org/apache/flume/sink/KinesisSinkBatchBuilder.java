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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.google.common.collect.Lists;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisSinkBatchBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisSinkBatchBuilder.class);

  private int batchSize;
  private int maxBatchByteSize;
  private int maxEventSize;
  private boolean usePartitionKeyFromEvent;

  public KinesisSinkBatchBuilder(
      int batchSize,
      int maxBatchByteSize,
      int maxEventSize,
      boolean usePartitionKeyFromEvent
  ) {
    this.batchSize = batchSize;
    this.usePartitionKeyFromEvent = usePartitionKeyFromEvent;
    this.maxBatchByteSize = maxBatchByteSize;
    this.maxEventSize = maxEventSize;
  }

  KinesisRecordsBatch buildBatch(Channel ch) {
    if (ch == null) {
      LOG.error("Channel was null");
      return KinesisRecordsBatch.EMPTY_RECORDS;
    }

    int currBatchByteSize = 0;
    PutRecordsRequestEntry spillOverRecord = null;
    List<PutRecordsRequestEntry> putRecordsRequestEntryList = Lists.newArrayList();

    while (putRecordsRequestEntryList.size() < batchSize) {

      //Take an event from the channel
      Event event = ch.take();
      if (event == null) {
        break;
      }

      int currEventSize = event.getBody().length;
      if (isOverEventSizeLimit(currEventSize)) {
        LOG.warn("Dropping an event of size {}, max event size is {} bytes", currEventSize, maxEventSize);
        continue;
      }

      PutRecordsRequestEntry entry = buildRequestEntry(event);
      if (isWithinSizeLimit(currEventSize, currBatchByteSize)) {
        putRecordsRequestEntryList.add(entry);
        currBatchByteSize += currEventSize;
      } else {
        //The event is taken out of the channel but does not fit within this batch
        spillOverRecord = entry;
        break;
      }
    }
    return new KinesisRecordsBatch(putRecordsRequestEntryList, spillOverRecord);
  }

  private boolean isWithinSizeLimit(int currEventSize, int currBatchByteSize) {
    return currBatchByteSize + currEventSize <= maxBatchByteSize;
  }

  private boolean isOverEventSizeLimit(int eventSize) {
    return eventSize > maxEventSize;
  }

  private PutRecordsRequestEntry buildRequestEntry(Event event) {
    String partitionKey;
    if (usePartitionKeyFromEvent && event.getHeaders().containsKey("key")) {
      partitionKey = event.getHeaders().get("key");
    } else {
      partitionKey = "pk_" + new Random().nextInt(Integer.MAX_VALUE);
    }
    LOG.debug("partitionKey: " + partitionKey);
    PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
    entry.setData(ByteBuffer.wrap(event.getBody()));
    entry.setPartitionKey(partitionKey);
    return entry;
  }
}
