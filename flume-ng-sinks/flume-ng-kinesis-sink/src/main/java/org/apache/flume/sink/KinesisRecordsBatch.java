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

import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;

import java.util.Collections;
import java.util.List;

public class KinesisRecordsBatch {
  private List<PutRecordsRequestEntry> batch;
  private PutRecordsRequestEntry spillOverRecord;
  static KinesisRecordsBatch EMPTY_RECORDS = new KinesisRecordsBatch(
    Collections.<PutRecordsRequestEntry>emptyList(), null);

  public KinesisRecordsBatch(List<PutRecordsRequestEntry> batch, PutRecordsRequestEntry spillOverRecord) {
    this.spillOverRecord = spillOverRecord;
    this.batch = batch;
  }

  public List<PutRecordsRequestEntry> getBatch() {
    return batch;
  }

  public Boolean hasSpillOverEvent() {
    return spillOverRecord != null;
  }

  public PutRecordsRequestEntry getSpillOverRecord() {
    return spillOverRecord;
  }

  public void unSetSpillOverEvent() {
    spillOverRecord = null;
  }
}
