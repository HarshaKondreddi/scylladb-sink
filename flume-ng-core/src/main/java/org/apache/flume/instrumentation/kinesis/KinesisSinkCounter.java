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
package org.apache.flume.instrumentation.kinesis;

import org.apache.flume.instrumentation.SinkCounter;

public class KinesisSinkCounter extends SinkCounter implements KinesisSinkCounterMBean {

  private static final String TX_ERROR_COUNT = "sink.tx.error.count";

  private static final String TX_TIME_SPENT = "sink.tx.timespent";

  private static final String TX_KINESIS_TIME_SPENT = "sink.kinesis.tx.timespent";

  private static final String TX_SUCCESS_COUNT = "sink.tx.success.count";

  private static final String COUNTER_EVENT_SPILL_OVER =
      "sink.event.drain.spillover";

  private static final String[] ATTRIBUTES = {COUNTER_EVENT_SPILL_OVER,
      TX_ERROR_COUNT, TX_SUCCESS_COUNT, TX_TIME_SPENT, TX_KINESIS_TIME_SPENT};

  public KinesisSinkCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public long incrementTxErrorCount() {
    return increment(TX_ERROR_COUNT);
  }

  public long incrementTxSuccessCount() {
    return increment(TX_SUCCESS_COUNT);
  }

  public long incrementTxTimeSpent(long delta) {
    return addAndGet(TX_TIME_SPENT, delta);
  }

  public long incrementKinesisTxTimeSpent(long delta) {
    return addAndGet(TX_KINESIS_TIME_SPENT, delta);
  }

  public long addToEventSpilloverCount(long delta) {
    return addAndGet(COUNTER_EVENT_SPILL_OVER, delta);
  }

  @Override
  public long getTxErrorCount() {
    return get(TX_ERROR_COUNT);
  }

  @Override
  public long getTxSuccessCount() {
    return get(TX_SUCCESS_COUNT);
  }

  public long getTxTimeSpent() {
    return get(TX_TIME_SPENT);
  }

  @Override
  public long getKinesisTxTimeSpent() {
    return get(TX_KINESIS_TIME_SPENT);
  }

  @Override
  public long getSpillOverRecords() {
    return 0;
  }
}
