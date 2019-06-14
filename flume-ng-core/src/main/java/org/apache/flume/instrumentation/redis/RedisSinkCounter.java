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
package org.apache.flume.instrumentation.redis;

import org.apache.flume.instrumentation.SinkCounter;

public class RedisSinkCounter extends SinkCounter implements RedisSinkCounterMBean {

  private static final String DESERIALIZATION_ERROR_COUNT =
      "sink.redis.deserialization.error.count";

  private static final String PROCESSING_ERROR_COUNT =
      "sink.redis.processing.error.count";

  private static final String TX_ERROR_COUNT =
      "sink.redis.tx.error.count";

  private static final String TX_TIME_SPENT =
      "sink.redis.tx.timespent";

  private static final String TX_SUCCESS_COUNT =
      "sink.redis.tx.success.count";

  private static final String[] ATTRIBUTES = {
      DESERIALIZATION_ERROR_COUNT, PROCESSING_ERROR_COUNT,
      TX_ERROR_COUNT, TX_SUCCESS_COUNT,
      TX_TIME_SPENT};

  public RedisSinkCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public long incrementDeserializationErrorCount() {
    return increment(DESERIALIZATION_ERROR_COUNT);
  }

  public long incrementTxErrorCount() {
    return increment(TX_ERROR_COUNT);
  }

  public long incrementProcessingErrorCount() {
    return increment(PROCESSING_ERROR_COUNT);
  }

  public long incrementTxSuccessCount() {
    return increment(TX_SUCCESS_COUNT);
  }

  public long incrementTxTimeSpent(long delta) {
    return addAndGet(TX_TIME_SPENT, delta);
  }

  @Override
  public long getDeserializationErrorCount() {
    return get(DESERIALIZATION_ERROR_COUNT);
  }

  @Override
  public long getProcessingErrorCount() {
    return get(PROCESSING_ERROR_COUNT);
  }

  @Override
  public long getTxErrorCount() {
    return get(TX_ERROR_COUNT);
  }

  @Override
  public long getTxSuccessCount() {
    return get(TX_SUCCESS_COUNT);
  }

  @Override
  public long getTxTimeSpent() {
    return get(TX_TIME_SPENT);
  }
}
