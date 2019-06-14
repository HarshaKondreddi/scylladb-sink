/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.instrumentation.jdbc;

import org.apache.flume.instrumentation.SinkCounter;

public class JdbcSinkCounter extends SinkCounter implements JdbcSinkCounterMBean {

  private static final String JDBC_DESERIALIZATION_ERROR_COUNT =
      "sink.jdbc.event.deserialization.error.count";

  private static final String TX_ERROR_COUNT =
      "sink.jdbc.tx.error.count";

  private static final String TX_TIME_SPENT =
      "sink.jdbc.tx.timespent";

  private static final String TX_SUCCESS_COUNT =
      "sink.jdbc.tx.success.count";

  private static final String[] ATTRIBUTES = {JDBC_DESERIALIZATION_ERROR_COUNT,
      TX_ERROR_COUNT, TX_SUCCESS_COUNT, TX_TIME_SPENT};

  public JdbcSinkCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public long incrementDeserializationErrorCount() {
    return increment(JDBC_DESERIALIZATION_ERROR_COUNT);
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

  public long getDeserializationErrorCount() {
    return get(JDBC_DESERIALIZATION_ERROR_COUNT);
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
}
