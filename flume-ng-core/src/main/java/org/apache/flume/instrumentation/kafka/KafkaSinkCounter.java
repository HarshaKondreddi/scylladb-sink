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
package org.apache.flume.instrumentation.kafka;

import org.apache.flume.instrumentation.SinkCounter;

public class KafkaSinkCounter extends SinkCounter implements KafkaSinkCounterMBean {

  private static final String TIMER_KAFKA_EVENT_SEND =
      "channel.kafka.event.send.time";

  private static final String COUNT_BATCH_ROLLBACK =
      "channel.batch.rollback.count";

  private static final String COUNT_ROLLBACK =
      "channel.rollback.count";

  private static final String COUNT_EVENT_ROLLBACK = "channel.event.rollback.count";

  private static final String[] ATTRIBUTES = {COUNT_BATCH_ROLLBACK, TIMER_KAFKA_EVENT_SEND, COUNT_EVENT_ROLLBACK};

  public KafkaSinkCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public long addToKafkaEventSendTimer(long delta) {
    return addAndGet(TIMER_KAFKA_EVENT_SEND, delta);
  }

  public long incrementRollbackBatchCount() {
    return increment(COUNT_BATCH_ROLLBACK);
  }

  public long addToRollbackEventCount(long delta) {
    return addAndGet(COUNT_EVENT_ROLLBACK, delta);
  }

  public long incrementRollbackEventCount() {
    return increment(COUNT_EVENT_ROLLBACK);
  }

  public long getKafkaEventSendTimer() {
    return get(TIMER_KAFKA_EVENT_SEND);
  }

  public long getRollbackBatchCount() {
    return get(COUNT_BATCH_ROLLBACK);
  }

  public long getRollbackEventsCount() {
    return get(COUNT_EVENT_ROLLBACK);
  }

  public long incrementRollbackCount() {
    return increment(COUNT_ROLLBACK);
  }

  public long getRollbackCount() {
    return get(COUNT_ROLLBACK);
  }
}
