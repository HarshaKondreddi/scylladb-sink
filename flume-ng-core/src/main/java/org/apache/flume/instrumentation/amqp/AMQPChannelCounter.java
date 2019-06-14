/*
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
package org.apache.flume.instrumentation.amqp;

import org.apache.flume.instrumentation.ChannelCounter;

public class AMQPChannelCounter extends ChannelCounter implements AMQPChannelCounterMBean {

  private static final String TIMER_EVENT_GET = "channel.amqp.event.get.time";

  private static final String TIMER_EVENT_SEND = "channel.amqp.event.send.time";

  private static final String TIMER_COMMIT = "channel.amqp.commit.time";

  private static final String TIMER_ROLLBACK = "channel.amqp.rollbak.time";

  private static final String COUNT_ROLLBACK = "channel.rollback.count";

  private static final String COUNT_READ = "channel.read.count";

  private static final String COUNTER_INMEMORY_RETRIES =
      "channel.inmemory.retries";

  private static final String COUNTER_DROP =
      "channel.count.drop";

  private static final String COUNTER_DEAD_LETTER =
      "channel.count.deadletter";

  private static String[] ATTRIBUTES = {
      TIMER_COMMIT, TIMER_ROLLBACK, TIMER_EVENT_SEND, TIMER_EVENT_GET,
      COUNT_ROLLBACK, COUNT_READ, COUNTER_INMEMORY_RETRIES, COUNTER_DROP, COUNTER_DEAD_LETTER
  };

  public AMQPChannelCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public long addToEventGetTimer(long delta) {
    return addAndGet(TIMER_EVENT_GET, delta);
  }

  public long addToEventSendTimer(long delta) {
    return addAndGet(TIMER_EVENT_SEND, delta);
  }

  public long addToCommitTimer(long delta) {
    return addAndGet(TIMER_COMMIT, delta);
  }

  public long addToRollbackTimer(long delta) {
    return addAndGet(TIMER_ROLLBACK, delta);
  }

  public long addToRollbackCount(long delta) {
    return addAndGet(COUNT_ROLLBACK, delta);
  }

  public long increamentReadCount() {
    return increment(COUNT_READ);
  }

  public long incrementDroppedCount() {
    return increment(COUNTER_DROP);
  }

  public long incrementRetryCount() {
    return increment(COUNTER_INMEMORY_RETRIES);
  }

  public long incrementDeadLetterCount() {
    return increment(COUNTER_DEAD_LETTER);
  }

  @Override
  public long getEventGetTimer() {
    return get(TIMER_EVENT_GET);
  }

  @Override
  public long getEventSendTimer() {
    return get(TIMER_EVENT_SEND);
  }

  @Override
  public long getCommitTimer() {
    return get(TIMER_COMMIT);
  }

  @Override
  public long getRollbackTimer() {
    return get(TIMER_ROLLBACK);
  }

  @Override
  public long getRollbackCount() {
    return get(COUNT_ROLLBACK);
  }

  @Override
  public long getReadCount() {
    return get(COUNT_READ);
  }

  @Override
  public long getRetryCount() {
    return get(COUNTER_INMEMORY_RETRIES);
  }

  @Override
  public long getDropCount() {
    return get(COUNTER_DROP);
  }

  @Override
  public long getDeadLetterCount() {
    return get(COUNTER_DEAD_LETTER);
  }
}
