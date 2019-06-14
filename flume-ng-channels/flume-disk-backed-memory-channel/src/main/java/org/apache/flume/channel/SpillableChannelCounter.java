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
package org.apache.flume.channel;

import org.apache.flume.instrumentation.ChannelCounter;

public class SpillableChannelCounter extends ChannelCounter implements SpillableChannelCounterMBean {

  private static final String SPOOL_FILES_COUNT = "channel.spillable.spool.files";
  private static final String DESPOOL_FILES_COUNT = "channel.spillable.despool.files";
  private static final String DESPOOL_FILE_FAILURES_COUNT = "channel.spillable.despool.file.failures";
  private static final String DESPOOL_EVENT_FAILURES_COUNT = "channel.spillable.despool.event.failures";
  private static final String SPOOL_EVENTS_COUNT = "channel.spillable.spool.events";
  private static final String DESPOOL_EVENTS_COUNT = "channel.spillable.despool.events";
  private static final String DESPOOL_EVENTS_TX_RETRIES_COUNT = "channel.spillable.despool.txretries";
  private static final String DESPOOL_DUPLICATES_COUNT = "channel.spillable.despool.duplicates";

  private static final String[] ATTRIBUTES = {SPOOL_FILES_COUNT, DESPOOL_FILES_COUNT, SPOOL_EVENTS_COUNT,
      DESPOOL_EVENTS_COUNT, DESPOOL_FILE_FAILURES_COUNT, DESPOOL_EVENT_FAILURES_COUNT,
      DESPOOL_EVENTS_TX_RETRIES_COUNT, DESPOOL_DUPLICATES_COUNT};

  public SpillableChannelCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public long addToSpoolFilesCount(long delta) {
    return addAndGet(SPOOL_FILES_COUNT, delta);
  }

  public long incrementSpoolFilesCount() {
    return increment(SPOOL_FILES_COUNT);
  }

  public long incrementTxRetryCount() {
    return increment(DESPOOL_EVENTS_TX_RETRIES_COUNT);
  }

  public long incrementDespoolDuplicatesCount(long count) {
    return addAndGet(DESPOOL_DUPLICATES_COUNT, count);
  }

  @Override
  public long getDespoolDuplicatesCount() {
    return get(DESPOOL_DUPLICATES_COUNT);
  }

  @Override
  public long getTxRetriesCount() {
    return get(DESPOOL_EVENTS_TX_RETRIES_COUNT);
  }

  @Override
  public long getSpoolFilesCount() {
    return get(SPOOL_FILES_COUNT);
  }

  public long incrementDespoolFilesCount() {
    return increment(DESPOOL_FILES_COUNT);
  }

  @Override
  public long getDespoolFilesCount() {
    return get(DESPOOL_FILES_COUNT);
  }

  public long incrementDespoolFileFailuresCount() {
    return increment(DESPOOL_FILE_FAILURES_COUNT);
  }

  @Override
  public long getDespoolFileFailuresCount() {
    return get(DESPOOL_FILE_FAILURES_COUNT);
  }

  public long incrementDespoolEventFailuresCount() {
    return increment(DESPOOL_EVENT_FAILURES_COUNT);
  }

  @Override
  public long getDespoolEventFailuresCount() {
    return get(DESPOOL_EVENT_FAILURES_COUNT);
  }

  public long incrementSpoolEventsCount() {
    return increment(SPOOL_EVENTS_COUNT);
  }

  public long addToSpoolEventsCount(long delta) {
    return addAndGet(SPOOL_EVENTS_COUNT, delta);
  }

  @Override
  public long getSpoolEventsCount() {
    return get(SPOOL_EVENTS_COUNT);
  }

  public long incrementDespoolEventsCount() {
    return increment(DESPOOL_EVENTS_COUNT);
  }

  public long addToDespoolEventsCount(long delta) {
    return addAndGet(DESPOOL_EVENTS_COUNT, delta);
  }

  @Override
  public long getDespoolEventsCount() {
    return get(DESPOOL_EVENTS_COUNT);
  }
}
