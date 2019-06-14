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

import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.apache.flume.channel.Configuration.BYTES_PER_KB;
import static org.apache.flume.channel.Configuration.DEFAULT_DESPOOL_THRESHOLD_PCT;
import static org.apache.flume.channel.Configuration.DEFAULT_SPOOL_CHECK_INTERVAL_MS;
import static org.apache.flume.channel.Configuration.DEFAULT_SPOOL_ONLY_ON_SHUTDOWN;
import static org.apache.flume.channel.Configuration.DEFAULT_SPOOL_THRESHOLD_PCT;
import static org.apache.flume.channel.Configuration.DESPOOL_THRESHOLD_PCT;
import static org.apache.flume.channel.Configuration.SPOOL_CHECK_INTERVAL_MS;
import static org.apache.flume.channel.Configuration.SPOOL_ONLY_ON_SHUTDOWN;
import static org.apache.flume.channel.Configuration.SPOOL_THRESHOLD_PCT;
import static org.apache.flume.channel.Configuration.SPOOL_WORKER_MEM_KB;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpoolWorker extends AbstractWorker {
  private static Logger LOGGER = LoggerFactory.getLogger(SpoolWorker.class);

  private DiskBackedMemoryChannel channel;
  private BlockingQueue<SpoolManager.Wrapper> activeLoggers;

  private boolean spoolOnlyOnShutdown;
  private int spoolCheckIntervalMs;
  private int spoolThresholdPct;
  private int despoolThresholdPct;
  private int spoolStartSize;
  private int spoolEndSize;
  private List<Event> batch = Lists.newArrayList();

  private ByteArrayOutputStream baos;
  private SpoolEventSerializer ser;

  public SpoolWorker(int id, DiskBackedMemoryChannel channel,
                     BlockingQueue<SpoolManager.Wrapper> activeLoggers) {
    super("spool-worker# " + id + " for " + channel.getName(), LOGGER);
    this.channel = channel;
    this.activeLoggers = activeLoggers;
  }

  @Override
  public void configure(Context context) {
    long capacity = channel.getCapacity();
    spoolOnlyOnShutdown = context.getBoolean(SPOOL_ONLY_ON_SHUTDOWN, DEFAULT_SPOOL_ONLY_ON_SHUTDOWN);
    if (spoolOnlyOnShutdown == true) {
      LOGGER.warn("spooling is disabled except on shutdown.");
    }

    spoolCheckIntervalMs = getInteger(context, SPOOL_CHECK_INTERVAL_MS, DEFAULT_SPOOL_CHECK_INTERVAL_MS, 50, 1000);
    spoolThresholdPct = getInteger(context, SPOOL_THRESHOLD_PCT, DEFAULT_SPOOL_THRESHOLD_PCT, 1, 100);
    despoolThresholdPct = getInteger(context, DESPOOL_THRESHOLD_PCT, DEFAULT_DESPOOL_THRESHOLD_PCT, 1, 100);

    if (despoolThresholdPct >= spoolThresholdPct) {
      throw new ConfigurationException("despool threshold cannot be greater than spool threshold");
    }

    spoolStartSize = (int) (capacity * spoolThresholdPct / 100.0);
    spoolEndSize = (int) (capacity * (spoolThresholdPct + despoolThresholdPct) / (100.0 * 2));

    baos = new ByteArrayOutputStream(SPOOL_WORKER_MEM_KB * BYTES_PER_KB);
    ser = (SpoolEventSerializer) new SpoolEventSerializer.Builder().build(context, baos);

    LOGGER.info("channel capacity: {}, spooling start threshold: {}, spooling end threshold: {}," +
        " spool check interval: {}", capacity, spoolStartSize, spoolEndSize, spoolCheckIntervalMs);
  }

  @Override
  public int getInterval() {
    return spoolCheckIntervalMs;
  }

  @Override
  public void doTask() throws Exception {
    if (spoolOnlyOnShutdown == false) {
      if (channel.getSize() >= spoolStartSize) {
        spool(spoolEndSize);
      }
    }
  }

  private void spool(int endSize) throws Exception {
    Transaction tx = null;
    int count = 0;
    Event event = null;
    boolean success = false;
    int currentTxEventCnt = 0;

    LOGGER.debug("spooling started. channel size: {}, end goal: {}", channel.getSize(), endSize);
    do {
      tx = channel.getTransaction();
      try {
        tx.begin();
        success = false;
        currentTxEventCnt = 0;

        batch.clear();
        for (int i = 0; i < channel.getTxCapacity(); i++) {
          event = channel.take();
          if (event == null) break;
          batch.add(event);
        }

        SpoolManager.Wrapper wrapper = null;
        // drop all the inactive loggers if found, spool manager will take care of adding them back.
        do {
          wrapper = activeLoggers.take();
        } while (wrapper.active == false);

        for (Event e : batch) {
          baos.reset();
          ser.write(e);
          ser.flush();
          wrapper.log.info(Base64.getEncoder().encodeToString(baos.toByteArray()));
          count++;
          currentTxEventCnt++;
        }

        activeLoggers.put(wrapper);

        tx.commit();
        channel.getChannelCounter().addToSpoolEventsCount(currentTxEventCnt);
        success = true;
      } finally {
        if (tx != null) {
          if (!success) {
            tx.rollback();
          }
          tx.close();
        }
      }
    } while (channel.getSize() > endSize);

    LOGGER.debug("spool completed. channel size: {}, event count: {}", channel.getSize(), count);
  }

  @Override
  public void awaitTermination() {
    super.awaitTermination();
    try {
      if (channel.getSize() > 0) {
        LOGGER.info("spooling the remaining events in channel before shutdown. channel size: {}, " +
            "spool worker name: {}", channel.getSize(), getName());
        spool(0);
      }
    } catch (Exception e) {
      LOGGER.warn("error while spooling during shutdown. possible loss if in-flight events");
    }
  }
}
