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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.flume.channel.Configuration.BYTES_PER_KB;
import static org.apache.flume.channel.Configuration.DEFAULT_DESPOOL_CHECK_INTERVAL_MS;
import static org.apache.flume.channel.Configuration.DEFAULT_DESPOOL_PAUSE_INTERVAL_MS;
import static org.apache.flume.channel.Configuration.DEFAULT_DESPOOL_THRESHOLD_PCT;
import static org.apache.flume.channel.Configuration.DEFAULT_SPOOL_THRESHOLD_PCT;
import static org.apache.flume.channel.Configuration.DEFAULT_TX_FAIL_RETRY_INTERVAL_MS;
import static org.apache.flume.channel.Configuration.DESPOOL_CHECK_INTERVAL_MS;
import static org.apache.flume.channel.Configuration.DESPOOL_THRESHOLD_PCT;
import static org.apache.flume.channel.Configuration.DESPOOL_WORKER_MEM_KB;
import static org.apache.flume.channel.Configuration.SPOOL_THRESHOLD_PCT;

import org.apache.commons.io.FileUtils;
import org.apache.flume.ChannelFullException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DespoolWorker extends AbstractWorker {
  private static Logger LOGGER = LoggerFactory.getLogger(DespoolWorker.class);

  private DiskBackedMemoryChannel channel;
  private BlockingQueue<File> filesToDespool;

  private int despoolCheckIntervalMs;
  private int spoolThresholdPct;
  private int despoolThresholdPct;
  private int despoolStartSize;
  private int despoolEndSize;

  private byte[] auxBuffer;
  private ByteBuffer buffer;
  private ByteArrayInputStream bais;
  private SpoolEventDeserializer deser;

  public DespoolWorker(int id, DiskBackedMemoryChannel channel, BlockingQueue<File> filesToDespool) {
    super("despool-worker# " + id + " for " + channel.getName(), LOGGER);
    this.channel = channel;
    this.filesToDespool = filesToDespool;
  }

  @Override
  public void configure(Context context) {
    long capacity = channel.getCapacity();

    despoolCheckIntervalMs = getInteger(context, DESPOOL_CHECK_INTERVAL_MS,
        DEFAULT_DESPOOL_CHECK_INTERVAL_MS, 1000, Integer.MAX_VALUE);
    spoolThresholdPct = getInteger(context, SPOOL_THRESHOLD_PCT, DEFAULT_SPOOL_THRESHOLD_PCT, 1, 100);
    despoolThresholdPct = getInteger(context, DESPOOL_THRESHOLD_PCT, DEFAULT_DESPOOL_THRESHOLD_PCT, 1, 100);

    if (despoolThresholdPct >= spoolThresholdPct) {
      throw new ConfigurationException("despool threshold cannot be greater than spool threshold");
    }

    despoolStartSize = (int) (capacity * despoolThresholdPct / 100.0);
    despoolEndSize = (int) (capacity * (spoolThresholdPct + despoolThresholdPct) / (100.0 * 2));

    LOGGER.info("queue capacity: {}, despooling start threshold: {}, despooling end threshold: {}," +
        " despool check interval: {}", capacity, despoolStartSize, despoolEndSize, despoolCheckIntervalMs);

    buffer = ByteBuffer.allocate(DESPOOL_WORKER_MEM_KB * BYTES_PER_KB);
    auxBuffer = new byte[buffer.capacity()];

    bais = new ByteArrayInputStream(buffer.array());
    deser = new SpoolEventDeserializer.Builder().build(context, bais);
  }

  @Override
  public int getInterval() {
    return despoolCheckIntervalMs;
  }

  @Override
  public void doTask() throws Exception {

    if (channel.getSize() < despoolStartSize && filesToDespool.size() > 0) {
      despool(despoolEndSize, filesToDespool.take());
    } else {
      LOGGER.debug("queue size: {}, files pending to despool: {}, going to sleep for: {}",
          channel.getSize(), filesToDespool.size(), getInterval());
    }
  }

  private void despool(int endSize, File file) throws Exception {
    String line;
    int successCount = 0, errorCount = 0, bytes;
    boolean success = false;
    Transaction tx = null;
    int currentTxEventCnt = 0;
    int retryCnt = 0;
    boolean putSuccess = false;

    LOGGER.debug("despooling started. file: {}, queue size: {}, end goal: {}",
        file.getAbsolutePath(), channel.getSize(), endSize);

    try (final BufferedReader brd = new BufferedReader(new FileReader(file), 64 * BYTES_PER_KB)) {
      tx = channel.getTransaction();
      tx.begin();
      // TODO queue check (queue.size() < endSize)
      // TODO empty line used as marker. skip it.

      while ((line = brd.readLine()) != null) {
        if (line.length() == 0) {
          continue;
        }
        bais.reset();
        buffer.clear();
        try {
          bytes = Base64.getDecoder().decode(line.getBytes(), auxBuffer);
          buffer.put(auxBuffer, 0, bytes);
          Event event = deser.readEvent();

          retryCnt = 0;
          putSuccess = false;

          while (!putSuccess) {
            try {
              while (channel.getSize() >= endSize) {
                logger.debug("channel size reached threshold {}. despool will be paused for {} ms", endSize,
                    DEFAULT_DESPOOL_PAUSE_INTERVAL_MS);
                Thread.sleep(DEFAULT_DESPOOL_PAUSE_INTERVAL_MS);
              }
              channel.put(event);
              putSuccess = true;
            } catch (ChannelFullException ce) {
              logger.warn("failed to put in channel. will retry after {} ms. retry#: {}, error: {}",
                  DEFAULT_TX_FAIL_RETRY_INTERVAL_MS, retryCnt, ce);
              retryCnt++;
              Thread.sleep(DEFAULT_TX_FAIL_RETRY_INTERVAL_MS);
            }
          }
          currentTxEventCnt++;

        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          LOGGER.warn("interrupted while despool in progress for file: {}. may result in duplicates : {}",
              file, successCount - currentTxEventCnt);
          throw ie;
        } catch (Exception e) {
          errorCount++;
          channel.getChannelCounter().incrementDespoolEventFailuresCount();
        }
        successCount++;

        if (currentTxEventCnt >= channel.getTxCapacity()) {
          retryCommit(tx);
          tx = null;
          channel.getChannelCounter().addToDespoolEventsCount(currentTxEventCnt);
          currentTxEventCnt = 0;
          tx = channel.getTransaction();
          tx.begin();
        }
      }

      retryCommit(tx);
      tx = null;
      success = true;
      channel.getChannelCounter().addToDespoolEventsCount(currentTxEventCnt);
    } finally {
      if (success) {
        boolean deleteStatus = FileUtils.deleteQuietly(file);
        channel.getChannelCounter().incrementDespoolFilesCount();
        if (errorCount > 0) {
          LOGGER.warn("despooling completed. file: {}, queue size: {}, success count: {}," +
                  "error count: {}, delete status: {}.",
              file.getAbsolutePath(), channel.getSize(), successCount, errorCount, deleteStatus);
        } else {
          LOGGER.debug("despooling completed. file: {}, queue size: {}, success count: {}, " +
                  "error count: {}, delete status: {}.",
              file.getAbsolutePath(), channel.getSize(), successCount, errorCount, deleteStatus);
        }
      } else {
        channel.getChannelCounter().incrementDespoolFileFailuresCount();
        LOGGER.warn("error while despooling file: {}, queue size: {}, success count: {}, " +
                "error count: {}. will retry to despool it.",
            file.getAbsolutePath(), channel.getSize(), successCount - currentTxEventCnt, errorCount);
        filesToDespool.add(file);
        channel.getChannelCounter().incrementDespoolDuplicatesCount(successCount - currentTxEventCnt);
      }
      if (tx != null) {
        if (!success) {
          tx.rollback();
        }
        tx.close();
      }
    }
  }

  private void retryCommit(Transaction transaction) throws InterruptedException {
    int counter = 0;
    while (true) {
      try {
        transaction.commit();
        transaction.close();
        return;
      } catch (ChannelFullException cfe) {
        logger.error("Error while committing transaction in DespoolWorker, retying {} ", ++counter, cfe);
        channel.getChannelCounter().incrementTxRetryCount();
        TimeUnit.MILLISECONDS.sleep(DEFAULT_TX_FAIL_RETRY_INTERVAL_MS);
      }
    }
  }
}
