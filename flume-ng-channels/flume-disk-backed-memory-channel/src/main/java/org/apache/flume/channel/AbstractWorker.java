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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import static org.apache.flume.channel.Configuration.DEFAULT_POLL_INTERVAL_MS;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;

public abstract class AbstractWorker extends Thread implements Worker {
  private static final int INTERVAL = 500;

  Logger logger;
  volatile boolean running = true;

  public AbstractWorker(String name, Logger logger) {
    this.setName(name);
    this.logger = logger;
  }

  @Override
  public void doStart() {
    logger.info("starting::{}", this.getName());
    this.start();
  }

  @Override
  public void run() {
    while (running) {
      try {
        Thread.sleep(getInterval());
        doTask();
      } catch (InterruptedException ie) {
        // expected on shutdown. noop
      } catch (Throwable e) {
        // don't want to shutdown in any error unless asked to
        logger.warn("error occurred in {}", this.getName(), e);
      }
    }
  }

  public int getInterval() {
    return DEFAULT_POLL_INTERVAL_MS;
  }

  @Override
  public void doStop() {
    logger.info("stopping::{}", this.getName());
    running = false;
    this.interrupt();
  }

  @Override
  public void awaitTermination() {
    try {
      this.join();
    } catch (Exception e) {
      logger.warn("error occurred while waiting to stop: {}", this.getName(), e);
    }

    logger.info("stopped::{}", this.getName());
  }

  public abstract void doTask() throws Exception;

  Integer getInteger(Context ctx, String key, Integer defaultValue, Integer minValue, Integer maxValue) {
    int value;
    try {
      value = ctx.getInteger(key, defaultValue);
    } catch (NumberFormatException e) {
      value = defaultValue;
      logger.warn("invalid {} specified, initializing to default value {}", key, defaultValue);
    }

    if (value < minValue) {
      value = minValue;
      logger.warn("{} is out of range, initializing to minimum {}", key, minValue);
    }

    if (value > maxValue) {
      value = maxValue;
      logger.warn("{} is out of range, initializing to maximum {}", key, maxValue);
    }

    return value;
  }

  FileLock createAndLockDir(File lockFile) throws IOException {

    File directory = lockFile.getParentFile();

    FileUtils.forceMkdir(directory);

    if (!(directory.canExecute() && directory.canRead() && directory.canWrite())) {
      throw new IOException(String.format("configured spool directory %s doesn't have " +
          "one or many required permissions(rwx).", directory));
    }

    FileChannel dirLockChannel = new RandomAccessFile(lockFile, "rw").getChannel();
    FileLock dirLock = dirLockChannel.tryLock();

    if (dirLock == null) {
      throw new IOException(String.format("Unable to lock the directory %s. " +
          "May be used by some other program.", directory));
    }

    return dirLock;
  }

  public ChannelCounter createCounter() {
    return new ChannelCounter(getName());
  }
}
