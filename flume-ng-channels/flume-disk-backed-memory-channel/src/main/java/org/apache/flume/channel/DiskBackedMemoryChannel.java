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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import static org.apache.flume.channel.Configuration.SPOOL_FILE_COMPARATOR;

import org.apache.flume.Context;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.annotations.Recyclable;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * DiskBackedMemoryChannel is the recommended channel to use when both speed
 * and durability of data is required.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Recyclable
public class DiskBackedMemoryChannel extends MemoryChannel {
  private static Logger LOGGER = LoggerFactory.getLogger(DiskBackedMemoryChannel.class);

  private BlockingQueue<File> spooledFiles;
  private SpoolManager spoolManager;
  private DespoolManager despoolManager;

  public DiskBackedMemoryChannel() {
    super();
  }

  @Override
  public void configure(Context context) {
    super.configure(context);

    spooledFiles = new PriorityBlockingQueue<>(64, SPOOL_FILE_COMPARATOR);

    despoolManager = new DespoolManager(this, spooledFiles);
    despoolManager.configure(context);

    spoolManager = new SpoolManager(this, spooledFiles);
    spoolManager.configure(context);
  }

  @Override
  public synchronized void start() {
    super.start();
    despoolManager.doStart();
    spoolManager.doStart();
  }

  @Override
  public synchronized void stop() {
    try {
      despoolManager.doStop();
    } catch (Exception e) {
      LOGGER.warn("error occurred while stopping despool-manager", e);
    }

    try {
      spoolManager.doStop();
    } catch (Exception e) {
      LOGGER.warn("error occurred while stopping spool-manager", e);
    }
    super.stop();
  }

  protected long getSize() {
    return channelCounter.getChannelSize();
  }

  protected long getCapacity() {
    return capacity;
  }

  protected long getTxCapacity() {
    return transCapacity;
  }

  protected SpillableChannelCounter getChannelCounter() {
    return (SpillableChannelCounter) channelCounter;
  }

  @Override
  public ChannelCounter createCounter() {
    return new SpillableChannelCounter(getName());
  }
}
