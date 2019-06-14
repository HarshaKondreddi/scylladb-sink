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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.apache.flume.channel.Configuration.DEFAULT_DESPOOL_WORKERS;
import static org.apache.flume.channel.Configuration.DESPOOL_LOCK_FILENAME;
import static org.apache.flume.channel.Configuration.DESPOOL_WORKERS;
import static org.apache.flume.channel.Configuration.SPOOL_DIRS;
import static org.apache.flume.channel.Configuration.SPOOL_FILE_FILTER;

public class DespoolManager extends AbstractWorker {
  private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DespoolManager.class);

  private DiskBackedMemoryChannel channel;
  private List<DespoolWorker> workers = Lists.newArrayList();
  private BlockingQueue<File> filesToDespool;

  private List<Wrapper> wrappers;

  public DespoolManager(DiskBackedMemoryChannel channel, BlockingQueue<File> spooledFiles) {
    super("despool-manager for " + channel.getName(), LOGGER);
    this.channel = channel;
    this.filesToDespool = spooledFiles;
  }

  @Override
  public void doStart() {
    super.doStart();
    for (DespoolWorker worker : workers) {
      worker.doStart();
    }
  }
  @Override
  public void doTask() throws Exception {
    //no-op
  }

  @Override
  public void doStop() {
    super.doStop();

    // stop workers
    for (DespoolWorker worker : workers) {
      worker.doStop();
    }

    for (DespoolWorker worker : workers) {
      worker.awaitTermination();
    }

    for (Wrapper wrapper : wrappers) {
      try {
        wrapper.dirLock.close();
        wrapper.dirLock.channel().close();
        FileUtils.deleteQuietly(wrapper.lockFile);
      } catch (Exception e) {
        LOGGER.warn("Error while releasing lock for directory {}", wrapper.dir, e);
      }
    }
  }

  @Override
  public void configure(Context context) {
    String dirsStr = context.getString(SPOOL_DIRS);
    if (dirsStr == null || dirsStr.trim().length() == 0) {
      LOGGER.warn("{} is not configured which is mandatory", SPOOL_DIRS);
      throw new ConfigurationException(SPOOL_DIRS + " is not configured which is mandatory when spooling is enabled.");
    }

    wrappers = Lists.newArrayList();

    for (String dir : Sets.newHashSet(Splitter.on(",").trimResults().split(dirsStr))) {
      try {
        wrappers.add(createWatcher(new File(dir)));
      } catch (IOException ioe) {
        LOGGER.warn("Unable to use directory {} to despool. Reason: {}", dir, ioe);
      }
    }

    if (wrappers.isEmpty()) {
      throw new FlumeException(String.format("None of the configured directories are usable. %s=%s",
          SPOOL_DIRS, dirsStr));
    }

    int numWorkers = getInteger(context, DESPOOL_WORKERS, DEFAULT_DESPOOL_WORKERS,
        wrappers.size(), Integer.MAX_VALUE);

    for (int i = 0; i < numWorkers; i++) {
      DespoolWorker worker = new DespoolWorker(i, channel, filesToDespool);
      worker.configure(context);
      workers.add(worker);
    }
  }

  private Wrapper createWatcher(File dir) throws IOException {
    File lockFile = new File(dir, DESPOOL_LOCK_FILENAME);
    FileLock dirLock = createAndLockDir(lockFile);

    File[] files = dir.listFiles(SPOOL_FILE_FILTER);
    LOGGER.info("despooling {} existing files from directory {}", files.length, dir);
    filesToDespool.addAll(Arrays.asList(files));
    channel.getChannelCounter().addToSpoolFilesCount(files.length);
    Wrapper wrapper = new Wrapper();
    wrapper.dir = dir;
    wrapper.lockFile = lockFile;
    wrapper.dirLock = dirLock;

    return wrapper;
  }

  class Wrapper {
    File dir;
    File lockFile;
    FileLock dirLock;
  }
}
