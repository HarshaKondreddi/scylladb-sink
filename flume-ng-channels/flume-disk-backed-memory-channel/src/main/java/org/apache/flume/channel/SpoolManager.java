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
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.flume.channel.Configuration.BYTES_PER_MB;
import static org.apache.flume.channel.Configuration.DEFAULT_LOG_FILENAME;
import static org.apache.flume.channel.Configuration.DEFAULT_ROLLED_LOG_FILENAME_PATTERN;
import static org.apache.flume.channel.Configuration.DEFAULT_SPOOL_DISK_CAPACITY_MB;
import static org.apache.flume.channel.Configuration.DEFAULT_SPOOL_DISK_CHECKER_INTERVAL;
import static org.apache.flume.channel.Configuration.DEFAULT_SPOOL_MAX_FILE_SIZE_MB;
import static org.apache.flume.channel.Configuration.DEFAULT_SPOOL_MIN_DISK_FREE_MB;
import static org.apache.flume.channel.Configuration.DEFAULT_SPOOL_WORKERS;
import static org.apache.flume.channel.Configuration.SPOOL_DIRS;
import static org.apache.flume.channel.Configuration.SPOOL_DISK_CAPACITY_MB;
import static org.apache.flume.channel.Configuration.SPOOL_DISK_CHECKER_INTERVAL;
import static org.apache.flume.channel.Configuration.SPOOL_DISK_MIN_FREE_MB;
import static org.apache.flume.channel.Configuration.SPOOL_LOCK_FILENAME;
import static org.apache.flume.channel.Configuration.SPOOL_MAX_FILE_SIZE_MB;
import static org.apache.flume.channel.Configuration.SPOOL_WORKERS;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.RolloverFailure;
import ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy;
import ch.qos.logback.core.util.FileSize;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.LoggerFactory;

public class SpoolManager extends AbstractWorker {
  private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SpoolManager.class);

  private DiskBackedMemoryChannel channel;
  private final BlockingQueue<File> spooledFiles;

  private Integer maxLogFileSizeMB;
  private Integer spoolDiskCapacityMB;
  private Integer spoolMinDiskFreeMB;
  private LoggerContext loggerContext;
  private List<Wrapper> logWrappers = Lists.newArrayList();
  private BlockingQueue<Wrapper> activeLoggers = Queues.newLinkedBlockingQueue();
  private List<SpoolWorker> workers = Lists.newArrayList();
  private Integer diskCheckerInterval;
  private Timer spoolTimer;

  public SpoolManager(DiskBackedMemoryChannel channel, BlockingQueue<File> spooledFiles) {
    super("spool-manager for " + channel.getName(), LOGGER);
    this.channel = channel;
    this.spooledFiles = spooledFiles;
  }

  @Override
  public void doStart() {
    super.doStart();
    for (SpoolWorker worker : workers) {
      worker.doStart();
    }
  }

  @Override
  public void doTask() {
    // noop
  }

  @Override
  public void doStop() {
    super.doStop();

    // stop workers
    for (SpoolWorker worker : workers) {
      worker.doStop();
    }

    for (SpoolWorker worker : workers) {
      worker.awaitTermination();
    }

    // release locks and close dirLock channels
    for (Wrapper logWrapper : logWrappers) {
      try {
        logWrapper.dirLock.close();
        logWrapper.dirLockChannel.close();
        FileUtils.deleteQuietly(logWrapper.lockFile);
      } catch (Exception e) {
        LOGGER.warn("Error while releasing lock for directory {}", logWrapper.directory, e);
      }

      logWrapper.log.getAppender(logWrapper.directory.getAbsolutePath()).stop();
    }

    logWrappers.clear();
  }

  @Override
  public void configure(Context context) {
    maxLogFileSizeMB = getInteger(context, SPOOL_MAX_FILE_SIZE_MB,
        DEFAULT_SPOOL_MAX_FILE_SIZE_MB, 1, Integer.MAX_VALUE);
    spoolDiskCapacityMB = getInteger(context, SPOOL_DISK_CAPACITY_MB,
        DEFAULT_SPOOL_DISK_CAPACITY_MB, 2 * maxLogFileSizeMB, Integer.MAX_VALUE);
    spoolMinDiskFreeMB = getInteger(context, SPOOL_DISK_MIN_FREE_MB,
        DEFAULT_SPOOL_MIN_DISK_FREE_MB, 2 * maxLogFileSizeMB, Integer.MAX_VALUE);

    String dirsStr = context.getString(SPOOL_DIRS);
    if (dirsStr == null || dirsStr.trim().length() == 0) {
      LOGGER.warn("{} is not configured which is mandatory", SPOOL_DIRS);
      throw new ConfigurationException(SPOOL_DIRS +
          " is not configured which is mandatory when spooling is enabled.");
    }

    loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
    for (String dir : Sets.newHashSet(Splitter.on(",").trimResults().split(dirsStr))) {
      try {
        logWrappers.add(initLogger(dir));
      } catch (IOException ioe) {
        LOGGER.warn("Unable to use directory {}. Reason: {}", dir, ioe);
      }
    }

    if (logWrappers.isEmpty()) {
      throw new FlumeException(String.format("None of the configured directories are usable. %s=%s",
          SPOOL_DIRS, dirsStr));
    }
    activeLoggers = new LinkedBlockingQueue<Wrapper>(logWrappers);

    diskCheckerInterval = getInteger(context, SPOOL_DISK_CHECKER_INTERVAL,
        DEFAULT_SPOOL_DISK_CHECKER_INTERVAL, 1, Integer.MAX_VALUE);
    spoolTimer = new Timer("spool-task-scheduler for " + channel.getName());
    spoolTimer.schedule(new DiskSpaceChecker(), 0, TimeUnit.SECONDS.toMillis(diskCheckerInterval));
    // force rollover by sending a dummy event every minute.
    spoolTimer.schedule(new DummyEventLogger(), DateUtils.ceiling(new Date(), Calendar.MINUTE),
        TimeUnit.MINUTES.toMillis(1));

    int numWorkers = getInteger(context, SPOOL_WORKERS, DEFAULT_SPOOL_WORKERS,
        logWrappers.size(), Integer.MAX_VALUE);

    for (int i = 0; i < numWorkers; i++) {
      SpoolWorker worker = new SpoolWorker(i, channel, activeLoggers);
      worker.configure(context);
      workers.add(worker);
    }
  }

  private Wrapper initLogger(String dir) throws IOException {
    File directory = new File(dir);
    File lockFile = new File(directory, SPOOL_LOCK_FILENAME);
    FileLock dirLock = createAndLockDir(lockFile);

    LOGGER.info("starting spool logger for directory {}", directory);

    PatternLayoutEncoder ple = new PatternLayoutEncoder();
    RollingPolicy rollingPolicy = new RollingPolicy();
    RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();

    ple.setContext(loggerContext);
    ple.setPattern("%msg%n");

    rollingPolicy.setContext(loggerContext);
    rollingPolicy.setMaxFileSize(new FileSize(maxLogFileSizeMB * BYTES_PER_MB));
    rollingPolicy.setFileNamePattern(new File(dir, DEFAULT_ROLLED_LOG_FILENAME_PATTERN).getAbsolutePath());
    rollingPolicy.setParent(appender);

    appender.setContext(loggerContext);
    appender.setName(directory.getAbsolutePath());
    appender.setFile(new File(dir, DEFAULT_LOG_FILENAME).getAbsolutePath());
    appender.setEncoder(ple);
    appender.setRollingPolicy(rollingPolicy);

    ple.start();
    rollingPolicy.start();
    appender.start();

    Logger logger = (Logger) LoggerFactory.getLogger(dir);
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    logger.setAdditive(false);

    LOGGER.info("started spool logger for directory {}", directory);

    Wrapper wrapper = new Wrapper();
    wrapper.directory = directory;
    wrapper.log = logger;
    wrapper.lockFile = lockFile;
    wrapper.dirLockChannel = dirLock.channel();
    wrapper.dirLock = dirLock;

    return wrapper;
  }

  static class Wrapper {
    Logger log;
    File directory;
    File lockFile;
    FileChannel dirLockChannel;
    FileLock dirLock;
    volatile boolean active;
  }

  class DiskSpaceChecker extends TimerTask {
    @Override
    public void run() {
      try {
        for (Wrapper wrapper : logWrappers) {
          Long usableMb = wrapper.directory.getUsableSpace() / BYTES_PER_MB;
          Long sizeMb = FileUtils.sizeOfDirectory(wrapper.directory) / BYTES_PER_MB;
          if (sizeMb >= spoolDiskCapacityMB || usableMb <= spoolMinDiskFreeMB) {
            if (wrapper.active == true) {
              LOGGER.warn("marking the directory {} unusable. " +
                      "spool directory current size: {}MB, spool directory capacity: {}MB, " +
                      "spool directory current usable space: {}MB, spool directory minimum free space: {}MB",
                  wrapper.directory.getAbsolutePath(), sizeMb, spoolDiskCapacityMB, usableMb, spoolDiskCapacityMB);
              wrapper.active = false;
              activeLoggers.remove(wrapper);
            }
          } else {
            if (wrapper.active == false) {
              LOGGER.info("marking the directory {} usable. " +
                      "spool directory current size: {}MB, spool directory capacity: {}MB, " +
                      "spool directory current usable space: {}MB, spool directory minimum free space: {}MB",
                  wrapper.directory.getAbsolutePath(), sizeMb, spoolDiskCapacityMB, usableMb, spoolDiskCapacityMB);
              wrapper.active = true;
              if (activeLoggers.contains(wrapper) == false) {
                activeLoggers.add(wrapper);
              }
            }
          }
        }
      } catch (Throwable t) {
        LOGGER.warn("error while checking disk space for channel {}" + channel.getName());
      }
    }
  }

  // log a dummy event
  class DummyEventLogger extends TimerTask {
    @Override
    public void run() {
      LOGGER.debug("forcing a time-based rollover");
      for (Wrapper wrapper : logWrappers) {
        wrapper.log.info("");
      }
    }
  }

  class RollingPolicy<T> extends SizeAndTimeBasedRollingPolicy<T> {
    @Override
    public void rollover() throws RolloverFailure {
      super.rollover();
      File fileFound = new File(getTimeBasedFileNamingAndTriggeringPolicy().getElapsedPeriodsFileName());
      LOGGER.debug("new spool file found. file: {}", fileFound);
      spooledFiles.add(fileFound);
      channel.getChannelCounter().incrementSpoolFilesCount();
    }
  }
}
