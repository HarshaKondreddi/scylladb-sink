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
import java.io.FilenameFilter;
import java.util.Comparator;
import java.util.regex.Pattern;

public class Configuration {
  public static final String CAPACITY = "capacity";
  public static final String TXN_CAPACITY = "transactionCapacity";
  public static final String KEEP_ALIVE = "keep-alive";

  public static final String SPOOL_ONLY_ON_SHUTDOWN = "spoolOnlyOnShutdown";
  public static final String SPOOL_DIRS = "spoolDirectories";
  public static final String SPOOL_CHECK_INTERVAL_MS = "spoolCheckIntervalMs";
  public static final String DESPOOL_CHECK_INTERVAL_MS = "despoolCheckIntervalMs";
  public static final String SPOOL_MAX_FILE_SIZE_MB = "spoolMaxFileSizeMB";
  public static final String SPOOL_DISK_CAPACITY_MB = "spoolDiskCapacityMB";
  public static final String SPOOL_DISK_MIN_FREE_MB = "spoolMinDiskFreeMB";
  public static final String SPOOL_DISK_CHECKER_INTERVAL = "spoolDiskCheckerInterval";
  public static final String SPOOL_WORKERS = "spoolWorkers";
  public static final String DESPOOL_WORKERS = "despoolWorkers";
  public static final String SPOOL_THRESHOLD_PCT = "spoolThresholdPct";
  public static final String DESPOOL_THRESHOLD_PCT = "despoolThresholdPct";

  public static final Integer DEFAULT_CAPACITY = 100;
  public static final Integer DEFAULT_TXN_CAPACITY = 100;
  public static final double byteCapacitySlotSize = 100;
  public static final Long defaultByteCapacity = (long) (Runtime.getRuntime().maxMemory() * .80);
  public static final Integer DEFAULT_BYTE_CAPACITY_BUFFER_PCT = 20;
  public static final Integer DEFAULT_KEEP_ALIVE = 3;

  public static final Boolean DEFAULT_SPOOL_ONLY_ON_SHUTDOWN = false;
  public static final Integer DEFAULT_SPOOL_MAX_FILE_SIZE_MB = 64;
  public static final Integer DEFAULT_SPOOL_DISK_CAPACITY_MB = 2048;
  public static final Integer DEFAULT_SPOOL_MIN_DISK_FREE_MB = 512;
  public static final Integer DEFAULT_SPOOL_DISK_CHECKER_INTERVAL = 30;
  // spool check interval needs to aggressive to prevent queue getting full
  public static final Integer DEFAULT_SPOOL_CHECK_INTERVAL_MS = 50;
  // despool check interval needs to be lazy enough to prevent to prevent despool-spool chain again
  public static final Integer DEFAULT_DESPOOL_CHECK_INTERVAL_MS = 5000;
  public static final Integer DEFAULT_TX_FAIL_RETRY_INTERVAL_MS = 200;

  public static final Integer DEFAULT_SPOOL_WORKERS = 3;
  public static final Integer DEFAULT_DESPOOL_WORKERS = 3;
  public static final Integer DEFAULT_SPOOL_THRESHOLD_PCT = 80;
  public static final Integer DEFAULT_DESPOOL_THRESHOLD_PCT = 40;
  public static final Integer DEFAULT_DESPOOL_PAUSE_INTERVAL_MS = 100;

  public static final String DEFAULT_LOG_FILENAME = "message.log";
  public static final String DEFAULT_ROLLED_LOG_FILENAME_PATTERN = "message-%d{yyyyMMddHHmm, UTC}-%i.log";
  public static final Pattern DEFAULT_ROLLED_LOG_FILENAME_REGEX = Pattern.compile("message-.*.log");

  public static final String SPOOL_LOCK_FILENAME = ".spool.in_use";
  public static final String DESPOOL_LOCK_FILENAME = ".despool.in_use";

  public static final Integer BYTES_PER_KB = 1024;
  public static final Long BYTES_PER_MB = 1024 * 1024L;
  public static final Integer DEFAULT_POLL_INTERVAL_MS = 500;
  public static final Integer SPOOL_WORKER_MEM_KB = 64;
  public static final Integer DESPOOL_WORKER_MEM_KB = 64;

  public static final Comparator<File> SPOOL_FILE_COMPARATOR = Comparator.comparing(File::getName);

  public static final FilenameFilter SPOOL_FILE_FILTER = (dir, name)
      -> DEFAULT_ROLLED_LOG_FILENAME_REGEX.matcher(name).matches();
}
