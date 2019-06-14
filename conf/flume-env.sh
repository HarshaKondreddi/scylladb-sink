#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or impslied.
# See the License for the specific language governing permissions and
# limitations under the License.

# If this file is placed at FLUME_CONF_DIR/flume-env.sh, it will be sourced
# during Flume startup.

# Enviroment variables can be set here.

# export JAVA_HOME=/usr/lib/jvm/java-6-sun

export CURRENT_USER=$(whoami)
export FLUME_ROOT_LOG_LEVEL=INFO
export FLUME_LOGS_HOME=/var/log/flume
export FLUME_LOGS_BACKUP_HOME=$FLUME_LOGS_HOME
export FLUME_PID_DIR=$FLUME_LOGS_HOME

export MIN_HEAPSIZE=256M
export MAX_HEAPSIZE=512M
export MAX_METASPACE_SIZE=64M
export MAX_DIRECT_MEMORY_SIZE=64M

export FLUME_HTTP_PORT=2510
export FLUME_JMX_PORT=2520

export FLUME_STOP_TIMEOUT=120

#Set it to true if using Hdfs Sink
export HADOOP_REQUIRED=false

# Give Flume more memory and pre-allocate
export JAVA_OPTS="-Xms$MIN_HEAPSIZE -Xmx$MAX_HEAPSIZE -XX:MaxMetaspaceSize=$MAX_METASPACE_SIZE -XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY_SIZE -XX:+UseG1GC -XX:+PrintFlagsFinal"

# GC logs and rotation
export JAVA_OPTS="$JAVA_OPTS -Xloggc:$FLUME_LOGS_HOME/flume-$CURRENT_USER-gc.log -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=10M"

# enable remote monitoring via JMX
export JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$FLUME_JMX_PORT -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

# enable web metrics
export JAVA_OPTS="$JAVA_OPTS -Dflume.monitoring.type=http -Dflume.monitoring.port=$FLUME_HTTP_PORT"

# Flume logs settings.
export JAVA_OPTS="$JAVA_OPTS -Dflume.root.log.level=$FLUME_ROOT_LOG_LEVEL -Dflume.log.dir=$FLUME_LOGS_HOME -Dflume.log.backup.dir=$FLUME_LOGS_BACKUP_HOME"

export HDP_VERSION=current
export LD_LIBRARY_PATH=/usr/hdp/$HDP_VERSION/hadoop-client/lib/native:/usr/hdp/$HDP_VERSION/hadoop-client/lib/native/Linux-amd64-64
export JAVA_OPTS="$JAVA_OPTS -Djava.library.path=$LD_LIBRARY_PATH"

# Note that the Flume conf directory is always included in the classpath.
#FLUME_CLASSPATH=""
