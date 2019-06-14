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
package org.apache.flume.source.amqp;

import org.slf4j.Logger;

public abstract class AbstractWorker extends Thread implements Worker {

  protected Logger logger;

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
  public void doStop() {
    logger.info("stopping::{}", this.getName());
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
}
