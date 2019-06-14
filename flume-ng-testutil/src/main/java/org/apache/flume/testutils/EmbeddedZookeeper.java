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
package org.apache.flume.testutils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedZookeeper implements Service {

  @Getter
  @Accessors(fluent = true)
  private int port;
  @Getter
  @Accessors(fluent = true)
  private File logDir;
  private ZooKeeperServer server;

  private TemporaryFolder temporaryFolder;
  private Logger log = LoggerFactory.getLogger(EmbeddedZookeeper.class);

  public EmbeddedZookeeper(int port) throws IOException {
    this.port = port;
    this.temporaryFolder = new TemporaryFolder();
    this.logDir = temporaryFolder.newFolder("zk");
  }

  @Override
  public int[] port() {
    return new int[]{port};
  }

  @Override
  public String name() {
    return "EmbeddedZookeeper";
  }

  /**
   * Start the embed Zookeeper cluster with the giver ports
   *
   * @return
   * @throws Exception
   */
  @Override
  public Service start() throws Exception {
    int tickTime = 5000;
    FileTxnSnapLog snapLog = new FileTxnSnapLog(new File(logDir.getAbsoluteFile().getPath()),
        new File(logDir.getAbsoluteFile().getPath()));
    server = new ZooKeeperServer(snapLog, tickTime, 60000, 60000,
        new ZooKeeperServer.BasicDataTreeBuilder(), new ZKDatabase(snapLog));
    NIOServerCnxnFactory factory = new NIOServerCnxnFactory();
    factory.configure(new InetSocketAddress(InetAddress.getLoopbackAddress(), port()[0]), 100);
    factory.startup(server);
    waitForZKServerUp(port()[0], TimeUnit.SECONDS.toSeconds(10));
    return this;
  }

  @Override
  public void close() throws Exception {
    if (null != server) {
      server.shutdown();
    }
    temporaryFolder.delete();
  }

  private boolean waitForZKServerUp(int port, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {

      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        log.error("Error sleeping", e);
      }

      try {
        BufferedReader reader = null;
        try (Socket sock = new Socket(InetAddress.getLoopbackAddress(), port)) {
          OutputStream outstream = sock.getOutputStream();
          outstream.write("stat".getBytes(Charset.forName("UTF-8")));
          outstream.flush();

          Reader isr = new InputStreamReader(sock.getInputStream(), Charset.forName("UTF-8"));
          reader = new BufferedReader(isr);
          String line = reader.readLine();
          if (line != null && line.startsWith("Zookeeper version:")) {
            return true;
          }
        } finally {
          if (reader != null) {
            reader.close();
          }
        }
      } catch (IOException e) {
        log.error("Error waitForZKServerUp", e);
      }

      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
    }
    return false;
  }
}
