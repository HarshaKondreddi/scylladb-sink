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
package org.apache.flume.sink.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.apache.flume.sink.jdbc.Config.DESERIALIZER_CLASS;
import static org.apache.flume.sink.jdbc.Config.JDBC_DRIVER;

import com.google.common.base.Strings;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.instrumentation.jdbc.JdbcSinkCounter;
import org.apache.flume.serialization.SimpleEventDeserializer;
import org.apache.flume.sink.AbstractSink;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCSink extends AbstractSink implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(JDBCSink.class);

  private Connection connection;
  private DSLContext dslContext;
  private int batchSize;
  private QueryGenerator queryGenerator;
  private List<String> whereClauseFields;

  private JdbcSinkCounter sinkCounter;
  private SimpleEventDeserializer<SimpleEvent> deserializer;

  @Override
  public Status process() {
    LOG.debug("Executing JDBCSink.process()...");
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();

    try {
      txn.begin();
      List<SimpleEvent> jdbcEventList = new ArrayList<>();
      for (int count = 0; count < batchSize; ++count) {
        Event event = channel.take();
        if (event == null) {
          break;
        }
        try {
          List<SimpleEvent> jdbcEvents = deserializer.transform(event.getBody());
          if (jdbcEvents != null && jdbcEvents.size() > 0) {
            jdbcEventList.addAll(jdbcEvents);
          } else {
            sinkCounter.incrementDeserializationErrorCount();
          }
        } catch (Throwable t) {
          LOG.warn("Error deserializing the event {}", new String(event.getBody()), t);
          sinkCounter.incrementDeserializationErrorCount();
        }
      }

      if (jdbcEventList.size() <= 0) {
        sinkCounter.incrementBatchEmptyCount();
        status = Status.BACKOFF;
      } else {
        if (jdbcEventList.size() < batchSize) {
          sinkCounter.incrementBatchUnderflowCount();
        } else {
          sinkCounter.incrementBatchCompleteCount();
        }
        long b4 = System.currentTimeMillis();
        try {
          final boolean success = this.queryGenerator.executeQuery(dslContext, jdbcEventList);
          if (!success) {
            throw new JDBCSinkException("Query failed");
          }
        } finally {
          sinkCounter.incrementTxTimeSpent(System.currentTimeMillis() - b4);
        }
        sinkCounter.addToEventDrainAttemptCount(jdbcEventList.size());
        connection.commit();
      }
      txn.commit();
      sinkCounter.incrementTxSuccessCount();
      sinkCounter.addToEventDrainSuccessCount(jdbcEventList.size());
    } catch (Throwable t) {
      LOG.error("Exception during process", t);
      status = handleExceptions(txn, t);
    } finally {
      txn.close();
    }
    return status;
  }

  private Status handleExceptions(Transaction txn, Throwable t) {
    Status status;
    try {
      connection.rollback();
    } catch (SQLException ex) {
      LOG.error("Exception on rollback", ex);
    } finally {
      txn.rollback();
      status = Status.BACKOFF;
      sinkCounter.incrementTxErrorCount();
      sinkCounter.incrementConnectionFailedCount();
      if (t instanceof Error) {
        throw new JDBCSinkException(t);
      }
    }
    return status;
  }

  @Override
  public void configure(Context context) {

    LOG.info("Configuring jdbc sink {}", getName());
    final String connectionString = context.getString(Config.CONNECTION_URL);

    this.batchSize = context.getInteger(Config.BATCH_SIZE, Config.DEFAULT_BATCH_SIZE);
    String whereFieldsStr = context.getString(Config.WHERE_CLAUSE_FIELDS);
    if (null != whereFieldsStr && 0 < whereFieldsStr.length()) {
      this.whereClauseFields = Arrays.asList(whereFieldsStr.split("\\s*,\\s*"));
    }

    String username = context.getString(Config.USER_NAME);
    String password = context.getString(Config.PASSWORD);
    String tableName = context.getString(Config.CONF_TABLE);
    connection = null;
    try {
      if (Strings.isNullOrEmpty(username) || Strings.isNullOrEmpty(password)) { //Non authorized
        connection = DriverManager.getConnection(connectionString);
      } else { //Authorized
        connection = DriverManager.getConnection(connectionString, username, password);
      }
    } catch (SQLException ex) {
      throw new JDBCSinkException(ex);
    }

    try {
      connection.setAutoCommit(false);
    } catch (SQLException ex) {
      throw new JDBCSinkException(ex);
    }

    final ConnectionProvider connectionProvider = new DefaultConnectionProvider(connection);
    final SQLDialect sqlDialect = SQLDialect
        .valueOf(context.getString(Config.CONF_SQL_DIALECT)
            .toUpperCase(Locale.ENGLISH));

    dslContext = DSL.using(connectionProvider, sqlDialect);
    final String sql = context.getString(Config.CONF_SQL);
    if (sql == null) {
      this.queryGenerator = new MappingUpsertQueryGenerator(dslContext, tableName, whereClauseFields);
    } else {
      this.queryGenerator = new TemplateQueryGenerator(sqlDialect, sql);
    }

    this.deserializer = buildSerializer(context.getString(JDBC_DRIVER),
        context.getString(DESERIALIZER_CLASS));
    this.sinkCounter = new JdbcSinkCounter(this.getName());
  }

  private SimpleEventDeserializer<SimpleEvent> buildSerializer(String driver, String serializerClass) {
    try {
      Class.forName(driver).newInstance();
      return (SimpleEventDeserializer) Class.forName(serializerClass).newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
      throw new JDBCSinkException(ex);
    }
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting jdbc sink {}", getName());
    sinkCounter.start();
    sinkCounter.incrementConnectionCreatedCount();
    super.start();
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping jdbc sink {}", getName());
    sinkCounter.incrementConnectionClosedCount();
    sinkCounter.stop();
    super.stop();
  }
}
