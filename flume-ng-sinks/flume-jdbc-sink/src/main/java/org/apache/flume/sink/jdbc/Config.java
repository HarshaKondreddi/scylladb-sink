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

public class Config {

  public static final String JDBC_PREFIX = "jdbc.";
  public static final String JDBC_DRIVER = JDBC_PREFIX + "driver";
  public static final String CONNECTION_URL = JDBC_PREFIX + "url";
  public static final String USER_NAME = JDBC_PREFIX + "username";
  public static final String PASSWORD = JDBC_PREFIX + "password";
  public static final String CONF_SQL_DIALECT = JDBC_PREFIX + "dialect";
  public static final String BATCH_SIZE = "batch.size";
  public static final Integer DEFAULT_BATCH_SIZE = 100;
  public static final String INGESTION_MODE = "ingestion.mode";
  public static final String CONF_SQL = JDBC_PREFIX + "statement";
  public static final String CONF_TABLE = JDBC_PREFIX + "tablename";
  public static final String WHERE_CLAUSE_FIELDS = "where_clause_fields";
  public static final String DESERIALIZER_CLASS = "deserializer";
}
