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
package org.apache.flume.source.sql;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.json.simple.parser.ParseException.ERROR_UNEXPECTED_EXCEPTION;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper to manage configuration parameters and utility methods <p>
 * <p>
 * Configuration parameters readed from flume configuration file:
 * <tt>type: </tt> org.keedio.flume.source.SQLSource <p>
 * <tt>table: </tt> table to read from <p>
 * <tt>columns.to.select: </tt> columns to select for import data (* will import all) <p>
 * <tt>run.query.delay: </tt> delay time to execute each query to database <p>
 * <tt>status.file.path: </tt> Directory to save status file <p>
 * <tt>status.file.name: </tt> Name for status file (saves last row index processed) <p>
 * <tt>batch.size: </tt> Batch size to send events from flume source to flume channel <p>
 * <tt>max.rows: </tt> Max rows to import from DB in one query <p>
 * <tt>custom.query: </tt> Custom query to execute to database (be careful) <p>
 */

public class SQLSourceHelper {

  private static final Logger LOG = LoggerFactory.getLogger(SQLSourceHelper.class);
  private static final String DEFAULT_STATUS_DIRECTORY = "/var/lib/flume";
  private static final int DEFAULT_QUERY_DELAY = 10000;
  private static final int DEFAULT_BATCH_SIZE = 100;
  private static final int DEFAULT_MAX_ROWS = 10000;
  private static final String DEFAULT_INCREMENTAL_VALUE = "0";
  private static final String DEFAULT_DELIMITER_ENTRY = ",";
  private static final Boolean DEFAULT_ENCLOSE_BY_QUOTES = true;
  private static final String SOURCE_NAME_STATUS_FILE = "SourceName";
  private static final String URL_STATUS_FILE = "URL";
  private static final String COLUMNS_TO_SELECT_STATUS_FILE = "ColumnsToSelect";
  private static final String TABLE_STATUS_FILE = "Table";
  private static final String LAST_INDEX_STATUS_FILE = "LastIndex";
  private static final String QUERY_STATUS_FILE = "Query";
  private File file;
  private File directory;
  private int runQueryDelay;
  private int batchSize;
  private int maxRows;
  private String startFrom;
  private String currentIndex;
  private String statusFilePath;
  private String statusFileName;
  private String connectionURL;
  private String table;
  private String columnsToSelect;
  private String customQuery;
  private String query;
  private String sourceName;
  private String delimiterEntry;
  private String connectionUserName;
  private String connectionPassword;
  private Boolean encloseByQuotes;
  private Context context;
  private Map<String, String> statusFileJsonMap = new LinkedHashMap<String, String>();
  private boolean readOnlySession;

  /**
   * Builds an SQLSourceHelper containing the configuration parameters and
   * usefull utils for SQL Source
   *
   * @param context    Flume source context, contains the properties from configuration file
   * @param sourceName source file name for store status
   */
  public SQLSourceHelper(Context context, String sourceName) {

    this.context = context;

    statusFilePath = context.getString("status.file.path", DEFAULT_STATUS_DIRECTORY);
    statusFileName = context.getString("status.file.name");
    table = context.getString("table");
    columnsToSelect = context.getString("columns.to.select", "*");
    runQueryDelay = context.getInteger("run.query.delay", DEFAULT_QUERY_DELAY);
    directory = new File(statusFilePath);
    customQuery = context.getString("custom.query");
    batchSize = context.getInteger("batch.size", DEFAULT_BATCH_SIZE);
    maxRows = context.getInteger("max.rows", DEFAULT_MAX_ROWS);
    connectionURL = context.getString("hibernate.connection.url");
    connectionUserName = context.getString("hibernate.connection.user");
    connectionPassword = context.getString("hibernate.connection.password");
    readOnlySession = context.getBoolean("read.only", false);

    this.sourceName = sourceName;
    startFrom = context.getString("start.from", DEFAULT_INCREMENTAL_VALUE);
    delimiterEntry = context.getString("delimiter.entry", DEFAULT_DELIMITER_ENTRY);
    encloseByQuotes = context.getBoolean("enclose.by.quotes", DEFAULT_ENCLOSE_BY_QUOTES);
    statusFileJsonMap = new LinkedHashMap<String, String>();

    checkMandatoryProperties();

    if (!(isStatusDirectoryCreated())) {
      createDirectory();
    }

    file = new File(statusFilePath + "/" + statusFileName);

    if (!isStatusFileCreated()) {
      currentIndex = startFrom;
      createStatusFile();
    } else {
      currentIndex = getStatusFileIndex(startFrom);
    }
    query = buildQuery();
  }

  public String buildQuery() {

    if (customQuery == null) {
      return "SELECT " + columnsToSelect + " FROM " + table;
    } else {
      if (customQuery.contains("$@$")) {
        return customQuery.replace("$@$", currentIndex);
      } else {
        return customQuery;
      }
    }
  }

  private boolean isStatusFileCreated() {
    return file.exists() && !file.isDirectory() ? true : false;
  }

  private boolean isStatusDirectoryCreated() {
    return directory.exists() && !directory.isFile() ? true : false;
  }

  /**
   * Converter from a List of Object List to a List of String arrays <p>
   * Useful for csvWriter
   *
   * @param queryResult Query Result from hibernate executeQuery method
   * @return A list of String arrays, ready for csvWriter.writeall method
   */
  public List<String[]> getAllRows(List<List<Object>> queryResult) {

    List<String[]> allRows = new ArrayList<String[]>();

    if (queryResult == null || queryResult.isEmpty()) {
      return allRows;
    }
    String[] row = null;

    for (int i = 0; i < queryResult.size(); i++) {
      List<Object> rawRow = queryResult.get(i);
      row = new String[rawRow.size()];
      for (int j = 0; j < rawRow.size(); j++) {
        if (rawRow.get(j) != null) {
          row[j] = rawRow.get(j).toString();
        } else {
          row[j] = "";
        }
      }
      allRows.add(row);
    }

    return allRows;
  }

  /**
   * Create status file
   */
  public void createStatusFile() {

    statusFileJsonMap.put(SOURCE_NAME_STATUS_FILE, sourceName);
    statusFileJsonMap.put(URL_STATUS_FILE, connectionURL);
    statusFileJsonMap.put(LAST_INDEX_STATUS_FILE, currentIndex);

    if (isCustomQuerySet()) {
      statusFileJsonMap.put(QUERY_STATUS_FILE, customQuery);
    } else {
      statusFileJsonMap.put(COLUMNS_TO_SELECT_STATUS_FILE, columnsToSelect);
      statusFileJsonMap.put(TABLE_STATUS_FILE, table);
    }

    try {
      Writer fileWriter = new FileWriter(file, false);
      JSONValue.writeJSONString(statusFileJsonMap, fileWriter);
      fileWriter.close();
    } catch (IOException e) {
      LOG.error("Error creating value to status file!!!", e);
    }
  }

  /**
   * Update status file with last read row index
   */
  public void updateStatusFile() {

    statusFileJsonMap.put(LAST_INDEX_STATUS_FILE, currentIndex);
    try {
      Writer fileWriter = new FileWriter(file, false);
      JSONValue.writeJSONString(statusFileJsonMap, fileWriter);
      fileWriter.close();
    } catch (IOException e) {
      LOG.error("Error writing incremental value to status file!!!", e);
    }
  }

  private String getStatusFileIndex(String configuredStartValue) {

    if (!isStatusFileCreated()) {
      LOG.info("Status file not created, using start value from config file and creating file");
      return configuredStartValue;
    } else {
      try {
        FileReader fileReader = new FileReader(file);

        JSONParser jsonParser = new JSONParser();
        statusFileJsonMap = (Map) jsonParser.parse(fileReader);
        checkJsonValues();
        return statusFileJsonMap.get(LAST_INDEX_STATUS_FILE);

      } catch (Exception e) {
        LOG.error("Exception reading status file, doing back up and creating new status file", e);
        backupStatusFile();
        return configuredStartValue;
      }
    }
  }

  private void checkJsonValues() throws ParseException {

    // Check commons values to default and custom query
    if (!statusFileJsonMap.containsKey(SOURCE_NAME_STATUS_FILE)
        || !statusFileJsonMap.containsKey(URL_STATUS_FILE) ||
        !statusFileJsonMap.containsKey(LAST_INDEX_STATUS_FILE)) {
      LOG.error("Status file doesn't contains all required values");
      throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
    }
    if (!statusFileJsonMap.get(URL_STATUS_FILE).equals(connectionURL)) {
      LOG.error("Connection url in status file doesn't match with configured in properties file");
      throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
    } else if (!statusFileJsonMap.get(SOURCE_NAME_STATUS_FILE).equals(sourceName)) {
      LOG.error("Source name in status file doesn't match with configured in properties file");
      throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
    }

    // Check default query values
    if (customQuery == null) {
      if (!statusFileJsonMap.containsKey(COLUMNS_TO_SELECT_STATUS_FILE)
          || !statusFileJsonMap.containsKey(TABLE_STATUS_FILE)) {
        LOG.error("Expected ColumsToSelect and Table fields in status file");
        throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
      }
      if (!statusFileJsonMap.get(COLUMNS_TO_SELECT_STATUS_FILE).equals(columnsToSelect)) {
        LOG.error("ColumsToSelect value in status file doesn't match with configured in properties file");
        throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
      }
      if (!statusFileJsonMap.get(TABLE_STATUS_FILE).equals(table)) {
        LOG.error("Table value in status file doesn't match with configured in properties file");
        throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
      }
      return;
    }

    // Check custom query values
    if (customQuery != null) {
      if (!statusFileJsonMap.containsKey(QUERY_STATUS_FILE)) {
        LOG.error("Expected Query field in status file");
        throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
      }
      if (!statusFileJsonMap.get(QUERY_STATUS_FILE).equals(customQuery)) {
        LOG.error("Query value in status file doesn't match with configured in properties file");
        throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
      }
      return;
    }
  }

  private void backupStatusFile() {
    file.renameTo(new File(statusFilePath + "/" + statusFileName + ".bak." + System.currentTimeMillis()));
  }

  public void checkMandatoryProperties() {

    if (connectionURL == null) {
      throw new ConfigurationException("hibernate.connection.url property not set");
    }
    if (statusFileName == null) {
      throw new ConfigurationException("status.file.name property not set");
    }
    if (table == null && customQuery == null) {
      throw new ConfigurationException("property table not set");
    }

    if (connectionUserName == null) {
      throw new ConfigurationException("hibernate.connection.user property not set");
    }

    if (connectionPassword == null) {
      throw new ConfigurationException("hibernate.connection.password property not set");
    }
  }

  /*
   * @return boolean pathname into directory
   */
  public boolean createDirectory() {
    return directory.mkdir();
  }

  /*
   * @return long incremental value as parameter from this
   */
  public String getCurrentIndex() {
    return currentIndex;
  }

  /*
   * @void set incrementValue
   */
  public void setCurrentIndex(String newValue) {
    currentIndex = newValue;
  }

  /*
   * @return int delay in ms
   */
  public int getRunQueryDelay() {
    return runQueryDelay;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public int getMaxRows() {
    return maxRows;
  }

  public String getQuery() {
    return query;
  }

  public String getConnectionURL() {
    return connectionURL;
  }

  public boolean isCustomQuerySet() {
    return (customQuery != null);
  }

  public Context getContext() {
    return context;
  }

  public boolean isReadOnlySession() {
    return readOnlySession;
  }

  public boolean encloseByQuotes() {
    return encloseByQuotes;
  }

  public String getDelimiterEntry() {
    return delimiterEntry;
  }

  public String getConnectionUserName() {
    return connectionUserName;
  }

  public String getConnectionPassword() {
    return connectionPassword;
  }


}
