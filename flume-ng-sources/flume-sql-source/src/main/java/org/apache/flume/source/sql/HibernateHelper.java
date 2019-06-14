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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.hibernate.CacheMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.transform.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to manage hibernate sessions and perform queries
 */
public class HibernateHelper {

  private static final Logger LOG = LoggerFactory
      .getLogger(HibernateHelper.class);

  private static SessionFactory factory;
  private Session session;
  private ServiceRegistry serviceRegistry;
  private Configuration config;
  private SQLSourceHelper sqlSourceHelper;

  /**
   * Constructor to initialize hibernate configuration parameters
   *
   * @param sqlSourceHelper Contains the configuration parameters from flume config file
   */
  public HibernateHelper(SQLSourceHelper sqlSourceHelper) {

    this.sqlSourceHelper = sqlSourceHelper;
    Context context = sqlSourceHelper.getContext();
    /* check for mandatory propertis */
    sqlSourceHelper.checkMandatoryProperties();

    Map<String, String> hibernateProperties = context.getSubProperties("hibernate.");
    Iterator<Map.Entry<String, String>> it = hibernateProperties.entrySet().iterator();

    config = new Configuration();
    Map.Entry<String, String> e;

    while (it.hasNext()) {
      e = it.next();
      config.setProperty("hibernate." + e.getKey(), e.getValue());
    }
  }

  /**
   * Connect to database using hibernate
   */
  public void establishSession() {

    LOG.info("Opening hibernate session");

    serviceRegistry = new StandardServiceRegistryBuilder()
        .applySettings(config.getProperties()).build();
    factory = config.buildSessionFactory(serviceRegistry);
    session = factory.openSession();
    session.setCacheMode(CacheMode.IGNORE);

    session.setDefaultReadOnly(sqlSourceHelper.isReadOnlySession());
  }

  /**
   * Close database connection
   */
  public void closeSession() {

    LOG.info("Closing hibernate session");

    session.close();
    factory.close();
  }

  /**
   * Execute the selection query in the database
   *
   * @return The query result. Each Object is a cell content. <p>
   * The cell contents use database types (date,int,string...),
   * keep in mind in case of future conversions/castings.
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  public List<List<Object>> executeQuery() throws InterruptedException {

    List<List<Object>> rowsList = new ArrayList<>();
    Query query;

    if (!session.isConnected()) {
      resetConnection();
    }

    if (sqlSourceHelper.isCustomQuerySet()) {
      query = session.createNativeQuery(sqlSourceHelper.buildQuery());
      if (sqlSourceHelper.getMaxRows() != 0) {
        query = query.setMaxResults(sqlSourceHelper.getMaxRows());
      }
    } else {
      query = session
          .createNativeQuery(sqlSourceHelper.getQuery())
          .setFirstResult(Integer.parseInt(sqlSourceHelper.getCurrentIndex()));
      if (sqlSourceHelper.getMaxRows() != 0) {
        query = query.setMaxResults(sqlSourceHelper.getMaxRows());
      }
    }

    try {
      rowsList = query.setFetchSize(sqlSourceHelper.getMaxRows()).setResultTransformer(Transformers.TO_LIST).list();
    } catch (Exception e) {
      LOG.error("Exception thrown, resetting connection.", e);
      resetConnection();
    }

    if (!rowsList.isEmpty()) {
      if (sqlSourceHelper.isCustomQuerySet()) {
        sqlSourceHelper.setCurrentIndex(rowsList.get(rowsList.size() - 1).get(0).toString());
      } else {
        sqlSourceHelper.setCurrentIndex(Integer.toString(
            (Integer.parseInt(sqlSourceHelper.getCurrentIndex()) + rowsList.size())));
      }
    }

    return rowsList;
  }

  private void resetConnection() throws InterruptedException {
    session.close();
    factory.close();
    establishSession();
  }
}
