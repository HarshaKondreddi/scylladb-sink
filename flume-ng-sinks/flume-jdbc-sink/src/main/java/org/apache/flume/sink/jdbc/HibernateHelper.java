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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.hibernate.CacheMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HibernateHelper {

  private static final Logger LOG = LoggerFactory.getLogger(HibernateHelper.class);

  private JDBCSinkHelper jdbcSinkHelper;
  private Context context;
  private ServiceRegistry serviceRegistry;
  private Session session;
  private Configuration configuration;
  private static SessionFactory sessionFactory;

  public HibernateHelper(JDBCSinkHelper jdbcSinkHelper) {
    this.jdbcSinkHelper = jdbcSinkHelper;
    Context context = jdbcSinkHelper.getContext();

    jdbcSinkHelper.validateConfiguration();
    configuration = new Configuration();
  }

  public void upsert(List<Event> events) {
    String query = buildQuery();
    session.createQuery(query).executeUpdate();
  }

  public void createSession() {

    serviceRegistry = new StandardServiceRegistryBuilder()
              .applySettings(configuration.getProperties()).build();
    sessionFactory = configuration.buildSessionFactory(serviceRegistry);
    session = sessionFactory.openSession();
    session.setCacheMode(CacheMode.IGNORE);
  }

  public void closeSession() {

    LOG.info("Closing hibernate session");

    session.close();
    sessionFactory.close();
  }

  private void resetConnection() throws InterruptedException {
    session.close();
    sessionFactory.close();
    createSession();
  }

  private String buildQuery() {
    return "";
  }

}
