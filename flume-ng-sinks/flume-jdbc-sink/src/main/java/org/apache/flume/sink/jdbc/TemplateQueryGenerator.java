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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Objects.toStringHelper;

import org.apache.flume.event.SimpleEvent;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultDataType;
import org.slf4j.LoggerFactory;

public class TemplateQueryGenerator implements QueryGenerator {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TemplateQueryGenerator.class);

  private static final Pattern PARAMETER_PATTERN = Pattern.compile(
      "\\$\\{(?<part>[^\\s]+)?:(?<type>[^\\s]+)\\}");

  private static final String BODY = "BODY";
  private static final String HEADER = "HEADER";

  private final List<Parameter> parameters;

  final String sql;

  public TemplateQueryGenerator(SQLDialect sqlDialect, String sql) {
    final Matcher m = PARAMETER_PATTERN.matcher(sql);

    parameters = new ArrayList<>();

    while (m.find()) {
      final String part = m.group("part").toUpperCase(Locale.ENGLISH);
      final String type = m.group("type").toUpperCase(Locale.ENGLISH);
      DataType<?> dataType = DefaultDataType.getDataType(sqlDialect, type);
      final Parameter parameter = new Parameter(part, dataType);
      parameters.add(parameter);
      LOG.debug("Parameter: {}", parameter);
    }

    this.sql = m.replaceAll("?");
    LOG.debug("Generated SQL: {}", this.sql);
  }

  @Override
  public boolean executeQuery(final DSLContext dslContext, final List<SimpleEvent> events)
      throws IllegalAccessException {
    List<Query> queries = new ArrayList<>();
    for (SimpleEvent event : events) {
      final Object[] bindings = new Object[this.parameters.size()];
      for (int j = 0; j < this.parameters.size(); j++) {
        bindings[j] = this.parameters.get(j).binding(event);
      }
      queries.add(dslContext.query(this.sql, bindings));
    }
    dslContext.batch(queries).execute();
    return true;
  }

  private static class Parameter {

    private final String header;
    private final DataType<?> dataType;

    public Parameter(final String header, final DataType<?> dataType) {
      this.header = header;
      this.dataType = dataType;
    }

    public Object binding(final SimpleEvent event) throws IllegalAccessException {
      java.lang.reflect.Field[] fields = event.getClass().getDeclaredFields();
      for (java.lang.reflect.Field entry : fields) {
        if (entry.getName().equalsIgnoreCase(header)) {
          entry.setAccessible(true);
          return dataType.convert(entry.get(event));
        }
      }
      return null;
    }

    @Override
    public String toString() {
      return toStringHelper(Parameter.class)
          .add("header", header).add("dataType", dataType).toString();
    }
  }
}
