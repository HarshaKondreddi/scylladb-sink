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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.event.SimpleEvent;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.InsertSetMoreStep;
import org.jooq.InsertSetStep;
import org.jooq.Meta;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappingQueryGenerator implements QueryGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(JDBCSink.class);

  private Table table;

  public MappingQueryGenerator(DSLContext dslContext, String tableName) {
    Meta meta = dslContext.meta();

    for (Table table : meta.getTables()) {
      System.out.println(table.getName());
      if (table.getName().equalsIgnoreCase(tableName)) {
        this.table = table;
        break;
      }
    }
    if (this.table == null) {
      throw new JDBCSinkException("Table not found: " + tableName);
    }
  }

  public boolean executeQuery(DSLContext dslContext, List<SimpleEvent> events)
      throws IllegalAccessException {
    InsertSetStep insert = dslContext.insertInto(this.table);
    int mappedEvents = 0;
    for (SimpleEvent event : events) {
      Map<Field, Object> fieldValues = new HashMap<>();
      java.lang.reflect.Field[] fields = event.getClass().getDeclaredFields();
      for (java.lang.reflect.Field field : fields) {
        Field column = null;
        for (Field f : this.table.fields()) {
          if (f.getName().equalsIgnoreCase(field.getName())) {
            column = f;
            break;
          }
        }
        if (column == null) {
          LOG.trace("Ignoring field: {}", field.getName());
          continue;
        }
        DataType dataType = column.getDataType();
        field.setAccessible(true);
        fieldValues.put(column, dataType.convert(field.get(event)));
      }
      if (fieldValues.isEmpty()) {
        LOG.debug("Ignoring event, no mapped fields.");
      } else {
        mappedEvents++;
        if (insert instanceof InsertSetMoreStep) {
          insert = ((InsertSetMoreStep) insert).newRecord();
        }
        for (Map.Entry<Field, Object> entry : fieldValues.entrySet()) {
          if (entry.getValue() == null) {
            continue;
          }
          insert = insert.set(entry.getKey(), entry.getValue());
        }
      }
    }
    if (insert instanceof InsertSetMoreStep) {
      int result = ((InsertSetMoreStep) insert).execute();
      if (result != mappedEvents) {
        LOG.warn("Mapped {} events, inserted {}.", mappedEvents, result);
        return false;
      }
    } else {
      LOG.debug("No insert.");
    }
    return true;
  }
}
