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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flume.event.SimpleEvent;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.InsertFinalStep;
import org.jooq.InsertSetMoreStep;
import org.jooq.InsertSetStep;
import org.jooq.Meta;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappingUpsertQueryGenerator implements QueryGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(MappingUpsertQueryGenerator.class);

  private Table table;
  private Set<String> whereClauseFields;

  public MappingUpsertQueryGenerator(DSLContext dslContext, String tableName,
                                     List<String> whereClauseFields) {
    Meta meta = dslContext.meta();

    for (Table table : meta.getTables()) {
      if (table.getName().equalsIgnoreCase(tableName)) {
        this.table = table;
        break;
      }
    }
    if (this.table == null) {
      throw new JDBCSinkException("Table not found: " + tableName);
    }
    this.whereClauseFields = new HashSet<>(whereClauseFields);
  }

  @Override
  public boolean executeQuery(DSLContext dslContext, List<SimpleEvent> events) throws IllegalAccessException {
    int mappedEvents = 0;
    for (SimpleEvent event : events) {
      InsertSetStep insert = dslContext.insertInto(this.table);
      InsertSetMoreStep insertSetMoreStep = null;
      InsertFinalStep insertFinalStep = null;
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
        for (Map.Entry<Field, Object> entry : fieldValues.entrySet()) {
          if (entry.getValue() == null) {
            continue;
          }
          insertSetMoreStep = insert.set(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Field, Object> entry : fieldValues.entrySet()) {
          if (entry.getValue() == null) {
            continue;
          }
          insertFinalStep = insertSetMoreStep
              .onDuplicateKeyUpdate()
              .set(entry.getKey(), entry.getValue());
        }
      }
      if (insertFinalStep instanceof InsertFinalStep && insertFinalStep != null) {
        int result = insertFinalStep.execute();
      } else {
        LOG.warn("No insert.");
      }
    }
    return true;
  }
}
