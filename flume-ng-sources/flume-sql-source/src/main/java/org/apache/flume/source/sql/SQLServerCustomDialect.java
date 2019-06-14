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

import java.sql.Types;

import org.hibernate.dialect.SQLServerDialect;
import org.hibernate.type.StandardBasicTypes;

public class SQLServerCustomDialect extends SQLServerDialect {

  /**
   * Initializes a new instance of the {@link SQLServerDialect} class.
   */
  public SQLServerCustomDialect() {
    registerHibernateType(Types.ARRAY, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.BIGINT, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.BINARY, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.BIT, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.BLOB, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.BOOLEAN, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.CHAR, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.CLOB, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.DATALINK, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.DATE, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.DECIMAL, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.DISTINCT, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.DOUBLE, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.FLOAT, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.INTEGER, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.JAVA_OBJECT, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.LONGNVARCHAR, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.LONGVARBINARY, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.LONGVARCHAR, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.NCHAR, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.NCLOB, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.NULL, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.NUMERIC, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.NVARCHAR, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.OTHER, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.REAL, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.REF, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.ROWID, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.SMALLINT, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.SQLXML, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.STRUCT, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.TIME, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.TIMESTAMP, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.TINYINT, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.VARBINARY, StandardBasicTypes.STRING.getName());
    registerHibernateType(Types.VARCHAR, StandardBasicTypes.STRING.getName());
  }
}