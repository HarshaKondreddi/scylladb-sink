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
import org.apache.flume.conf.ConfigurationException;

public class JDBCSinkHelper {

  private Context context;
  private String sinkName;
  private String connectionURL;
  private String userName;
  private String password;

  public JDBCSinkHelper(Context context, String sinkName) {
    this.context = context;
    this.sinkName = sinkName;
    connectionURL = context.getString(Config.CONNECTION_URL);
    userName = context.getString(Config.USER_NAME);
    password = context.getString(Config.PASSWORD);
  }

  public void validateConfiguration() {
    if (connectionURL == null) {
      throw new ConfigurationException(Config.CONNECTION_URL + " is not set");
    }

    if (userName == null) {
      throw new ConfigurationException(Config.USER_NAME + " is not set");
    }

    if (password == null) {
      throw new ConfigurationException(Config.PASSWORD + " is not set");
    }
  }

  Context getContext() {
    return context;
  }


}
