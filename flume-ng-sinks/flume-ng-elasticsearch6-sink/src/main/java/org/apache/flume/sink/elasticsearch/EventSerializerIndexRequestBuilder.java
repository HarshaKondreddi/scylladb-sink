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
package org.apache.flume.sink.elasticsearch;

import java.io.IOException;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;

public class EventSerializerIndexRequestBuilder extends AbstractIndexRequestBuilder {

  protected final ElasticSearchEventSerializer serializer;

  public EventSerializerIndexRequestBuilder(
      ElasticSearchEventSerializer serializer) {
    this(serializer, ESIndexRequestBuilder.df);
  }

  protected EventSerializerIndexRequestBuilder(
      ElasticSearchEventSerializer serializer, FastDateFormat fdf) {
    super(fdf);
    this.serializer = serializer;
  }

  @Override
  public void configure(Context context) {
    serializer.configure(context);
  }

  @Override
  public void configure(ComponentConfiguration config) {
    serializer.configure(config);
  }

  @Override
  protected void prepareIndexRequest(org.elasticsearch.action.index.IndexRequestBuilder indexRequest,
                                     String indexName, String indexType, Event event) throws IOException {
    indexRequest.setIndex(indexName)
        .setType(indexType)
        .setSource(serializer.serialize(event));
  }
}
