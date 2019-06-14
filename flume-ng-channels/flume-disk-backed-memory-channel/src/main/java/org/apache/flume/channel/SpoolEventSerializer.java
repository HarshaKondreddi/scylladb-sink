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
package org.apache.flume.channel;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.serialization.EventSerializer;

import java.io.IOException;
import java.io.OutputStream;

public class SpoolEventSerializer implements EventSerializer, Configurable {
  private static final Schema SCHEMA = new Schema.Parser().parse(
      "{ \"type\":\"record\", \"name\": \"Event\", \"fields\": [" +
          " {\"name\": \"headers\", \"type\": { \"type\": \"map\", \"values\": \"string\" } }, " +
          " {\"name\": \"body\", \"type\": \"bytes\" } ] }");

  private ReflectDatumWriter<SimpleEvent> datumWriter;
  private OutputStream out;
  private BinaryEncoder encoder;

  public SpoolEventSerializer(OutputStream out) {
    this.out = out;
    this.datumWriter = new ReflectDatumWriter<SimpleEvent>(SimpleEvent.class);
    this.encoder = EncoderFactory.get().binaryEncoder(out, null);
  }

  @Override
  public void configure(Context context) {
    //no-op
  }

  @Override
  public void afterCreate() throws IOException {
    //no-op
  }

  @Override
  public void afterReopen() throws IOException {
    //no-op
  }

  @Override
  public void write(Event event) throws IOException {
    datumWriter.write((SimpleEvent) event, encoder);
  }

  @Override
  public void flush() throws IOException {
    encoder.flush();
    out.flush();
  }

  @Override
  public void beforeClose() throws IOException {
    encoder.flush();
    out.flush();
  }

  @Override
  public boolean supportsReopen() {
    return false;
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(Context context, OutputStream out) {
      SpoolEventSerializer writer = new SpoolEventSerializer(out);
      writer.configure(context);
      return writer;
    }
  }
}
