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

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;

import java.io.IOException;
import java.io.InputStream;

public class SpoolEventDeserializer implements Configurable {
  private ReflectDatumReader<SimpleEvent> datumReader;
  private BinaryDecoder decoder;

  public SpoolEventDeserializer(InputStream in) {
    this.datumReader = new ReflectDatumReader<SimpleEvent>(SimpleEvent.class);
    this.decoder = DecoderFactory.get().directBinaryDecoder(in, null);
  }

  @Override
  public void configure(Context context) {
    //no-op
  }

  public SimpleEvent readEvent() throws IOException {
    return datumReader.read(null, decoder);
  }

  public static class Builder {
    public SpoolEventDeserializer build(Context context, InputStream in) {
      SpoolEventDeserializer deserializer = new SpoolEventDeserializer(in);
      deserializer.configure(context);
      return deserializer;
    }
  }
}
