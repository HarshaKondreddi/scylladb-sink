/*
  * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.amqp;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.TimeoutException;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.powermock.api.easymock.PowerMock.createNiceMock;
import static org.powermock.api.easymock.PowerMock.replay;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurables;
import org.easymock.EasyMockRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class TestAMQPSource {

  private String queueName = "test-queue";
  private AMQPSource source;

  private Field getAccessibleField(String name) throws NoSuchFieldException {
    Field field = AMQPSource.class.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }

  @Before
  public void setUp() throws IOException, TimeoutException {
    ConnectionFactory mock = createNiceMock(ConnectionFactory.class);
    expect(mock.newConnection()).andReturn(createNiceMock(Connection.class));
    replay(mock);
    source = new AMQPSource(mock);

    Context context = new Context();
    context.put("queue", queueName);
    Configurables.configure(source, context);
  }

  @Test
  public void testHostnameDefaultValue() throws NoSuchFieldException, IllegalAccessException {
    assertEquals("localhost", getAccessibleField("hostname").get(source));
  }

  @Test
  public void testPortDefaultValue() throws NoSuchFieldException, IllegalAccessException {
    assertEquals(5672, getAccessibleField("port").get(source));
  }

  @Test
  public void testSSLDefaultValue() throws NoSuchFieldException, IllegalAccessException {
    assertEquals(false, getAccessibleField("enableSSL").get(source));
  }

  @Test
  public void testVirtualHostDefaultValue() throws NoSuchFieldException, IllegalAccessException {
    assertEquals("/", getAccessibleField("virtualHost").get(source));
  }

  @Test
  public void testUsernameDefaultValue() throws NoSuchFieldException, IllegalAccessException {
    assertEquals("guest", getAccessibleField("username").get(source));
  }

  @Test
  public void testPasswordDefaultValue() throws NoSuchFieldException, IllegalAccessException {
    assertEquals("guest", getAccessibleField("password").get(source));
  }

  @Test
  public void testPrefetchCountDefaultValue() throws NoSuchFieldException, IllegalAccessException {
    assertEquals(0, getAccessibleField("prefetchCount").get(source));
  }

  @Test
  public void testTimeoutDefaultValue() throws NoSuchFieldException, IllegalAccessException {
    assertEquals(-1, getAccessibleField("timeout").get(source));
  }

  @Test
  public void testAutoAckDefaultValue() throws NoSuchFieldException, IllegalAccessException {
    assertEquals(false, getAccessibleField("autoAck").get(source));
  }

  @Test
  public void testRequeuingDefaultValue() throws NoSuchFieldException, IllegalAccessException {
    assertEquals(false, getAccessibleField("requeuing").get(source));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyQueue() {
    Context context = new Context();
    Configurables.configure(source, context);
  }

  @Test
  public void testQueuePassedValue() throws NoSuchFieldException, IllegalAccessException {
    assertEquals(queueName, getAccessibleField("queue").get(source));
  }
}
