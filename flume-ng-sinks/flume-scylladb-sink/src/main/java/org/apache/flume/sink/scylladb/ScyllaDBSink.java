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
package org.apache.flume.sink.scylladb;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.instrumentation.scylla.ScyllaSinkCounter;
import org.apache.flume.processor.SimpleEventProcessor;
import org.apache.flume.serialization.SimpleEventDeserializer;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.scylla.ScyllaResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ScyllaDBSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(ScyllaDBSink.class);

    private String contactPoints;
    private Collection<InetSocketAddress> servers;
    private String keySpace;
    private String deserializerClazzName;
    private String eventProcessorClazzName;

    private static Cluster cluster;
    private static Session session;

    private ScyllaSinkCounter scyllaSinkCounter;

    private SimpleEventDeserializer<SimpleEvent> deserializer = null;
    private SimpleEventProcessor eventProcessor = null;
    private String preparedStatementQuery = null;

    private PreparedStatement preparedStatement = null;
    private Integer batchSize;

    @Override
    public void configure(Context context) {
        logger.info("Configuring scyllaDB sink..");
        this.contactPoints = context.getString(ScyllaConfiguration.CONTACT_POINTS);
        this.keySpace = context.getString(ScyllaConfiguration.KEYSPACE);
        this.deserializerClazzName = context.getString(ScyllaConfiguration.DESERIALIZER);
        this.eventProcessorClazzName = context.getString(ScyllaConfiguration.QUERY_PROCESSOR);
        this.batchSize = context.getInteger(ScyllaConfiguration.BATCH_SIZE, 100);

        try {
            this.deserializer = (SimpleEventDeserializer)Class.forName(deserializerClazzName).newInstance();
            this.eventProcessor = (SimpleEventProcessor) Class.forName(eventProcessorClazzName).newInstance();
            this.preparedStatementQuery = context.getString(ScyllaConfiguration.PREPARED_STATEMENT);
        } catch (Exception e) {
            logger.error("Error finding class in the class path, {}", e);
        }

        try {
            this.servers = getServers(this.contactPoints);
        } catch (UnknownHostException e) {
            logger.error("Error while configuring contact points for scylla db sinks. contact points: {}", contactPoints);
            throw new FlumeException("Error while configuring scylla db servers");
        }

        //Initialise the cluster and session and relevant classes
        this.cluster = Cluster.builder().addContactPointsWithPorts(servers).build();
        this.scyllaSinkCounter = new ScyllaSinkCounter(this.getName());
    }

    @Override
    public synchronized void start() {
        logger.info("Starting scylla sink");
        this.session = cluster.connect(this.keySpace);
        this.preparedStatement = session.prepare(preparedStatementQuery);
        scyllaSinkCounter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.info("Stopping Scylla Sink");
        scyllaSinkCounter.stop();
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;

        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        txn.begin();

        long b4 = System.currentTimeMillis();
        List<Event> channelEvents = new ArrayList<>();

        for (int i = 0; i < batchSize && status != Status.BACKOFF; i++) {
            Event event = channel.take();
            if (null != event) {
                channelEvents.add(event);
            } else {
                status = Status.BACKOFF;
            }
        }

        if (channelEvents.size() <= 0) {
            status = Status.BACKOFF;
            logger.warn("retrieved an empty event in sink {}", getName());
            scyllaSinkCounter.incrementBatchEmptyCount();
            txn.commit();
            txn.close();
            return status;
        } else if (batchSize > channelEvents.size()) {
            scyllaSinkCounter.incrementBatchUnderflowCount();
        } else {
            scyllaSinkCounter.incrementBatchCompleteCount();
        }

        int droppedEvents = 0;
        scyllaSinkCounter.addToEventDrainAttemptCount(channelEvents.size());
        try {
        boolean poisonedEventPresent = false;

        for (Event event : channelEvents) {
            SimpleEvent scyllaEvent;
            try {
                scyllaEvent = deserializer.transform(event.getBody()).get(0);
            } catch (Throwable e) {
                droppedEvents++;
                scyllaSinkCounter.incrementDeserializationErrorCount();
                logger.error("Could not de-serialize , dropping event {}", event, e);
                continue;
            }

            try {
                eventProcessor.processEvent(new ScyllaResource(preparedStatement, session), scyllaEvent);
                event.getHeaders().put(ScyllaConfiguration.IS_PROCESSED_SUCCESSFULLY, String.valueOf(Boolean.TRUE));
            } catch (Throwable ex) {
                poisonedEventPresent = true;
                logger.error("Error while processing the event: {}", new String(event.getBody()));
                scyllaSinkCounter.incrementProcessingErrorCount();
                event.getHeaders().put(ScyllaConfiguration.POISONED_EVENT, String.valueOf(true));
            }
        }
        scyllaSinkCounter.incrementTxTimeSpent(System.currentTimeMillis() - b4);
        if (poisonedEventPresent) {
            txn.rollback();
            logger.error("Poisoned events found, hence rolling back the events");
            scyllaSinkCounter.incrementTxErrorCount();
        } else {
            txn.commit();
            scyllaSinkCounter.incrementTxSuccessCount();
            scyllaSinkCounter.addToEventDrainSuccessCount(channelEvents.size() - droppedEvents);
        }
    } catch (Throwable t) {
        logger.error("Unexpected error {}", getName(), t);
        txn.rollback();
        scyllaSinkCounter.incrementTxErrorCount();
        status = Status.BACKOFF;
    } finally {
        txn.close();
    }
    return status;
    }

    /***
     * returns list of servers
     * @param contactPoints
     * @return
     */
    private Collection<InetSocketAddress> getServers(String contactPoints) throws UnknownHostException {
        Collection<InetSocketAddress> servers = new ArrayList<InetSocketAddress>();
        String[] tmp = contactPoints.split(",");
        for(String str : tmp) {
            String[] dns = str.split(":");
            servers.add(new InetSocketAddress(dns[0], Integer.valueOf(dns[1])));
        }
        return servers;
    }
}
