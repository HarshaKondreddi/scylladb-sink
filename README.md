<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

Note: The harsha Wagon build requires more memory than the default configuration.
We recommend you set the following Maven options:

`export MAVEN_OPTS="-Xms512m -Xmx1024m"`

To compile Wagon and build a distribution tarball, run `mvn install` from the
top level directory. The artifacts will be placed under `flume-ng-dist/target/`.

## Goals: Unified data ingestion platform as a service which is,
* Reliable
* Horizontally scalable*.
* Fault tolerant.
* Auto failover*.
* Out of box support for audit and latency metrics.
* One click pipeline setup.
* Zero operable cost.
* Easily extensible.

## harsha potential UseCases 
* Ingest pod raised events in RMQ to Kafka.
* Ingest pod instrumented API metrics to kafka (as a simple thrift rpc or http post).
* Ingest pod tracing data to elasticsearch from kafka.
* Ingest kafka data to S3.
* Ingest SQL query output to kafka/S3/Another table (experimental).
* Ingest MQTT broker data to kafka.
* Instrumented client side (device) data to MQ  (kafka/MQTT).
* Ingest data from kafka to any real time OLAP tools (like druid).
* S-S communication, fire and forget use case but with high reliability.
* Replicate SQL query output to another table.
* Data shipping from one AWS AZ-1 to another AWS AZ-2 (Future proof in case of disasters).
* Rule based routing messages, producer can decide any sink at message level.
