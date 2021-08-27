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

package org.apache.cassandra.schema;

import java.net.UnknownHostException;

import org.junit.Test;

import junit.framework.TestCase;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.dht.RandomPartitioner;

import org.apache.cassandra.locator.InetAddressAndPort;

public class SchemaMigrationEventTest extends TestCase
{
    @Test
    public void testSchemaMigrationEventCreation() throws UnknownHostException
    {
        SchemaLoader.prepareServer();
        SchemaMigrationEvent e1 = new SchemaMigrationEvent(SchemaMigrationEvent.MigrationManagerEventType.TASK_CREATED, null, null);

        InetAddressAndPort liveEndpoint = InetAddressAndPort.getByName("127.0.0.1");
        SchemaMigrationEvent e2 = new SchemaMigrationEvent(SchemaMigrationEvent.MigrationManagerEventType.TASK_CREATED, liveEndpoint, null);

        InetAddressAndPort deadEndpoint = InetAddressAndPort.getByName("127.0.0.2");
        Util.joinNodeToRing(deadEndpoint, new RandomPartitioner.BigIntegerToken("3"), new RandomPartitioner());
        Util.markNodeAsDead(deadEndpoint);

        SchemaMigrationEvent e3 = new SchemaMigrationEvent(SchemaMigrationEvent.MigrationManagerEventType.TASK_CREATED, deadEndpoint, null);
    }
}