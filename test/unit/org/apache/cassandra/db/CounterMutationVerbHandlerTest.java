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

package org.apache.cassandra.db;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;

public class CounterMutationVerbHandlerTest {

    private CounterMutationVerbHandler handler;
    private CounterMutation counterMutation;

    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";

    @BeforeClass
    public static void setup() throws Exception {
        // Set the partitioner globally before initializing anything else
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        // Initialize Cassandra configurations
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);

        CommitLog.instance.start();

        // Prepare the server
        SchemaLoader.prepareServer();

        // Check if the keyspace already exists and drop it if necessary
        if (Schema.instance.getKeyspaceMetadata(KEYSPACE) != null) {
            SchemaTestUtil.dropKeyspaceIfExist(KEYSPACE, true);
        }

        // Recreate the keyspace with Murmur3Partitioner explicitly set
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.counterCFMD(KEYSPACE, TABLE));

        // Initialize the StorageService to ensure tokens and gossip are ready
        StorageService.instance.initServer();

        // Enable gossip and mark the node as up
        StorageService.instance.setRpcReady(true);
        StorageService.instance.startGossiping();

        // Open the required keyspace to avoid 'Initialized: false' errors
        Keyspace.open(KEYSPACE);
    }

    @Before
    public void initializeTestObjects() {
        // Initialize the handler and counter mutation for each test
        handler = CounterMutationVerbHandler.instance;
        counterMutation = getCounterMutation();
    }

    @Test
    public void testMessageHandlingForExpiredMessage() throws Exception {
        // Create an expired message
        Message<CounterMutation> expiredMessage = createMessage(true);

        // Capture the initial state
        int initialDroppedCount = MessagingService.instance().getDroppedMessages().get(Verb.COUNTER_MUTATION_REQ.toString());

        // Process the expired message
        handler.doVerb(expiredMessage);

        // Check the final state
        int finalDroppedCount = MessagingService.instance().getDroppedMessages().get(Verb.COUNTER_MUTATION_REQ.toString());

        assertEquals(1, finalDroppedCount - initialDroppedCount);
    }

    @Test
    public void testMessageHandlingForNonExpiredMessage() throws Exception {
        // Create a non-expired message
        Message<CounterMutation> nonExpiredMessage = createMessage(false);

        // Capture the initial state
        int initialDroppedCount = MessagingService.instance().getDroppedMessages().get(Verb.COUNTER_MUTATION_REQ.toString());

        // Process the non-expired message
        handler.doVerb(nonExpiredMessage);

        // Check the final state
        int finalDroppedCount = MessagingService.instance().getDroppedMessages().get(Verb.COUNTER_MUTATION_REQ.toString());

        assertEquals(0, finalDroppedCount - initialDroppedCount);
    }

    @Test
    public void testLegitMessageOutWithFlagsInMutate() {
        // Get the drop count before the method call
        long dropCountBefore = MessagingService.instance().getDroppedMessages().get(Verb.COUNTER_MUTATION_REQ.toString());

        // Execute the mutate function with LOCAL_ONE consistency level
        StorageProxy.mutate(Collections.singletonList(counterMutation), ConsistencyLevel.LOCAL_ONE, new Dispatcher.RequestTime(nanoTime()));

        // Get the drop count after the method call
        long dropCountAfter = MessagingService.instance().getDroppedMessages().get(Verb.COUNTER_MUTATION_REQ.toString());

        // Assert that the drop count hasn't changed
        assertEquals(dropCountBefore, dropCountAfter);
    }

    @Test
    public void testExpiredMessageOutWithFlagsInMutate() {
        // Get the drop count before the method call
        long dropCountBefore = MessagingService.instance().getDroppedMessages().get(Verb.COUNTER_MUTATION_REQ.toString());

        try {
            StorageProxy.mutate(Collections.singletonList(counterMutation), ConsistencyLevel.LOCAL_ONE, new Dispatcher.RequestTime(nanoTime() - TimeUnit.SECONDS.toNanos(60)));
        } catch (WriteTimeoutException e) {
            // Get the drop count after the method call
            long dropCountAfter = MessagingService.instance().getDroppedMessages().get(Verb.COUNTER_MUTATION_REQ.toString());
            // Assert that the drop count equals 1
            assertEquals(1, dropCountAfter - dropCountBefore);
        }
    }

    private Message<CounterMutation> createMessage(boolean expired) throws Exception {
        // If expired, simulate expiration by setting a past expiration time
        long expiresAtNanos = expired ? System.nanoTime() - TimeUnit.SECONDS.toNanos(60) : System.nanoTime() + TimeUnit.SECONDS.toNanos(60);

        // Build the message with the counter mutation payload
        return Message.builder(Verb.COUNTER_MUTATION_REQ, counterMutation)
                      .from(InetAddressAndPort.getByName("127.0.0.1"))
                      .withCreatedAt(System.nanoTime())
                      .withExpiresAt(expiresAtNanos)  // Set expiration time
                      .build();
    }

    private CounterMutation getCounterMutation() {
        // Retrieve the actual table metadata from the schema and create dummy objects for counter mutation.
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);

        ByteBuffer partitionKey = ByteBufferUtil.bytes(123);
        DecoratedKey decoratedKey = Murmur3Partitioner.instance.decorateKey(partitionKey);
        PartitionUpdate dummyPartitionUpdate = PartitionUpdate.emptyUpdate(metadata, decoratedKey);
        Mutation dummyMutation = new Mutation(dummyPartitionUpdate);
        ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

        return new CounterMutation(dummyMutation, consistencyLevel);
    }
}
