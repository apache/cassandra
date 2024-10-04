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
import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchStoreVerbHandler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceParams;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.TimeUUID;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;

public class BatchStoreVerbHandlerTest {

    private static BatchStoreVerbHandler handler;
    private static final String KEYSPACE = "ks_for_batch_test";
    private static final String TABLE = "tbl_for_batch_test";
    private static TableMetadata metadata;
    private static DecoratedKey key;

    @BeforeClass
    public static void setup() {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        SchemaLoader.prepareServer();
        handler = BatchStoreVerbHandler.instance;

        // Define the table metadata for the keyspace and table
        metadata = TableMetadata.builder(KEYSPACE, TABLE)
                                .addPartitionKeyColumn("pk", UTF8Type.instance)
                                .addClusteringColumn("ck", UTF8Type.instance)
                                .addRegularColumn("rc", UTF8Type.instance)
                                .build();

        // Decorate the key with Murmur3Partitioner
        key = ByteOrderedPartitioner.instance.decorateKey(bytes("key"));

        // Create the keyspace
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), metadata);

        StorageService.instance.initServer();
    }

    @Test
    public void testMessageHandlingForExpiredMessage() throws Exception {

        // Create an expired message
        Message<Batch> expiredMessage = createMessage(true);

        // Capture the initial state
        int initialDroppedCount = MessagingService.instance().getDroppedMessages().get(Verb.BATCH_STORE_REQ.toString());

        // Process the expired message
        handler.doVerb(expiredMessage);

        // Check the final state
        int finalDroppedCount = MessagingService.instance().getDroppedMessages().get(Verb.BATCH_STORE_REQ.toString());

        assertEquals(1, finalDroppedCount - initialDroppedCount);

    }

    @Test
    public void testMessageHandlingForNonExpiredMessage() throws Exception {
        // Create a non-expired message
        Message<Batch> nonExpiredMessage = createMessage(false);

        // Capture the initial state
        int initialDroppedCount = MessagingService.instance().getDroppedMessages().get(Verb.BATCH_STORE_REQ.toString());

        // Process the non-expired message
        handler.doVerb(nonExpiredMessage);

        // Check the final state
        int finalDroppedCount = MessagingService.instance().getDroppedMessages().get(Verb.BATCH_STORE_REQ.toString());

        assertEquals(0, finalDroppedCount - initialDroppedCount);
    }

    @Test
    public void testMessageOutWithFlagsInMutateAtomically() {
        // Get the drop count before the method call
        long dropCountBefore = MessagingService.instance().getDroppedMessages().get(Verb.BATCH_STORE_REQ.toString());

        // Execute the mutateAtomically function
        StorageProxy.mutateAtomically(Collections.singletonList(createMutation()), ConsistencyLevel.ONE, false, new Dispatcher.RequestTime(nanoTime()));

        // Get the drop count after the method call
        long dropCountAfter = MessagingService.instance().getDroppedMessages().get(Verb.BATCH_STORE_REQ.toString());

        // Assert that the drop count hasn't changed
        assertEquals(dropCountBefore, dropCountAfter);
    }

    private Mutation createMutation() {
        Mutation.SimpleBuilder builder = Mutation.simpleBuilder(KEYSPACE, key);
        builder.update(metadata)
               .timestamp(0)
               .row("ck_1")
               .add("rc", "value0");

        return builder.build();
    }

    private Message<Batch> createMessage(boolean expired) throws Exception {
        // Create a unique batch object for testing
        Batch batch = Batch.createLocal(
        TimeUUID.Generator.nextTimeUUID(),
        System.nanoTime(),
        Collections.emptyList()
        );

        // If expired, simulate expiration by setting a past expiration time
        long expiresAtNanos = expired ? System.nanoTime() - TimeUnit.SECONDS.toNanos(60)  : System.nanoTime() + TimeUnit.SECONDS.toNanos(60);

        // Build the message with the batch payload
        return Message.builder(Verb.BATCH_STORE_REQ, batch)
                      .from(InetAddressAndPort.getByName("127.0.0.1"))
                      .withCreatedAt(System.nanoTime())
                      .withExpiresAt(expiresAtNanos)  // Set expiration time
                      .build();
    }
}
