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

package org.apache.cassandra.db.compression;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class CompressionDictionaryEventHandlerTest
{
    private static final String TEST_NAME = "compression_dict_event_handler_test_";
    private static final String KEYSPACE = TEST_NAME + "keyspace";
    private static final String TABLE = "test_table";
    private static final DictId TEST_DICTIONARY_ID = new DictId(Kind.ZSTD, 12345L);

    private static TableMetadata tableMetadata;
    private static ColumnFamilyStore cfs;

    private CompressionDictionaryEventHandler eventHandler;
    private ZstdCompressionDictionary testDictionary;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        ServerTestUtils.prepareServerNoRegister();

        // Create a table with dictionary compression enabled
        CompressionParams compressionParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                                     Map.of("compression_level", "3"));

        TableMetadata.Builder tableBuilder = TableMetadata.builder(KEYSPACE, TABLE)
                                                          .addPartitionKeyColumn("pk", org.apache.cassandra.db.marshal.UTF8Type.instance)
                                                          .addRegularColumn("data", org.apache.cassandra.db.marshal.UTF8Type.instance)
                                                          .compression(compressionParams);

        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    tableBuilder);

        tableMetadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);

        // Register some nodes for cluster testing
        InetAddressAndPort ep1 = InetAddressAndPort.getByName("127.0.0.2:9042");
        InetAddressAndPort ep2 = InetAddressAndPort.getByName("127.0.0.3:9042");
        InetAddressAndPort ep3 = FBUtilities.getBroadcastAddressAndPort();

        NodeId node1 = Register.register(new NodeAddresses(UUID.randomUUID(), ep1, ep1, ep1));
        NodeId node2 = Register.register(new NodeAddresses(UUID.randomUUID(), ep2, ep2, ep2));
        NodeId node3 = Register.register(new NodeAddresses(UUID.randomUUID(), ep3, ep3, ep3));

        // Simple token distribution for testing
        UnsafeJoin.unsafeJoin(node1, Collections.singleton(key(tableMetadata, 1).getToken()));
        UnsafeJoin.unsafeJoin(node2, Collections.singleton(key(tableMetadata, 2).getToken()));
        UnsafeJoin.unsafeJoin(node3, Collections.singleton(key(tableMetadata, 3).getToken()));
    }

    @Before
    public void setUp()
    {
        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().outboundSink.clear();
        testDictionary = createTestDictionary();
        eventHandler = new CompressionDictionaryEventHandler(cfs, new CompressionDictionaryCache());
    }

    @After
    public void tearDown()
    {
        if (testDictionary != null)
        {
            testDictionary.close();
        }
        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().outboundSink.clear();
    }

    @Test
    public void testOnNewDictionaryTrained() throws InterruptedException
    {
        // Expect messages to 2 other nodes (excluding self)
        CountDownLatch messageSentLatch = new CountDownLatch(2);
        Set<InetAddressAndPort> receivers = ConcurrentHashMap.newKeySet(2);
        AtomicReference<CompressionDictionaryUpdateMessage> capturedMessage = new AtomicReference<>();

        // Capture outbound messages
        MessagingService.instance().outboundSink.add((message, to, type) -> {
            if (message.verb() == Verb.DICTIONARY_UPDATE_REQ)
            {
                capturedMessage.set((CompressionDictionaryUpdateMessage) message.payload);
                receivers.add(to);
                messageSentLatch.countDown();
            }
            return false; // Don't actually send
        });

        eventHandler.onNewDictionaryTrained(TEST_DICTIONARY_ID);

        // Wait for message to be processed
        assertThat(messageSentLatch.await(5, TimeUnit.SECONDS))
        .as("Dictionary update notification should be sent")
        .isTrue();

        assertThat(receivers)
        .as("Should not send notification to self")
        .hasSize(2)
        .doesNotContain(FBUtilities.getBroadcastAddressAndPort());

        CompressionDictionaryUpdateMessage message = capturedMessage.get();
        assertThat(message)
        .as("Message should be captured")
        .isNotNull();
        assertThat(message.tableId)
        .as("Message should contain correct table ID")
        .isEqualTo(tableMetadata.id);
        assertThat(message.dictionaryId)
        .as("Message should contain correct dictionary ID")
        .isEqualTo(TEST_DICTIONARY_ID);
    }

    @Test
    public void testMessageSerialization()
    {
        TableId testTableId = tableMetadata.id;
        CompressionDictionaryUpdateMessage message = new CompressionDictionaryUpdateMessage(testTableId, TEST_DICTIONARY_ID);

        assertThat(message.tableId)
        .as("Message should contain correct table ID")
        .isEqualTo(testTableId);
        assertThat(message.dictionaryId)
        .as("Message should contain correct dictionary ID")
        .isEqualTo(TEST_DICTIONARY_ID);
        assertThat(CompressionDictionaryUpdateMessage.serializer)
        .as("Message should have serializer")
        .isNotNull();
    }

    @Test
    public void testMessageSerializationRoundTrip() throws Exception
    {
        TableId testTableId = tableMetadata.id;
        CompressionDictionaryUpdateMessage originalMessage = new CompressionDictionaryUpdateMessage(testTableId, TEST_DICTIONARY_ID);

        // Serialize
        org.apache.cassandra.io.util.DataOutputBuffer out = new org.apache.cassandra.io.util.DataOutputBuffer();
        CompressionDictionaryUpdateMessage.serializer.serialize(originalMessage, out, MessagingService.current_version);

        // Deserialize
        org.apache.cassandra.io.util.DataInputBuffer in = new org.apache.cassandra.io.util.DataInputBuffer(out.getData());
        CompressionDictionaryUpdateMessage deserializedMessage =
        CompressionDictionaryUpdateMessage.serializer.deserialize(in, MessagingService.current_version);

        assertThat(deserializedMessage.tableId)
        .as("Deserialized table ID should match")
        .isEqualTo(originalMessage.tableId);
        assertThat(deserializedMessage.dictionaryId)
        .as("Deserialized dictionary ID should match")
        .isEqualTo(originalMessage.dictionaryId);
    }

    @Test
    public void testSendNotificationRobustness()
    {
        // Test that sending notifications doesn't throw even if messaging fails
        MessagingService.instance().outboundSink.add((message, to, type) -> {
            if (message.verb() == Verb.DICTIONARY_UPDATE_REQ)
            {
                throw new RuntimeException("Simulated messaging failure");
            }
            return false;
        });

        assertThatNoException().isThrownBy(() -> eventHandler.onNewDictionaryTrained(TEST_DICTIONARY_ID));
    }

    private static ZstdCompressionDictionary createTestDictionary()
    {
        byte[] dictBytes = "test dictionary data for event handler testing".getBytes();
        return new ZstdCompressionDictionary(TEST_DICTIONARY_ID, dictBytes);
    }

    private static DecoratedKey key(TableMetadata metadata, int key)
    {
        return metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key));
    }
}
