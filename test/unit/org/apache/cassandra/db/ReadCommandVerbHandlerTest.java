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

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.token;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadCommandVerbHandlerTest
{
    private final static Random random = new Random();

    private static ReadCommandVerbHandler handler;
    private static TableMetadata metadata;
    private static TableMetadata metadata_with_transient;
    private static DecoratedKey KEY;

    private static final String TEST_NAME = "read_command_vh_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace_replicated";
    private static final String KEYSPACE_WITH_TRANSIENT = TEST_NAME + "ks_with_transient";
    private static final String TABLE = "table1";

    @BeforeClass
    public static void init() throws Throwable
    {
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition(TEST_NAME);
        metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        metadata_with_transient = Schema.instance.getTableMetadata(KEYSPACE_WITH_TRANSIENT, TABLE);
        KEY = key(metadata, 1);

        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(KEY.getToken(), InetAddressAndPort.getByName("127.0.0.2"));
        tmd.updateNormalToken(key(metadata, 2).getToken(), InetAddressAndPort.getByName("127.0.0.3"));
        tmd.updateNormalToken(key(metadata, 3).getToken(), FBUtilities.getBroadcastAddressAndPort());
    }

    @Before
    public void setup()
    {
        MessagingService.instance().clearMessageSinks();
        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddressAndPort to)
            {
                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return false;
            }
        });

        handler = new ReadCommandVerbHandler();
    }

    @Test
    public void setRepairedDataTrackingFlagIfHeaderPresent()
    {
        SinglePartitionReadCommand command = command(metadata);
        assertFalse(command.isTrackingRepairedStatus());
        Map<ParameterType, Object> params = ImmutableMap.of(ParameterType.TRACK_REPAIRED_DATA,
                                                            MessagingService.ONE_BYTE);
        handler.doVerb(MessageIn.create(peer(),
                                        command,
                                        params,
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       messageId());
        assertTrue(command.isTrackingRepairedStatus());
    }

    @Test
    public void dontSetRepairedDataTrackingFlagUnlessHeaderPresent()
    {
        SinglePartitionReadCommand command = command(metadata);
        assertFalse(command.isTrackingRepairedStatus());
        Map<ParameterType, Object> params = ImmutableMap.of(ParameterType.TRACE_SESSION,
                                                            UUID.randomUUID());
        handler.doVerb(MessageIn.create(peer(),
                                        command,
                                        params,
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       messageId());
        assertFalse(command.isTrackingRepairedStatus());
    }

    @Test
    public void dontSetRepairedDataTrackingFlagIfHeadersEmpty()
    {
        SinglePartitionReadCommand command = command(metadata);
        assertFalse(command.isTrackingRepairedStatus());
        handler.doVerb(MessageIn.create(peer(),
                                        command,
                                        ImmutableMap.of(),
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       messageId());
        assertFalse(command.isTrackingRepairedStatus());
    }

    @Test (expected = InvalidRequestException.class)
    public void rejectsRequestWithNonMatchingTransientness()
    {
        SinglePartitionReadCommand command = command(metadata_with_transient);
        handler.doVerb(MessageIn.create(peer(),
                                        command,
                                        ImmutableMap.of(),
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       messageId());
    }

    private static int messageId()
    {
        return random.nextInt();
    }

    private static InetAddressAndPort peer()
    {
        try
        {
            return InetAddressAndPort.getByAddress(new byte[]{ 127, 0, 0, 9});
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static SinglePartitionReadCommand command(TableMetadata metadata)
    {
        return new SinglePartitionReadCommand(false,
                                              0,
                                              false,
                                              metadata,
                                              FBUtilities.nowInSeconds(),
                                              ColumnFilter.all(metadata),
                                              RowFilter.NONE,
                                              DataLimits.NONE,
                                              KEY,
                                              new ClusteringIndexSliceFilter(Slices.ALL, false),
                                              null);
    }

    private static DecoratedKey key(TableMetadata metadata, int key)
    {
        return metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key));
    }
}
