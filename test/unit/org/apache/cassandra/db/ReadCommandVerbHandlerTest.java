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
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadCommandVerbHandlerTest
{
    private static final String TEST_NAME = "read_command_vh_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace";
    private static final String TABLE = "table1";

    private final Random random = new Random();
    private ReadCommandVerbHandler handler;
    private TableMetadata metadata;

    @BeforeClass
    public static void init()
    {
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition(TEST_NAME);
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

        metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        handler = new ReadCommandVerbHandler();
    }

    @Test
    public void setRepairedDataTrackingFlagIfHeaderPresent()
    {
        ReadCommand command = command(key());
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
        ReadCommand command = command(key());
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
        ReadCommand command = command(key());
        assertFalse(command.isTrackingRepairedStatus());
        handler.doVerb(MessageIn.create(peer(),
                                        command,
                                        ImmutableMap.of(),
                                        MessagingService.Verb.READ,
                                        MessagingService.current_version),
                       messageId());
        assertFalse(command.isTrackingRepairedStatus());
    }

    private int key()
    {
        return random.nextInt();
    }

    private int messageId()
    {
        return random.nextInt();
    }

    private InetAddressAndPort peer()
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

    private ReadCommand command(int key)
    {
        return new SinglePartitionReadCommand(false,
              0,
              false,
              metadata,
              FBUtilities.nowInSeconds(),
              ColumnFilter.all(metadata),
              RowFilter.NONE,
              DataLimits.NONE,
              metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key)),
              new ClusteringIndexSliceFilter(Slices.ALL, false),
              null);
    }

}
