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

package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.db.compaction.CompactionsTest.populate;
import static org.junit.Assert.assertTrue;

public class CompactionValidationTest
{

    private static final String keyspace = "ThrottlingCompactionValidationTest";
    private static final String columnFamily = "Standard1";
    private static final int ROWS = 500_000;

    private ColumnFamilyStore cfs;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(keyspace,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(keyspace, columnFamily));
    }

    @Before
    public void setup()
    {
        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
            {
                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return false;
            }
        });

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(columnFamily);

        cfs.disableAutoCompaction();

        populate(keyspace, columnFamily, 0, ROWS, 0);

        cfs.forceBlockingFlush();
    }

    @After
    public void tearDown()
    {
        MessagingService.instance().clearMessageSinks();
        // set it back how it was
        DatabaseDescriptor.setCompactionThroughputMbPerSec(0);
    }

    @Test
    public void throttledValidationIsSlowerThanUnthrottledValidationTest() throws Exception
    {
        // unthrottle, validate as fast as possible
        DatabaseDescriptor.setCompactionThroughputMbPerSec(0);
        final long unthrottledRuntime = executeValidation();

        // throttle to 1 MBPS
        DatabaseDescriptor.setCompactionThroughputMbPerSec(1);
        final long throttledRuntime = executeValidation();

        // throttle to 2 MBPS
        DatabaseDescriptor.setCompactionThroughputMbPerSec(2);
        final long throttledRuntime2 = executeValidation();

        assertTrue(format("Validation compaction with throttled throughtput to 1 Mbps took less time (in ms) than unthrottled validation compaction: %s vs. %s",
                          throttledRuntime, unthrottledRuntime),
                   throttledRuntime > unthrottledRuntime);

        assertTrue(format("Validation compaction with throttled throughtput on 1 Mbps took less time (in ms) than throttled validation compaction to 2 Mbps: %s vs. %s",
                          throttledRuntime, throttledRuntime2),
                   throttledRuntime > throttledRuntime2);
    }

    private long executeValidation() throws Exception
    {
        final UUID repairSessionId = UUIDGen.getTimeUUID();

        final List<Range<Token>> ranges = Stream.of(cfs.getLiveSSTables().iterator().next())
                                                .map(sstable -> new Range<>(sstable.first.getToken(), sstable.last.getToken()))
                                                .collect(toList());

        final RepairJobDesc repairJobDesc = new RepairJobDesc(repairSessionId,
                                                              UUIDGen.getTimeUUID(),
                                                              cfs.keyspace.getName(),
                                                              cfs.getTableName(),
                                                              ranges);

        ActiveRepairService.instance.registerParentRepairSession(repairSessionId,
                                                                 FBUtilities.getBroadcastAddress(),
                                                                 Collections.singletonList(cfs),
                                                                 repairJobDesc.ranges,
                                                                 false,
                                                                 ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                 false);

        final Validator validator = new Validator(repairJobDesc, FBUtilities.getBroadcastAddress(), 0, true);

        final long start = System.currentTimeMillis();

        CompactionManager.instance.submitValidation(cfs, validator).get();

        return System.currentTimeMillis() - start;
    }
}
