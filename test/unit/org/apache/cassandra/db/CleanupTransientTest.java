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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.locator.RangesAtEndpoint;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class CleanupTransientTest
{
    private static final IPartitioner partitioner = RandomPartitioner.instance;
    private static IPartitioner oldPartitioner;

    public static final int LOOPS = 200;
    public static final String KEYSPACE1 = "CleanupTest1";
    public static final String CF_INDEXED1 = "Indexed1";
    public static final String CF_STANDARD1 = "Standard1";

    public static final String KEYSPACE2 = "CleanupTestMultiDc";
    public static final String CF_INDEXED2 = "Indexed2";
    public static final String CF_STANDARD2 = "Standard2";

    public static final ByteBuffer COLUMN = ByteBufferUtil.bytes("birthdate");
    public static final ByteBuffer VALUE = ByteBuffer.allocate(8);
    static
    {
        VALUE.putLong(20101229);
        VALUE.flip();
    }

    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        oldPartitioner = StorageService.instance.setPartitionerUnsafe(partitioner);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple("2/1"),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE1, CF_INDEXED1, true));

        StorageService ss = StorageService.instance;
        final int RING_SIZE = 2;

        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();

        endpointTokens.add(RandomPartitioner.MINIMUM);
        endpointTokens.add(RandomPartitioner.instance.midpoint(RandomPartitioner.MINIMUM, new RandomPartitioner.BigIntegerToken(RandomPartitioner.MAXIMUM)));

        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, RING_SIZE);
        PendingRangeCalculatorService.instance.blockUntilFinished();


        DatabaseDescriptor.setEndpointSnitch(new AbstractNetworkTopologySnitch()
        {
            @Override
            public String getRack(InetAddressAndPort endpoint)
            {
                return "RC1";
            }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return "DC1";
            }
        });
    }

    @Test
    public void testCleanup() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);


        // insert data and verify we get it back w/ range query
        fillCF(cfs, "val", LOOPS);

        // record max timestamps of the sstables pre-cleanup
        List<Long> expectedMaxTimestamps = getMaxTimestampList(cfs);

        assertEquals(LOOPS, Util.getAll(Util.cmd(cfs).build()).size());

        // with two tokens RF=2/1 and the sstable not repaired this should do nothing
        CompactionManager.instance.performCleanup(cfs, 2);

        // ensure max timestamp of the sstables are retained post-cleanup
        assert expectedMaxTimestamps.equals(getMaxTimestampList(cfs));

        // check data is still there
        assertEquals(LOOPS, Util.getAll(Util.cmd(cfs).build()).size());

        //Get an exact count of how many partitions are in the fully replicated range and should
        //be retained
        int fullCount = 0;
        RangesAtEndpoint localRanges = StorageService.instance.getLocalReplicas(keyspace.getName()).filter(Replica::isFull);
        for (FilteredPartition partition : Util.getAll(Util.cmd(cfs).build()))
        {
            Token token = partition.partitionKey().getToken();
            for (Replica r : localRanges)
            {
                if (r.range().contains(token))
                    fullCount++;
            }
        }

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, 1, null, false);
        sstable.reloadSSTableMetadata();

        // This should remove approximately 50% of the data, specifically whatever was transiently replicated
        CompactionManager.instance.performCleanup(cfs, 2);

        // ensure max timestamp of the sstables are retained post-cleanup
        assert expectedMaxTimestamps.equals(getMaxTimestampList(cfs));

        // check less data is there, all transient data should be gone since the table was repaired
        assertEquals(fullCount, Util.getAll(Util.cmd(cfs).build()).size());
    }

    protected void fillCF(ColumnFamilyStore cfs, String colName, int rowsPerSSTable)
    {
        CompactionManager.instance.disableAutoCompaction();

        for (int i = 0; i < rowsPerSSTable; i++)
        {
            String key = String.valueOf(i);
            // create a row and update the birthdate value, test that the index query fetches the new version
            new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), ByteBufferUtil.bytes(key))
                    .clustering(COLUMN)
                    .add(colName, VALUE)
                    .build()
                    .applyUnsafe();
        }

        Util.flush(cfs);
    }

    protected List<Long> getMaxTimestampList(ColumnFamilyStore cfs)
    {
        List<Long> list = new LinkedList<Long>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            list.add(sstable.getMaxTimestamp());
        return list;
    }
}
