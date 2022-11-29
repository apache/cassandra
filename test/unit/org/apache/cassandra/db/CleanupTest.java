/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CleanupTest
{
    public static final int LOOPS = 200;
    public static final String KEYSPACE1 = "CleanupTest1";
    public static final String CF_INDEXED1 = "Indexed1";
    public static final String CF_STANDARD1 = "Standard1";

    public static final String KEYSPACE2 = "CleanupTestMultiDc";
    public static final String CF_INDEXED2 = "Indexed2";
    public static final String CF_STANDARD2 = "Standard2";

    public static final String KEYSPACE3 = "CleanupSkipSSTables";
    public static final String CF_STANDARD3 = "Standard3";

    public static final ByteBuffer COLUMN = ByteBufferUtil.bytes("birthdate");
    public static final ByteBuffer VALUE = ByteBuffer.allocate(8);
    static
    {
        VALUE.putLong(20101229);
        VALUE.flip();
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE1, CF_INDEXED1, true));


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

        SchemaLoader.createKeyspace(KEYSPACE2,
                                    KeyspaceParams.nts("DC1", 1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD2),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE2, CF_INDEXED2, true));
        SchemaLoader.createKeyspace(KEYSPACE3,
                                    KeyspaceParams.nts("DC1", 1),
                                    SchemaLoader.standardCFMD(KEYSPACE3, CF_STANDARD3));
    }

    @Test
    public void testCleanup() throws ExecutionException, InterruptedException, UnknownHostException
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        tmd.updateNormalToken(token(new byte[]{ 50 }), InetAddressAndPort.getByName("127.0.0.1"));

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);


        // insert data and verify we get it back w/ range query
        fillCF(cfs, "val", LOOPS);

        // record max timestamps of the sstables pre-cleanup
        List<Long> expectedMaxTimestamps = getMaxTimestampList(cfs);

        assertEquals(LOOPS, Util.getAll(Util.cmd(cfs).build()).size());

        // with one token in the ring, owned by the local node, cleanup should be a no-op
        CompactionManager.instance.performCleanup(cfs, 2);

        // ensure max timestamp of the sstables are retained post-cleanup
        assert expectedMaxTimestamps.equals(getMaxTimestampList(cfs));

        // check data is still there
        assertEquals(LOOPS, Util.getAll(Util.cmd(cfs).build()).size());
    }

    @Test
    public void testCleanupWithIndexes() throws IOException, ExecutionException, InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_INDEXED1);


        // insert data and verify we get it back w/ range query
        fillCF(cfs, "birthdate", LOOPS);
        assertEquals(LOOPS, Util.getAll(Util.cmd(cfs).build()).size());

        ColumnMetadata cdef = cfs.metadata().getColumn(COLUMN);
        String indexName = "birthdate_key_index";
        long start = nanoTime();
        while (!cfs.getBuiltIndexes().contains(indexName) && nanoTime() - start < TimeUnit.SECONDS.toNanos(10))
            Thread.sleep(10);

        RowFilter cf = RowFilter.create();
        cf.add(cdef, Operator.EQ, VALUE);
        assertEquals(LOOPS, Util.getAll(Util.cmd(cfs).filterOn("birthdate", Operator.EQ, VALUE).build()).size());

        // we don't allow cleanup when the local host has no range to avoid wipping up all data when a node has not join the ring.
        // So to make sure cleanup erase everything here, we give the localhost the tiniest possible range.
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        byte[] tk1 = new byte[1], tk2 = new byte[1];
        tk1[0] = 2;
        tk2[0] = 1;
        tmd.updateNormalToken(new BytesToken(tk1), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateNormalToken(new BytesToken(tk2), InetAddressAndPort.getByName("127.0.0.2"));

        CompactionManager.instance.performCleanup(cfs, 2);

        // row data should be gone
        assertEquals(0, Util.getAll(Util.cmd(cfs).build()).size());

        // not only should it be gone but there should be no data on disk, not even tombstones
        assert cfs.getLiveSSTables().isEmpty();

        // 2ary indexes should result in no results, too (although tombstones won't be gone until compacted)
        assertEquals(0, Util.getAll(Util.cmd(cfs).filterOn("birthdate", Operator.EQ, VALUE).build()).size());
    }

    @Test
    public void testCleanupWithNewToken() throws ExecutionException, InterruptedException, UnknownHostException
    {
        StorageService.instance.getTokenMetadata().clearUnsafe();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, "val", LOOPS);

        assertEquals(LOOPS, Util.getAll(Util.cmd(cfs).build()).size());
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        byte[] tk1 = new byte[1], tk2 = new byte[1];
        tk1[0] = 2;
        tk2[0] = 1;
        tmd.updateNormalToken(new BytesToken(tk1), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateNormalToken(new BytesToken(tk2), InetAddressAndPort.getByName("127.0.0.2"));
        CompactionManager.instance.performCleanup(cfs, 2);

        assertEquals(0, Util.getAll(Util.cmd(cfs).build()).size());
    }

    @Test
    public void testCleanupWithNoTokenRange() throws Exception
    {
        testCleanupWithNoTokenRange(false);
    }

    @Test
    public void testUserDefinedCleanupWithNoTokenRange() throws Exception
    {
        testCleanupWithNoTokenRange(true);
    }

    private void testCleanupWithNoTokenRange(boolean isUserDefined) throws Exception
    {

        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        tmd.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.0.0.1"));
        byte[] tk1 = {2};
        tmd.updateNormalToken(new BytesToken(tk1), InetAddressAndPort.getByName("127.0.0.1"));


        Keyspace keyspace = Keyspace.open(KEYSPACE2);
        keyspace.setMetadata(KeyspaceMetadata.create(KEYSPACE2, KeyspaceParams.nts("DC1", 1)));
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD2);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, "val", LOOPS);
        assertEquals(LOOPS, Util.getAll(Util.cmd(cfs).build()).size());

        // remove replication on DC1
        keyspace.setMetadata(KeyspaceMetadata.create(KEYSPACE2, KeyspaceParams.nts("DC1", 0)));

        // clear token range for localhost on DC1
        if (isUserDefined)
        {
            for (SSTableReader r : cfs.getLiveSSTables())
                CompactionManager.instance.forceUserDefinedCleanup(r.getFilename());
        }
        else
        {
            CompactionManager.instance.performCleanup(cfs, 2);
        }
        assertEquals(0, Util.getAll(Util.cmd(cfs).build()).size());
        assertTrue(cfs.getLiveSSTables().isEmpty());
    }

    @Test
    public void testCleanupSkippingSSTables() throws UnknownHostException, ExecutionException, InterruptedException
    {
        testCleanupSkippingSSTablesHelper(false);
    }

    @Test
    public void testCleanupSkippingRepairedSSTables() throws UnknownHostException, ExecutionException, InterruptedException
    {
        testCleanupSkippingSSTablesHelper(true);
    }

    public void testCleanupSkippingSSTablesHelper(boolean repaired) throws UnknownHostException, ExecutionException, InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE3);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD3);
        cfs.disableAutoCompaction();
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        tmd.updateNormalToken(token(new byte[]{ 50 }), InetAddressAndPort.getByName("127.0.0.1"));

        for (byte i = 0; i < 100; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), ByteBuffer.wrap(new byte[]{ i }))
            .clustering(COLUMN)
            .add("val", VALUE)
            .build()
            .applyUnsafe();
            Util.flush(cfs);
        }

        Set<SSTableReader> beforeFirstCleanup = Sets.newHashSet(cfs.getLiveSSTables());
        if (repaired)
        {
            beforeFirstCleanup.forEach((sstable) -> {
                try
                {
                    sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, System.currentTimeMillis(), null, false);
                    sstable.reloadSSTableMetadata();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            });
        }
        // single token - 127.0.0.1 owns everything, cleanup should be noop
        cfs.forceCleanup(2);
        assertEquals(beforeFirstCleanup, cfs.getLiveSSTables());
        tmd.updateNormalToken(token(new byte[]{ 120 }), InetAddressAndPort.getByName("127.0.0.2"));

        cfs.forceCleanup(2);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            assertEquals(sstable.getFirst(), sstable.getLast()); // single-token sstables
            assertTrue(sstable.getFirst().getToken().compareTo(token(new byte[]{ 50 })) <= 0);
            // with single-token sstables they should all either be skipped or dropped:
            assertTrue(beforeFirstCleanup.contains(sstable));
        }

    }


    @Test
    public void testuserDefinedCleanupWithNewToken() throws ExecutionException, InterruptedException, UnknownHostException
    {
        StorageService.instance.getTokenMetadata().clearUnsafe();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, "val", LOOPS);

        assertEquals(LOOPS, Util.getAll(Util.cmd(cfs).build()).size());
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        byte[] tk1 = new byte[1], tk2 = new byte[1];
        tk1[0] = 2;
        tk2[0] = 1;
        tmd.updateNormalToken(new BytesToken(tk1), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateNormalToken(new BytesToken(tk2), InetAddressAndPort.getByName("127.0.0.2"));

        for(SSTableReader r: cfs.getLiveSSTables())
            CompactionManager.instance.forceUserDefinedCleanup(r.getFilename());

        assertEquals(0, Util.getAll(Util.cmd(cfs).build()).size());
    }

    @Test
    public void testNeedsCleanup() throws Exception
    {
        // setup
        StorageService.instance.getTokenMetadata().clearUnsafe();
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        fillCF(cfs, "val", LOOPS);

        // prepare SSTable and some useful tokens
        SSTableReader ssTable = cfs.getLiveSSTables().iterator().next();
        final Token ssTableMin = ssTable.getFirst().getToken();
        final Token ssTableMax = ssTable.getLast().getToken();

        final Token min = token((byte) 0);
        final Token before1 = token((byte) 2);
        final Token before2 = token((byte) 5);
        final Token before3 = token((byte) 10);
        final Token before4 = token((byte) 47);
        final Token insideSsTable1 = token((byte) 50);
        final Token insideSsTable2 = token((byte) 55);
        final Token max = token((byte) 127, (byte) 127, (byte) 127, (byte) 127);

        // test sanity check
        assert (min.compareTo(ssTableMin) < 0);
        assert (before1.compareTo(ssTableMin) < 0);
        assert (before2.compareTo(ssTableMin) < 0);
        assert (before3.compareTo(ssTableMin) < 0);
        assert (before4.compareTo(ssTableMin) < 0);
        assert (ssTableMin.compareTo(insideSsTable1) < 0);
        assert (insideSsTable1.compareTo(ssTableMax) < 0);
        assert (ssTableMin.compareTo(insideSsTable2) < 0);
        assert (insideSsTable2.compareTo(ssTableMax) < 0);
        assert (ssTableMax.compareTo(max) < 0);

        // test cases
        // key: needs cleanup?
        // value: owned ranges
        List<Map.Entry<Boolean, List<Range<Token>>>> testCases = new LinkedList<Map.Entry<Boolean, List<Range<Token>>>>()
        {
            {
                add(entry(false, Arrays.asList(range(min, max)))); // SSTable owned as a whole
                add(entry(true, Arrays.asList(range(min, insideSsTable1)))); // SSTable owned only partially
                add(entry(true, Arrays.asList(range(insideSsTable1, max)))); // SSTable owned only partially
                add(entry(true, Arrays.asList(range(min, ssTableMin)))); // SSTable not owned at all
                add(entry(true, Arrays.asList(range(ssTableMax, max)))); // only last token of SSTable is owned
                add(entry(true, Arrays.asList(range(min, insideSsTable1), range(insideSsTable2, max)))); // SSTable partially owned by two ranges
                add(entry(true, Arrays.asList(range(ssTableMin, ssTableMax)))); // first token of SSTable is not owned
                add(entry(false, Arrays.asList(range(before4, max)))); // first token of SSTable is not owned
                add(entry(false, Arrays.asList(range(min, before1), range(before2, before3), range(before4, max)))); // SSTable owned by the last range
                add(entry(true, Collections.EMPTY_LIST)); // empty token range means discard entire sstable
            }
        };

        // check all test cases
        for (Map.Entry<Boolean, List<Range<Token>>> testCase : testCases)
        {
            assertEquals(testCase.getKey(), CompactionManager.needsCleanup(ssTable, testCase.getValue()));
        }
    }
    private static BytesToken token(byte ... value)
    {
        return new BytesToken(value);
    }
    private static <K, V> Map.Entry<K, V> entry(K k, V v)
    {
       return new AbstractMap.SimpleEntry<K, V>(k, v);
    }
    private static Range<Token> range(Token from, Token to)
    {
        return new Range<>(from, to);
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
