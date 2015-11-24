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
package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.transport.Server;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.*;

@RunWith(OrderedJUnit4ClassRunner.class)
public class QueryPagerTest
{
    public static final String KEYSPACE1 = "QueryPagerTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String KEYSPACE_CQL = "cql_keyspace";
    public static final String CF_CQL = "table2";
    public static final int nowInSec = FBUtilities.nowInSeconds();

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));
        SchemaLoader.createKeyspace(KEYSPACE_CQL,
                                    KeyspaceParams.simple(1),
                                    CFMetaData.compile("CREATE TABLE " + CF_CQL + " ("
                                            + "k text,"
                                            + "c text,"
                                            + "v text,"
                                            + "PRIMARY KEY (k, c))", KEYSPACE_CQL));
        addData();
    }

    private static String string(ByteBuffer bb)
    {
        try
        {
            return ByteBufferUtil.string(bb);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void addData()
    {
        cfs().clearUnsafe();

        int nbKeys = 10;
        int nbCols = 10;

        // *
        // * Creates the following data:
        // *   k1: c1 ... cn
        // *   ...
        // *   ki: c1 ... cn
        // *
        for (int i = 0; i < nbKeys; i++)
        {
            for (int j = 0; j < nbCols; j++)
            {
                RowUpdateBuilder builder = new RowUpdateBuilder(cfs().metadata, FBUtilities.timestampMicros(), "k" + i);
                builder.clustering("c" + j).add("val", "").build().applyUnsafe();
            }
        }
    }

    private static ColumnFamilyStore cfs()
    {
        return Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD);
    }

    private static List<FilteredPartition> query(QueryPager pager, int expectedSize)
    {
        return query(pager, expectedSize, expectedSize);
    }

    private static List<FilteredPartition> query(QueryPager pager, int toQuery, int expectedSize)
    {
        StringBuilder sb = new StringBuilder();
        List<FilteredPartition> partitionList = new ArrayList<>();
        int rows = 0;
        try (ReadExecutionController executionController = pager.executionController();
             PartitionIterator iterator = pager.fetchPageInternal(toQuery, executionController))
        {
            while (iterator.hasNext())
            {
                try (RowIterator rowIter = iterator.next())
                {
                    FilteredPartition partition = FilteredPartition.create(rowIter);
                    sb.append(partition);
                    partitionList.add(partition);
                    rows += partition.rowCount();
                }
            }
        }
        assertEquals(sb.toString(), expectedSize, rows);
        return partitionList;
    }

    private static ReadCommand namesQuery(String key, String... names)
    {
        AbstractReadCommandBuilder builder = Util.cmd(cfs(), key);
        for (String name : names)
            builder.includeRow(name);
        return builder.withPagingLimit(100).build();
    }

    private static SinglePartitionReadCommand sliceQuery(String key, String start, String end, int count)
    {
        return sliceQuery(key, start, end, false, count);
    }

    private static SinglePartitionReadCommand sliceQuery(String key, String start, String end, boolean reversed, int count)
    {
        ClusteringComparator cmp = cfs().getComparator();
        CFMetaData metadata = cfs().metadata;

        Slice slice = Slice.make(cmp.make(start), cmp.make(end));
        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(Slices.with(cmp, slice), reversed);

        return SinglePartitionReadCommand.create(cfs().metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.NONE, DataLimits.NONE, Util.dk(key), filter);
    }

    private static ReadCommand rangeNamesQuery(String keyStart, String keyEnd, int count, String... names)
    {
        AbstractReadCommandBuilder builder = Util.cmd(cfs())
                                                 .fromKeyExcl(keyStart)
                                                 .toKeyIncl(keyEnd)
                                                 .withPagingLimit(count);
        for (String name : names)
            builder.includeRow(name);

        return builder.build();
    }

    private static ReadCommand rangeSliceQuery(String keyStart, String keyEnd, int count, String start, String end)
    {
        return Util.cmd(cfs())
                   .fromKeyExcl(keyStart)
                   .toKeyIncl(keyEnd)
                   .fromIncl(start)
                   .toIncl(end)
                   .withPagingLimit(count)
                   .build();
    }

    private static void assertRow(FilteredPartition r, String key, String... names)
    {
        ByteBuffer[] bbs = new ByteBuffer[names.length];
        for (int i = 0; i < names.length; i++)
            bbs[i] = bytes(names[i]);
        assertRow(r, key, bbs);
    }

    private static void assertRow(FilteredPartition partition, String key, ByteBuffer... names)
    {
        assertEquals(key, string(partition.partitionKey().getKey()));
        assertFalse(partition.isEmpty());
        int i = 0;
        for (Row row : Util.once(partition.iterator()))
        {
            ByteBuffer expected = names[i++];
            assertEquals("column " + i + " doesn't match "+string(expected)+" vs "+string(row.clustering().get(0)), expected, row.clustering().get(0));
        }
    }

    private QueryPager maybeRecreate(QueryPager pager, ReadQuery command, boolean testPagingState, int protocolVersion)
    {
        if (!testPagingState)
            return pager;

        PagingState state = PagingState.deserialize(pager.state().serialize(protocolVersion), protocolVersion);
        return command.getPager(state, protocolVersion);
    }

    @Test
    public void namesQueryTest() throws Exception
    {
        QueryPager pager = namesQuery("k0", "c1", "c5", "c7", "c8").getPager(null, Server.CURRENT_VERSION);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partition = query(pager, 5, 4);
        assertRow(partition.get(0), "k0", "c1", "c5", "c7", "c8");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void sliceQueryTest() throws Exception
    {
        sliceQueryTest(false, Server.VERSION_3);
        sliceQueryTest(true, Server.VERSION_4);
        sliceQueryTest(false, Server.VERSION_3);
        sliceQueryTest(true, Server.VERSION_4);
    }

    public void sliceQueryTest(boolean testPagingState, int protocolVersion) throws Exception
    {
        ReadCommand command = sliceQuery("k0", "c1", "c8", 10);
        QueryPager pager = command.getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partition = query(pager, 3);
        assertRow(partition.get(0), "k0", "c1", "c2", "c3");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager, 3);
        assertRow(partition.get(0), "k0", "c4", "c5", "c6");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager, 3, 2);
        assertRow(partition.get(0), "k0", "c7", "c8");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void reversedSliceQueryTest() throws Exception
    {
        reversedSliceQueryTest(false, Server.VERSION_3);
        reversedSliceQueryTest(true, Server.VERSION_4);
        reversedSliceQueryTest(false, Server.VERSION_3);
        reversedSliceQueryTest(true, Server.VERSION_4);
    }

    public void reversedSliceQueryTest(boolean testPagingState, int protocolVersion) throws Exception
    {
        ReadCommand command = sliceQuery("k0", "c1", "c8", true, 10);
        QueryPager pager = command.getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partition = query(pager, 3);
        assertRow(partition.get(0), "k0", "c6", "c7", "c8");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager, 3);
        assertRow(partition.get(0), "k0", "c3", "c4", "c5");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager, 3, 2);
        assertRow(partition.get(0), "k0", "c1", "c2");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void multiQueryTest() throws Exception
    {
        multiQueryTest(false, Server.VERSION_3);
        multiQueryTest(true, Server.VERSION_4);
        multiQueryTest(false, Server.VERSION_3);
        multiQueryTest(true, Server.VERSION_4);
    }

    public void multiQueryTest(boolean testPagingState, int protocolVersion) throws Exception
    {
        ReadQuery command = new SinglePartitionReadCommand.Group(new ArrayList<SinglePartitionReadCommand>()
        {{
            add(sliceQuery("k1", "c2", "c6", 10));
            add(sliceQuery("k4", "c3", "c5", 10));
        }}, DataLimits.NONE);
        QueryPager pager = command.getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partition = query(pager, 3);
        assertRow(partition.get(0), "k1", "c2", "c3", "c4");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager , 4);
        assertRow(partition.get(0), "k1", "c5", "c6");
        assertRow(partition.get(1), "k4", "c3", "c4");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager, 3, 1);
        assertRow(partition.get(0), "k4", "c5");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void rangeNamesQueryTest() throws Exception
    {
        rangeNamesQueryTest(false, Server.VERSION_3);
        rangeNamesQueryTest(true, Server.VERSION_4);
        rangeNamesQueryTest(false, Server.VERSION_3);
        rangeNamesQueryTest(true, Server.VERSION_4);
    }

    public void rangeNamesQueryTest(boolean testPagingState, int protocolVersion) throws Exception
    {
        ReadCommand command = rangeNamesQuery("k0", "k5", 100, "c1", "c4", "c8");
        QueryPager pager = command.getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partitions = query(pager, 3 * 3);
        for (int i = 1; i <= 3; i++)
            assertRow(partitions.get(i-1), "k" + i, "c1", "c4", "c8");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 3 * 3, 2 * 3);
        for (int i = 4; i <= 5; i++)
            assertRow(partitions.get(i-4), "k" + i, "c1", "c4", "c8");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void rangeSliceQueryTest() throws Exception
    {
        rangeSliceQueryTest(false, Server.VERSION_3);
        rangeSliceQueryTest(true, Server.VERSION_4);
        rangeSliceQueryTest(false, Server.VERSION_3);
        rangeSliceQueryTest(true, Server.VERSION_4);
    }

    public void rangeSliceQueryTest(boolean testPagingState, int protocolVersion) throws Exception
    {
        ReadCommand command = rangeSliceQuery("k1", "k5", 100, "c1", "c7");
        QueryPager pager = command.getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partitions = query(pager, 5);
        assertRow(partitions.get(0), "k2", "c1", "c2", "c3", "c4", "c5");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 4);
        assertRow(partitions.get(0), "k2", "c6", "c7");
        assertRow(partitions.get(1), "k3", "c1", "c2");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 6);
        assertRow(partitions.get(0), "k3", "c3", "c4", "c5", "c6", "c7");
        assertRow(partitions.get(1), "k4", "c1");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 5);
        assertRow(partitions.get(0), "k4", "c2", "c3", "c4", "c5", "c6");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 5);
        assertRow(partitions.get(0), "k4", "c7");
        assertRow(partitions.get(1), "k5", "c1", "c2", "c3", "c4");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 5, 3);
        assertRow(partitions.get(0), "k5", "c5", "c6", "c7");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void SliceQueryWithTombstoneTest() throws Exception
    {
        // Testing for the bug of #6748
        String keyspace = "cql_keyspace";
        String table = "table2";
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        // Insert rows but with a tombstone as last cell
        for (int i = 0; i < 5; i++)
            executeInternal(String.format("INSERT INTO %s.%s (k, c, v) VALUES ('k%d', 'c%d', null)", keyspace, table, 0, i));

        ReadCommand command = SinglePartitionReadCommand.create(cfs.metadata, nowInSec, Util.dk("k0"), Slice.ALL);

        QueryPager pager = command.getPager(null, Server.CURRENT_VERSION);

        for (int i = 0; i < 5; i++)
        {
            List<FilteredPartition> partitions = query(pager, 1);
            // The only live cell we should have each time is the row marker
            assertRow(partitions.get(0), "k0", "c" + i);
        }
    }
}
