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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadQuery;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand.Group;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.AggregationQueryPager;
import org.apache.cassandra.service.pager.MultiPartitionPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.PartitionRangeQueryPager;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(OrderedJUnit4ClassRunner.class)
public class QueryPagerTest
{
    private final static Logger logger = LoggerFactory.getLogger(QueryPagerTest.class);

    public static final String KEYSPACE1 = "QueryPagerTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String KEYSPACE_CQL = "cql_keyspace";
    public static final String CF_CQL = "table2";
    public static final String CF_CQL_WITH_STATIC = "with_static";
    public static final int nowInSec = FBUtilities.nowInSeconds();
    public static List<String> tokenOrderedKeys;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));

        SchemaLoader.createKeyspace(KEYSPACE_CQL,
                                    KeyspaceParams.simple(1),
                                    CreateTableStatement.parse("CREATE TABLE " + CF_CQL + " ("
                                                               + "k text,"
                                                               + "c text,"
                                                               + "v text,"
                                                               + "PRIMARY KEY (k, c))", KEYSPACE_CQL),
                                    CreateTableStatement.parse("CREATE TABLE " + CF_CQL_WITH_STATIC + " ("
                                                               + "k text, "
                                                               + "c text, "
                                                               + "st int static, "
                                                               + "v1 int, "
                                                               + "v2 int, "
                                                               + "PRIMARY KEY(k, c))", KEYSPACE_CQL));
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
        cfs(KEYSPACE1, CF_STANDARD).clearUnsafe();

        int nbKeys = 10;
        int nbCols = 10;

        SortedSet<String> tokens = Sets.newTreeSet(Comparator.comparing(a -> cfs(KEYSPACE1, CF_STANDARD).getPartitioner().decorateKey(bytes(a))));

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
                tokens.add("k" + i);
                RowUpdateBuilder builder = new RowUpdateBuilder(cfs(KEYSPACE1, CF_STANDARD).metadata(), FBUtilities.timestampMicros(), "k" + i);
                builder.clustering("c" + j).add("val", "").build().applyUnsafe();
            }
        }

        tokenOrderedKeys = Lists.newArrayList(tokens);
    }

    private static ColumnFamilyStore cfs(String ks, String cf)
    {
        return Keyspace.open(ks).getColumnFamilyStore(cf);
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
             PartitionIterator iterator = pager.fetchPageInternal(PageSize.inRows(toQuery), executionController))
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

    private static Map<DecoratedKey, List<Row>> fetchPage(QueryPager pager, int pageSize, PageSize.PageUnit pageUnit)
    {
        logger.info("----------------------------------------------------------------");
        Map<DecoratedKey, List<Row>> ret = Maps.newHashMap();
        try (ReadExecutionController ec = pager.executionController();
             PartitionIterator iterator = pager.fetchPageInternal(new PageSize(pageSize, pageUnit), ec))
        {
            while (iterator.hasNext())
            {
                try (RowIterator partition = iterator.next())
                {
                    logger.info("Partition {}", partition.partitionKey());
                    List<Row> rows = new ArrayList<>();
                    Row staticRow = partition.staticRow();
                    if (!partition.hasNext() && !staticRow.isEmpty())
                    {
                        rows.add(staticRow);
                        logger.info("\tStatic row {}", staticRow.toString(partition.metadata()));
                    }

                    while (partition.hasNext())
                    {
                        Row row = partition.next();
                        rows.add(row);
                        logger.info("\tRow {}", row.toString(partition.metadata()));
                    }

                    ret.put(partition.partitionKey(), rows);
                }
            }
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            throw t;
        }
        return ret;
    }

    private static ReadCommand namesQuery(int count, int partitionCount, PageSize pageSize, ColumnFamilyStore cfs, String key, String... names)
    {
        AbstractReadCommandBuilder builder = Util.cmd(cfs, key).withNowInSeconds(nowInSec);
        for (String name : names)
            builder.includeRow(name);
        if (count > 0)
            builder.withLimit(count);
        if (partitionCount > 0)
            builder.withPerPartitionLimit(partitionCount);
        if (pageSize != null && !pageSize.equals(PageSize.NONE))
            builder.withPageSize(pageSize);

        return builder.build();
    }

    private static SinglePartitionReadCommand sliceQuery(ColumnFamilyStore cfs, String key, String start, String end)
    {
        return sliceQuery(-1, -1, PageSize.NONE, cfs, key, start, end, false);
    }

    private static SinglePartitionReadCommand sliceQuery(ColumnFamilyStore cfs, String key, String start, String end, boolean reversed)
    {
        return sliceQuery(-1, -1, PageSize.NONE, cfs, key, start, end, reversed);
    }

    private static SinglePartitionReadCommand sliceQuery(int count, int partitionCount, PageSize paging, ColumnFamilyStore cfs, String key, String start, String end, boolean reversed)
    {
        AbstractReadCommandBuilder builder = Util.cmd(cfs, key).fromIncl(start).toIncl(end).withNowInSeconds(nowInSec);
        if (reversed)
            builder.reverse();
        if (count > 0)
            builder.withLimit(count);
        if (partitionCount > 0)
            builder.withPerPartitionLimit(partitionCount);
        if (paging != null && !paging.equals(PageSize.NONE))
            builder.withPageSize(paging);

        return (SinglePartitionReadCommand) builder.build();
    }

    private static ReadCommand rangeNamesQuery(int count, int partitionCount, PageSize paging, ColumnFamilyStore cfs, String keyStart, String keyEnd, String... names)
    {
        AbstractReadCommandBuilder builder = Util.cmd(cfs)
                                                 .fromKeyExcl(keyStart)
                                                 .toKeyIncl(keyEnd)
                                                 .withNowInSeconds(nowInSec);
        for (String name : names)
            builder.includeRow(name);
        if (count > 0)
            builder.withLimit(count);
        if (partitionCount > 0)
            builder.withPerPartitionLimit(partitionCount);
        if (paging != null && !paging.equals(PageSize.NONE))
            builder.withPageSize(paging);

        return builder.build();
    }

    private static ReadCommand rangeSliceQuery(int count, int partitionCount, PageSize paging, ColumnFamilyStore cfs, String keyStart, String keyEnd, String start, String end)
    {
        AbstractReadCommandBuilder builder = Util.cmd(cfs)
                                                 .fromKeyExcl(keyStart)
                                                 .toKeyIncl(keyEnd)
                                                 .fromIncl(start)
                                                 .toIncl(end)
                                                 .withNowInSeconds(nowInSec);
        if (count > 0)
            builder.withLimit(count);
        if (partitionCount > 0)
            builder.withPerPartitionLimit(partitionCount);
        if (paging != null && !paging.equals(PageSize.NONE))
            builder.withPageSize(paging);

        return builder.build();
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
            assertEquals("column " + i + " doesn't match " + string(expected) + " vs " + string(row.clustering().bufferAt(0)), expected, row.clustering().bufferAt(0));
        }
    }

    private QueryPager maybeRecreate(QueryPager pager, ReadQuery command, boolean testPagingState, ProtocolVersion protocolVersion)
    {
        if (!testPagingState)
            return pager;

        PagingState state = PagingState.deserialize(pager.state().serialize(protocolVersion), protocolVersion);
        return command.getPager(state, protocolVersion);
    }

    @Test
    public void namesQueryTest()
    {
        for (ProtocolVersion protocolVersion : ProtocolVersion.SUPPORTED)
            namesQueryTest(protocolVersion);
    }

    public void namesQueryTest(ProtocolVersion protocolVersion)
    {
        QueryPager pager = namesQuery(-1, -1, new PageSize(100, PageSize.PageUnit.ROWS),
                                      cfs(KEYSPACE1, CF_STANDARD),
                                      "k0", "c1", "c5", "c7", "c8")
                           .getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partition = query(pager, 5, 4);
        assertRow(partition.get(0), "k0", "c1", "c5", "c7", "c8");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void sliceQueryTest()
    {
        for (ProtocolVersion protocolVersion : ProtocolVersion.SUPPORTED)
        {
            sliceQueryTest(false, protocolVersion);
            sliceQueryTest(true, protocolVersion);
        }
    }

    public void sliceQueryTest(boolean testPagingState, ProtocolVersion protocolVersion)
    {
        ReadCommand command = sliceQuery(cfs(KEYSPACE1, CF_STANDARD), "k0", "c1", "c8");
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
    public void sliceQueryWithLimitsTest() throws Exception
    {
        boolean testPagingState = true;
        ProtocolVersion protocolVersion = ProtocolVersion.CURRENT;

        // Test with count < partitionCount

        int count = 1;
        int partitionCount = 2;

        ReadCommand command = sliceQuery(count, partitionCount, PageSize.NONE, cfs(KEYSPACE1, CF_STANDARD), "k0", "c1", "c8", false);
        QueryPager pager = command.getPager(null, protocolVersion);
        List<FilteredPartition> partition = query(pager, 3, count);
        assertRow(partition.get(0), "k0", "c1");
        assertTrue(pager.isExhausted());

        // Test with count > partitionCount

        count = 2;
        partitionCount = 1;

        command = sliceQuery(count, partitionCount, PageSize.NONE, cfs(KEYSPACE1, CF_STANDARD), "k0", "c1", "c8", false);
        pager = command.getPager(null, protocolVersion);
        partition = query(pager, 3, partitionCount);
        assertRow(partition.get(0), "k0", "c1");
        assertTrue(pager.isExhausted());

        // Test with counts spanning multiple pages

        count = 5;
        partitionCount = 5;

        command = sliceQuery(count, partitionCount, PageSize.NONE, cfs(KEYSPACE1, CF_STANDARD), "k0", "c1", "c8", false);
        pager = command.getPager(null, protocolVersion);
        partition = query(pager, 3, 3);
        assertRow(partition.get(0), "k0", "c1", "c2", "c3");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        partition = query(pager, 3, 2);
        assertRow(partition.get(0), "k0", "c4", "c5");
        assertTrue(pager.isExhausted());
    }

    @Test
    public void reversedSliceQueryTest()
    {
        for (ProtocolVersion protocolVersion : ProtocolVersion.SUPPORTED)
        {
            reversedSliceQueryTest(false, protocolVersion);
            reversedSliceQueryTest(true, protocolVersion);
        }
    }

    public void reversedSliceQueryTest(boolean testPagingState, ProtocolVersion protocolVersion)
    {
        ReadCommand command = sliceQuery(cfs(KEYSPACE1, CF_STANDARD), "k0", "c1", "c8", true);
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
    public void multiQueryTest()
    {
        for (ProtocolVersion protocolVersion : ProtocolVersion.SUPPORTED)
        {
            multiQueryTest(false, protocolVersion);
            multiQueryTest(true, protocolVersion);
        }
    }

    public void multiQueryTest(boolean testPagingState, ProtocolVersion protocolVersion)
    {
        ReadQuery command = new Group(new ArrayList<SinglePartitionReadCommand>()
        {{
            add(sliceQuery(cfs(KEYSPACE1, CF_STANDARD), "k1", "c2", "c6"));
            add(sliceQuery(cfs(KEYSPACE1, CF_STANDARD), "k4", "c3", "c5"));
        }}, DataLimits.NONE);
        QueryPager pager = command.getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partition = query(pager, 3);
        assertRow(partition.get(0), "k1", "c2", "c3", "c4");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager, 4);
        assertRow(partition.get(0), "k1", "c5", "c6");
        assertRow(partition.get(1), "k4", "c3", "c4");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager, 3, 1);
        assertRow(partition.get(0), "k4", "c5");

        assertTrue(pager.isExhausted());
    }

    /**
     * Test a query with 1 CQL row per partition with various page sizes.
     */
    @Test
    public void multiPartitionSingleRowQueryTest() throws Exception
    {
        int totQueryRows = 4;
        ReadQuery command = new Group(new ArrayList<SinglePartitionReadCommand>()
        {{
            add(sliceQuery(cfs(KEYSPACE1, CF_STANDARD), "k1", "c1", "c1"));
            add(sliceQuery(cfs(KEYSPACE1, CF_STANDARD), "k2", "c1", "c1"));
            add(sliceQuery(cfs(KEYSPACE1, CF_STANDARD), "k3", "c1", "c1"));
            add(sliceQuery(cfs(KEYSPACE1, CF_STANDARD), "k4", "c1", "c1"));
        }}, DataLimits.NONE);

        checkRows(command, PageSize.PageUnit.ROWS, totQueryRows, new int[]{ 7, 8, 9, 10, 15, 16, 20 });
    }

    /**
     * Test a query with 4 CQL rows per partition with various page sizes.
     */
    @Test
    public void multiPartitionFourRowsQueryTest() throws Exception
    {
        int totQueryRows = 8;
        ReadQuery command = new Group(new ArrayList<SinglePartitionReadCommand>()
        {{
            add(sliceQuery(cfs(KEYSPACE1, CF_STANDARD), "k1", "c1", "c4"));
            add(sliceQuery(cfs(KEYSPACE1, CF_STANDARD), "k2", "c1", "c4"));
            add(sliceQuery(cfs(KEYSPACE1, CF_STANDARD), "k3", "c1", "c4"));
            add(sliceQuery(cfs(KEYSPACE1, CF_STANDARD), "k4", "c1", "c4"));
        }}, DataLimits.cqlLimits(8));

        checkRows(command, PageSize.PageUnit.ROWS, totQueryRows, new int[]{ 2, 7, 8, 9, 10, 15, 16, 20 });
    }

    @Test
    public void multiPartitionQueryWithRowLimitTest() throws Exception
    {
        int count = 8;
        int partitionCount = DataLimits.NO_LIMIT;
        int totQueryRows = 8;
        ReadQuery command = new Group(new ArrayList<SinglePartitionReadCommand>()
        {{
            add(sliceQuery(count, partitionCount, PageSize.NONE, cfs(KEYSPACE1, CF_STANDARD), "k1", "c1", "c4", false));
            add(sliceQuery(count, partitionCount, PageSize.NONE, cfs(KEYSPACE1, CF_STANDARD), "k2", "c1", "c4", false));
            add(sliceQuery(count, partitionCount, PageSize.NONE, cfs(KEYSPACE1, CF_STANDARD), "k3", "c1", "c4", false));
            add(sliceQuery(count, partitionCount, PageSize.NONE, cfs(KEYSPACE1, CF_STANDARD), "k4", "c1", "c4", false));
        }}, DataLimits.cqlLimits(count, partitionCount));

        checkRows(command, PageSize.PageUnit.ROWS, totQueryRows, new int[]{ 2, 7, 8, 9, 10, 15, 16, 20 });
    }

    @Test
    public void multiPartitionQueryWithPartitionLimitTest() throws Exception
    {
        int count = DataLimits.NO_LIMIT;
        int partitionCount = 2;
        int totQueryRows = 8;
        ReadQuery command = new Group(new ArrayList<SinglePartitionReadCommand>()
        {{
            add(sliceQuery(count, partitionCount, PageSize.NONE, cfs(KEYSPACE1, CF_STANDARD), "k1", "c1", "c4", false));
            add(sliceQuery(count, partitionCount, PageSize.NONE, cfs(KEYSPACE1, CF_STANDARD), "k2", "c1", "c4", false));
            add(sliceQuery(count, partitionCount, PageSize.NONE, cfs(KEYSPACE1, CF_STANDARD), "k3", "c1", "c4", false));
            add(sliceQuery(count, partitionCount, PageSize.NONE, cfs(KEYSPACE1, CF_STANDARD), "k4", "c1", "c4", false));
        }}, DataLimits.cqlLimits(count, partitionCount));

        checkRows(command, PageSize.PageUnit.ROWS, totQueryRows, new int[]{ 2, 7, 8, 9, 10, 15, 16, 20 });
    }

    private void checkRows(ReadQuery command, PageSize.PageUnit pageUnit, int totQueryRows, int... pages)
    {
        for (int pageSize : pages)
        {
            Map<DecoratedKey, Set<Row>> allRows = Maps.newHashMap();
            int currentRows = 0;
            QueryPager pager = command.getPager(null, ProtocolVersion.CURRENT);
            assertFalse(String.format("Failed due to exhausted pager at page size %s %s", pageSize, pageUnit),
                        pager.isExhausted());

            logger.info("Testing with page size: {}", pageSize);
            while (!pager.isExhausted())
            {
                Map<DecoratedKey, List<Row>> rows = fetchPage(pager, pageSize, pageUnit);

                if (rows.size() > 0)
                {
                    int numRows = rows.values().stream().map(List::size).reduce(0, Integer::sum);
                    int numBytes = rows.values().stream().flatMap(r -> r.stream()).reduce(0, (s, r) -> s + r.dataSize(), Integer::sum);

                    for (Map.Entry<DecoratedKey, List<Row>> entry : rows.entrySet())
                        allRows.merge(entry.getKey(), new HashSet(entry.getValue()), ((rows1, rows2) -> {
                            rows1.addAll(rows2);
                            return rows1;
                        }));

                    if (pageUnit == PageSize.PageUnit.ROWS)
                    {
                        int expectedSize = Math.min(pageSize, totQueryRows - currentRows);
                        assertEquals(String.format("Failed after %d rows with rows page size %d and current number of rows %d;\n%s",
                                                   currentRows, pageSize, numRows, formatRows(allRows)),
                                     expectedSize, numRows);
                    }
                    else
                    {
                        boolean bytesRead = numBytes < (pageSize + (numBytes / numRows));
                        assertTrue(String.format("Failed after %d rows with bytes page size %d and current number of rows %d due to bytes read %d;\n%s",
                                                 currentRows, pageSize, numRows, numBytes, formatRows(allRows)),
                                   bytesRead);
                    }

                    currentRows += numRows;

                    if (!pager.isExhausted())
                        pager = maybeRecreate(pager, command, true, ProtocolVersion.CURRENT);
                }
                else
                    assertTrue(String.format("Failed due to non-exhausted pager at page size %s %s", pageSize, pageUnit),
                               pager.isExhausted());
            }

            assertEquals(String.format("Failed with page size %d %s - expected %d rows in total but got:\n%s",
                                       pageSize, pageUnit, totQueryRows, formatRows(allRows)),
                         totQueryRows, (long) allRows.values().stream().map(Set::size).reduce(0, Integer::sum));
        }
    }

    private String formatRows(Map<DecoratedKey, Set<Row>> rows)
    {
        TableMetadata metadata = cfs(KEYSPACE1, CF_STANDARD).metadata();

        StringBuilder str = new StringBuilder();
        for (Map.Entry<DecoratedKey, Set<Row>> entry : rows.entrySet())
        {
            for (Row row : entry.getValue())
            {
                str.append(entry.getKey().toString());
                str.append(' ');
                str.append(row.toString(metadata));
                str.append('\n');
            }
        }
        return str.toString();
    }

    @Test
    public void rangeNamesQueryTest()
    {
        for (ProtocolVersion protocolVersion : ProtocolVersion.SUPPORTED)
        {
            rangeNamesQueryTest(false, protocolVersion);
            rangeNamesQueryTest(true, protocolVersion);
        }
    }

    public void rangeNamesQueryTest(boolean testPagingState, ProtocolVersion protocolVersion)
    {
        ReadCommand command = rangeNamesQuery(-1, -1, new PageSize(100, PageSize.PageUnit.ROWS),
                                              cfs(KEYSPACE1, CF_STANDARD),
                                              tokenOrderedKeys.get(0), tokenOrderedKeys.get(5),
                                              "c1", "c4", "c8");
        QueryPager pager = command.getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partitions = query(pager, 3 * 3);
        for (int i = 1; i <= 3; i++)
            assertRow(partitions.get(i - 1), tokenOrderedKeys.get(i), "c1", "c4", "c8");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 3 * 3, 2 * 3);
        for (int i = 4; i <= 5; i++)
            assertRow(partitions.get(i - 4), tokenOrderedKeys.get(i), "c1", "c4", "c8");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void rangeSliceQueryTest()
    {
        for (ProtocolVersion protocolVersion : ProtocolVersion.SUPPORTED)
        {
            rangeSliceQueryTest(false, protocolVersion);
            rangeSliceQueryTest(true, protocolVersion);
        }
    }

    public void rangeSliceQueryTest(boolean testPagingState, ProtocolVersion protocolVersion)
    {
        ReadCommand command = rangeSliceQuery(-1, -1, new PageSize(100, PageSize.PageUnit.ROWS),
                                              cfs(KEYSPACE1, CF_STANDARD),
                                              tokenOrderedKeys.get(0), tokenOrderedKeys.get(4),
                                              "c1", "c7");
        QueryPager pager = command.getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partitions = query(pager, 5);
        assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1", "c2", "c3", "c4", "c5");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 4);
        assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c6", "c7");
        assertRow(partitions.get(1), tokenOrderedKeys.get(2), "c1", "c2");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 6);
        assertRow(partitions.get(0), tokenOrderedKeys.get(2), "c3", "c4", "c5", "c6", "c7");
        assertRow(partitions.get(1), tokenOrderedKeys.get(3), "c1");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 5);
        assertRow(partitions.get(0), tokenOrderedKeys.get(3), "c2", "c3", "c4", "c5", "c6");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 5);
        assertRow(partitions.get(0), tokenOrderedKeys.get(3), "c7");
        assertRow(partitions.get(1), tokenOrderedKeys.get(4), "c1", "c2", "c3", "c4");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 5, 3);
        assertRow(partitions.get(0), tokenOrderedKeys.get(4), "c5", "c6", "c7");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void rangeSliceQueryWithLimitsTest() throws Exception
    {
        boolean testPagingState = true;
        ProtocolVersion protocolVersion = ProtocolVersion.CURRENT;

        // Test with count < partitionCount

        int count = 1;
        int partitionCount = 2;

        ReadCommand command = rangeSliceQuery(count, partitionCount, new PageSize(100, PageSize.PageUnit.ROWS),
                                              cfs(KEYSPACE1, CF_STANDARD),
                                              tokenOrderedKeys.get(0), tokenOrderedKeys.get(4),
                                              "c1", "c7");

        QueryPager pager = command.getPager(null, protocolVersion);
        List<FilteredPartition> partitions = query(pager, 5, count);
        assertEquals(1, partitions.size());
        assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1");
        assertTrue(pager.isExhausted());

        // Test with count > partitionCount

        count = 2;
        partitionCount = 1;

        command = rangeSliceQuery(count, partitionCount, new PageSize(100, PageSize.PageUnit.ROWS),
                                  cfs(KEYSPACE1, CF_STANDARD),
                                  tokenOrderedKeys.get(0), tokenOrderedKeys.get(4),
                                  "c1", "c7");

        pager = command.getPager(null, protocolVersion);
        partitions = query(pager, 5, count);
        assertEquals(2, partitions.size());
        assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1");
        assertRow(partitions.get(1), tokenOrderedKeys.get(2), "c1");
        assertTrue(pager.isExhausted());

        // Test with count spanning multiple partitions

        count = 4;
        partitionCount = 2;

        command = rangeSliceQuery(count, partitionCount, new PageSize(100, PageSize.PageUnit.ROWS),
                                  cfs(KEYSPACE1, CF_STANDARD),
                                  tokenOrderedKeys.get(0), tokenOrderedKeys.get(4),
                                  "c1", "c7");

        pager = command.getPager(null, protocolVersion);
        partitions = query(pager, 5, count);
        assertEquals(2, partitions.size());
        assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1", "c2");
        assertRow(partitions.get(1), tokenOrderedKeys.get(2), "c1", "c2");
        assertTrue(pager.isExhausted());

        // Test with count spanning multiple pages

        count = 8;
        partitionCount = 2;

        command = rangeSliceQuery(count, partitionCount, new PageSize(100, PageSize.PageUnit.ROWS),
                                  cfs(KEYSPACE1, CF_STANDARD),
                                  tokenOrderedKeys.get(0), tokenOrderedKeys.get(4),
                                  "c1", "c7");

        pager = command.getPager(null, protocolVersion);
        partitions = query(pager, 5, 5);
        assertEquals(3, partitions.size());
        assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1", "c2");
        assertRow(partitions.get(1), tokenOrderedKeys.get(2), "c1", "c2");
        assertRow(partitions.get(2), tokenOrderedKeys.get(3), "c1");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        partitions = query(pager, 5, 3);
        assertEquals(2, partitions.size());
        assertRow(partitions.get(0), tokenOrderedKeys.get(3), "c2");
        assertRow(partitions.get(1), tokenOrderedKeys.get(4), "c1", "c2");
        assertTrue(pager.isExhausted());
    }

    @Test
    public void SliceQueryWithTombstoneTest()
    {
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
            SliceQueryWithTombstoneTest(version);
    }

    public void SliceQueryWithTombstoneTest(ProtocolVersion protocolVersion)
    {
        // Testing for the bug of #6748
        String keyspace = KEYSPACE_CQL;
        String table = CF_CQL;
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        // Insert rows but with a tombstone as last cell
        for (int i = 0; i < 5; i++)
            executeInternal(String.format("INSERT INTO %s.%s (k, c, v) VALUES ('k%d', 'c%d', null)", keyspace, table, 0, i));

        ReadCommand command = SinglePartitionReadCommand.create(cfs.metadata(), nowInSec, Util.dk("k0"), Slice.ALL);

        QueryPager pager = command.getPager(null, protocolVersion);

        for (int i = 0; i < 5; i++)
        {
            List<FilteredPartition> partitions = query(pager, 1);
            // The only live cell we should have each time is the row marker
            assertRow(partitions.get(0), "k0", "c" + i);
        }
    }

    @Test
    public void pagingReversedQueriesWithStaticColumnsTest()
    {
        // There was a bug in paging for reverse queries when the schema includes static columns in
        // 2.1 & 2.2. This was never a problem in 3.0, so this test just guards against regressions
        // see CASSANDRA-13222

        // insert some rows into a single partition
        for (int i = 0; i < 5; i++)
            executeInternal(String.format("INSERT INTO %s.%s (k, c, st, v1, v2) VALUES ('k0', '%3$s', %3$s, %3$s, %3$s)",
                                          KEYSPACE_CQL, CF_CQL_WITH_STATIC, i));

        // query the table in reverse with page size = 1 & check that the returned rows contain the correct cells
        TableMetadata table = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(CF_CQL_WITH_STATIC).metadata();
        queryAndVerifyCells(table, true, "k0");
    }

    private void queryAndVerifyCells(TableMetadata table, boolean reversed, String key)
    {
        ClusteringIndexFilter rowfilter = new ClusteringIndexSliceFilter(Slices.ALL, reversed);
        ReadCommand command = SinglePartitionReadCommand.create(table, nowInSec, Util.dk(key), ColumnFilter.all(table), rowfilter);
        QueryPager pager = command.getPager(null, ProtocolVersion.CURRENT);

        ColumnMetadata staticColumn = table.staticColumns().getSimple(0);
        assertEquals(staticColumn.name.toCQLString(), "st");

        for (int i = 0; i < 5; i++)
        {
            try (ReadExecutionController controller = pager.executionController();
                 PartitionIterator partitions = pager.fetchPageInternal(PageSize.inRows(1), controller))
            {
                try (RowIterator partition = partitions.next())
                {
                    assertCell(partition.staticRow(), staticColumn, 4);

                    Row row = partition.next();
                    int cellIndex = !reversed ? i : 4 - i;

                    assertEquals(row.clustering().bufferAt(0), ByteBufferUtil.bytes(""+cellIndex));
                    assertCell(row, table.getColumn(new ColumnIdentifier("v1", false)), cellIndex);
                    assertCell(row, table.getColumn(new ColumnIdentifier("v2", false)), cellIndex);

                    // the partition/page should contain just a single regular row
                    assertFalse(partition.hasNext());
                }
            }
        }

        // After processing the 5 rows there should be no more rows to return
        try (ReadExecutionController controller = pager.executionController();
             PartitionIterator partitions = pager.fetchPageInternal(PageSize.inRows(1), controller))
        {
            assertFalse(partitions.hasNext());
        }
    }

    private void assertCell(Row row, ColumnMetadata column, int value)
    {
        Cell<?> cell = row.getCell(column);
        assertNotNull(cell);
        assertEquals(value, ByteBufferUtil.toInt(cell.buffer()));
    }

    @Test
    public void testSinglePartitionPagingByBytes()
    {
        executeInternal(String.format("TRUNCATE TABLE %s.%s", KEYSPACE_CQL, CF_CQL));

        int rows = 10;

        for (int i = 0; i < rows; i++)
            executeInternal(String.format("INSERT INTO %s.%s(k, c, v) VALUES('k', 'c%s', 'ignored')", KEYSPACE_CQL, CF_CQL, i));

        // Test with rows limit:

        int maxExpected = rows;
        for (int count = 0; count <= maxExpected; count++)
        {
            SinglePartitionReadCommand q = sliceQuery(count, -1, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL), "k", "c0", "c9", false);
            checkRows(q, PageSize.PageUnit.BYTES, count > 0 ? count : maxExpected, 1, 128, 256, 1024);
        }

        // Test with partition limit:

        for (int partitionCount = 1; partitionCount <= rows; partitionCount++)
        {
            SinglePartitionReadCommand q = sliceQuery(-1, partitionCount, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL), "k", "c0", "c9", false);
            checkRows(q, PageSize.PageUnit.BYTES, partitionCount, 1, 128, 256, 1024);
        }
    }

    @Test
    public void testPartitionRangePagingByBytes()
    {
        executeInternal(String.format("TRUNCATE TABLE %s.%s", KEYSPACE_CQL, CF_CQL));

        int pks = 10;
        int cs = 10;

        SortedSet<String> tokens = Sets.newTreeSet(Comparator.comparing(a -> cfs(KEYSPACE_CQL, CF_CQL).getPartitioner().decorateKey(bytes(a))));
        for (int i = 0; i < pks; i++)
        {
            for (int j = 0; j < cs; j++)
            {
                executeInternal(String.format("INSERT INTO %s.%s(k, c, v) VALUES('k%s', 'c%s', 'ignored')", KEYSPACE_CQL, CF_CQL, i, j));
            }
            tokens.add("k" + i);
        }

        // Test with rows limit:

        int maxExpected = pks - 1;
        for (int count = 0; count <= maxExpected; count++)
        {
            ReadCommand q = rangeSliceQuery(count, -1, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL), tokens.first(), tokens.last(), "c0", "c0");
            checkRows(q, PageSize.PageUnit.BYTES, count > 0 ? count : maxExpected, 1, 128, 256, 1024);
        }

        // Test with partition limit:

        for (int partitionCount = 1; partitionCount <= cs; partitionCount++)
        {
            ReadCommand q = rangeSliceQuery(-1, partitionCount, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL), tokens.first(), tokens.last(), "c0", "c9");
            checkRows(q, PageSize.PageUnit.BYTES, partitionCount * (pks - 1), 1, 128, 256, 1024);
        }
    }

    @Test
    public void testMultiPartitionPagingByBytes()
    {
        executeInternal(String.format("TRUNCATE TABLE %s.%s", KEYSPACE_CQL, CF_CQL));

        int pks = 10;
        int cs = 10;

        for (int i = 0; i < pks; i++)
            for (int j = 0; j < cs; j++)
                executeInternal(String.format("INSERT INTO %s.%s(k, c, v) VALUES('k%s', 'c%s', 'ignored')", KEYSPACE_CQL, CF_CQL, i, j));

        // Test with rows limit:

        int maxExpected = 22; // the sum of the clustering keys in the command group below
        for (int count = 0; count <= maxExpected; count++)
        {
            SinglePartitionReadCommand q1 = sliceQuery(count, -1, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL), "k0", "c0", "c1", false);
            SinglePartitionReadCommand q2 = sliceQuery(count, -1, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL), "k1", "c0", "c3", false);
            SinglePartitionReadCommand q3 = sliceQuery(count, -1, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL), "k2", "c0", "c5", false);
            SinglePartitionReadCommand q4 = sliceQuery(count, -1, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL), "k3", "c0", "c9", false);
            Group q = new Group(
            Arrays.asList(q1, q2, q3, q4),
            count > 0 ? DataLimits.cqlLimits(count) : DataLimits.NONE);
            checkRows(q, PageSize.PageUnit.BYTES, count > 0 ? count : maxExpected, 1, 128, 256, 1024);
        }

        // Test with partition limit:

        for (int partitionCount = 1; partitionCount <= cs; partitionCount++)
        {
            SinglePartitionReadCommand q1 = sliceQuery(-1, partitionCount, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL), "k0", "c0", "c9", false);
            SinglePartitionReadCommand q2 = sliceQuery(-1, partitionCount, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL), "k1", "c0", "c9", false);
            SinglePartitionReadCommand q3 = sliceQuery(-1, partitionCount, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL), "k2", "c0", "c9", false);
            SinglePartitionReadCommand q4 = sliceQuery(-1, partitionCount, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL), "k3", "c0", "c9", false);
            Group q = new Group(
            Arrays.asList(q1, q2, q3, q4),
            DataLimits.cqlLimits(Integer.MAX_VALUE, partitionCount));
            checkRows(q, PageSize.PageUnit.BYTES, partitionCount * 4, 1, 128, 256, 1024);
        }
    }

    @Test
    public void testStaticRowsPagingByBytes()
    {
        executeInternal(String.format("TRUNCATE TABLE %s.%s", KEYSPACE_CQL, CF_CQL_WITH_STATIC));

        int rows = 10;

        for (int i = 0; i < rows; i++)
            executeInternal(String.format("INSERT INTO %s.%s(k, c, st) VALUES('k%s', 'c', 0)", KEYSPACE_CQL, CF_CQL_WITH_STATIC, i));

        int maxExpected = 4;
        for (int count = 0; count <= maxExpected; count++)
        {
            SinglePartitionReadCommand q1 = sliceQuery(count, -1, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL_WITH_STATIC), "k0", "c", "c", false);
            SinglePartitionReadCommand q2 = sliceQuery(count, -1, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL_WITH_STATIC), "k1", "c", "c", false);
            SinglePartitionReadCommand q3 = sliceQuery(count, -1, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL_WITH_STATIC), "k2", "c", "c", false);
            SinglePartitionReadCommand q4 = sliceQuery(count, -1, PageSize.NONE, cfs(KEYSPACE_CQL, CF_CQL_WITH_STATIC), "k3", "c", "c", false);
            Group q = new Group(
            Arrays.asList(q1, q2, q3, q4),
            count > 0 ? DataLimits.cqlLimits(count) : DataLimits.NONE);
            checkRows(q, PageSize.PageUnit.BYTES, count > 0 ? count : maxExpected, 1, 128, 256, 1024);
        }
    }

    @Test
    public void toStringTest()
    {
        TableMetadata metadata = TableMetadata.builder("ks", "tab")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addClusteringColumn("c", Int32Type.instance)
                                              .addColumn(ColumnMetadata.regularColumn("ks", "tab", "v", Int32Type.instance))
                                              .build();

        DataLimits limits = DataLimits.cqlLimits(31, 29);

        Clustering clustering = Clustering.make(bytes(11));
        Row row = mock(Row.class);
        when(row.clustering()).thenReturn(clustering);
        when(row.isRow()).thenReturn(true);

        PagingState state = new PagingState(ByteBufferUtil.bytes(1), PagingState.RowMark.create(metadata, row, ProtocolVersion.CURRENT), 19, 17);

        SinglePartitionReadQuery singlePartitionReadQuery = mock(SinglePartitionReadQuery.class);
        when(singlePartitionReadQuery.metadata()).thenReturn(metadata);
        when(singlePartitionReadQuery.limits()).thenReturn(limits);
        when(singlePartitionReadQuery.partitionKey()).thenReturn(metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1)));
        QueryPager singlePartitionPager = new SinglePartitionPager(singlePartitionReadQuery, state, ProtocolVersion.CURRENT);
        Assertions.assertThat(singlePartitionPager.toString())
                  .contains(limits.toString())
                  .contains("remaining=19")
                  .contains("remainingInPartition=17")
                  .contains("lastReturned=c=11")
                  .contains("lastCounter=null")
                  .contains("lastKey=DecoratedKey(00000001, 00000001)")
                  .contains("exhausted=false");

        PartitionRangeReadQuery partitionRangeReadQuery = mock(PartitionRangeReadQuery.class);
        when(partitionRangeReadQuery.metadata()).thenReturn(metadata);
        when(partitionRangeReadQuery.limits()).thenReturn(limits);
        QueryPager partitionRangeQueryPager = new PartitionRangeQueryPager(partitionRangeReadQuery, state, ProtocolVersion.CURRENT);
        Assertions.assertThat(partitionRangeQueryPager.toString())
                  .contains(limits.toString())
                  .contains("remaining=19")
                  .contains("remainingInPartition=17")
                  .contains("lastReturnedRow=c=11")
                  .contains("lastCounter=null")
                  .contains("lastKey=DecoratedKey(00000001, 00000001)")
                  .contains("lastReturnedKey=DecoratedKey(00000001, 00000001)")
                  .contains("exhausted=false");

        Group singlePartitionReadQueryGroup = Group.create(metadata,
                                                           FBUtilities.nowInSeconds(),
                                                           ColumnFilter.all(metadata),
                                                           RowFilter.NONE, limits,
                                                           Arrays.asList(metadata.partitioner.decorateKey(bytes(1)), metadata.partitioner.decorateKey(bytes(2))),
                                                           new ClusteringIndexSliceFilter(Slices.ALL, false));
        QueryPager multiPartitionPager = new MultiPartitionPager<>(singlePartitionReadQueryGroup, state, ProtocolVersion.CURRENT);
        Assertions.assertThat(multiPartitionPager.toString())
                  .contains("pagers.length=2")
                  .contains("limit=" + limits)
                  .contains("remaining=19")
                  .contains("current=0");

        AggregationQueryPager aggregationQueryPager = new AggregationQueryPager(singlePartitionPager, PageSize.inBytes(512), limits);
        Assertions.assertThat(aggregationQueryPager.toString())
                  .contains("limits=" + limits)
                  .contains("subPageSize=512 bytes")
                  .contains("subPager=" + singlePartitionPager)
                  .contains("lastReturned=c=11");
    }
}
