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
package org.apache.cassandra.service.pager;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.function.Consumer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@Ignore
public abstract class QueryPagerTest
{
    public static final String KEYSPACE1 = "QueryPagerTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_STANDARD2 = "Standard2";
    public static final String KEYSPACE_CQL = "cql_keyspace";
    public static final String CF_CQL = "table_clust1";
    public static final String PER_TEST_CF_CQL_WITH_STATIC = "table_with_static";
    public static final long nowInSec = FBUtilities.nowInSeconds();
    public static List<String> tokenOrderedKeys;

    int pageSizeInRows(int n)
    {
        return n;
    }

    public ProtocolVersion protocolVersion = ProtocolVersion.CURRENT;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        ServerTestUtils.startEmbeddedCassandraService();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD),
                                    TableMetadata.builder(KEYSPACE1, CF_STANDARD2)
                                                 .addPartitionKeyColumn("key", AsciiType.instance)
                                                 .addClusteringColumn("name", AsciiType.instance)
                                                 .addClusteringColumn("c", Int32Type.instance)
                                                 .addRegularColumn("val", AsciiType.instance)
                                                 .compression(SchemaLoader.getCompressionParameters())
        );

        SchemaLoader.createKeyspace(KEYSPACE_CQL,
                                    KeyspaceParams.simple(1),
                                    CreateTableStatement.parse("CREATE TABLE " + CF_CQL + " ("
                                                               + "k text,"
                                                               + "c text,"
                                                               + "v text,"
                                                               + "PRIMARY KEY (k, c))", KEYSPACE_CQL),
                                    CreateTableStatement.parse("CREATE TABLE " + PER_TEST_CF_CQL_WITH_STATIC + " ("
                                                               + "k text, "
                                                               + "c text, "
                                                               + "st int static, "
                                                               + "v1 int, "
                                                               + "v2 int, "
                                                               + "PRIMARY KEY(k, c))", KEYSPACE_CQL));
        addData();
    }

    static String string(ByteBuffer bb)
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
        cfs(KEYSPACE1, CF_STANDARD2).clearUnsafe();

        int nbKeys = 10;
        int nbCols = 10;
        int nbCols2 = 10;

        SortedSet<String> tokens = Sets.newTreeSet(Comparator.comparing(a -> cfs(KEYSPACE1, CF_STANDARD).getPartitioner().decorateKey(bytes(a))));

        // *
        // * Creates the following data:
        // *   k1: c1 ... cn
        // *   ...
        // *   ki: c1 ... cn
        // *
        for (int i = 0; i < nbKeys; i++)
        {
            tokens.add("k" + i);
            for (int j = 0; j < nbCols; j++)
            {
                RowUpdateBuilder builder = new RowUpdateBuilder(cfs(KEYSPACE1, CF_STANDARD).metadata(), FBUtilities.timestampMicros(), "k" + i);
                builder.clustering("c" + j).add("val", "").build().applyUnsafe();
                for (int k = 0; k < nbCols2; k++)
                {
                    RowUpdateBuilder builder2 = new RowUpdateBuilder(cfs(KEYSPACE1, CF_STANDARD2).metadata(), FBUtilities.timestampMicros(), "k" + i);
                    builder2.clustering("c" + j, k).add("val", "").build().applyUnsafe();
                }
            }
        }

        tokenOrderedKeys = Lists.newArrayList(tokens);
    }

    @After
    public void cleanUp()
    {
        QueryProcessor.executeInternal(String.format("TRUNCATE \"%s\".\"%s\"", KEYSPACE_CQL, PER_TEST_CF_CQL_WITH_STATIC));
    }

    static ColumnFamilyStore cfs(String ks, String cf)
    {
        return Keyspace.open(ks).getColumnFamilyStore(cf);
    }

    static List<FilteredPartition> query(QueryPager pager, int pageSize, int expectedRows)
    {
        StringBuilder sb = new StringBuilder();
        List<FilteredPartition> partitionList = new ArrayList<>();
        int rows = 0;
        try (ReadExecutionController executionController = pager.executionController();
             PartitionIterator iterator = pager.fetchPageInternal(pageSize, executionController))
        {
            while (iterator.hasNext())
            {
                try (RowIterator rowIter = iterator.next())
                {
                    FilteredPartition partition = FilteredPartition.create(rowIter);
                    sb.append(partition).append('\n');
                    partitionList.add(partition);
                    rows += partition.rowCount();
                }
            }
        }
        assertEquals(sb.toString(), expectedRows, rows);
        return partitionList;
    }

    static SinglePartitionReadCommand namesQuery(ColumnFamilyStore cfs, String key, String... names)
    {
        return namesQuery(-1, -1, cfs, key, names);
    }

    static SinglePartitionReadCommand namesQuery(int limit, int perPartitionLimit, ColumnFamilyStore cfs, String key, String... names)
    {
        AbstractReadCommandBuilder builder = Util.cmd(cfs, key).withNowInSeconds(nowInSec);
        for (String name : names)
            builder.includeRow(name);

        return (SinglePartitionReadCommand) builder.build().withUpdatedLimit(DataLimits.cqlLimits(limit > 0 ? limit : Integer.MAX_VALUE,
                                                                                                perPartitionLimit > 0 ? perPartitionLimit : Integer.MAX_VALUE));
    }

    static SinglePartitionReadCommand sliceQuery(ColumnFamilyStore cfs, String key, String start, String end)
    {
        return sliceQuery(-1, -1, cfs, key, start, end, false);
    }

    static SinglePartitionReadCommand sliceQuery(ColumnFamilyStore cfs, String key, String start, String end, boolean reversed)
    {
        return sliceQuery(-1, -1, cfs, key, start, end, reversed);
    }

    static SinglePartitionReadCommand sliceQuery(int limit, int perPartitionLimit, ColumnFamilyStore cfs, String key, String start, String end, boolean reversed)
    {
        AbstractReadCommandBuilder builder = Util.cmd(cfs, key).fromIncl(start).toExcl(end).withNowInSeconds(nowInSec);
        if (reversed)
            builder.reverse();

        return (SinglePartitionReadCommand) builder.build().withUpdatedLimit(DataLimits.cqlLimits(limit > 0 ? limit : Integer.MAX_VALUE,
                                                                                                perPartitionLimit > 0 ? perPartitionLimit : Integer.MAX_VALUE));
    }

    static ReadCommand rangeNamesQuery(int limit, int perPartitionLimit, ColumnFamilyStore cfs, String keyStart, String keyEnd, String... names)
    {
        AbstractReadCommandBuilder builder = Util.cmd(cfs)
                                                 .fromKeyIncl(keyStart)
                                                 .toKeyExcl(keyEnd)
                                                 .withNowInSeconds(nowInSec);
        for (String name : names)
            builder.includeRow(name);

        return builder.build().withUpdatedLimit(DataLimits.cqlLimits(limit > 0 ? limit : Integer.MAX_VALUE,
                                                                    perPartitionLimit > 0 ? perPartitionLimit : Integer.MAX_VALUE));
    }

    static ReadCommand rangeSliceQuery(int limit, int perPartitionLimit, ColumnFamilyStore cfs, String keyStart, String keyEnd, String start, String end)
    {
        AbstractReadCommandBuilder builder = Util.cmd(cfs)
                                                 .fromKeyIncl(keyStart)
                                                 .toKeyExcl(keyEnd)
                                                 .fromIncl(start)
                                                 .toExcl(end)
                                                 .withNowInSeconds(nowInSec);

        return builder.build().withUpdatedLimit(DataLimits.cqlLimits(limit > 0 ? limit : Integer.MAX_VALUE,
                                                                    perPartitionLimit > 0 ? perPartitionLimit : Integer.MAX_VALUE));
    }

    void assertRow(FilteredPartition p, String key, String... names)
    {
        ByteBuffer[] bbs = new ByteBuffer[names.length];
        for (int i = 0; i < names.length; i++)
            bbs[i] = bytes(names[i]);
        assertRow(p, key, bbs);
    }

    void assertRow(FilteredPartition p, String key, ByteBuffer... names)
    {
        assertEquals(key, string(p.partitionKey().getKey()));
        assertFalse(p.isEmpty());
        int i = 0;
        for (Row row : Util.once(p.iterator()))
        {
            assert i < names.length : "Found more rows than expected (" + (i + 1) + ") in partition " + key;
            ByteBuffer expected = names[i++];
            assertEquals("column " + i + " doesn't match " + string(expected) + " vs " + string(row.clustering().bufferAt(0)), expected, row.clustering().bufferAt(0));
        }
        assertEquals("Row count in partition " + key, names.length, i);
    }

    QueryPager maybeRecreate(QueryPager pager, ReadQuery command, boolean testPagingState)
    {
        if (!testPagingState)
            return pager;

        PagingState state = PagingState.deserialize(pager.state().serialize(protocolVersion), protocolVersion);
        return command.getPager(state, protocolVersion);
    }

    QueryPager checkNextPage(QueryPager pager, ReadQuery command, boolean testPagingState, int pageSize, int expectedRows, Consumer<List<FilteredPartition>> assertion)
    {
        if (pager == null)
        {
            pager = command.getPager(null, protocolVersion);
        }
        else
        {
            assertFalse(pager.isExhausted());
            pager = maybeRecreate(pager, command, testPagingState);
        }
        List<FilteredPartition> partition = query(pager, pageSize, expectedRows);
        assertion.accept(partition);
        return pager;
    }

    void queryAndVerifyCells(TableMetadata table, boolean reversed, String key)
    {
        ClusteringIndexFilter rowfilter = new ClusteringIndexSliceFilter(Slices.ALL, reversed);
        ReadCommand command = SinglePartitionReadCommand.create(table, nowInSec, Util.dk(key), ColumnFilter.all(table), rowfilter);
        QueryPager pager = command.getPager(null, protocolVersion);

        ColumnMetadata staticColumn = table.staticColumns().getSimple(0);
        assertEquals(staticColumn.name.toCQLString(), "st");

        for (int i = 0; i < 5; i++)
        {
            try (ReadExecutionController controller = pager.executionController();
                 PartitionIterator partitions = pager.fetchPageInternal(1, controller))
            {
                try (RowIterator partition = partitions.next())
                {
                    assertCell(partition.staticRow(), staticColumn, 4);

                    Row row = partition.next();
                    int cellIndex = !reversed ? i : 4 - i;

                    assertEquals(string(row.clustering().bufferAt(0)), "" + cellIndex);
                    assertCell(row, table.getColumn(new ColumnIdentifier("v1", false)), cellIndex);
                    assertCell(row, table.getColumn(new ColumnIdentifier("v2", false)), cellIndex);

                    // the partition/page should contain just a single regular row
                    assertFalse(partition.hasNext());
                }
            }
        }

        // After processing the 5 rows there should be no more rows to return
        try (ReadExecutionController controller = pager.executionController();
             PartitionIterator partitions = pager.fetchPageInternal(1, controller))
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
}
