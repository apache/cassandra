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
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.AbstractReadCommandBuilder.SinglePartitionBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
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
import org.apache.cassandra.utils.Pair;
import org.assertj.core.data.Offset;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@SuppressWarnings("SameParameterValue")
public abstract class QueryPagerTests
{
    public final static Logger logger = LoggerFactory.getLogger(QueryPagerTests.class);

    public static final String KEYSPACE1 = "QueryPagerTests";
    public static final String CF_WITH_ONE_CLUSTERING = "Standard1";
    public static final String CF_WITH_TWO_CLUSTERINGS = "Standard2";
    public static final String KEYSPACE_CQL = "cql_keyspace";
    public static final String CF_CQL = "table_clust1";
    public static final String PER_TEST_CF_CQL_WITH_STATIC = "table_with_static";
    public static final long nowInSec = FBUtilities.nowInSeconds();
    public static List<String> tokenOrderedKeys;

    static final int ONE_CLUSTERING_ROW_BYTES = 53;
    static final int TWO_CLUSTERINGS_ROW_BYTES = 57;

    static PageSize pageSizeInRows(int n)
    {
        return PageSize.inRows(n);
    }

    static PageSize pageSizeInBytesForOneClustering(int n)
    {
        return PageSize.inBytes(ONE_CLUSTERING_ROW_BYTES * n);
    }
    static PageSize pageSizeInBytesForTwoClusterings(int n)
    {
        return PageSize.inBytes(TWO_CLUSTERINGS_ROW_BYTES * n);
    }

    public ProtocolVersion protocolVersion = ProtocolVersion.CURRENT;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        ServerTestUtils.startEmbeddedCassandraService();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_WITH_ONE_CLUSTERING),
                                    TableMetadata.builder(KEYSPACE1, CF_WITH_TWO_CLUSTERINGS)
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
        cfs(KEYSPACE1, CF_WITH_ONE_CLUSTERING).clearUnsafe();
        cfs(KEYSPACE1, CF_WITH_TWO_CLUSTERINGS).clearUnsafe();

        int nbKeys = 10;
        int nbCols = 10;
        int nbCols2 = 10;

        SortedSet<String> tokens = Sets.newTreeSet(Comparator.comparing(a -> cfs(KEYSPACE1, CF_WITH_ONE_CLUSTERING).getPartitioner().decorateKey(bytes(a))));

        // *
        // * Creates the following data:
        // *   k1: c1 ... cn
        // *   ...
        // *   ki: c1 ... cn
        // *
        for (int i = 0; i < nbKeys; i++)
        {
            tokens.add(key(i));
            for (int j = 0; j < nbCols; j++)
            {
                RowUpdateBuilder builder = new RowUpdateBuilder(cfs(KEYSPACE1, CF_WITH_ONE_CLUSTERING).metadata(), FBUtilities.timestampMicros(), key(i));
                builder.clustering("c" + j).add("val", "").build().applyUnsafe();
                for (int k = 0; k < nbCols2; k++)
                {
                    RowUpdateBuilder builder2 = new RowUpdateBuilder(cfs(KEYSPACE1, CF_WITH_TWO_CLUSTERINGS).metadata(), FBUtilities.timestampMicros(), key(i));
                    builder2.clustering("c" + j, k).add("val", "").build().applyUnsafe();
                }
            }
        }

        tokenOrderedKeys = Lists.newArrayList(tokens);
    }

    static String key(int i)
    {
        return "k" + StringUtils.leftPad(String.valueOf(i), 9, '0');
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

    static List<FilteredPartition> query(QueryPager pager, PageSize pageSize, int expectedRows)
    {
        StringBuilder sb = new StringBuilder("\n");
        List<FilteredPartition> partitionList = new ArrayList<>();
        int rows = 0;
        try (ReadExecutionController executionController = pager.executionController();
             PartitionIterator iterator = pager.fetchPageInternal(pageSize, executionController))
//             PartitionIterator iterator = pager.fetchPage(pageSize, ConsistencyLevel.ONE, ClientState.forInternalCalls(), System.nanoTime()))
        {
            while (iterator.hasNext())
            {
                try (RowIterator rowIter = iterator.next())
                {
                    FilteredPartition partition = FilteredPartition.create(rowIter);
                    sb.append(partition.toString(false)).append('\n');
                    partitionList.add(partition);
                    rows += partition.rowCount();
                }
            }
        }
        logger.info("Fetched page of {} rows", rows);
        if (expectedRows >= 0)
            assertEquals(sb.toString(), expectedRows, rows);
        return partitionList;
    }

    static <T extends AbstractReadCommandBuilder<?>> void setNamesFilter(T builder, String... names)
    {
        for (String name : names)
            builder.includeRow(name);
    }
    
    static <T extends  AbstractReadCommandBuilder<?>> void setSliceFilter(T builder, String start, String end, boolean reversed)
    {
        builder.fromIncl(start);
        builder.toExcl(end);
        if (reversed)
            builder.reverse();
    }
    
    static <T extends AbstractReadCommandBuilder<?>> void setLimits(T builder, int limit, int perPartitionLimit, PageSize pageSize)
    {
        if (limit > 0)
            builder.withLimit(limit);
        if (perPartitionLimit > 0)
            builder.withPerPartitionLimit(perPartitionLimit);
        if (pageSize != null && !pageSize.equals(PageSize.NONE))
            builder.withPageSize(pageSize);
    }

    static SinglePartitionBuilder sliceQuery(int limit, int perPartitionLimit, PageSize pageSize, ColumnFamilyStore cfs, String key, String start, String end, boolean reversed)
    {
        SinglePartitionBuilder builder = (SinglePartitionBuilder) Util.cmd(cfs, key).withNowInSeconds(nowInSec);
        setSliceFilter(builder, start, end, reversed);
        setLimits(builder, limit, perPartitionLimit, pageSize);
        return builder;
    }

    /**
     * Asserts that the returned partition contains the expected rows.
     *
     * @param returnedPartition the returned partition
     * @param key               expected partition key
     * @param names             expected clustering keys
     */
    void assertRow(FilteredPartition returnedPartition, String key, String... names)
    {
        ByteBuffer[] bbs = new ByteBuffer[names.length];
        for (int i = 0; i < names.length; i++)
            bbs[i] = bytes(names[i]);
        assertRow(returnedPartition, key, bbs);
    }

    /**
     * Asserts that the returned partition contains the expected rows.
     *
     * @param returnedPartition the returned partition
     * @param key               expected partition key
     * @param names             expected clustering keys
     */
    void assertRow(FilteredPartition returnedPartition, String key, ByteBuffer... names)
    {
        assertEquals(key, string(returnedPartition.partitionKey().getKey()));
        assertFalse(returnedPartition.isEmpty());
        int i = 0;
        for (Row row : Util.once(returnedPartition.iterator()))
        {
            logger.info("Found row {}:{}", key, row.toString(returnedPartition.metadata()));
            assert i < names.length : "Found more rows than expected (" + (i + 1) + ") in partition " + key;
            ByteBuffer expected = names[i++];
            assertEquals("column " + i + " doesn't match " + string(expected) + " vs " + string(row.clustering().bufferAt(0)), expected, row.clustering().bufferAt(0));
        }
    }

    QueryPager maybeRecreate(QueryPager pager, ReadQuery command, boolean testPagingState)
    {
        if (!testPagingState)
            return pager;

        PagingState state = PagingState.deserialize(pager.state().serialize(protocolVersion), protocolVersion);
        return command.getPager(state, protocolVersion);
    }

    public static String pagerStateToString(QueryPager pager, TableMetadata metadata)
    {
        if (pager == null || pager.state() == null)
            return null;
        return String.format("%s:%s, remaining=%s, remainingInPartition=%s",
                             pager.state().partitionKey != null ? metadata.partitioner.decorateKey(pager.state().partitionKey).toCQLString(metadata) : null,
                             pager.state().rowMark != null ? pager.state().rowMark.clustering(metadata).toString(metadata) : null,
                             pager.state().remaining,
                             pager.state().remainingInPartition);
    }

    protected PageSize getSubPageSize()
    {
        return PageSize.NONE;
    }


    QueryPager checkNextPage(QueryPager pager, ReadQuery command, boolean testPagingState, PageSize pageSize, int expectedRows, Consumer<List<FilteredPartition>> assertion)
    {
        return checkNextPage(pager, command, testPagingState, pageSize, expectedRows, 1, assertion);
    }

    final QueryPager checkNextPage(QueryPager pager, ReadQuery command, boolean testPagingState, PageSize pageSize, int expectedRows, int expectedSubPages, Consumer<List<FilteredPartition>> assertion)
    {
        logger.info("Checking next page: pageSize={}, subPageSize={}, expectedRows={}, state={}", pageSize, getSubPageSize(), expectedRows, pagerStateToString(pager, command.metadata()));
        if (pager == null)
        {
            pager = command.getPager(null, protocolVersion);
        }
        else
        {
            assertFalse(pager.isExhausted());
            pager = maybeRecreate(pager, command, testPagingState);
        }
        if (command.limits().isGroupByLimit() && !(pager instanceof AggregationQueryPager))
            pager = new AggregationQueryPager(pager, command.limits());
        List<FilteredPartition> partition = query(pager, pageSize, expectedRows);
        assertion.accept(partition);

        if (getSubPageSize().isDefined() && pager instanceof AggregationQueryPager)
        {
            int allowedOffsetPerSubQuery =  1;
            if (command instanceof SinglePartitionReadQuery.Group<?>)
                allowedOffsetPerSubQuery = ((SinglePartitionReadQuery.Group<?>) command).queries.size();
            logger.info("Fetched a page of groups in {} sub-pages", ((AggregationQueryPager) pager).getNumberOfSubPages());
            assertThat(((AggregationQueryPager) pager).getNumberOfSubPages()).isCloseTo(expectedSubPages, Offset.offset(allowedOffsetPerSubQuery));
        }

        return pager;
    }

    protected void checkAllPages(ReadCommand cmd, boolean testPagingState, PageSize pageSize, int expectedPages, int expectedRows, String firstPartitionKey, String firstName, String lastPartitionKey, String lastName)
    {
        QueryPager pager = cmd.getPager(null, protocolVersion);
        List<Pair<DecoratedKey, List<Row>>> queryResults = null;
        int pages = 0;
        while (!pager.isExhausted())
        {
            if (cmd.limits().isGroupByLimit() && !(pager instanceof AggregationQueryPager))
                pager = new AggregationQueryPager(pager, cmd.limits());
            List<Pair<DecoratedKey, List<Row>>> partitions = asPairs(query(pager, pageSize, -1));

            if (queryResults == null)
                queryResults = new ArrayList<>(partitions);
            else
                mergeTo(cmd.metadata(), queryResults, partitions);

            if (getSubPageSize().isDefined() && pager instanceof AggregationQueryPager)
            {
                logger.info("Fetched a page of groups in {} sub-pages", ((AggregationQueryPager) pager).getNumberOfSubPages());
                int expectedSubPages = expectedRows / getSubPageSize().rows;
                assertThat(((AggregationQueryPager) pager).getNumberOfSubPages()).isEqualTo(expectedSubPages);
            }

            if (!pager.isExhausted())
                pager = maybeRecreate(pager, cmd, testPagingState);

            pages++;
        }

        Pair<DecoratedKey, List<Row>> firstPartition = queryResults.get(0);
        Pair<DecoratedKey, List<Row>> lastPartition = queryResults.get(queryResults.size() - 1);

        assertThat(string(firstPartition.left.getKey())).isEqualTo(firstPartitionKey);
        assertThat(string(lastPartition.left.getKey())).isEqualTo(lastPartitionKey);

        Row firstRow = firstPartition.right.get(0);
        Row lastRow = lastPartition.right.get(lastPartition.right.size() - 1);

        int rows = queryResults.stream().mapToInt(p -> p.right.size()).sum();
        assertThat(rows).isEqualTo(expectedRows);

        assertThat(pages).isEqualTo(expectedPages);

        assertThat(string(firstRow.clustering().bufferAt(0))).isEqualTo(firstName);
        assertThat(string(lastRow.clustering().bufferAt(0))).isEqualTo(lastName);

    }

    private void mergeTo(TableMetadata metadata, List<Pair<DecoratedKey, List<Row>>> target, List<Pair<DecoratedKey, List<Row>>> source)
    {
        if (target.isEmpty())
        {
            target.addAll(source);
            return;
        }

        if (source.isEmpty())
            return;

        Pair<DecoratedKey, List<Row>> lastTarget = target.get(target.size() - 1);
        Pair<DecoratedKey, List<Row>> firstSource = source.get(0);
        assertThat(lastTarget.left).isLessThanOrEqualTo(firstSource.left);
        assertThat(lastTarget.right).isNotEmpty();
        assertThat(firstSource.right).isNotEmpty();

        if (lastTarget.left.equals(firstSource.left))
        {
            Row lastTargetLastRow = lastTarget.right.get(lastTarget.right.size() - 1);
            Row firstSourceFirstRow = firstSource.right.get(0);
            assertThat(metadata.comparator.compare(lastTargetLastRow.clustering(), firstSourceFirstRow.clustering())).isLessThan(0);
            lastTarget.right.addAll(firstSource.right);
            source.remove(0);
        }

        target.addAll(source);
    }

    private List<Pair<DecoratedKey, List<Row>>> asPairs(List<FilteredPartition> partitions)
    {
        List<Pair<DecoratedKey, List<Row>>> pairs = new ArrayList<>(partitions.size());
        for (FilteredPartition partition : partitions)
        {
            List<Row> rows = new ArrayList<>();
            try (RowIterator iter = partition.rowIterator())
            {
                while (iter.hasNext())
                    rows.add(iter.next());
            }
            pairs.add(Pair.create(partition.partitionKey(), rows));
        }
        return pairs;
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
                 PartitionIterator partitions = pager.fetchPageInternal(PageSize.inRows(1), controller))
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
}
