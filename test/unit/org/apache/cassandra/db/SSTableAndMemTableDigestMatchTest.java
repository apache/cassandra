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
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.function.Function;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class SSTableAndMemTableDigestMatchTest extends CQLTester
{
    private final static long writeTime = System.currentTimeMillis() * 1000L;

    @Test
    public void testSelectAllColumns() throws Throwable
    {
        testWithFilter(tableMetadata ->
                       ColumnFilter.all(tableMetadata));
    }

    @Test
    public void testSelectNoColumns() throws Throwable
    {
        testWithFilter(tableMetadata ->
                       ColumnFilter.selection(tableMetadata, RegularAndStaticColumns.builder().build(), false));
    }

    @Test
    public void testSelectEmptyColumn() throws Throwable
    {
        testWithFilter(tableMetadata ->
                       ColumnFilter.selection(tableMetadata, RegularAndStaticColumns.of(tableMetadata.getColumn(ColumnIdentifier.getInterned("e", false))), false));
    }

    @Test
    public void testSelectNonEmptyColumn() throws Throwable
    {
        testWithFilter(tableMetadata ->
                       ColumnFilter.selection(tableMetadata, RegularAndStaticColumns.of(tableMetadata.getColumn(ColumnIdentifier.getInterned("v1", false))), false));
    }

    @Test
    public void testSelectEachNonEmptyColumn() throws Throwable
    {
        testWithFilter(tableMetadata ->
                       ColumnFilter.selection(tableMetadata,
                                              RegularAndStaticColumns.builder()
                                                                     .add(tableMetadata.getColumn(ColumnIdentifier.getInterned("v1", false)))
                                                                     .add(tableMetadata.getColumn(ColumnIdentifier.getInterned("v2", false)))
                                                                     .build(),
                                              false));
    }

    @Test
    public void testSelectCellsFromEmptyComplexColumn() throws Throwable
    {
        testWithFilter(tableMetadata -> ColumnFilter.selectionBuilder().select(tableMetadata.getColumn(ColumnIdentifier.getInterned("em", false)),
                                                                               CellPath.create(Int32Type.instance.decompose(5))).build());
    }

    @Test
    public void testSelectNonEmptyCellsFromComplexColumn() throws Throwable
    {
        testWithFilter(tableMetadata -> ColumnFilter.selectionBuilder().select(tableMetadata.getColumn(ColumnIdentifier.getInterned("m", false)),
                                                                               CellPath.create(Int32Type.instance.decompose(1))).build());
    }

    @Test
    public void testSelectEmptyCellsFromNonEmptyComplexColumn() throws Throwable
    {
        testWithFilter(tableMetadata -> ColumnFilter.selectionBuilder().select(tableMetadata.getColumn(ColumnIdentifier.getInterned("m", false)),
                                                                               CellPath.create(Int32Type.instance.decompose(5))).build());
    }

    @Test
    public void testSelectRegularColumnOnPartitionWithOnlyStaticData() throws Throwable
    {
        testWithFilterAndStaticColumnsOnly(tableMetadata ->
                                           ColumnFilter.selection(tableMetadata, RegularAndStaticColumns.of(tableMetadata.getColumn(ColumnIdentifier.getInterned("v", false))), false));
        testWithFilterAndStaticColumnsOnly(tableMetadata ->
                                           ColumnFilter.selection(tableMetadata, RegularAndStaticColumns.of(tableMetadata.getColumn(ColumnIdentifier.getInterned("v", false))), true));
    }

    @Test
    public void testSelectStaticColumnOnPartitionWithOnlyStaticData() throws Throwable
    {
        testWithFilterAndStaticColumnsOnly(tableMetadata ->
                                           ColumnFilter.selection(tableMetadata, RegularAndStaticColumns.of(tableMetadata.getColumn(ColumnIdentifier.getInterned("s2", false))), false));
        testWithFilterAndStaticColumnsOnly(tableMetadata ->
                                           ColumnFilter.selection(tableMetadata, RegularAndStaticColumns.of(tableMetadata.getColumn(ColumnIdentifier.getInterned("s2", false))), true));
    }

    @Test
    public void testSelectNullStaticColumnOnPartitionWithOnlyStaticData() throws Throwable
    {
        testWithFilterAndStaticColumnsOnly(tableMetadata ->
                                           ColumnFilter.selection(tableMetadata, RegularAndStaticColumns.of(tableMetadata.getColumn(ColumnIdentifier.getInterned("s1", false))), false));
        testWithFilterAndStaticColumnsOnly(tableMetadata ->
                                           ColumnFilter.selection(tableMetadata, RegularAndStaticColumns.of(tableMetadata.getColumn(ColumnIdentifier.getInterned("s1", false))), true));
    }

    @Test
    public void testSelectAllColumnsOnPartitionWithOnlyStaticData() throws Throwable
    {
        testWithFilterAndStaticColumnsOnly(tableMetadata -> ColumnFilter.all(tableMetadata));
    }

    private void testWithFilter(Function<TableMetadata, ColumnFilter> filterFactory) throws Throwable
    {
        Map<Integer, Integer> m = new HashMap<>();
        m.put(1, 10);
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, e text, m map<int, int>, em map<int, int>)");
        execute("INSERT INTO %s (k, v1, v2, m) values (?, ?, ?, ?) USING TIMESTAMP ?", 1, 2, 3, m, writeTime);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertDigestsAreEqualsBeforeAndAfterFlush(filterFactory.apply(cfs.metadata()), Clustering.EMPTY);
    }

    private void testWithFilterAndStaticColumnsOnly(Function<TableMetadata, ColumnFilter> filterFactory) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s1 int static, s2 int static, v int, PRIMARY KEY(pk, ck))");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        execute("INSERT INTO %s (pk, s1, s2) VALUES (1, 1, 1) USING TIMESTAMP 1000");
        assertDigestsAreEqualsBeforeAndAfterFlush(filterFactory.apply(cfs.metadata()));

        execute("INSERT INTO %s (pk, s1) VALUES (1, 2) USING TIMESTAMP 2000");
        assertDigestsAreEqualsBeforeAndAfterFlush(filterFactory.apply(cfs.metadata()));

        execute("DELETE s1 FROM %s USING TIMESTAMP 3000 WHERE pk = 1");
        assertDigestsAreEqualsBeforeAndAfterFlush(filterFactory.apply(cfs.metadata()));
    }

    private void assertDigestsAreEqualsBeforeAndAfterFlush(ColumnFilter filter, Clustering<?>... clusterings)
    {
        String digest1 = getDigest(filter, clusterings);
        flush();
        String digest2 = getDigest(filter, clusterings);

        assertEquals(digest1, digest2);
    }

    private String getDigest(ColumnFilter filter, Clustering<?>... clusterings)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        NavigableSet<Clustering<?>> clusteringSet = Sets.newTreeSet(new ClusteringComparator());
        for (Clustering<?> clustering : clusterings)
            clusteringSet.add(clustering);
        BufferDecoratedKey key = new BufferDecoratedKey(DatabaseDescriptor.getPartitioner().getToken(Int32Type.instance.decompose(1)),
                                                        Int32Type.instance.decompose(1));
        SinglePartitionReadCommand cmd = SinglePartitionReadCommand
                                         .create(cfs.metadata(),
                                                 (int) (System.currentTimeMillis() / 1000),
                                                 key,
                                                 filter,
                                                 new ClusteringIndexNamesFilter(clusteringSet, false)).copyAsDigestQuery();
        cmd.setDigestVersion(MessagingService.current_version);
        ReadResponse resp;
        try (ReadExecutionController ctrl = ReadExecutionController.forCommand(cmd, false); UnfilteredRowIterator iterator = cmd.queryMemtableAndDisk(cfs, ctrl))
        {
            resp = ReadResponse.createDataResponse(new SingletonUnfilteredPartitionIterator(iterator), cmd, ctrl.getRepairedDataInfo());
            logger.info("Response is: {}", resp.toDebugString(cmd, key));
            ByteBuffer digest = resp.digest(cmd);
            return ByteBufferUtil.bytesToHex(digest);
        }
    }
}
