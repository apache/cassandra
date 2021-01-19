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

import com.sun.xml.internal.xsom.impl.scd.Iterators;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class SSTableAndMemTableDigestMatchTest extends CQLTester
{
    private final static long writeTime = System.currentTimeMillis() * 1000L;

    @Test
    public void testSelectAllColumns() throws Throwable
    {
        testWithFilter(cfs -> ColumnFilter.all(cfs.metadata));
    }

    @Test
    public void testSelectNoColumns() throws Throwable
    {
        testWithFilter(cfs -> ColumnFilter.selection(cfs.metadata, PartitionColumns.NONE));
    }

    @Test
    public void testSelectEmptyColumn() throws Throwable
    {
        testWithFilter(cfs -> ColumnFilter.selection(cfs.metadata, PartitionColumns.of(cfs.metadata.getColumnDefinition(ColumnIdentifier.getInterned("e", false)))));
    }

    @Test
    public void testSelectNonEmptyColumn() throws Throwable
    {
        testWithFilter(cfs -> ColumnFilter.selection(cfs.metadata, PartitionColumns.of(cfs.metadata.getColumnDefinition(ColumnIdentifier.getInterned("v1", false)))));
    }

    @Test
    public void testSelectEachNonEmptyColumn() throws Throwable
    {
        testWithFilter(cfs -> ColumnFilter.selection(cfs.metadata,
                                                     PartitionColumns.builder()
                                                                     .add(cfs.metadata.getColumnDefinition(ColumnIdentifier.getInterned("v1", false)))
                                                                     .add(cfs.metadata.getColumnDefinition(ColumnIdentifier.getInterned("v2", false)))
                                                                     .add(cfs.metadata.getColumnDefinition(ColumnIdentifier.getInterned("m", false)))
                                                                     .build()));
    }

    @Test
    public void testSelectEmptyComplexColumn() throws Throwable
    {
        testWithFilter(cfs -> ColumnFilter.selection(cfs.metadata,
                                                     PartitionColumns.builder()
                                                                     .add(cfs.metadata.getColumnDefinition(ColumnIdentifier.getInterned("em", false)))
                                                                     .build()));
    }

    @Test
    public void testSelectCellsFromEmptyComplexColumn() throws Throwable
    {
        testWithFilter(cfs -> ColumnFilter.selectionBuilder()
                                          .select(cfs.metadata.getColumnDefinition(ColumnIdentifier.getInterned("em", false)),
                                                  CellPath.create(Int32Type.instance.decompose(5))).build());
    }

    @Test
    public void testSelectNonEmptyCellsFromComplexColumn() throws Throwable
    {
        testWithFilter(cfs -> ColumnFilter.selectionBuilder()
                                          .select(cfs.metadata.getColumnDefinition(ColumnIdentifier.getInterned("m", false)),
                                                  CellPath.create(Int32Type.instance.decompose(1))).build());
    }

    @Test
    public void testSelectEmptyCellsFromNonEmptyComplexColumn() throws Throwable
    {
        testWithFilter(cfs -> ColumnFilter.selectionBuilder()
                                          .select(cfs.metadata.getColumnDefinition(ColumnIdentifier.getInterned("m", false)),
                                                  CellPath.create(Int32Type.instance.decompose(5))).build());
    }

    private void testWithFilter(Function<ColumnFamilyStore, ColumnFilter> filterFactory) throws Throwable
    {
        Map<Integer, Integer> m = new HashMap<>();
        m.put(1, 10);
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, e text, m map<int, int>, em map<int, int>)");
        execute("INSERT INTO %s (k, v1, v2, m) values (?, ?, ?, ?) USING TIMESTAMP ?", 1, 2, 3, m, writeTime);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ColumnFilter filter = filterFactory.apply(cfs);
        String digest1 = getDigest(filter);
        flush();
        String digest2 = getDigest(filter);

        assertEquals(digest1, digest2);
    }

    private String getDigest(ColumnFilter filter)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        NavigableSet<Clustering> clusterings = Sets.newTreeSet(new ClusteringComparator());
        clusterings.add(Clustering.EMPTY);
        BufferDecoratedKey key = new BufferDecoratedKey(DatabaseDescriptor.getPartitioner().getToken(Int32Type.instance.decompose(1)),
                                                        Int32Type.instance.decompose(1));
        SinglePartitionReadCommand cmd = SinglePartitionReadCommand
                                         .create(cfs.metadata,
                                                 (int) (System.currentTimeMillis() / 1000),
                                                 key,
                                                 filter,
                                                 new ClusteringIndexNamesFilter(clusterings, false)).copyAsDigestQuery();
        cmd.setDigestVersion(MessagingService.current_version);
        ReadResponse resp;
        try (ReadExecutionController ctrl = ReadExecutionController.forCommand(cmd); UnfilteredRowIterator iterator = cmd.queryMemtableAndDisk(cfs, ctrl))
        {
            resp = ReadResponse.createDataResponse(new SingletonUnfilteredPartitionIterator(iterator, false), cmd);
            logger.info("Response is: {}", resp.toDebugString(cmd, key));
            ByteBuffer digest = resp.digest(cmd);
            return ByteBufferUtil.bytesToHex(digest);
        }
    }
}