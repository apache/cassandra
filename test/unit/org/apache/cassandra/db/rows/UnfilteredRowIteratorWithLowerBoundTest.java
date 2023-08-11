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

package org.apache.cassandra.db.rows;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnfilteredRowIteratorWithLowerBoundTest
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";
    private static final String SLICES_TABLE = "tbl_slices";
    private static TableMetadata tableMetadata;
    private static TableMetadata slicesTableMetadata;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();

        tableMetadata =
        TableMetadata.builder(KEYSPACE, TABLE)
                     .addPartitionKeyColumn("k", UTF8Type.instance)
                     .addStaticColumn("s", UTF8Type.instance)
                     .addClusteringColumn("i", IntegerType.instance)
                     .addRegularColumn("v", UTF8Type.instance)
                     .build();

        slicesTableMetadata = TableMetadata.builder(KEYSPACE, SLICES_TABLE)
                                           .addPartitionKeyColumn("k", UTF8Type.instance)
                                           .addClusteringColumn("c1", Int32Type.instance)
                                           .addClusteringColumn("c2", Int32Type.instance)
                                           .addRegularColumn("v", IntegerType.instance)
                                           .build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), tableMetadata, slicesTableMetadata);
    }

    @Before
    public void truncate()
    {
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).truncateBlocking();
        Keyspace.open(KEYSPACE).getColumnFamilyStore(SLICES_TABLE).truncateBlocking();
    }

    @Test
    public void testLowerBoundApplicableSingleColumnAsc()
    {
        String query = "INSERT INTO %s.%s (k, i) VALUES ('k1', %s)";
        SSTableReader sstable = createSSTable(tableMetadata, KEYSPACE, TABLE, query);
        assertEquals(Slice.make(Util.clustering(tableMetadata.comparator, BigInteger.valueOf(0)),
                                Util.clustering(tableMetadata.comparator, BigInteger.valueOf(9))),
                     sstable.getSSTableMetadata().coveredClustering);
        DecoratedKey key = tableMetadata.partitioner.decorateKey(ByteBufferUtil.bytes("k1"));

        Slice slice1 = Slice.make(Util.clustering(tableMetadata.comparator, BigInteger.valueOf(3)).asStartBound(), ClusteringBound.TOP);
        assertFalse(lowerBoundApplicable(tableMetadata, key, slice1, sstable, false));
        assertTrue(lowerBoundApplicable(tableMetadata, key, slice1, sstable, true));

        Slice slice2 = Slice.make(ClusteringBound.BOTTOM, Util.clustering(tableMetadata.comparator, BigInteger.valueOf(3)).asEndBound());
        assertTrue(lowerBoundApplicable(tableMetadata, key, slice2, sstable, false));
        assertFalse(lowerBoundApplicable(tableMetadata, key, slice2, sstable, true));

        // corner cases
        Slice slice3 = Slice.make(Util.clustering(tableMetadata.comparator, BigInteger.valueOf(0)).asStartBound(), ClusteringBound.TOP);
        assertFalse(lowerBoundApplicable(tableMetadata, key, slice3, sstable, false));
        assertTrue(lowerBoundApplicable(tableMetadata, key, slice3, sstable, true));

        Slice slice4 = Slice.make(ClusteringBound.BOTTOM, Util.clustering(tableMetadata.comparator, BigInteger.valueOf(9)).asEndBound());
        assertTrue(lowerBoundApplicable(tableMetadata, key, slice4, sstable, false));
        assertFalse(lowerBoundApplicable(tableMetadata, key, slice4, sstable, true));
    }

    @Test
    public void testLowerBoundApplicableSingleColumnDesc()
    {
        String TABLE_REVERSED = "tbl_reversed";
        String createTable = String.format(
        "CREATE TABLE %s.%s (k text, i varint, v int, primary key (k, i)) WITH CLUSTERING ORDER BY (i DESC)",
        KEYSPACE, TABLE_REVERSED);
        QueryProcessor.executeOnceInternal(createTable);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE_REVERSED);
        TableMetadata metadata = cfs.metadata();
        String query = "INSERT INTO %s.%s (k, i) VALUES ('k1', %s)";
        SSTableReader sstable = createSSTable(metadata, KEYSPACE, TABLE_REVERSED, query);
        assertEquals(Slice.make(Util.clustering(metadata.comparator, BigInteger.valueOf(9)),
                                Util.clustering(metadata.comparator, BigInteger.valueOf(0))),
                     sstable.getSSTableMetadata().coveredClustering);
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes("k1"));

        Slice slice1 = Slice.make(Util.clustering(metadata.comparator, BigInteger.valueOf(8)).asStartBound(), ClusteringBound.TOP);
        assertFalse(lowerBoundApplicable(metadata, key, slice1, sstable, false));
        assertTrue(lowerBoundApplicable(metadata, key, slice1, sstable, true));

        Slice slice2 = Slice.make(ClusteringBound.BOTTOM, Util.clustering(metadata.comparator, BigInteger.valueOf(8)).asEndBound());
        assertTrue(lowerBoundApplicable(metadata, key, slice2, sstable, false));
        assertFalse(lowerBoundApplicable(metadata, key, slice2, sstable, true));

        // corner cases
        Slice slice3 = Slice.make(Util.clustering(metadata.comparator, BigInteger.valueOf(9)).asStartBound(), ClusteringBound.TOP);
        assertFalse(lowerBoundApplicable(metadata, key, slice3, sstable, false));
        assertTrue(lowerBoundApplicable(metadata, key, slice3, sstable, true));

        Slice slice4 = Slice.make(ClusteringBound.BOTTOM, Util.clustering(metadata.comparator, BigInteger.valueOf(0)).asEndBound());
        assertTrue(lowerBoundApplicable(metadata, key, slice4, sstable, false));
        assertFalse(lowerBoundApplicable(metadata, key, slice4, sstable, true));
    }

    @Test
    public void testLowerBoundApplicableMultipleColumnsAsc()
    {
        String query = "INSERT INTO %s.%s (k, c1, c2) VALUES ('k1', 0, %s)";
        SSTableReader sstable = createSSTable(slicesTableMetadata, KEYSPACE, SLICES_TABLE, query);
        assertEquals(Slice.make(Util.clustering(slicesTableMetadata.comparator, 0, 0),
                                Util.clustering(slicesTableMetadata.comparator, 0, 9)),
                     sstable.getSSTableMetadata().coveredClustering);
        DecoratedKey key = slicesTableMetadata.partitioner.decorateKey(ByteBufferUtil.bytes("k1"));

        Slice slice1 = Slice.make(Util.clustering(slicesTableMetadata.comparator, 0, 3).asStartBound(), ClusteringBound.TOP);
        assertFalse(lowerBoundApplicable(slicesTableMetadata, key, slice1, sstable, false));
        assertTrue(lowerBoundApplicable(slicesTableMetadata, key, slice1, sstable, true));

        Slice slice2 = Slice.make(ClusteringBound.BOTTOM, Util.clustering(slicesTableMetadata.comparator, 0, 3).asEndBound());
        assertTrue(lowerBoundApplicable(slicesTableMetadata, key, slice2, sstable, false));
        assertFalse(lowerBoundApplicable(slicesTableMetadata, key, slice2, sstable, true));

        // corner cases
        Slice slice3 = Slice.make(Util.clustering(slicesTableMetadata.comparator, 0, 0).asStartBound(), ClusteringBound.TOP);
        assertFalse(lowerBoundApplicable(slicesTableMetadata, key, slice3, sstable, false));
        assertTrue(lowerBoundApplicable(slicesTableMetadata, key, slice3, sstable, true));

        Slice slice4 = Slice.make(ClusteringBound.BOTTOM, Util.clustering(slicesTableMetadata.comparator, 0, 9).asEndBound());
        assertTrue(lowerBoundApplicable(slicesTableMetadata, key, slice4, sstable, false));
        assertFalse(lowerBoundApplicable(slicesTableMetadata, key, slice4, sstable, true));
    }

    @Test
    public void testLowerBoundApplicableMultipleColumnsDesc()
    {
        String TABLE_REVERSED = "tbl_slices_reversed";
        String createTable = String.format(
        "CREATE TABLE %s.%s (k text, c1 int, c2 int, v int, primary key (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)",
        KEYSPACE, TABLE_REVERSED);
        QueryProcessor.executeOnceInternal(createTable);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE_REVERSED);
        TableMetadata metadata = cfs.metadata();

        String query = "INSERT INTO %s.%s (k, c1, c2) VALUES ('k1', 0, %s)";
        SSTableReader sstable = createSSTable(metadata, KEYSPACE, TABLE_REVERSED, query);
        assertEquals(Slice.make(Util.clustering(metadata.comparator, 0, 9),
                                Util.clustering(metadata.comparator, 0, 0)),
                     sstable.getSSTableMetadata().coveredClustering);
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes("k1"));

        Slice slice1 = Slice.make(Util.clustering(metadata.comparator, 0, 8).asStartBound(), ClusteringBound.TOP);
        assertFalse(lowerBoundApplicable(metadata, key, slice1, sstable, false));
        assertTrue(lowerBoundApplicable(metadata, key, slice1, sstable, true));

        Slice slice2 = Slice.make(ClusteringBound.BOTTOM, Util.clustering(metadata.comparator, 0, 8).asEndBound());
        assertTrue(lowerBoundApplicable(metadata, key, slice2, sstable, false));
        assertFalse(lowerBoundApplicable(metadata, key, slice2, sstable, true));

        // corner cases
        Slice slice3 = Slice.make(Util.clustering(metadata.comparator, 0, 9).asStartBound(), ClusteringBound.TOP);
        assertFalse(lowerBoundApplicable(metadata, key, slice3, sstable, false));
        assertTrue(lowerBoundApplicable(metadata, key, slice3, sstable, true));

        Slice slice4 = Slice.make(ClusteringBound.BOTTOM, Util.clustering(metadata.comparator, 0, 0).asEndBound());
        assertTrue(lowerBoundApplicable(metadata, key, slice4, sstable, false));
        assertFalse(lowerBoundApplicable(metadata, key, slice4, sstable, true));
    }

    private SSTableReader createSSTable(TableMetadata metadata, String keyspace, String table, String query)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        for (int i = 0; i < 10; i++)
            QueryProcessor.executeInternal(String.format(query, keyspace, table, i));
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes("k1"));
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, key));
        assertEquals(1, view.sstables.size());
        return view.sstables.get(0);
    }

    private boolean lowerBoundApplicable(TableMetadata metadata, DecoratedKey key, Slice slice, SSTableReader sstable, boolean isReversed)
    {
        Slices.Builder slicesBuilder = new Slices.Builder(metadata.comparator);
        slicesBuilder.add(slice);
        Slices slices = slicesBuilder.build();
        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(slices, isReversed);

        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(metadata,
                                                                           FBUtilities.nowInSeconds(),
                                                                           ColumnFilter.all(metadata),
                                                                           RowFilter.none(),
                                                                           DataLimits.NONE,
                                                                           key,
                                                                           filter);

        try (UnfilteredRowIteratorWithLowerBound iter = new UnfilteredRowIteratorWithLowerBound(key,
                                                                                                sstable,
                                                                                                slices,
                                                                                                isReversed,
                                                                                                ColumnFilter.all(metadata),
                                                                                                Mockito.mock(SSTableReadsListener.class)))
        {
            return iter.lowerBound() != null;
        }
    }
}
