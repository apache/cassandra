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
package org.apache.cassandra.db.partition;

import org.apache.cassandra.UpdateBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowAndDeletionMergeIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

import junit.framework.Assert;

import static org.junit.Assert.assertEquals;


public class PartitionUpdateTest extends CQLTester
{
    @Test
    public void testOperationCount()
    {
        createTable("CREATE TABLE %s (key text, clustering int, a int, s int static, PRIMARY KEY(key, clustering))");
        CFMetaData cfm = currentTableMetadata();

        UpdateBuilder builder = UpdateBuilder.create(cfm, "key0");
        Assert.assertEquals(0, builder.build().operationCount());
        Assert.assertEquals(1, builder.newRow(1).add("a", 1).build().operationCount());

        builder = UpdateBuilder.create(cfm, "key0");
        Assert.assertEquals(1, builder.newRow().add("s", 1).build().operationCount());

        builder = UpdateBuilder.create(cfm, "key0");
        builder.newRow().add("s", 1);
        builder.newRow(1).add("a", 1);
        Assert.assertEquals(2, builder.build().operationCount());
    }

    @Test
    public void testMutationSize()
    {
        createTable("CREATE TABLE %s (key text, clustering int, a int, s int static, PRIMARY KEY(key, clustering))");
        CFMetaData cfm = currentTableMetadata();

        UpdateBuilder builder = UpdateBuilder.create(cfm, "key0");
        builder.newRow().add("s", 1);
        builder.newRow(1).add("a", 2);
        int size1 = builder.build().dataSize();
        Assert.assertEquals(44, size1);

        builder = UpdateBuilder.create(cfm, "key0");
        builder.newRow(1).add("a", 2);
        int size2 = builder.build().dataSize();
        Assert.assertTrue(size1 != size2);

        builder = UpdateBuilder.create(cfm, "key0");
        int size3 = builder.build().dataSize();
        Assert.assertTrue(size2 != size3);

    }

    @Test
    public void testOperationCountWithCompactTable()
    {
        createTable("CREATE TABLE %s (key text PRIMARY KEY, a int) WITH COMPACT STORAGE");
        CFMetaData cfm = currentTableMetadata();

        PartitionUpdate update = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "key0").add("a", 1)
                                                                                                 .buildUpdate();
        Assert.assertEquals(1, update.operationCount());

        update = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "key0").buildUpdate();
        Assert.assertEquals(0, update.operationCount());
    }

    /**
     * Makes sure we merge duplicate rows, see CASSANDRA-15789
     */
    @Test
    public void testDuplicate()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v map<text, text>, PRIMARY KEY (pk, ck))");
        CFMetaData cfm = currentTableMetadata();

        DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));

        List<Row> rows = new ArrayList<>();
        Row.Builder builder = BTreeRow.unsortedBuilder(FBUtilities.nowInSeconds());
        builder.newRow(Clustering.make(ByteBufferUtil.bytes(2)));
        builder.addComplexDeletion(cfm.getColumnDefinition(ByteBufferUtil.bytes("v")), new DeletionTime(2, 1588586647));

        Cell c = BufferCell.live(cfm.getColumnDefinition(ByteBufferUtil.bytes("v")), 3, ByteBufferUtil.bytes("h"), CellPath.create(ByteBufferUtil.bytes("g")));
        builder.addCell(c);

        Row r = builder.build();
        rows.add(r);

        builder.newRow(Clustering.make(ByteBufferUtil.bytes(2)));
        builder.addRowDeletion(new Row.Deletion(new DeletionTime(1588586647, 1), false));
        r = builder.build();
        rows.add(r);

        RowAndDeletionMergeIterator rmi = new RowAndDeletionMergeIterator(cfm,
                                                                          dk,
                                                                          DeletionTime.LIVE,
                                                                          ColumnFilter.all(cfm),
                                                                          Rows.EMPTY_STATIC_ROW,
                                                                          false,
                                                                          EncodingStats.NO_STATS,
                                                                          rows.iterator(),
                                                                          Collections.emptyIterator(),
                                                                          true);

        PartitionUpdate pu = PartitionUpdate.fromPre30Iterator(rmi, ColumnFilter.all(cfm));
        pu.iterator();

        Mutation m = new Mutation(getCurrentColumnFamilyStore().keyspace.getName(), dk);
        m.add(pu);
        m.apply();
        getCurrentColumnFamilyStore().forceBlockingFlush();

        SSTableReader sst = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        int count = 0;
        try (ISSTableScanner scanner = sst.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator iter = scanner.next())
                {
                    while (iter.hasNext())
                    {
                        iter.next();
                        count++;
                    }
                }
            }
        }
        assertEquals(1, count);
    }

    /**
     * Makes sure we don't create duplicates when merging 2 partition updates
     */
    @Test
    public void testMerge()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v map<text, text>, PRIMARY KEY (pk, ck))");
        CFMetaData cfm = currentTableMetadata();

        DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));

        Row.Builder builder = BTreeRow.unsortedBuilder(FBUtilities.nowInSeconds());
        builder.newRow(Clustering.make(ByteBufferUtil.bytes(2)));
        builder.addComplexDeletion(cfm.getColumnDefinition(ByteBufferUtil.bytes("v")), new DeletionTime(2, 1588586647));
        Cell c = BufferCell.live(cfm.getColumnDefinition(ByteBufferUtil.bytes("v")), 3, ByteBufferUtil.bytes("h"), CellPath.create(ByteBufferUtil.bytes("g")));
        builder.addCell(c);
        Row r = builder.build();

        PartitionUpdate p1 = new PartitionUpdate(cfm, dk, cfm.partitionColumns(), 2);
        p1.add(r);

        builder.newRow(Clustering.make(ByteBufferUtil.bytes(2)));
        builder.addRowDeletion(new Row.Deletion(new DeletionTime(1588586647, 1), false));
        r = builder.build();
        PartitionUpdate p2 = new PartitionUpdate(cfm, dk, cfm.partitionColumns(), 2);
        p2.add(r);

        Mutation m = new Mutation(getCurrentColumnFamilyStore().keyspace.getName(), dk);
        m.add(PartitionUpdate.merge(Lists.newArrayList(p1, p2)));
        m.apply();

        getCurrentColumnFamilyStore().forceBlockingFlush();

        SSTableReader sst = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();
        int count = 0;
        try (ISSTableScanner scanner = sst.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator iter = scanner.next())
                {
                    while (iter.hasNext())
                    {
                        iter.next();
                        count++;
                    }
                }
            }
        }
        assertEquals(1, count);
    }
}
