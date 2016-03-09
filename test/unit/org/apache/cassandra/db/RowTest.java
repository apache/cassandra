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
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RowTest
{
    private static final String KEYSPACE1 = "RowTest";
    private static final String CF_STANDARD1 = "Standard1";

    private int nowInSeconds;
    private DecoratedKey dk;
    private ColumnFamilyStore cfs;
    private CFMetaData cfm;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        CFMetaData cfMetadata = CFMetaData.Builder.create(KEYSPACE1, CF_STANDARD1)
                                                  .addPartitionKey("key", BytesType.instance)
                                                  .addClusteringColumn("col1", AsciiType.instance)
                                                  .addRegularColumn("a", AsciiType.instance)
                                                  .addRegularColumn("b", AsciiType.instance)
                                                  .build();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    cfMetadata);
    }

    @Before
    public void setup()
    {
        nowInSeconds = FBUtilities.nowInSeconds();
        dk = Util.dk("key0");
        cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        cfm = cfs.metadata;
    }

    @Test
    public void testMergeRangeTombstones() throws InterruptedException
    {
        PartitionUpdate update1 = new PartitionUpdate(cfm, dk, cfm.partitionColumns(), 1);
        writeRangeTombstone(update1, "1", "11", 123, 123);
        writeRangeTombstone(update1, "2", "22", 123, 123);
        writeRangeTombstone(update1, "3", "31", 123, 123);
        writeRangeTombstone(update1, "4", "41", 123, 123);

        PartitionUpdate update2 = new PartitionUpdate(cfm, dk, cfm.partitionColumns(), 1);
        writeRangeTombstone(update2, "1", "11", 123, 123);
        writeRangeTombstone(update2, "111", "112", 1230, 123);
        writeRangeTombstone(update2, "2", "24", 123, 123);
        writeRangeTombstone(update2, "3", "31", 1230, 123);
        writeRangeTombstone(update2, "4", "41", 123, 1230);
        writeRangeTombstone(update2, "5", "51", 123, 1230);

        try (UnfilteredRowIterator merged = UnfilteredRowIterators.merge(ImmutableList.of(update1.unfilteredIterator(), update2.unfilteredIterator()), nowInSeconds))
        {
            Object[][] expected = new Object[][]{ { "1", "11", 123l, 123 },
                                                  { "111", "112", 1230l, 123 },
                                                  { "2", "24", 123l, 123 },
                                                  { "3", "31", 1230l, 123 },
                                                  { "4", "41", 123l, 1230 },
                                                  { "5", "51", 123l, 1230 } };
            int i = 0;
            while (merged.hasNext())
            {
                RangeTombstoneBoundMarker openMarker = (RangeTombstoneBoundMarker)merged.next();
                Slice.Bound openBound = openMarker.clustering();
                DeletionTime openDeletion = new DeletionTime(openMarker.deletionTime().markedForDeleteAt(),
                                                                   openMarker.deletionTime().localDeletionTime());

                RangeTombstoneBoundMarker closeMarker = (RangeTombstoneBoundMarker)merged.next();
                Slice.Bound closeBound = closeMarker.clustering();
                DeletionTime closeDeletion = new DeletionTime(closeMarker.deletionTime().markedForDeleteAt(),
                                                                    closeMarker.deletionTime().localDeletionTime());

                assertEquals(openDeletion, closeDeletion);
                assertRangeTombstoneMarkers(openBound, closeBound, openDeletion, expected[i++]);
            }
        }
    }

    @Test
    public void testResolve()
    {
        ColumnDefinition defA = cfm.getColumnDefinition(new ColumnIdentifier("a", true));
        ColumnDefinition defB = cfm.getColumnDefinition(new ColumnIdentifier("b", true));

        Row.Builder builder = BTreeRow.unsortedBuilder(nowInSeconds);
        builder.newRow(cfm.comparator.make("c1"));
        writeSimpleCellValue(builder, cfm, defA, "a1", 0);
        writeSimpleCellValue(builder, cfm, defA, "a2", 1);
        writeSimpleCellValue(builder, cfm, defB, "b1", 1);
        Row row = builder.build();

        PartitionUpdate update = PartitionUpdate.singleRowUpdate(cfm, dk, row);

        Unfiltered unfiltered = update.unfilteredIterator().next();
        assertTrue(unfiltered.kind() == Unfiltered.Kind.ROW);
        row = (Row) unfiltered;
        assertEquals("a2", defA.cellValueType().getString(row.getCell(defA).value()));
        assertEquals("b1", defB.cellValueType().getString(row.getCell(defB).value()));
        assertEquals(2, row.columns().size());
    }

    @Test
    public void testExpiringColumnExpiration() throws IOException
    {
        int ttl = 1;
        ColumnDefinition def = cfm.getColumnDefinition(new ColumnIdentifier("a", true));

        Cell cell = BufferCell.expiring(def, 0, ttl, nowInSeconds, ((AbstractType) def.cellValueType()).decompose("a1"));

        PartitionUpdate update = PartitionUpdate.singleRowUpdate(cfm, dk, BTreeRow.singleCellRow(cfm.comparator.make("c1"), cell));
        new Mutation(update).applyUnsafe();

        // when we read with a nowInSeconds before the cell has expired,
        // the PartitionIterator includes the row we just wrote
        Row row = Util.getOnlyRow(Util.cmd(cfs, dk).includeRow("c1").withNowInSeconds(nowInSeconds).build());
        assertEquals("a1", ByteBufferUtil.string(row.getCell(def).value()));

        // when we read with a nowInSeconds after the cell has expired, the row is filtered
        // so the PartitionIterator is empty
        Util.assertEmpty(Util.cmd(cfs, dk).includeRow("c1").withNowInSeconds(nowInSeconds + ttl + 1).build());
    }

    @Test
    public void testHashCode()
    {
        ColumnDefinition defA = cfm.getColumnDefinition(new ColumnIdentifier("a", true));
        ColumnDefinition defB = cfm.getColumnDefinition(new ColumnIdentifier("b", true));

        Row.Builder builder = BTreeRow.unsortedBuilder(nowInSeconds);
        builder.newRow(cfm.comparator.make("c1"));
        writeSimpleCellValue(builder, cfm, defA, "a1", 0);
        writeSimpleCellValue(builder, cfm, defA, "a2", 1);
        writeSimpleCellValue(builder, cfm, defB, "b1", 1);
        Row row = builder.build();

        Map<Row, Integer> map = new HashMap<>();
        map.put(row, 1);
        assertEquals(Integer.valueOf(1), map.get(row));
    }

    private void assertRangeTombstoneMarkers(Slice.Bound start, Slice.Bound end, DeletionTime deletionTime, Object[] expected)
    {
        AbstractType clusteringType = (AbstractType)cfm.comparator.subtype(0);

        assertEquals(1, start.size());
        assertEquals(start.kind(), Slice.Bound.Kind.INCL_START_BOUND);
        assertEquals(expected[0], clusteringType.getString(start.get(0)));

        assertEquals(1, end.size());
        assertEquals(end.kind(), Slice.Bound.Kind.INCL_END_BOUND);
        assertEquals(expected[1], clusteringType.getString(end.get(0)));

        assertEquals(expected[2], deletionTime.markedForDeleteAt());
        assertEquals(expected[3], deletionTime.localDeletionTime());
    }

    public void writeRangeTombstone(PartitionUpdate update, Object start, Object end, long markedForDeleteAt, int localDeletionTime)
    {
        ClusteringComparator comparator = cfs.getComparator();
        update.add(new RangeTombstone(Slice.make(comparator.make(start), comparator.make(end)), new DeletionTime(markedForDeleteAt, localDeletionTime)));
    }

    private void writeSimpleCellValue(Row.Builder builder,
                                      CFMetaData cfm,
                                      ColumnDefinition columnDefinition,
                                      String value,
                                      long timestamp)
    {
       builder.addCell(BufferCell.live(columnDefinition, timestamp, ((AbstractType) columnDefinition.cellValueType()).decompose(value)));
    }
}
