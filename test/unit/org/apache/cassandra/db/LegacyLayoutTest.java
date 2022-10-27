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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.AfterClass;
import org.apache.cassandra.db.LegacyLayout.CellGrouper;
import org.apache.cassandra.db.LegacyLayout.LegacyBound;
import org.apache.cassandra.db.LegacyLayout.LegacyCell;
import org.apache.cassandra.db.LegacyLayout.LegacyRangeTombstone;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.db.transform.FilteredRows;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.Hex;

import static org.apache.cassandra.net.MessagingService.VERSION_21;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.*;

public class LegacyLayoutTest
{
    static Util.PartitionerSwitcher sw;
    static String KEYSPACE = "Keyspace1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
        sw = Util.switchPartitioner(Murmur3Partitioner.instance);
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
    }

    @AfterClass
    public static void resetPartitioner()
    {
        sw.close();
    }

    @Test
    public void testFromUnfilteredRowIterator() throws Throwable
    {
        CFMetaData table = CFMetaData.Builder.create("ks", "table")
                                             .withPartitioner(Murmur3Partitioner.instance)
                                             .addPartitionKey("k", Int32Type.instance)
                                             .addRegularColumn("a", SetType.getInstance(Int32Type.instance, true))
                                             .addRegularColumn("b", SetType.getInstance(Int32Type.instance, true))
                                             .build();

        ColumnDefinition a = table.getColumnDefinition(new ColumnIdentifier("a", false));
        ColumnDefinition b = table.getColumnDefinition(new ColumnIdentifier("b", false));

        Row.Builder builder = BTreeRow.unsortedBuilder(0);
        builder.newRow(Clustering.EMPTY);
        builder.addComplexDeletion(a, new DeletionTime(1L, 1));
        builder.addComplexDeletion(b, new DeletionTime(1L, 1));
        Row row = builder.build();

        ByteBuffer key = bytes(1);
        PartitionUpdate upd = PartitionUpdate.singleRowUpdate(table, key, row);

        LegacyLayout.LegacyUnfilteredPartition p = LegacyLayout.fromUnfilteredRowIterator(null, upd.unfilteredIterator());
        assertEquals(DeletionTime.LIVE, p.partitionDeletion);
        assertEquals(0, p.cells.size());

        LegacyLayout.LegacyRangeTombstoneList l = p.rangeTombstones;
        assertEquals("a", l.starts[0].collectionName.name.toString());
        assertEquals("a", l.ends[0].collectionName.name.toString());

        assertEquals("b", l.starts[1].collectionName.name.toString());
        assertEquals("b", l.ends[1].collectionName.name.toString());
    }

    /**
     * Tests with valid sstables containing duplicate RT entries at index boundaries
     * in 2.1 format, where DATA below is a > 1000 byte long string of letters,
     * and the column index is set to 1kb

     [
     {"key": "1",
     "cells": [["1:_","1:!",1513015245,"t",1513015263],
     ["1:1:","",1513015467727335],
     ["1:1:val1","DATA",1513015467727335],
     ["1:_","1:!",1513015245,"t",1513015263],
     ["1:1:val2","DATA",1513015467727335],
     ["1:_","1:!",1513015245,"t",1513015263],
     ["1:1:val3","DATA",1513015467727335],
     ["1:_","1:!",1513015245,"t",1513015263],
     ["1:2:","",1513015458470156],
     ["1:2:val1","DATA",1513015458470156],
     ["1:_","1:!",1513015245,"t",1513015263],
     ["1:2:val2","DATA",1513015458470156],
     ["1:_","1:!",1513015245,"t",1513015263],
     ["1:2:val3","DATA",1513015458470156],
     ["1:_","1:!",1513015245,"t",1513015263],
     ["1:3:","",1513015450253602],
     ["1:3:val1","DATA",1513015450253602],
     ["1:_","1:!",1513015245,"t",1513015263],
     ["1:3:val2","DATA",1513015450253602],
     ["1:_","1:!",1513015245,"t",1513015263],
     ["1:3:val3","DATA",1513015450253602]]}
     ]
     *
     * See CASSANDRA-14008 for details.
     */
    @Test
    public void testRTBetweenColumns() throws Throwable
    {
        QueryProcessor.executeInternal(String.format("CREATE TABLE \"%s\".legacy_ka_repeated_rt (k1 int, c1 int, c2 int, val1 text, val2 text, val3 text, primary key (k1, c1, c2))", KEYSPACE));

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("legacy_ka_repeated_rt");

        Path legacySSTableRoot = Paths.get("test/data/legacy-sstables/ka/legacy_tables/legacy_ka_repeated_rt/");

        for (String filename : new String[]{ "Keyspace1-legacy_ka_repeated_rt-ka-1-CompressionInfo.db",
                                             "Keyspace1-legacy_ka_repeated_rt-ka-1-Data.db",
                                             "Keyspace1-legacy_ka_repeated_rt-ka-1-Digest.sha1",
                                             "Keyspace1-legacy_ka_repeated_rt-ka-1-Filter.db",
                                             "Keyspace1-legacy_ka_repeated_rt-ka-1-Index.db",
                                             "Keyspace1-legacy_ka_repeated_rt-ka-1-Statistics.db",
                                             "Keyspace1-legacy_ka_repeated_rt-ka-1-Summary.db",
                                             "Keyspace1-legacy_ka_repeated_rt-ka-1-TOC.txt" })
        {
            Files.copy(Paths.get(legacySSTableRoot.toString(), filename), cfs.getDirectories().getDirectoryForNewSSTables().toPath().resolve(filename));
        }

        cfs.loadNewSSTables();

        UntypedResultSet rs = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".legacy_ka_repeated_rt WHERE k1=1", KEYSPACE));
        assertEquals(3, rs.size());

        UntypedResultSet rs2 = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".legacy_ka_repeated_rt WHERE k1=1 AND c1=1", KEYSPACE));
        assertEquals(3, rs2.size());

        for (int i = 1; i <= 3; i++)
        {
            UntypedResultSet rs3 = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".legacy_ka_repeated_rt WHERE k1=1 AND c1=1 AND c2=%s", KEYSPACE, i));
            assertEquals(1, rs3.size());
        }

    }


    private static UnfilteredRowIterator roundTripVia21(UnfilteredRowIterator partition) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            LegacyLayout.serializeAsLegacyPartition(null, partition, out, VERSION_21);
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                return LegacyLayout.deserializeLegacyPartition(in, VERSION_21, SerializationHelper.Flag.LOCAL, partition.partitionKey().getKey());
            }
        }
    }

    @Test
    public void testStaticRangeTombstoneRoundTripUnexpectedDeletion() throws Throwable
    {
        // this variant of the bug deletes a row with the same clustering key value as the name of the static collection
        QueryProcessor.executeInternal(String.format("CREATE TABLE \"%s\".legacy_static_rt_rt_1 (pk int, ck1 text, ck2 text, v int, s set<text> static, primary key (pk, ck1, ck2))", KEYSPACE));
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        CFMetaData table = keyspace.getColumnFamilyStore("legacy_static_rt_rt_1").metadata;
        ColumnDefinition v = table.getColumnDefinition(new ColumnIdentifier("v", false));
        ColumnDefinition bug = table.getColumnDefinition(new ColumnIdentifier("s", false));

        Row.Builder builder;
        builder = BTreeRow.unsortedBuilder(0);
        builder.newRow(Clustering.STATIC_CLUSTERING);
        builder.addComplexDeletion(bug, new DeletionTime(1L, 1));
        Row staticRow = builder.build();

        builder = BTreeRow.unsortedBuilder(0);
        builder.newRow(new BufferClustering(UTF8Serializer.instance.serialize("s"), UTF8Serializer.instance.serialize("anything")));
        builder.addCell(new BufferCell(v, 1L, Cell.NO_TTL, Cell.NO_DELETION_TIME, Int32Serializer.instance.serialize(1), null));
        Row row = builder.build();

        DecoratedKey pk = table.decorateKey(bytes(1));
        PartitionUpdate upd = PartitionUpdate.singleRowUpdate(table, pk, row, staticRow);

        try (RowIterator before = FilteredRows.filter(upd.unfilteredIterator(), FBUtilities.nowInSeconds());
             RowIterator after = FilteredRows.filter(roundTripVia21(upd.unfilteredIterator()), FBUtilities.nowInSeconds()))
        {
            while (before.hasNext() || after.hasNext())
                assertEquals(before.hasNext() ? before.next() : null, after.hasNext() ? after.next() : null);
        }
    }

    @Test
    public void testStaticRangeTombstoneRoundTripCorruptRead() throws Throwable
    {
        // this variant of the bug corrupts the byte stream of the partition, so that a sequential read starting before
        // this partition will fail with a CorruptSSTableException, and possible yield junk results
        QueryProcessor.executeInternal(String.format("CREATE TABLE \"%s\".legacy_static_rt_rt_2 (pk int, ck int, nameWithLengthGreaterThan4 set<int> static, primary key (pk, ck))", KEYSPACE));
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        CFMetaData table = keyspace.getColumnFamilyStore("legacy_static_rt_rt_2").metadata;

        ColumnDefinition bug = table.getColumnDefinition(new ColumnIdentifier("nameWithLengthGreaterThan4", false));

        Row.Builder builder = BTreeRow.unsortedBuilder(0);
        builder.newRow(Clustering.STATIC_CLUSTERING);
        builder.addComplexDeletion(bug, new DeletionTime(1L, 1));
        Row row = builder.build();

        DecoratedKey pk = table.decorateKey(bytes(1));
        PartitionUpdate upd = PartitionUpdate.singleRowUpdate(table, pk, row);

        UnfilteredRowIterator afterRoundTripVia32 = roundTripVia21(upd.unfilteredIterator());
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            // we only encounter a corruption/serialization error after writing this to a 3.0 format and reading it back
            UnfilteredRowIteratorSerializer.serializer.serialize(afterRoundTripVia32, ColumnFilter.all(table), out, MessagingService.current_version);
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
                 UnfilteredRowIterator afterSerialization = UnfilteredRowIteratorSerializer.serializer.deserialize(in, MessagingService.current_version, table, ColumnFilter.all(table), SerializationHelper.Flag.LOCAL))
            {
                while (afterSerialization.hasNext())
                    afterSerialization.next();
            }
        }
    }

    @Test
    public void testCollectionDeletionRoundTripForDroppedColumn() throws Throwable
    {
        // this variant of the bug deletes a row with the same clustering key value as the name of the static collection
        QueryProcessor.executeInternal(String.format("CREATE TABLE \"%s\".legacy_rt_rt_dc (pk int, ck1 text, v int, s set<text>, primary key (pk, ck1))", KEYSPACE));
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        CFMetaData table = keyspace.getColumnFamilyStore("legacy_rt_rt_dc").metadata;
        ColumnDefinition v = table.getColumnDefinition(new ColumnIdentifier("v", false));
        ColumnDefinition bug = table.getColumnDefinition(new ColumnIdentifier("s", false));

        Row.Builder builder;
        builder = BTreeRow.unsortedBuilder(0);
        builder.newRow(new BufferClustering(UTF8Serializer.instance.serialize("a")));
        builder.addCell(BufferCell.live(v, 0L, Int32Serializer.instance.serialize(1), null));
        builder.addComplexDeletion(bug, new DeletionTime(1L, 1));
        Row row = builder.build();

        DecoratedKey pk = table.decorateKey(bytes(1));
        PartitionUpdate upd = PartitionUpdate.singleRowUpdate(table, pk, row);

        // we need to perform the round trip in two parts here, with a column drop inbetween
        try (RowIterator before = FilteredRows.filter(upd.unfilteredIterator(), FBUtilities.nowInSeconds());
             DataOutputBuffer serialized21 = new DataOutputBuffer())
        {
            LegacyLayout.serializeAsLegacyPartition(null, upd.unfilteredIterator(), serialized21, VERSION_21);
            QueryProcessor.executeInternal(String.format("ALTER TABLE \"%s\".legacy_rt_rt_dc DROP s", KEYSPACE));
            try (DataInputBuffer in = new DataInputBuffer(serialized21.buffer(), false))
            {
                try (UnfilteredRowIterator deser21 = LegacyLayout.deserializeLegacyPartition(in, VERSION_21, SerializationHelper.Flag.LOCAL, upd.partitionKey().getKey());
                    RowIterator after = FilteredRows.filter(deser21, FBUtilities.nowInSeconds());)
                {
                    while (before.hasNext() || after.hasNext())
                        assertEquals(before.hasNext() ? before.next() : null, after.hasNext() ? after.next() : null);
                }
            }

        }
    }

    @Test
    public void testDecodeLegacyPagedRangeCommandSerializer() throws IOException
    {
        /*
         Run on 2.1
         public static void main(String[] args) throws IOException, ConfigurationException
         {
             Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
             Keyspace.setInitialized();
             CFMetaData cfMetaData = CFMetaData.sparseCFMetaData("ks", "cf", UTF8Type.instance)
             .addColumnDefinition(new ColumnDefinition("ks", "cf", new ColumnIdentifier("v", true), SetType.getInstance(Int32Type.instance, false), null, null, null, null, ColumnDefinition.Kind.REGULAR));
             KSMetaData ksMetaData = KSMetaData.testMetadata("ks", SimpleStrategy.class, KSMetaData.optsWithRF(3), cfMetaData);
             MigrationManager.announceNewKeyspace(ksMetaData);
             RowPosition position = RowPosition.ForKey.get(ByteBufferUtil.EMPTY_BYTE_BUFFER, new Murmur3Partitioner());
             SliceQueryFilter filter = new IdentityQueryFilter();
             Composite cellName = CellNames.compositeSparseWithCollection(new ByteBuffer[0], Int32Type.instance.decompose(1), new ColumnIdentifier("v", true), false);
             try (DataOutputBuffer buffer = new DataOutputBuffer(1024))
             {
                 PagedRangeCommand command = new PagedRangeCommand("ks", "cf", 1, AbstractBounds.bounds(position, true, position, true), filter, cellName, filter.finish(), Collections.emptyList(), 1, true);
                 PagedRangeCommand.serializer.serialize(command, buffer, MessagingService.current_version);
                 System.out.println(Hex.bytesToHex(buffer.toByteArray()));
             }
         }
         */

        DatabaseDescriptor.daemonInitialization();
        Keyspace.setInitialized();
        CFMetaData table = CFMetaData.Builder.create("ks", "cf")
                                             .addPartitionKey("k", Int32Type.instance)
                                             .addRegularColumn("v", SetType.getInstance(Int32Type.instance, true))
                                             .build();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1));
        MigrationManager.announceNewColumnFamily(table);

        byte[] bytes = Hex.hexToBytes("00026b73000263660000000000000001fffffffe01000000088000000000000000010000000880000000000000000000000100000000007fffffffffffffff000b00017600000400000001000000000000000000000101");
        ReadCommand.legacyPagedRangeCommandSerializer.deserialize(new DataInputBuffer(bytes), VERSION_21);
    }

    @Test
    public void testDecodeCollectionPageBoundary()
    {
        CFMetaData table = CFMetaData.Builder.create("ks", "cf")
                                             .addPartitionKey("k", Int32Type.instance)
                                             .addRegularColumn("v", SetType.getInstance(Int32Type.instance, true))
                                             .build();

        ColumnDefinition v = table.getColumnDefinition(new ColumnIdentifier("v", false));
        ByteBuffer bound = LegacyLayout.encodeCellName(table, Clustering.EMPTY, v.name.bytes, Int32Type.instance.decompose(1));

        LegacyLayout.decodeSliceBound(table, bound, true);
    }

    @Test
    public void testAsymmetricRTBoundSerializedSize()
    {
        CFMetaData table = CFMetaData.Builder.create("ks", "cf")
                                             .addPartitionKey("k", Int32Type.instance)
                                             .addClusteringColumn("c1", Int32Type.instance)
                                             .addClusteringColumn("c2", Int32Type.instance)
                                             .addRegularColumn("v", Int32Type.instance)
                                             .build();

        ByteBuffer one = Int32Type.instance.decompose(1);
        ByteBuffer two = Int32Type.instance.decompose(2);
        PartitionUpdate p = new PartitionUpdate(table, table.decorateKey(one), table.partitionColumns(), 0);
        p.add(new RangeTombstone(Slice.make(new ClusteringBound(ClusteringPrefix.Kind.EXCL_START_BOUND, new ByteBuffer[] { one, one }),
                                            new ClusteringBound(ClusteringPrefix.Kind.INCL_END_BOUND, new ByteBuffer[] { two })),
                                 new DeletionTime(1, 1)
        ));

        LegacyLayout.fromUnfilteredRowIterator(null, p.unfilteredIterator());
        LegacyLayout.serializedSizeAsLegacyPartition(null, p.unfilteredIterator(), VERSION_21);
    }

    @Test
    public void testCellGrouper()
    {
        // CREATE TABLE %s (pk int, ck int, v map<text, text>, PRIMARY KEY (pk, ck))
        CFMetaData cfm = CFMetaData.Builder.create("ks", "table")
                                           .addPartitionKey("pk", Int32Type.instance)
                                           .addClusteringColumn("ck", Int32Type.instance)
                                           .addRegularColumn("v", MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true))
                                           .build();
        SerializationHelper helper = new SerializationHelper(cfm, MessagingService.VERSION_22, SerializationHelper.Flag.LOCAL, ColumnFilter.all(cfm));
        LegacyLayout.CellGrouper cg = new LegacyLayout.CellGrouper(cfm, helper);

        ClusteringBound startBound = ClusteringBound.create(ClusteringPrefix.Kind.INCL_START_BOUND, new ByteBuffer[] {bytes(2)});
        ClusteringBound endBound = ClusteringBound.create(ClusteringPrefix.Kind.EXCL_END_BOUND, new ByteBuffer[] {bytes(2)});
        LegacyLayout.LegacyBound start = new LegacyLayout.LegacyBound(startBound, false, cfm.getColumnDefinition(bytes("v")));
        LegacyLayout.LegacyBound end = new LegacyLayout.LegacyBound(endBound, false, cfm.getColumnDefinition(bytes("v")));
        LegacyLayout.LegacyRangeTombstone lrt = new LegacyLayout.LegacyRangeTombstone(start, end, new DeletionTime(2, 1588598040));
        assertTrue(cg.addAtom(lrt));

        // add a real cell
        LegacyLayout.LegacyCell cell = new LegacyLayout.LegacyCell(LegacyLayout.LegacyCell.Kind.REGULAR,
                                                                   new LegacyLayout.LegacyCellName(Clustering.make(bytes(2)),
                                                                                                   cfm.getColumnDefinition(bytes("v")),
                                                                                                   bytes("g")),
                                                                   bytes("v"), 3, Integer.MAX_VALUE, 0);
        assertTrue(cg.addAtom(cell));

        // add legacy range tombstone where collection name is null for the end bound (this gets translated to a row tombstone)
        startBound = ClusteringBound.create(ClusteringPrefix.Kind.EXCL_START_BOUND, new ByteBuffer[] {bytes(2)});
        endBound = ClusteringBound.create(ClusteringPrefix.Kind.EXCL_END_BOUND, new ByteBuffer[] {bytes(2)});
        start = new LegacyLayout.LegacyBound(startBound, false, cfm.getColumnDefinition(bytes("v")));
        end = new LegacyLayout.LegacyBound(endBound, false, null);
        assertTrue(cg.addAtom(new LegacyLayout.LegacyRangeTombstone(start, end, new DeletionTime(1, 1588598040))));
    }

    private static LegacyCell cell(Clustering clustering, ColumnDefinition column, ByteBuffer value, long timestamp)
    {
        return new LegacyCell(LegacyCell.Kind.REGULAR,
                              new LegacyLayout.LegacyCellName(clustering, column, null),
                              value,
                              timestamp,
                              Cell.NO_DELETION_TIME,
                              Cell.NO_TTL);
    }

    /**
     * This tests that when {@link CellGrouper} gets a collection tombstone for
     * a non-fetched collection, then that tombstone does not incorrectly stop the grouping of the current row, as
     * was done before CASSANDRA-15805.
     *
     * <p>Please note that this rely on a query only _fetching_ some of the table columns, which in practice only
     * happens for thrift queries, and thrift queries shouldn't mess up with CQL tables and collection tombstones,
     * so this test is not of the utmost importance. Nonetheless, the pre-CASSANDRA-15805 behavior was incorrect and
     * this ensure it is fixed.
     */
    @Test
    public void testCellGrouperOnNonFecthedCollectionTombstone()
    {
        // CREATE TABLE %s (pk int, ck int, a text, b set<text>, c text, PRIMARY KEY (pk, ck))
        CFMetaData cfm = CFMetaData.Builder.create("ks", "table")
                                           .addPartitionKey("pk", Int32Type.instance)
                                           .addClusteringColumn("ck", Int32Type.instance)
                                           .addRegularColumn("a", UTF8Type.instance)
                                           .addRegularColumn("b", SetType.getInstance(UTF8Type.instance, true))
                                           .addRegularColumn("c", UTF8Type.instance)
                                           .build();

        // Creates a filter that _only_ fetches a and c, but not b.
        ColumnFilter filter = ColumnFilter.selectionBuilder()
                                          .add(cfm.getColumnDefinition(bytes("a")))
                                          .add(cfm.getColumnDefinition(bytes("c")))
                                          .build();
        SerializationHelper helper = new SerializationHelper(cfm,
                                                             MessagingService.VERSION_22,
                                                             SerializationHelper.Flag.LOCAL,
                                                             filter);
        CellGrouper grouper = new CellGrouper(cfm, helper);
        Clustering clustering = new BufferClustering(bytes(1));

        // We add a cell for a, then a collection tombstone for b, and then a cell for c (for the same clustering).
        // All those additions should return 'true' as all belong to the same row.
        LegacyCell ca = cell(clustering, cfm.getColumnDefinition(bytes("a")), bytes("v1"), 1);
        assertTrue(grouper.addAtom(ca));

        ClusteringBound startBound = ClusteringBound.inclusiveStartOf(bytes(1));
        ClusteringBound endBound = ClusteringBound.inclusiveEndOf(bytes(1));
        ColumnDefinition bDef = cfm.getColumnDefinition(bytes("b"));
        assert bDef != null;
        LegacyBound start = new LegacyBound(startBound, false, bDef);
        LegacyBound end = new LegacyBound(endBound, false, bDef);
        LegacyRangeTombstone rtb = new LegacyRangeTombstone(start, end, new DeletionTime(1, 1588598040));
        assertTrue(rtb.isCollectionTombstone()); // Ensure we're testing what we think
        assertTrue(grouper.addAtom(rtb));

        LegacyCell cc = cell(clustering, cfm.getColumnDefinition(bytes("c")), bytes("v2"), 1);
        assertTrue(grouper.addAtom(cc));
    }

    @Test
    public void testSubColumnCellNameSparseTable() throws UnknownColumnException
    {
        // Sparse supercolumn table with statically defined subcolumn from column_metadata, "static_subcolumn".
        CFMetaData cfm = CFMetaData.Builder.create("ks", "table", false, true, true, false)
                                           .addPartitionKey("key", Int32Type.instance)
                                           .addClusteringColumn("column1", Int32Type.instance)
                                           .addRegularColumn("", MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true))
                                           .addRegularColumn("static_subcolumn", Int32Type.instance)
                                           .build();

        assertDecodedCellNameEquals(cfm, bytes("key"), cfm.compactValueColumn(), bytes("key"));
        assertDecodedCellNameEquals(cfm, bytes("column1"), cfm.compactValueColumn(), bytes("column1"));
        assertDecodedCellNameEquals(cfm, bytes(""), cfm.compactValueColumn(), bytes(""));

        assertDecodedCellNameEquals(cfm, bytes("static_subcolumn"), cfm.getColumnDefinition(bytes("static_subcolumn")), null);
        assertDecodedCellNameEquals(cfm, bytes("regular_cellname"), cfm.compactValueColumn(), bytes("regular_cellname"));
    }

    @Test
    public void testSubColumnCellNameDenseTable() throws UnknownColumnException
    {
        // Dense supercolumn table, with no statically defined subcolumns.
        CFMetaData cfm = CFMetaData.Builder.createSuper("ks", "table", false)
                                           .addPartitionKey("key", Int32Type.instance)
                                           .addClusteringColumn("column1", Int32Type.instance)
                                           .addRegularColumn("", MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true))
                                           .build();

        assertDecodedCellNameEquals(cfm, bytes("key"), cfm.compactValueColumn(), bytes("key"));
        assertDecodedCellNameEquals(cfm, bytes("column1"), cfm.compactValueColumn(), bytes("column1"));
        assertDecodedCellNameEquals(cfm, bytes(""), cfm.compactValueColumn(), bytes(""));
        assertDecodedCellNameEquals(cfm, bytes("column2"), cfm.compactValueColumn(), bytes("column2"));
        assertDecodedCellNameEquals(cfm, bytes("value"), cfm.compactValueColumn(), bytes("value"));
    }

    private void assertDecodedCellNameEquals(CFMetaData cfm,
                                             ByteBuffer subColumn,
                                             ColumnDefinition columnDefinition,
                                             ByteBuffer collectionElement)
    throws UnknownColumnException
    {
        ByteBuffer cellNameBuffer = CompositeType.build(bytes(1), subColumn);
        LegacyLayout.LegacyCellName decodedCellName = LegacyLayout.decodeCellName(cfm, cellNameBuffer, false);
        assertArrayEquals(new ByteBuffer[]{bytes(1)}, decodedCellName.clustering.getRawValues());
        assertEquals(columnDefinition, decodedCellName.column);
        assertEquals(collectionElement, decodedCellName.collectionElement);
    }
}
