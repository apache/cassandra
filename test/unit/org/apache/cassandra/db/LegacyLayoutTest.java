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
import org.apache.cassandra.db.filter.ColumnFilter;
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
import org.apache.cassandra.utils.ByteBufferUtil;

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

        ByteBuffer key = ByteBufferUtil.bytes(1);
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
            LegacyLayout.serializeAsLegacyPartition(null, partition, out, MessagingService.VERSION_21);
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                return LegacyLayout.deserializeLegacyPartition(in, MessagingService.VERSION_21, SerializationHelper.Flag.LOCAL, partition.partitionKey().getKey());
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

        DecoratedKey pk = table.decorateKey(ByteBufferUtil.bytes(1));
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

        DecoratedKey pk = table.decorateKey(ByteBufferUtil.bytes(1));
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

        DecoratedKey pk = table.decorateKey(ByteBufferUtil.bytes(1));
        PartitionUpdate upd = PartitionUpdate.singleRowUpdate(table, pk, row);

        // we need to perform the round trip in two parts here, with a column drop inbetween
        try (RowIterator before = FilteredRows.filter(upd.unfilteredIterator(), FBUtilities.nowInSeconds());
             DataOutputBuffer serialized21 = new DataOutputBuffer())
        {
            LegacyLayout.serializeAsLegacyPartition(null, upd.unfilteredIterator(), serialized21, MessagingService.VERSION_21);
            QueryProcessor.executeInternal(String.format("ALTER TABLE \"%s\".legacy_rt_rt_dc DROP s", KEYSPACE));
            try (DataInputBuffer in = new DataInputBuffer(serialized21.buffer(), false))
            {
                try (UnfilteredRowIterator deser21 = LegacyLayout.deserializeLegacyPartition(in, MessagingService.VERSION_21, SerializationHelper.Flag.LOCAL, upd.partitionKey().getKey());
                    RowIterator after = FilteredRows.filter(deser21, FBUtilities.nowInSeconds());)
                {
                    while (before.hasNext() || after.hasNext())
                        assertEquals(before.hasNext() ? before.next() : null, after.hasNext() ? after.next() : null);
                }
            }

        }
    }

}