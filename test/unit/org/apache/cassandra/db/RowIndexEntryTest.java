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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.Assert;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class RowIndexEntryTest extends CQLTester
{
    private static final List<AbstractType<?>> clusterTypes = Collections.<AbstractType<?>>singletonList(LongType.instance);
    private static final ClusteringComparator comp = new ClusteringComparator(clusterTypes);
    private static ClusteringPrefix cn(long l)
    {
        return Util.clustering(comp, l);
    }

    @Test
    public void testArtificialIndexOf() throws IOException
    {
        CFMetaData cfMeta = CFMetaData.compile("CREATE TABLE pipe.dev_null (pk bigint, ck bigint, val text, PRIMARY KEY(pk, ck))", "foo");

        DeletionTime deletionInfo = new DeletionTime(FBUtilities.timestampMicros(), FBUtilities.nowInSeconds());

        SerializationHeader header = new SerializationHeader(true, cfMeta, cfMeta.partitionColumns(), EncodingStats.NO_STATS);
        IndexHelper.IndexInfo.Serializer indexSerializer = new IndexHelper.IndexInfo.Serializer(cfMeta, BigFormat.latestVersion, header);

        DataOutputBuffer dob = new DataOutputBuffer();
        dob.writeUnsignedVInt(0);
        DeletionTime.serializer.serialize(DeletionTime.LIVE, dob);
        dob.writeUnsignedVInt(3);
        int off0 = dob.getLength();
        indexSerializer.serialize(new IndexHelper.IndexInfo(cn(0L), cn(5L), 0, 0, deletionInfo), dob);
        int off1 = dob.getLength();
        indexSerializer.serialize(new IndexHelper.IndexInfo(cn(10L), cn(15L), 0, 0, deletionInfo), dob);
        int off2 = dob.getLength();
        indexSerializer.serialize(new IndexHelper.IndexInfo(cn(20L), cn(25L), 0, 0, deletionInfo), dob);
        dob.writeInt(off0);
        dob.writeInt(off1);
        dob.writeInt(off2);

        @SuppressWarnings("resource") DataOutputBuffer dobRie = new DataOutputBuffer();
        dobRie.writeUnsignedVInt(42L);
        dobRie.writeUnsignedVInt(dob.getLength());
        dobRie.write(dob.buffer());

        ByteBuffer buf = dobRie.buffer();

        RowIndexEntry<IndexHelper.IndexInfo> rie = new RowIndexEntry.Serializer(cfMeta, BigFormat.latestVersion, header).deserialize(new DataInputBuffer(buf, false));

        Assert.assertEquals(42L, rie.position);

        Assert.assertEquals(0, IndexHelper.indexFor(cn(-1L), rie.columnsIndex(), comp, false, -1));
        Assert.assertEquals(0, IndexHelper.indexFor(cn(5L), rie.columnsIndex(), comp, false, -1));
        Assert.assertEquals(1, IndexHelper.indexFor(cn(12L), rie.columnsIndex(), comp, false, -1));
        Assert.assertEquals(2, IndexHelper.indexFor(cn(17L), rie.columnsIndex(), comp, false, -1));
        Assert.assertEquals(3, IndexHelper.indexFor(cn(100L), rie.columnsIndex(), comp, false, -1));
        Assert.assertEquals(3, IndexHelper.indexFor(cn(100L), rie.columnsIndex(), comp, false, 0));
        Assert.assertEquals(3, IndexHelper.indexFor(cn(100L), rie.columnsIndex(), comp, false, 1));
        Assert.assertEquals(3, IndexHelper.indexFor(cn(100L), rie.columnsIndex(), comp, false, 2));
        Assert.assertEquals(3, IndexHelper.indexFor(cn(100L), rie.columnsIndex(), comp, false, 3));

        Assert.assertEquals(-1, IndexHelper.indexFor(cn(-1L), rie.columnsIndex(), comp, true, -1));
        Assert.assertEquals(0, IndexHelper.indexFor(cn(5L), rie.columnsIndex(), comp, true, 3));
        Assert.assertEquals(0, IndexHelper.indexFor(cn(5L), rie.columnsIndex(), comp, true, 2));
        Assert.assertEquals(1, IndexHelper.indexFor(cn(17L), rie.columnsIndex(), comp, true, 3));
        Assert.assertEquals(2, IndexHelper.indexFor(cn(100L), rie.columnsIndex(), comp, true, 3));
        Assert.assertEquals(2, IndexHelper.indexFor(cn(100L), rie.columnsIndex(), comp, true, 4));
        Assert.assertEquals(1, IndexHelper.indexFor(cn(12L), rie.columnsIndex(), comp, true, 3));
        Assert.assertEquals(1, IndexHelper.indexFor(cn(12L), rie.columnsIndex(), comp, true, 2));
        Assert.assertEquals(1, IndexHelper.indexFor(cn(100L), rie.columnsIndex(), comp, true, 1));
        Assert.assertEquals(2, IndexHelper.indexFor(cn(100L), rie.columnsIndex(), comp, true, 2));
    }

    @Test
    public void testSerializedSize() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b text, c int, PRIMARY KEY(a, b))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);

        final RowIndexEntry simple = new RowIndexEntry(123);

        DataOutputBuffer buffer = new DataOutputBuffer();
        SerializationHeader header = new SerializationHeader(true, cfs.metadata, cfs.metadata.partitionColumns(), EncodingStats.NO_STATS);
        RowIndexEntry.Serializer serializer = new RowIndexEntry.Serializer(cfs.metadata, BigFormat.latestVersion, header);

        serializer.serialize(simple, buffer);

        assertEquals(buffer.getLength(), serializer.serializedSize(simple));

        // write enough rows to ensure we get a few column index entries
        for (int i = 0; i <= DatabaseDescriptor.getColumnIndexSize() / 4; i++)
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, "" + i, i);

        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs).build());

        File tempFile = File.createTempFile("row_index_entry_test", null);
        tempFile.deleteOnExit();
        SequentialWriter writer = SequentialWriter.open(tempFile);
        ColumnIndex columnIndex = ColumnIndex.writeAndBuildIndex(partition.unfilteredIterator(), writer, header, Collections.emptySet(), BigFormat.latestVersion);
        RowIndexEntry<IndexHelper.IndexInfo> withIndex = RowIndexEntry.create(0xdeadbeef, DeletionTime.LIVE, columnIndex);
        IndexHelper.IndexInfo.Serializer indexSerializer = new IndexHelper.IndexInfo.Serializer(cfs.metadata, BigFormat.latestVersion, header);

        // sanity check
        assertTrue(columnIndex.columnsIndex.size() >= 3);

        buffer = new DataOutputBuffer();
        serializer.serialize(withIndex, buffer);
        assertEquals(buffer.getLength(), serializer.serializedSize(withIndex));

        // serialization check

        ByteBuffer bb = buffer.buffer();
        DataInputBuffer input = new DataInputBuffer(bb, false);
        serializationCheck(withIndex, indexSerializer, bb, input);

        // test with an output stream that doesn't support a file-pointer
        buffer = new DataOutputBuffer()
        {
            public boolean hasPosition()
            {
                return false;
            }

            public long position()
            {
                throw new UnsupportedOperationException();
            }
        };
        serializer.serialize(withIndex, buffer);
        bb = buffer.buffer();
        input = new DataInputBuffer(bb, false);
        serializationCheck(withIndex, indexSerializer, bb, input);

        //

        bb = buffer.buffer();
        input = new DataInputBuffer(bb, false);
        RowIndexEntry.Serializer.skip(input, BigFormat.latestVersion);
        Assert.assertEquals(0, bb.remaining());
    }

    private void serializationCheck(RowIndexEntry<IndexHelper.IndexInfo> withIndex, IndexHelper.IndexInfo.Serializer indexSerializer, ByteBuffer bb, DataInputBuffer input) throws IOException
    {
        Assert.assertEquals(0xdeadbeef, input.readUnsignedVInt());
        Assert.assertEquals(withIndex.promotedSize(indexSerializer), input.readUnsignedVInt());

        Assert.assertEquals(withIndex.headerLength(), input.readUnsignedVInt());
        Assert.assertEquals(withIndex.deletionTime(), DeletionTime.serializer.deserialize(input));
        Assert.assertEquals(withIndex.columnsIndex().size(), input.readUnsignedVInt());

        int offset = bb.position();
        int[] offsets = new int[withIndex.columnsIndex().size()];
        for (int i = 0; i < withIndex.columnsIndex().size(); i++)
        {
            int pos = bb.position();
            offsets[i] = pos - offset;
            IndexHelper.IndexInfo info = indexSerializer.deserialize(input);
            int end = bb.position();

            Assert.assertEquals(indexSerializer.serializedSize(info), end - pos);

            Assert.assertEquals(withIndex.columnsIndex().get(i).offset, info.offset);
            Assert.assertEquals(withIndex.columnsIndex().get(i).width, info.width);
            Assert.assertEquals(withIndex.columnsIndex().get(i).endOpenMarker, info.endOpenMarker);
            Assert.assertEquals(withIndex.columnsIndex().get(i).firstName, info.firstName);
            Assert.assertEquals(withIndex.columnsIndex().get(i).lastName, info.lastName);
        }

        for (int i = 0; i < withIndex.columnsIndex().size(); i++)
            Assert.assertEquals(offsets[i], input.readInt());

        Assert.assertEquals(0, bb.remaining());
    }
}
