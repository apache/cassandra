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

package org.apache.cassandra.index.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import com.google.common.collect.Maps;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.accord.IndexDescriptor.IndexComponent;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.util.ChecksumedRandomAccessReader;
import org.apache.cassandra.io.util.ChecksumedSequentialWriter;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.utils.Clock.Global.nowInSeconds;

// A route index consists of a few files: cintia_sorted_list, cintia_checkpoints, and metadata
// metadata stores the segement mappings and stats needed for search selection
public class RouteIndexFormat
{
    public static final Supplier<Checksum> CHECKSUM_SUPPLIER = CRC32C::new;

    public interface Writer extends SSTableFlushObserver
    {

    }

    public static class SSTableIndexWriter extends MemtableRouteIndexWriter
    {
        private final RouteIndex index;
        private DecoratedKey current;

        public SSTableIndexWriter(RouteIndex index, IndexDescriptor id)
        {
            super(id, new MemtableIndex());
            this.index = index;
        }

        @Override
        public void startPartition(DecoratedKey key, long keyPosition, long keyPositionForSASI)
        {
            this.current = key;
        }

        @Override
        public void nextUnfilteredCluster(Unfiltered unfiltered)
        {
            // there is some duplication from org.apache.cassandra.index.accord.RouteMemtableIndexManager.index
            // should this be cleaned up?
            if (!unfiltered.isRow())
                return;
            Row row = (Row) unfiltered;
            // simplified version of org.apache.cassandra.index.sai.utils.IndexTermType.valueOf
            Cell<?> cell = row.getCell(index.column());
            ByteBuffer value = cell == null || !cell.isLive(nowInSeconds()) ? null : cell.buffer();
            indexer.index(current, row.clustering(), value);
        }
    }

    public static class MemtableRouteIndexWriter implements Writer
    {
        private final IndexDescriptor id;
        protected final MemtableIndex indexer;

        public MemtableRouteIndexWriter(IndexDescriptor id, MemtableIndex indexer)
        {
            this.id = id;
            this.indexer = indexer;
        }


        @Override
        public void begin()
        {
            // no-op
        }

        @Override
        public void startPartition(DecoratedKey key, long keyPosition, long keyPositionForSASI)
        {
            // no-op
        }

        @Override
        public void staticRow(Row staticRow)
        {
            // no-op
        }

        @Override
        public void nextUnfilteredCluster(Unfiltered unfiltered)
        {
            // no-op
        }

        @Override
        public void complete()
        {
            try
            {
                if (!indexer.isEmpty())
                {
                    Segment segment = indexer.write(id);
                    appendSegment(id, segment);
                }
                else
                {
                    // nothing to see here... need to still mark the SSTable as indexed, so need an empty segment
                    appendSegment(id, Segment.EMPTY);
                }
            }
            catch (IOException e)
            {
                abort(e);
                throw Throwables.unchecked(e);
            }
        }

        @Override
        public void abort(Throwable accumulator)
        {
            id.deleteIndex();
        }

        public void abort(Throwable accumulator, boolean fromIndex)
        {
            abort(accumulator);
            // If the abort was from an index error, propagate the error upstream so index builds, compactions, and
            // flushes can handle it correctly.
            if (fromIndex)
                throw Throwables.unchecked(accumulator);
        }
    }

    static List<Segment> readSegements(Map<IndexComponent, FileHandle> index) throws IOException
    {
        List<Segment> segments = new ArrayList<>();

        try (var metaReader = new ChecksumedRandomAccessReader(index.get(IndexComponent.METADATA).createReader(), CHECKSUM_SUPPLIER);
             var segmentReader = new ChecksumedRandomAccessReader(index.get(IndexComponent.SEGMENT).createReader(), CHECKSUM_SUPPLIER))
        {
            while (metaReader.getFilePointer() < metaReader.length())
            {
                metaReader.resetChecksum();
                long startPointer = metaReader.readUnsignedVInt();
                long endPointer = metaReader.readUnsignedVInt();
                int groupSize = metaReader.readUnsignedVInt32();
                int segmentChecksum = metaReader.readInt();
                int metadataChecksum = metaReader.getValue32AndResetChecksum();
                int actualChecksum = metaReader.readInt();
                assert actualChecksum == metadataChecksum;

                segmentReader.resetChecksum();
                segmentReader.seek(startPointer);
                Map<Group, Segment.Metadata> groups = Maps.newHashMapWithExpectedSize(groupSize);
                for (int i = 0; i < groupSize; i++)
                {
                    int storeId = segmentReader.readVInt32();
                    TableId tableId = TableId.fromUUID(new UUID(segmentReader.readLong(), segmentReader.readLong()));
                    Group group = new Group(storeId, tableId);
                    int metaSize = segmentReader.readUnsignedVInt32();
                    EnumMap<IndexComponent, Segment.ComponentMetadata> metas = new EnumMap<>(IndexComponent.class);
                    for (int j = 0; j < metaSize; j++)
                    {
                        IndexComponent c = IndexComponent.fromByte(segmentReader.readByte());
                        metas.put(c, new Segment.ComponentMetadata(segmentReader.readUnsignedVInt(), segmentReader.readUnsignedVInt()));
                    }
                    byte[] minTerm = ByteArrayUtil.readWithVIntLength(segmentReader);
                    byte[] maxTerm = ByteArrayUtil.readWithVIntLength(segmentReader);
                    Segment.Metadata existing = groups.put(group, new Segment.Metadata(metas, minTerm, maxTerm));
                    assert existing == null;
                }
                int actualSegmentChecksum = segmentReader.getValue32AndResetChecksum();
                assert actualSegmentChecksum == segmentChecksum;
                assert segmentReader.getFilePointer() == endPointer;
                segments.add(new Segment(groups));
            }
        }
        return segments;
    }

    static void appendSegment(IndexDescriptor id, Segment segment) throws IOException
    {
        List<Group> groups = new ArrayList<>(segment.groups.keySet());
        groups.sort(Comparator.naturalOrder());

        try (var segmentWriter = ChecksumedSequentialWriter.open(id.fileFor(IndexComponent.SEGMENT), true, CHECKSUM_SUPPLIER);
             var metadataWriter = ChecksumedSequentialWriter.open(id.fileFor(IndexComponent.METADATA), true, CHECKSUM_SUPPLIER))
        {
            long startPointer = segmentWriter.getFilePointer();
            for (Group group : groups)
            {
                Segment.Metadata metadata = segment.groups.get(group);
                writeGroup(segmentWriter, group, metadata);
            }
            long endPointer = segmentWriter.getFilePointer();

            int checksum = segmentWriter.getValue32AndResetChecksum();
            metadataWriter.writeUnsignedVInt(startPointer);
            metadataWriter.writeUnsignedVInt(endPointer);
            metadataWriter.writeUnsignedVInt32(segment.groups.size());
            metadataWriter.writeInt(checksum);
            metadataWriter.writeInt(metadataWriter.getValue32AndResetChecksum());
        }
    }

    private static void writeGroup(ChecksumedSequentialWriter seq, Group group, Segment.Metadata metadata) throws IOException
    {
        seq.writeVInt32(group.storeId);
        seq.write(UUIDSerializer.instance.serialize(group.tableId.asUUID()));
        seq.writeUnsignedVInt32(metadata.metas.size());
        for (Map.Entry<IndexComponent, Segment.ComponentMetadata> e : metadata.metas.entrySet())
        {
            seq.writeByte(e.getKey().value);
            seq.writeUnsignedVInt(e.getValue().offset);
            seq.writeUnsignedVInt(e.getValue().endOffset);
        }
        ByteArrayUtil.writeWithVIntLength(metadata.minTerm, seq);
        ByteArrayUtil.writeWithVIntLength(metadata.maxTerm, seq);
    }
}
