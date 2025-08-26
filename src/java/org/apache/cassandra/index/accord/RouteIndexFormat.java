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
import java.io.UncheckedIOException;
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

import javax.annotation.Nullable;

import com.google.common.collect.Maps;

import accord.local.MaxDecidedRX;
import accord.local.StoreParticipants;
import accord.primitives.Participants;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.accord.IndexDescriptor.IndexComponent;
import org.apache.cassandra.io.AsymmetricVersionedSerializer;
import org.apache.cassandra.io.EmbeddedAsymmetricVersionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.util.ChecksumedRandomAccessReader;
import org.apache.cassandra.io.util.ChecksumedSequentialWriter;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.service.accord.AccordJournal;
import org.apache.cassandra.service.accord.AccordJournalTable;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.utils.Clock.Global.nowInSeconds;

// A route index consists of a few files: cintia_sorted_list, cintia_checkpoints, and
// metadata stores the segement mappings and stats needed for search selection
public class RouteIndexFormat
{
    public static final Supplier<Checksum> CHECKSUM_SUPPLIER = CRC32C::new;

    static final EmbeddedAsymmetricVersionedSerializer<Participants<?>, Participants<?>, Version> touches = localSerializer(KeySerializers.participants);
    private static <T> EmbeddedAsymmetricVersionedSerializer<T, T, Version> localSerializer(UnversionedSerializer<T> serializer)
    {
        return new EmbeddedAsymmetricVersionedSerializer<>(Version.DOWNGRADE_SAFE_VERSION, Version.Serializer.instance, AsymmetricVersionedSerializer.from(serializer));
    }

    public static ByteBuffer serialize(Participants<?> value) throws IOException
    {
        return touches.serialize(value);
    }

    static Participants<?> deserializeTouches(ByteBuffer bytes) throws IOException
    {
        if (bytes == null || ByteBufferAccessor.instance.isEmpty(bytes))
            return null;

        return touches.deserialize(bytes);
    }

    public static boolean includeByDecidedRX(@Nullable MaxDecidedRX.DecidedRX decidedRX, TxnId txnId)
    {
        if (decidedRX == null || txnId.equals(TxnId.NONE) || !txnId.is(Txn.Kind.ExclusiveSyncPoint)) return true;
        return decidedRX.includeDecided(txnId);
    }

    public interface Writer extends SSTableFlushObserver
    {

    }

    public static class SSTableIndexWriter extends MemtableRouteIndexWriter
    {
        private final RouteJournalIndex index;
        private DecoratedKey current;
        private JournalKey journalKey;

        public SSTableIndexWriter(RouteJournalIndex index, IndexDescriptor id)
        {
            super(id, new MemtableIndex());
            this.index = index;
        }

        @Override
        public void startPartition(DecoratedKey key, long keyPosition, long keyPositionForSASI)
        {
            this.current = key;
            this.journalKey = AccordKeyspace.JournalColumns.getJournalKey(key);
        }

        @Override
        public void nextUnfilteredCluster(Unfiltered unfiltered)
        {
            // there is some duplication from org.apache.cassandra.index.accord.RouteMemtableIndexManager.index
            // should this be cleaned up?
            if (!unfiltered.isRow() || !RouteJournalIndex.allowed(journalKey))
                return;
            Row row = (Row) unfiltered;
            ByteBuffer value = extractParticipants(index, journalKey.id, row);
            indexer.index(current, row.clustering(), value);
        }
    }

    public static ByteBuffer extractParticipants(RouteJournalIndex index, TxnId txnId, Row row)
    {
        boolean recordNull = row.getCell(AccordKeyspace.JournalColumns.record) == null;
        boolean userVersionNull = row.getCell(AccordKeyspace.JournalColumns.user_version) == null;
        if (recordNull != userVersionNull)
            throw new IllegalStateException(String.format("Record is %s, but user_version is %s",
                                                          (recordNull ? "null" : "defined"),
                                                          (userVersionNull ? "null" : "defined")));
        if (recordNull)
            return null;
        Cell<?> recordCell = row.getCell(AccordKeyspace.JournalColumns.record);
        Cell<?> user_versionCell = row.getCell(AccordKeyspace.JournalColumns.user_version);
        long nowInSec = nowInSeconds();
        boolean recordLive = recordCell.isLive(nowInSec);
        boolean user_versionLive = user_versionCell.isLive(nowInSec);
        if (recordLive != user_versionLive)
            throw new IllegalStateException(String.format("Record is %s, but user_version is %s",
                                                          (recordLive ? "live" : "dead"),
                                                          (user_versionLive ? "live" : "dead")));
        if (!recordLive)
            return null;
        ByteBuffer record = recordCell.buffer();
        Version user_version = Version.fromVersion(Int32Type.instance.compose(user_versionCell.buffer()));
        AccordJournal.Builder builder = extract(txnId, record, user_version);
        StoreParticipants participants = builder.participants();
        if (participants == null)
            return null;

        Participants<?> touches = participants.touches();
        if (touches == null)
            return null;

        try
        {
            return serialize(touches);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    public static AccordJournal.Builder extract(TxnId txnId, ByteBuffer record, Version userVersion)
    {
        AccordJournal.Builder builder = new AccordJournal.Builder(txnId, AccordJournal.Load.ALL);
        AccordJournalTable.readBuffer(record, builder::deserializeNext, userVersion);
        return builder;
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

    static List<Segment> readSegments(Map<IndexComponent, FileHandle> index) throws IOException
    {
        List<Segment> segments = new ArrayList<>();

        try (ChecksumedRandomAccessReader metaReader = new ChecksumedRandomAccessReader(index.get(IndexComponent.METADATA).createReader(), CHECKSUM_SUPPLIER);
             ChecksumedRandomAccessReader segmentReader = new ChecksumedRandomAccessReader(index.get(IndexComponent.SEGMENT).createReader(), CHECKSUM_SUPPLIER))
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
                Map<Key, Segment.Metadata> groups = Maps.newHashMapWithExpectedSize(groupSize);
                for (int i = 0; i < groupSize; i++)
                {
                    int storeId = segmentReader.readVInt32();
                    TableId tableId = TableId.fromUUID(new UUID(segmentReader.readLong(), segmentReader.readLong()));
                    Key group = new Key(storeId, tableId);
                    int metaSize = segmentReader.readUnsignedVInt32();
                    EnumMap<IndexComponent, Segment.ComponentMetadata> metas = new EnumMap<>(IndexComponent.class);
                    for (int j = 0; j < metaSize; j++)
                    {
                        IndexComponent c = IndexComponent.fromByte(segmentReader.readByte());
                        metas.put(c, new Segment.ComponentMetadata(segmentReader.readUnsignedVInt(), segmentReader.readUnsignedVInt()));
                    }
                    byte[] minTerm = ByteArrayUtil.readWithVIntLength(segmentReader);
                    byte[] maxTerm = ByteArrayUtil.readWithVIntLength(segmentReader);
                    TxnId minTxnId = CommandSerializers.txnId.deserialize(segmentReader);
                    TxnId maxTxnId = CommandSerializers.txnId.deserialize(segmentReader);
                    TxnId maxRXId = CommandSerializers.txnId.deserialize(segmentReader);
                    Segment.Metadata existing = groups.put(group, new Segment.Metadata(metas, minTerm, maxTerm, minTxnId, maxTxnId, maxRXId));
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
        List<Key> groups = new ArrayList<>(segment.groups.keySet());
        groups.sort(Comparator.naturalOrder());

        try (ChecksumedSequentialWriter segmentWriter = ChecksumedSequentialWriter.open(id.fileFor(IndexComponent.SEGMENT), true, CHECKSUM_SUPPLIER);
             ChecksumedSequentialWriter metadataWriter = ChecksumedSequentialWriter.open(id.fileFor(IndexComponent.METADATA), true, CHECKSUM_SUPPLIER))
        {
            long startPointer = segmentWriter.getFilePointer();
            for (Key group : groups)
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

    private static void writeGroup(ChecksumedSequentialWriter seq, Key group, Segment.Metadata metadata) throws IOException
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
        CommandSerializers.txnId.serialize(metadata.minTxnId, seq);
        CommandSerializers.txnId.serialize(metadata.maxTxnId, seq);
        CommandSerializers.txnId.serialize(metadata.maxRxId, seq);
    }
}
