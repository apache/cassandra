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

import java.io.IOException;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.SearchIterator;

/**
 * Serialize/deserialize a single Unfiltered (both on-wire and on-disk).
 *
 * The encoded format for an unfiltered is <flags>(<row>|<marker>) where:
 *
 *   <flags> is a byte whose bits are flags used by the rest of the serialization. Each
 *       flag is defined/explained below as the "Unfiltered flags" constants.
 *   <row> is <clustering>[<timestamp>][<ttl>][<deletion>]<sc1>...<sci><cc1>...<ccj> where
 *       <clustering> is the row clustering as serialized by
 *       {@code Clustering.serializer}. Note that static row are an exception and
 *       don't have this. <timestamp>, <ttl> and <deletion> are the row timestamp, ttl and deletion
 *       whose presence is determined by the flags. <sci> is the simple columns of the row and <ccj> the
 *       complex ones.  There is actually 2 slightly different possible layout for those
 *       cell: a dense one and a sparse one. Which one is used depends on the serialization
 *       header and more precisely of {@link SerializationHeader#useSparseColumnLayout(boolean)}:
 *         1) in the dense layout, there will be as many <sci> and <ccj> as there is columns
 *            in the serialization header. *Each simple column <sci> will simply be a <cell>
 *            (which might have no value, see below), while each <ccj> will be
 *             [<delTime>]<cell1>...<celln><emptyCell> where <delTime> is the deletion for
 *             this complex column (if flags indicates it present), <celln> are the <cell>
 *             for this complex column and <emptyCell> is a last cell that will have no value
 *             to indicate the end of this column.
 *         2) in the sparse layout, there won't be "empty" cells, i.e. only the column that
 *            actually have a cell are represented. For that, each <sci> and <ccj> start
 *            by a 2 byte index that points to the column in the header it belongs to. After
 *            that, each <sci> and <ccj> is the same than for the dense layout. But contrarily
 *            to the dense layout we won't know how many elements are serialized so a 2 byte
 *            marker with a value of -1 will indicates the end of the row.
 *   <marker> is <bound><deletion> where <bound> is the marker bound as serialized
 *       by {@code Slice.Bound.serializer} and <deletion> is the marker deletion
 *       time.
 *
 *   <cell> A cell start with a 1 byte <flag>. Thre rightmost 1st bit indicates
 *       if there is actually a value for this cell. If this flag is unset,
 *       nothing more follows for the cell. The 2nd and third flag indicates if
 *       it's a deleted or expiring cell. The 4th flag indicates if the value
 *       is empty or not. The 5th and 6th indicates if the timestamp and ttl/
 *       localDeletionTime for the cell are the same than the row one (if that
 *       is the case, those are not repeated for the cell).Follows the <value>
 *       (unless it's marked empty in the flag) and a delta-encoded long <timestamp>
 *       (unless the flag tells to use the row level one).
 *       Then if it's a deleted or expiring cell a delta-encoded int <localDelTime>
 *       and if it's expiring a delta-encoded int <ttl> (unless it's an expiring cell
 *       and the ttl and localDeletionTime are indicated by the flags to be the same
 *       than the row ones, in which case none of those appears).
 */
public class UnfilteredSerializer
{
    public static final UnfilteredSerializer serializer = new UnfilteredSerializer();

    /*
     * Unfiltered flags constants.
     */
    private final static int END_OF_PARTITION     = 0x01; // Signal the end of the partition. Nothing follows a <flags> field with that flag.
    private final static int IS_MARKER            = 0x02; // Whether the encoded unfiltered is a marker or a row. All following markers applies only to rows.
    private final static int IS_STATIC            = 0x04; // Whether the encoded row is a static.
    private final static int HAS_TIMESTAMP        = 0x08; // Whether the encoded row has a timestamp (i.e. if row.partitionKeyLivenessInfo().hasTimestamp() == true).
    private final static int HAS_TTL              = 0x10; // Whether the encoded row has some expiration info (i.e. if row.partitionKeyLivenessInfo().hasTTL() == true).
    private final static int HAS_DELETION         = 0x20; // Whether the encoded row has some deletion info.
    private final static int HAS_COMPLEX_DELETION = 0x40; // Whether the encoded row has some complex deletion for at least one of its columns.

    public void serialize(Unfiltered unfiltered, SerializationHeader header, DataOutputPlus out, int version)
    throws IOException
    {
        if (unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
        {
            serialize((RangeTombstoneMarker) unfiltered, header, out, version);
        }
        else
        {
            serialize((Row) unfiltered, header, out, version);
        }
    }

    public void serialize(Row row, SerializationHeader header, DataOutputPlus out, int version)
    throws IOException
    {
        int flags = 0;
        boolean isStatic = row.isStatic();

        LivenessInfo pkLiveness = row.primaryKeyLivenessInfo();
        DeletionTime deletion = row.deletion();
        boolean hasComplexDeletion = row.hasComplexDeletion();

        if (isStatic)
            flags |= IS_STATIC;
        if (!pkLiveness.isEmpty())
            flags |= HAS_TIMESTAMP;
        if (pkLiveness.isExpiring())
            flags |= HAS_TTL;
        if (!deletion.isLive())
            flags |= HAS_DELETION;
        if (hasComplexDeletion)
            flags |= HAS_COMPLEX_DELETION;

        out.writeByte((byte)flags);
        if (!isStatic)
            Clustering.serializer.serialize(row.clustering(), out, version, header.clusteringTypes());

        if ((flags & HAS_TIMESTAMP) != 0)
            header.writeTimestamp(pkLiveness.timestamp(), out);
        if ((flags & HAS_TTL) != 0)
        {
            header.writeTTL(pkLiveness.ttl(), out);
            header.writeLocalDeletionTime(pkLiveness.localExpirationTime(), out);
        }
        if ((flags & HAS_DELETION) != 0)
            header.writeDeletionTime(deletion, out);

        Columns columns = header.columns(isStatic);
        int simpleCount = columns.simpleColumnCount();
        boolean useSparse = header.useSparseColumnLayout(isStatic);
        SearchIterator<ColumnDefinition, ColumnData> cells = row.searchIterator();

        for (int i = 0; i < simpleCount; i++)
            writeSimpleColumn(i, (Cell)cells.next(columns.getSimple(i)), pkLiveness, header, out, useSparse);

        for (int i = simpleCount; i < columns.columnCount(); i++)
            writeComplexColumn(i, (ComplexColumnData)cells.next(columns.getComplex(i - simpleCount)), hasComplexDeletion, pkLiveness, header, out, useSparse);

        if (useSparse)
            out.writeVInt(-1);
    }

    private void writeSimpleColumn(int idx, Cell cell, LivenessInfo rowLiveness, SerializationHeader header, DataOutputPlus out, boolean useSparse)
    throws IOException
    {
        if (useSparse)
        {
            if (cell == null)
                return;

            out.writeVInt(idx);
        }
        Cell.serializer.serialize(cell, out, rowLiveness, header);
    }

    private void writeComplexColumn(int idx, ComplexColumnData data, boolean hasComplexDeletion, LivenessInfo rowLiveness, SerializationHeader header, DataOutputPlus out, boolean useSparse)
    throws IOException
    {
        if (useSparse)
        {
            if (data == null)
                return;

            out.writeVInt(idx);
        }

        if (hasComplexDeletion)
            header.writeDeletionTime(data == null ? DeletionTime.LIVE : data.complexDeletion(), out);

        if (data != null)
        {
            for (Cell cell : data)
                Cell.serializer.serialize(cell, out, rowLiveness, header);
        }

        Cell.serializer.serialize(null, out, rowLiveness, header);
    }

    public void serialize(RangeTombstoneMarker marker, SerializationHeader header, DataOutputPlus out, int version)
    throws IOException
    {
        out.writeByte((byte)IS_MARKER);
        RangeTombstone.Bound.serializer.serialize(marker.clustering(), out, version, header.clusteringTypes());

        if (marker.isBoundary())
        {
            RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker)marker;
            header.writeDeletionTime(bm.endDeletionTime(), out);
            header.writeDeletionTime(bm.startDeletionTime(), out);
        }
        else
        {
            header.writeDeletionTime(((RangeTombstoneBoundMarker)marker).deletionTime(), out);
        }
    }

    public long serializedSize(Unfiltered unfiltered, SerializationHeader header, int version)
    {
        return unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER
             ? serializedSize((RangeTombstoneMarker) unfiltered, header, version)
             : serializedSize((Row) unfiltered, header, version);
    }

    public long serializedSize(Row row, SerializationHeader header, int version)
    {
        long size = 1; // flags

        boolean isStatic = row.isStatic();
        LivenessInfo pkLiveness = row.primaryKeyLivenessInfo();
        DeletionTime deletion = row.deletion();
        boolean hasComplexDeletion = row.hasComplexDeletion();

        if (!isStatic)
            size += Clustering.serializer.serializedSize(row.clustering(), version, header.clusteringTypes());

        if (!pkLiveness.isEmpty())
            size += header.timestampSerializedSize(pkLiveness.timestamp());
        if (pkLiveness.isExpiring())
        {
            size += header.ttlSerializedSize(pkLiveness.ttl());
            size += header.localDeletionTimeSerializedSize(pkLiveness.localExpirationTime());
        }
        if (!deletion.isLive())
            size += header.deletionTimeSerializedSize(deletion);

        Columns columns = header.columns(isStatic);
        int simpleCount = columns.simpleColumnCount();
        boolean useSparse = header.useSparseColumnLayout(isStatic);
        SearchIterator<ColumnDefinition, ColumnData> cells = row.searchIterator();

        for (int i = 0; i < simpleCount; i++)
            size += sizeOfSimpleColumn(i, (Cell)cells.next(columns.getSimple(i)), pkLiveness, header, useSparse);

        for (int i = simpleCount; i < columns.columnCount(); i++)
            size += sizeOfComplexColumn(i, (ComplexColumnData)cells.next(columns.getComplex(i - simpleCount)), hasComplexDeletion, pkLiveness, header, useSparse);

        if (useSparse)
            size += TypeSizes.sizeofVInt(-1);

        return size;
    }

    private long sizeOfSimpleColumn(int idx, Cell cell, LivenessInfo rowLiveness, SerializationHeader header, boolean useSparse)
    {
        long size = 0;
        if (useSparse)
        {
            if (cell == null)
                return size;

            size += TypeSizes.sizeofVInt(idx);
        }
        return size + Cell.serializer.serializedSize(cell, rowLiveness, header);
    }

    private long sizeOfComplexColumn(int idx, ComplexColumnData data, boolean hasComplexDeletion, LivenessInfo rowLiveness, SerializationHeader header, boolean useSparse)
    {
        long size = 0;
        if (useSparse)
        {
            if (data == null)
                return size;

            size += TypeSizes.sizeofVInt(idx);
        }

        if (hasComplexDeletion)
            size += header.deletionTimeSerializedSize(data == null ? DeletionTime.LIVE : data.complexDeletion());

        if (data != null)
        {
            for (Cell cell : data)
                size += Cell.serializer.serializedSize(cell, rowLiveness, header);
        }

        return size + Cell.serializer.serializedSize(null, rowLiveness, header);
    }

    public long serializedSize(RangeTombstoneMarker marker, SerializationHeader header, int version)
    {
        long size = 1 // flags
                  + RangeTombstone.Bound.serializer.serializedSize(marker.clustering(), version, header.clusteringTypes());

        if (marker.isBoundary())
        {
            RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker)marker;
            size += header.deletionTimeSerializedSize(bm.endDeletionTime());
            size += header.deletionTimeSerializedSize(bm.startDeletionTime());
        }
        else
        {
           size += header.deletionTimeSerializedSize(((RangeTombstoneBoundMarker)marker).deletionTime());
        }
        return size;
    }

    public void writeEndOfPartition(DataOutputPlus out) throws IOException
    {
        out.writeByte((byte)1);
    }

    public long serializedSizeEndOfPartition()
    {
        return 1;
    }

    public Unfiltered deserialize(DataInputPlus in, SerializationHeader header, SerializationHelper helper, Row.Builder builder)
    throws IOException
    {
        // It wouldn't be wrong per-se to use an unsorted builder, but it would be inefficient so make sure we don't do it by mistake
        assert builder.isSorted();

        int flags = in.readUnsignedByte();
        if (isEndOfPartition(flags))
            return null;

        if (kind(flags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
        {
            RangeTombstone.Bound bound = RangeTombstone.Bound.serializer.deserialize(in, helper.version, header.clusteringTypes());
            return deserializeMarkerBody(in, header, bound);
        }
        else
        {
            assert !isStatic(flags); // deserializeStaticRow should be used for that.
            builder.newRow(Clustering.serializer.deserialize(in, helper.version, header.clusteringTypes()));
            return deserializeRowBody(in, header, helper, flags, builder);
        }
    }

    public Row deserializeStaticRow(DataInputPlus in, SerializationHeader header, SerializationHelper helper)
    throws IOException
    {
        int flags = in.readUnsignedByte();
        assert !isEndOfPartition(flags) && kind(flags) == Unfiltered.Kind.ROW && isStatic(flags) : flags;
        Row.Builder builder = BTreeRow.sortedBuilder(helper.fetchedStaticColumns(header));
        builder.newRow(Clustering.STATIC_CLUSTERING);
        return deserializeRowBody(in, header, helper, flags, builder);
    }

    public RangeTombstoneMarker deserializeMarkerBody(DataInputPlus in, SerializationHeader header, RangeTombstone.Bound bound)
    throws IOException
    {
        if (bound.isBoundary())
            return new RangeTombstoneBoundaryMarker(bound, header.readDeletionTime(in), header.readDeletionTime(in));
        else
            return new RangeTombstoneBoundMarker(bound, header.readDeletionTime(in));
    }

    public Row deserializeRowBody(DataInputPlus in,
                                  SerializationHeader header,
                                  SerializationHelper helper,
                                  int flags,
                                  Row.Builder builder)
    throws IOException
    {
        try
        {
            boolean isStatic = isStatic(flags);
            boolean hasTimestamp = (flags & HAS_TIMESTAMP) != 0;
            boolean hasTTL = (flags & HAS_TTL) != 0;
            boolean hasDeletion = (flags & HAS_DELETION) != 0;
            boolean hasComplexDeletion = (flags & HAS_COMPLEX_DELETION) != 0;

            LivenessInfo rowLiveness = LivenessInfo.EMPTY;
            if (hasTimestamp)
            {
                long timestamp = header.readTimestamp(in);
                int ttl = hasTTL ? header.readTTL(in) : LivenessInfo.NO_TTL;
                int localDeletionTime = hasTTL ? header.readLocalDeletionTime(in) : LivenessInfo.NO_EXPIRATION_TIME;
                rowLiveness = LivenessInfo.create(timestamp, ttl, localDeletionTime);
            }

            builder.addPrimaryKeyLivenessInfo(rowLiveness);
            builder.addRowDeletion(hasDeletion ? header.readDeletionTime(in) : DeletionTime.LIVE);

            Columns columns = header.columns(isStatic);
            if (header.useSparseColumnLayout(isStatic))
            {
                int count = columns.columnCount();
                int simpleCount = columns.simpleColumnCount();
                int i;
                while ((i = (int)in.readVInt()) >= 0)
                {
                    if (i > count)
                        throw new IOException(String.format("Impossible column index %d, the header has only %d columns defined", i, count));

                    if (i < simpleCount)
                        readSimpleColumn(columns.getSimple(i), in, header, helper, builder, rowLiveness);
                    else
                        readComplexColumn(columns.getComplex(i - simpleCount), in, header, helper, hasComplexDeletion, builder, rowLiveness);
                }
            }
            else
            {
                for (int i = 0; i < columns.simpleColumnCount(); i++)
                    readSimpleColumn(columns.getSimple(i), in, header, helper, builder, rowLiveness);

                for (int i = 0; i < columns.complexColumnCount(); i++)
                    readComplexColumn(columns.getComplex(i), in, header, helper, hasComplexDeletion, builder, rowLiveness);
            }

                return builder.build();
        }
        catch (RuntimeException | AssertionError e)
        {
            // Corrupted data could be such that it triggers an assertion in the row Builder, or break one of its assumption.
            // Of course, a bug in said builder could also trigger this, but it's impossible a priori to always make the distinction
            // between a real bug and data corrupted in just the bad way. Besides, re-throwing as an IOException doesn't hide the
            // exception, it just make we catch it properly and mark the sstable as corrupted.
            throw new IOException("Error building row with data deserialized from " + in, e);
        }
    }

    private void readSimpleColumn(ColumnDefinition column, DataInputPlus in, SerializationHeader header, SerializationHelper helper, Row.Builder builder, LivenessInfo rowLiveness)
    throws IOException
    {
        if (helper.includes(column))
        {
            Cell cell = Cell.serializer.deserialize(in, rowLiveness, column, header, helper);
            if (cell != null && !helper.isDropped(cell, false))
                builder.addCell(cell);
        }
        else
        {
            Cell.serializer.skip(in, column, header);
        }
    }

    private void readComplexColumn(ColumnDefinition column, DataInputPlus in, SerializationHeader header, SerializationHelper helper, boolean hasComplexDeletion, Row.Builder builder, LivenessInfo rowLiveness)
    throws IOException
    {
        if (helper.includes(column))
        {
            helper.startOfComplexColumn(column);
            if (hasComplexDeletion)
            {
                DeletionTime complexDeletion = header.readDeletionTime(in);
                if (!helper.isDroppedComplexDeletion(complexDeletion))
                    builder.addComplexDeletion(column, complexDeletion);
            }

            Cell cell;
            while ((cell = Cell.serializer.deserialize(in, rowLiveness, column, header, helper)) != null)
            {
                if (helper.includes(cell.path()) && !helper.isDropped(cell, true))
                    builder.addCell(cell);
            }

            helper.endOfComplexColumn();
        }
        else
        {
            skipComplexColumn(in, column, header, hasComplexDeletion);
        }
    }

    public void skipRowBody(DataInputPlus in, SerializationHeader header, int flags) throws IOException
    {
        boolean isStatic = isStatic(flags);
        boolean hasTimestamp = (flags & HAS_TIMESTAMP) != 0;
        boolean hasTTL = (flags & HAS_TTL) != 0;
        boolean hasDeletion = (flags & HAS_DELETION) != 0;
        boolean hasComplexDeletion = (flags & HAS_COMPLEX_DELETION) != 0;

        // Note that we don't want want to use FileUtils.skipBytesFully for anything that may not have
        // the size we think due to VINT encoding
        if (hasTimestamp)
            header.skipTimestamp(in);
        if (hasTTL)
        {
            header.skipLocalDeletionTime(in);
            header.skipTTL(in);
        }
        if (hasDeletion)
            header.skipDeletionTime(in);

        Columns columns = header.columns(isStatic);
        if (header.useSparseColumnLayout(isStatic))
        {
            int count = columns.columnCount();
            int simpleCount = columns.simpleColumnCount();
            int i;
            while ((i = (int)in.readVInt()) >= 0)
            {
                if (i > count)
                    throw new IOException(String.format("Impossible column index %d, the header has only %d columns defined", i, count));

                if (i < simpleCount)
                    Cell.serializer.skip(in, columns.getSimple(i), header);
                else
                    skipComplexColumn(in, columns.getComplex(i - simpleCount), header, hasComplexDeletion);
            }
        }
        else
        {
            for (int i = 0; i < columns.simpleColumnCount(); i++)
                Cell.serializer.skip(in, columns.getSimple(i), header);

            for (int i = 0; i < columns.complexColumnCount(); i++)
                skipComplexColumn(in, columns.getComplex(i), header, hasComplexDeletion);
        }
    }

    public void skipStaticRow(DataInputPlus in, SerializationHeader header, SerializationHelper helper) throws IOException
    {
        int flags = in.readUnsignedByte();
        assert !isEndOfPartition(flags) && kind(flags) == Unfiltered.Kind.ROW && isStatic(flags) : "Flags is " + flags;
        skipRowBody(in, header, flags);
    }

    public void skipMarkerBody(DataInputPlus in, SerializationHeader header, boolean isBoundary) throws IOException
    {
        if (isBoundary)
        {
            header.skipDeletionTime(in);
            header.skipDeletionTime(in);
        }
        else
        {
            header.skipDeletionTime(in);
        }
    }

    private void skipComplexColumn(DataInputPlus in, ColumnDefinition column, SerializationHeader header, boolean hasComplexDeletion)
    throws IOException
    {
        if (hasComplexDeletion)
            header.skipDeletionTime(in);

        while (Cell.serializer.skip(in, column, header));
    }

    public static boolean isEndOfPartition(int flags)
    {
        return (flags & END_OF_PARTITION) != 0;
    }

    public static Unfiltered.Kind kind(int flags)
    {
        return (flags & IS_MARKER) != 0 ? Unfiltered.Kind.RANGE_TOMBSTONE_MARKER : Unfiltered.Kind.ROW;
    }

    public static boolean isStatic(int flags)
    {
        return (flags & IS_MARKER) == 0 && (flags & IS_STATIC) != 0;
    }
}
