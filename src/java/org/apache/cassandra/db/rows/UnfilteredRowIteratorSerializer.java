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

import java.io.DataInput;
import java.io.IOException;
import java.io.IOError;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Serialize/Deserialize an unfiltered row iterator.
 *
 * The serialization is composed of a header, follows by the rows and range tombstones of the iterator serialized
 * until we read the end of the partition (see UnfilteredSerializer for details). The header itself
 * is:
 *     <cfid><key><flags><s_header>[<partition_deletion>][<static_row>]
 * where:
 *     <cfid> is the table cfid.
 *     <key> is the partition key.
 *     <flags> contains bit flags. Each flag is set if it's corresponding bit is set. From rightmost
 *         bit to leftmost one, the flags are:
 *         - is empty: whether the iterator is empty. If so, nothing follows the <flags>
 *         - is reversed: whether the iterator is in reversed clustering order
 *         - has partition deletion: whether or not there is a <partition_deletion> following
 *         - has static row: whether or not there is a <static_row> following
 *         - has row estimate: whether or not there is a <row_estimate> following
 *     <s_header> is the SerializationHeader. More precisely it's
 *           <min_timetamp><min_localDelTime><min_ttl>[<static_columns>]<columns>
 *         where:
 *           - <min_timestamp> is the base timestamp used for delta-encoding timestamps
 *           - <min_localDelTime> is the base localDeletionTime used for delta-encoding local deletion times
 *           - <min_ttl> is the base localDeletionTime used for delta-encoding ttls
 *           - <static_columns> is the static columns if a static row is present. It's
 *             the number of columns as an unsigned short, followed by the column names.
 *           - <columns> is the columns of the rows of the iterator. It's serialized as <static_columns>.
 *     <partition_deletion> is the deletion time for the partition (delta-encoded)
 *     <static_row> is the static row for this partition as serialized by UnfilteredSerializer.
 *     <row_estimate> is the (potentially estimated) number of rows serialized. This is only use for
 *         the purpose of some sizing on the receiving end and should not be relied upon too strongly.
 *
 * !!! Please note that the serialized value depends on the schema and as such should not be used as is if
 *     it might be deserialized after the schema as changed !!!
 * TODO: we should add a flag to include the relevant metadata in the header for commit log etc.....
 */
public class UnfilteredRowIteratorSerializer
{
    protected static final Logger logger = LoggerFactory.getLogger(UnfilteredRowIteratorSerializer.class);

    private static final int IS_EMPTY               = 0x01;
    private static final int IS_REVERSED            = 0x02;
    private static final int HAS_PARTITION_DELETION = 0x04;
    private static final int HAS_STATIC_ROW         = 0x08;
    private static final int HAS_ROW_ESTIMATE       = 0x10;

    public static final UnfilteredRowIteratorSerializer serializer = new UnfilteredRowIteratorSerializer();

    public void serialize(UnfilteredRowIterator iterator, DataOutputPlus out, int version) throws IOException
    {
        serialize(iterator, out, version, -1);
    }

    public void serialize(UnfilteredRowIterator iterator, DataOutputPlus out, int version, int rowEstimate) throws IOException
    {
        SerializationHeader header = new SerializationHeader(iterator.metadata(),
                                                             iterator.columns(),
                                                             iterator.stats());
        serialize(iterator, out, header, version, rowEstimate);
    }

    public void serialize(UnfilteredRowIterator iterator, DataOutputPlus out, SerializationHeader header, int version, int rowEstimate) throws IOException
    {
        CFMetaData.serializer.serialize(iterator.metadata(), out, version);
        ByteBufferUtil.writeWithLength(iterator.partitionKey().getKey(), out);

        int flags = 0;
        if (iterator.isReverseOrder())
            flags |= IS_REVERSED;

        if (iterator.isEmpty())
        {
            out.writeByte((byte)(flags | IS_EMPTY));
            return;
        }

        DeletionTime partitionDeletion = iterator.partitionLevelDeletion();
        if (!partitionDeletion.isLive())
            flags |= HAS_PARTITION_DELETION;
        Row staticRow = iterator.staticRow();
        boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;
        if (hasStatic)
            flags |= HAS_STATIC_ROW;

        if (rowEstimate >= 0)
            flags |= HAS_ROW_ESTIMATE;

        out.writeByte((byte)flags);

        SerializationHeader.serializer.serializeForMessaging(header, out, hasStatic);

        if (!partitionDeletion.isLive())
            writeDelTime(partitionDeletion, header, out);

        if (hasStatic)
            UnfilteredSerializer.serializer.serialize(staticRow, header, out, version);

        if (rowEstimate >= 0)
            out.writeInt(rowEstimate);

        while (iterator.hasNext())
            UnfilteredSerializer.serializer.serialize(iterator.next(), header, out, version);
        UnfilteredSerializer.serializer.writeEndOfPartition(out);
    }

    // Please note that this consume the iterator, and as such should not be called unless we have a simple way to
    // recreate an iterator for both serialize and serializedSize, which is mostly only PartitionUpdate
    public long serializedSize(UnfilteredRowIterator iterator, int version, int rowEstimate)
    {
        SerializationHeader header = new SerializationHeader(iterator.metadata(),
                                                             iterator.columns(),
                                                             iterator.stats());

        assert rowEstimate >= 0;

        long size = CFMetaData.serializer.serializedSize(iterator.metadata(), version)
                  + TypeSizes.sizeofWithLength(iterator.partitionKey().getKey())
                  + 1; // flags

        if (iterator.isEmpty())
            return size;

        DeletionTime partitionDeletion = iterator.partitionLevelDeletion();
        Row staticRow = iterator.staticRow();
        boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;

        size += SerializationHeader.serializer.serializedSizeForMessaging(header, hasStatic);

        if (!partitionDeletion.isLive())
            size += delTimeSerializedSize(partitionDeletion, header);

        if (hasStatic)
            size += UnfilteredSerializer.serializer.serializedSize(staticRow, header, version);

        if (rowEstimate >= 0)
            size += TypeSizes.sizeof(rowEstimate);

        while (iterator.hasNext())
            size += UnfilteredSerializer.serializer.serializedSize(iterator.next(), header, version);
        size += UnfilteredSerializer.serializer.serializedSizeEndOfPartition();

        return size;
    }

    public Header deserializeHeader(DataInputPlus in, int version, SerializationHelper.Flag flag) throws IOException
    {
        CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
        DecoratedKey key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithLength(in));
        int flags = in.readUnsignedByte();
        boolean isReversed = (flags & IS_REVERSED) != 0;
        if ((flags & IS_EMPTY) != 0)
        {
            SerializationHeader sh = new SerializationHeader(metadata, PartitionColumns.NONE, RowStats.NO_STATS);
            return new Header(sh, metadata, key, isReversed, true, null, null, 0);
        }

        boolean hasPartitionDeletion = (flags & HAS_PARTITION_DELETION) != 0;
        boolean hasStatic = (flags & HAS_STATIC_ROW) != 0;
        boolean hasRowEstimate = (flags & HAS_ROW_ESTIMATE) != 0;

        SerializationHeader header = SerializationHeader.serializer.deserializeForMessaging(in, metadata, hasStatic);

        DeletionTime partitionDeletion = hasPartitionDeletion ? readDelTime(in, header) : DeletionTime.LIVE;

        Row staticRow = Rows.EMPTY_STATIC_ROW;
        if (hasStatic)
            staticRow = UnfilteredSerializer.serializer.deserializeStaticRow(in, header, new SerializationHelper(version, flag));

        int rowEstimate = hasRowEstimate ? in.readInt() : -1;
        return new Header(header, metadata, key, isReversed, false, partitionDeletion, staticRow, rowEstimate);
    }

    public void deserialize(DataInput in, SerializationHelper helper, SerializationHeader header, Row.Writer rowWriter, RangeTombstoneMarker.Writer markerWriter) throws IOException
    {
        while (UnfilteredSerializer.serializer.deserialize(in, header, helper, rowWriter, markerWriter) != null);
    }

    public UnfilteredRowIterator deserialize(final DataInputPlus in, int version, SerializationHelper.Flag flag) throws IOException
    {
        final Header h = deserializeHeader(in, version, flag);

        if (h.isEmpty)
            return UnfilteredRowIterators.emptyIterator(h.metadata, h.key, h.isReversed);

        final int clusteringSize = h.metadata.clusteringColumns().size();
        final SerializationHelper helper = new SerializationHelper(version, flag);

        return new AbstractUnfilteredRowIterator(h.metadata, h.key, h.partitionDeletion, h.sHeader.columns(), h.staticRow, h.isReversed, h.sHeader.stats())
        {
            private final ReusableRow row = new ReusableRow(clusteringSize, h.sHeader.columns().regulars, true, h.metadata.isCounter());
            private final RangeTombstoneMarker.Builder markerBuilder = new RangeTombstoneMarker.Builder(clusteringSize);

            protected Unfiltered computeNext()
            {
                try
                {
                    Unfiltered.Kind kind = UnfilteredSerializer.serializer.deserialize(in, h.sHeader, helper, row.writer(), markerBuilder.reset());
                    if (kind == null)
                        return endOfData();

                    return kind == Unfiltered.Kind.ROW ? row : markerBuilder.build();
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        };
    }

    public static void writeDelTime(DeletionTime dt, SerializationHeader header, DataOutputPlus out) throws IOException
    {
        out.writeLong(header.encodeTimestamp(dt.markedForDeleteAt()));
        out.writeInt(header.encodeDeletionTime(dt.localDeletionTime()));
    }

    public static long delTimeSerializedSize(DeletionTime dt, SerializationHeader header)
    {
        return TypeSizes.sizeof(header.encodeTimestamp(dt.markedForDeleteAt()))
             + TypeSizes.sizeof(header.encodeDeletionTime(dt.localDeletionTime()));
    }

    public static DeletionTime readDelTime(DataInput in, SerializationHeader header) throws IOException
    {
        long markedAt = header.decodeTimestamp(in.readLong());
        int localDelTime = header.decodeDeletionTime(in.readInt());
        return new SimpleDeletionTime(markedAt, localDelTime);
    }

    public static void skipDelTime(DataInput in, SerializationHeader header) throws IOException
    {
        // Note that since we might use VINT, we shouldn't assume the size of a long or an int
        in.readLong();
        in.readInt();
    }

    public static class Header
    {
        public final SerializationHeader sHeader;
        public final CFMetaData metadata;
        public final DecoratedKey key;
        public final boolean isReversed;
        public final boolean isEmpty;
        public final DeletionTime partitionDeletion;
        public final Row staticRow;
        public final int rowEstimate; // -1 if no estimate

        private Header(SerializationHeader sHeader,
                       CFMetaData metadata,
                       DecoratedKey key,
                       boolean isReversed,
                       boolean isEmpty,
                       DeletionTime partitionDeletion,
                       Row staticRow,
                       int rowEstimate)
        {
            this.sHeader = sHeader;
            this.metadata = metadata;
            this.key = key;
            this.isReversed = isReversed;
            this.isEmpty = isEmpty;
            this.partitionDeletion = partitionDeletion;
            this.staticRow = staticRow;
            this.rowEstimate = rowEstimate;
        }

        @Override
        public String toString()
        {
            return String.format("{header=%s, table=%s.%s, key=%s, isReversed=%b, isEmpty=%b, del=%s, staticRow=%s, rowEstimate=%d}",
                                 sHeader, metadata.ksName, metadata.cfName, key, isReversed, isEmpty, partitionDeletion, staticRow.toString(metadata), rowEstimate);
        }
    }
}
