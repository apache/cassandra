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
package org.apache.cassandra.service.pager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.LegacyLayout;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;
import static org.apache.cassandra.utils.ByteBufferUtil.*;
import static org.apache.cassandra.utils.vint.VIntCoding.computeUnsignedVIntSize;
import static org.apache.cassandra.utils.vint.VIntCoding.getUnsignedVInt;

@SuppressWarnings("WeakerAccess")
public class PagingState
{
    public final ByteBuffer partitionKey;  // Can be null for single partition queries.
    public final RowMark rowMark;          // Can be null if not needed.
    public final int remaining;
    public final int remainingInPartition;

    public PagingState(ByteBuffer partitionKey, RowMark rowMark, int remaining, int remainingInPartition)
    {
        this.partitionKey = partitionKey;
        this.rowMark = rowMark;
        this.remaining = remaining;
        this.remainingInPartition = remainingInPartition;
    }

    public ByteBuffer serialize(ProtocolVersion protocolVersion)
    {
        assert rowMark == null || protocolVersion == rowMark.protocolVersion;
        try
        {
            return protocolVersion.isGreaterThan(ProtocolVersion.V3) ? modernSerialize() : legacySerialize(true);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public int serializedSize(ProtocolVersion protocolVersion)
    {
        assert rowMark == null || protocolVersion == rowMark.protocolVersion;

        return protocolVersion.isGreaterThan(ProtocolVersion.V3) ? modernSerializedSize() : legacySerializedSize(true);
    }

    /**
     * It's possible to receive a V3 paging state on a V4 client session, and vice versa - so we cannot
     * blindly rely on the protocol version provided. We must verify first that the buffer indeed contains
     * a paging state that adheres to the protocol version provided, or, if not - see if it is in a different
     * version, in which case we try the other format.
     */
    public static PagingState deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
    {
        if (bytes == null)
            return null;

        try
        {
            /*
             * We can't just attempt to deser twice, as we risk to misinterpet short/vint
             * lengths and allocate huge byte arrays for readWithVIntLength() or,
             * to a lesser extent, readWithShortLength()
             */

            if (protocolVersion.isGreaterThan(ProtocolVersion.V3))
            {
                if (isModernSerialized(bytes)) return modernDeserialize(bytes, protocolVersion);
                if (isLegacySerialized(bytes)) return legacyDeserialize(bytes, ProtocolVersion.V3);
            }

            if (protocolVersion.isSmallerThan(ProtocolVersion.V4))
            {
                if (isLegacySerialized(bytes)) return legacyDeserialize(bytes, protocolVersion);
                if (isModernSerialized(bytes)) return modernDeserialize(bytes, ProtocolVersion.V4);
            }
        }
        catch (IOException e)
        {
            throw new ProtocolException("Invalid value for the paging state");
        }

        throw new ProtocolException("Invalid value for the paging state");
    }

    /*
     * Modern serde (> VERSION_3)
     */

    @SuppressWarnings({ "resource", "RedundantSuppression" })
    private ByteBuffer modernSerialize() throws IOException
    {
        DataOutputBuffer out = new DataOutputBufferFixed(modernSerializedSize());
        writeWithVIntLength(null == partitionKey ? EMPTY_BYTE_BUFFER : partitionKey, out);
        writeWithVIntLength(null == rowMark ? EMPTY_BYTE_BUFFER : rowMark.mark, out);
        out.writeUnsignedVInt(remaining);
        out.writeUnsignedVInt(remainingInPartition);
        return out.buffer(false);
    }

    private static boolean isModernSerialized(ByteBuffer bytes)
    {
        int index = bytes.position();
        int limit = bytes.limit();

        long partitionKeyLen = getUnsignedVInt(bytes, index, limit);
        if (partitionKeyLen < 0)
            return false;
        index += computeUnsignedVIntSize(partitionKeyLen) + partitionKeyLen;
        if (index >= limit)
            return false;

        long rowMarkerLen = getUnsignedVInt(bytes, index, limit);
        if (rowMarkerLen < 0)
            return false;
        index += computeUnsignedVIntSize(rowMarkerLen) + rowMarkerLen;
        if (index >= limit)
            return false;

        long remaining = getUnsignedVInt(bytes, index, limit);
        if (remaining < 0)
            return false;
        index += computeUnsignedVIntSize(remaining);
        if (index >= limit)
            return false;

        long remainingInPartition = getUnsignedVInt(bytes, index, limit);
        if (remainingInPartition < 0)
            return false;
        index += computeUnsignedVIntSize(remainingInPartition);
        return index == limit;
    }

    @SuppressWarnings({ "resource", "RedundantSuppression" })
    private static PagingState modernDeserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws IOException
    {
        if (protocolVersion.isSmallerThan(ProtocolVersion.V4))
            throw new IllegalArgumentException();

        DataInputBuffer in = new DataInputBuffer(bytes, false);

        ByteBuffer partitionKey = readWithVIntLength(in);
        ByteBuffer rawMark = readWithVIntLength(in);
        int remaining = Ints.checkedCast(in.readUnsignedVInt());
        int remainingInPartition = Ints.checkedCast(in.readUnsignedVInt());

        return new PagingState(partitionKey.hasRemaining() ? partitionKey : null,
                               rawMark.hasRemaining() ? new RowMark(rawMark, protocolVersion) : null,
                               remaining,
                               remainingInPartition);
    }

    private int modernSerializedSize()
    {
        return serializedSizeWithVIntLength(null == partitionKey ? EMPTY_BYTE_BUFFER : partitionKey)
             + serializedSizeWithVIntLength(null == rowMark ? EMPTY_BYTE_BUFFER : rowMark.mark)
             + sizeofUnsignedVInt(remaining)
             + sizeofUnsignedVInt(remainingInPartition);
    }

    /*
     * Legacy serde (< VERSION_4)
     *
     * There are two versions of legacy PagingState format - one used by 2.1/2.2 and one used by 3.0+.
     * The latter includes remainingInPartition count, while the former doesn't.
     */

    @VisibleForTesting
    @SuppressWarnings({ "resource", "RedundantSuppression" })
    ByteBuffer legacySerialize(boolean withRemainingInPartition) throws IOException
    {
        DataOutputBuffer out = new DataOutputBufferFixed(legacySerializedSize(withRemainingInPartition));
        writeWithShortLength(null == partitionKey ? EMPTY_BYTE_BUFFER : partitionKey, out);
        writeWithShortLength(null == rowMark ? EMPTY_BYTE_BUFFER : rowMark.mark, out);
        out.writeInt(remaining);
        if (withRemainingInPartition)
            out.writeInt(remainingInPartition);
        return out.buffer(false);
    }

    private static boolean isLegacySerialized(ByteBuffer bytes)
    {
        int index = bytes.position();
        int limit = bytes.limit();

        if (limit - index < 2)
            return false;
        short partitionKeyLen = bytes.getShort(index);
        if (partitionKeyLen < 0)
            return false;
        index += 2 + partitionKeyLen;

        if (limit - index < 2)
            return false;
        short rowMarkerLen = bytes.getShort(index);
        if (rowMarkerLen < 0)
            return false;
        index += 2 + rowMarkerLen;

        if (limit - index < 4)
            return false;
        int remaining = bytes.getInt(index);
        if (remaining < 0)
            return false;
        index += 4;

        // V3 encoded by 2.1/2.2 - sans remainingInPartition
        if (index == limit)
            return true;

        if (limit - index == 4)
        {
            int remainingInPartition = bytes.getInt(index);
            return remainingInPartition >= 0; // the value must make sense
        }
        return false;
    }

    @SuppressWarnings({ "resource", "RedundantSuppression" })
    private static PagingState legacyDeserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws IOException
    {
        if (protocolVersion.isGreaterThan(ProtocolVersion.V3))
            throw new IllegalArgumentException();

        DataInputBuffer in = new DataInputBuffer(bytes, false);

        ByteBuffer partitionKey = readWithShortLength(in);
        ByteBuffer rawMark = readWithShortLength(in);
        int remaining = in.readInt();
        /*
         * 2.1/2.2 implementations of V3 protocol did not write remainingInPartition, but C* 3.0+ does, so we need
         * to handle both variants of V3 serialization for compatibility.
         */
        int remainingInPartition = in.available() > 0 ? in.readInt() : Integer.MAX_VALUE;

        return new PagingState(partitionKey.hasRemaining() ? partitionKey : null,
                               rawMark.hasRemaining() ? new RowMark(rawMark, protocolVersion) : null,
                               remaining,
                               remainingInPartition);
    }

    @VisibleForTesting
    int legacySerializedSize(boolean withRemainingInPartition)
    {
        return serializedSizeWithShortLength(null == partitionKey ? EMPTY_BYTE_BUFFER : partitionKey)
             + serializedSizeWithShortLength(null == rowMark ? EMPTY_BYTE_BUFFER : rowMark.mark)
             + sizeof(remaining)
             + (withRemainingInPartition ? sizeof(remainingInPartition) : 0);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(partitionKey, rowMark, remaining, remainingInPartition);
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof PagingState))
            return false;
        PagingState that = (PagingState)o;
        return Objects.equals(this.partitionKey, that.partitionKey)
            && Objects.equals(this.rowMark, that.rowMark)
            && this.remaining == that.remaining
            && this.remainingInPartition == that.remainingInPartition;
    }

    @Override
    public String toString()
    {
        return String.format("PagingState(key=%s, cellname=%s, remaining=%d, remainingInPartition=%d",
                             partitionKey != null ? bytesToHex(partitionKey) : null,
                             rowMark,
                             remaining,
                             remainingInPartition);
    }

    /**
     * Marks the last row returned by paging, the one from which paging should continue.
     * This class essentially holds a row clustering, but due to backward compatibility reasons,
     * we need to actually store  the cell name for the last cell of the row we're marking when
     * the protocol v3 is in use, and this class abstract that complication.
     *
     * See CASSANDRA-10254 for more details.
     */
    public static class RowMark
    {
        // This can be null for convenience if no row is marked.
        private final ByteBuffer mark;
        private final ProtocolVersion protocolVersion;

        private RowMark(ByteBuffer mark, ProtocolVersion protocolVersion)
        {
            this.mark = mark;
            this.protocolVersion = protocolVersion;
        }

        private static List<AbstractType<?>> makeClusteringTypes(CFMetaData metadata)
        {
            // This is the types that will be used when serializing the clustering in the paging state. We can't really use the actual clustering
            // types however because we can't guarantee that there won't be a schema change between when we send the paging state and get it back,
            // and said schema change could theoretically change one of the clustering types from a fixed width type to a non-fixed one
            // (say timestamp -> blob). So we simply use a list of BytesTypes (for both reading and writting), which may be slightly inefficient
            // for fixed-width types, but avoid any risk during schema changes.
            int size = metadata.clusteringColumns().size();
            List<AbstractType<?>> l = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                l.add(BytesType.instance);
            return l;
        }

        public static RowMark create(CFMetaData metadata, Row row, ProtocolVersion protocolVersion)
        {
            ByteBuffer mark;
            if (protocolVersion.isSmallerOrEqualTo(ProtocolVersion.V3))
            {
                // We need to be backward compatible with 2.1/2.2 nodes paging states. Which means we have to send
                // the full cellname of the "last" cell in the row we get (since that's how 2.1/2.2 nodes will start after
                // that last row if they get that paging state).
                Iterator<Cell> cells = row.cellsInLegacyOrder(metadata, true).iterator();
                if (!cells.hasNext())
                {
                    // If the last returned row has no cell, this means in 2.1/2.2 terms that we stopped on the row
                    // marker. Note that this shouldn't happen if the table is COMPACT.
                    assert !metadata.isCompactTable();
                    mark = LegacyLayout.encodeCellName(metadata, row.clustering(), EMPTY_BYTE_BUFFER, null);
                }
                else
                {
                    Cell cell = cells.next();
                    mark = LegacyLayout.encodeCellName(metadata, row.clustering(), cell.column().name.bytes, cell.column().isComplex() ? cell.path().get(0) : null);
                }
            }
            else
            {
                // We froze the serialization version to 3.0 as we need to make this this doesn't change (that is, it has to be
                // fix for a given version of the protocol).
                mark = Clustering.serializer.serialize(row.clustering(), MessagingService.VERSION_30, makeClusteringTypes(metadata));
            }
            return new RowMark(mark, protocolVersion);
        }

        public Clustering clustering(CFMetaData metadata)
        {
            if (mark == null)
                return null;

            return protocolVersion.isSmallerOrEqualTo(ProtocolVersion.V3)
                 ? LegacyLayout.decodeClustering(metadata, mark)
                 : Clustering.serializer.deserialize(mark, MessagingService.VERSION_30, makeClusteringTypes(metadata));
        }

        @Override
        public final int hashCode()
        {
            return Objects.hash(mark, protocolVersion);
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof RowMark))
                return false;
            RowMark that = (RowMark)o;
            return Objects.equals(this.mark, that.mark) && this.protocolVersion == that.protocolVersion;
        }

        @Override
        public String toString()
        {
            return mark == null ? "null" : bytesToHex(mark);
        }
    }
}
