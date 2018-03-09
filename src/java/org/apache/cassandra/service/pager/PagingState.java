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

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.CompactTables;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.utils.ByteBufferUtil;

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

    public static PagingState deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
    {
        if (bytes == null)
            return null;

        try (DataInputBuffer in = new DataInputBuffer(bytes, true))
        {
            ByteBuffer pk;
            RowMark mark;
            int remaining, remainingInPartition;
            if (protocolVersion.isSmallerOrEqualTo(ProtocolVersion.V3))
            {
                pk = ByteBufferUtil.readWithShortLength(in);
                mark = new RowMark(ByteBufferUtil.readWithShortLength(in), protocolVersion);
                remaining = in.readInt();
                // Note that while 'in.available()' is theoretically an estimate of how many bytes are available
                // without blocking, we know that since we're reading a ByteBuffer it will be exactly how many
                // bytes remain to be read. And the reason we want to condition this is for backward compatility
                // as we used to not set this.
                remainingInPartition = in.available() > 0 ? in.readInt() : Integer.MAX_VALUE;
            }
            else
            {
                pk = ByteBufferUtil.readWithVIntLength(in);
                mark = new RowMark(ByteBufferUtil.readWithVIntLength(in), protocolVersion);
                remaining = (int)in.readUnsignedVInt();
                remainingInPartition = (int)in.readUnsignedVInt();
            }
            return new PagingState(pk.hasRemaining() ? pk : null,
                                   mark.mark.hasRemaining() ? mark : null,
                                   remaining,
                                   remainingInPartition);
        }
        catch (IOException e)
        {
            throw new ProtocolException("Invalid value for the paging state");
        }
    }

    public ByteBuffer serialize(ProtocolVersion protocolVersion)
    {
        assert rowMark == null || protocolVersion == rowMark.protocolVersion;
        try (DataOutputBuffer out = new DataOutputBufferFixed(serializedSize(protocolVersion)))
        {
            ByteBuffer pk = partitionKey == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : partitionKey;
            ByteBuffer mark = rowMark == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : rowMark.mark;
            if (protocolVersion.isSmallerOrEqualTo(ProtocolVersion.V3))
            {
                ByteBufferUtil.writeWithShortLength(pk, out);
                ByteBufferUtil.writeWithShortLength(mark, out);
                out.writeInt(remaining);
                out.writeInt(remainingInPartition);
            }
            else
            {
                ByteBufferUtil.writeWithVIntLength(pk, out);
                ByteBufferUtil.writeWithVIntLength(mark, out);
                out.writeUnsignedVInt(remaining);
                out.writeUnsignedVInt(remainingInPartition);
            }
            return out.buffer();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public int serializedSize(ProtocolVersion protocolVersion)
    {
        assert rowMark == null || protocolVersion == rowMark.protocolVersion;
        ByteBuffer pk = partitionKey == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : partitionKey;
        ByteBuffer mark = rowMark == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : rowMark.mark;
        if (protocolVersion.isSmallerOrEqualTo(ProtocolVersion.V3))
        {
            return ByteBufferUtil.serializedSizeWithShortLength(pk)
                 + ByteBufferUtil.serializedSizeWithShortLength(mark)
                 + 8; // remaining & remainingInPartition
        }
        else
        {
            return ByteBufferUtil.serializedSizeWithVIntLength(pk)
                 + ByteBufferUtil.serializedSizeWithVIntLength(mark)
                 + TypeSizes.sizeofUnsignedVInt(remaining)
                 + TypeSizes.sizeofUnsignedVInt(remainingInPartition);
        }
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
                             partitionKey != null ? ByteBufferUtil.bytesToHex(partitionKey) : null,
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

        private static List<AbstractType<?>> makeClusteringTypes(TableMetadata metadata)
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

        public static RowMark create(TableMetadata metadata, Row row, ProtocolVersion protocolVersion)
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
                    mark = encodeCellName(metadata, row.clustering(), ByteBufferUtil.EMPTY_BYTE_BUFFER, null);
                }
                else
                {
                    Cell cell = cells.next();
                    mark = encodeCellName(metadata, row.clustering(), cell.column().name.bytes, cell.column().isComplex() ? cell.path().get(0) : null);
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

        public Clustering clustering(TableMetadata metadata)
        {
            if (mark == null)
                return null;

            return protocolVersion.isSmallerOrEqualTo(ProtocolVersion.V3)
                 ? decodeClustering(metadata, mark)
                 : Clustering.serializer.deserialize(mark, MessagingService.VERSION_30, makeClusteringTypes(metadata));
        }

        // Old (pre-3.0) encoding of cells. We need that for the protocol v3 as that is how things where encoded
        private static ByteBuffer encodeCellName(TableMetadata metadata, Clustering clustering, ByteBuffer columnName, ByteBuffer collectionElement)
        {
            boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;

            if (!metadata.isCompound())
            {
                if (isStatic)
                    return columnName;

                assert clustering.size() == 1 : "Expected clustering size to be 1, but was " + clustering.size();
                return clustering.get(0);
            }

            // We use comparator.size() rather than clustering.size() because of static clusterings
            int clusteringSize = metadata.comparator.size();
            int size = clusteringSize + (metadata.isDense() ? 0 : 1) + (collectionElement == null ? 0 : 1);
            if (metadata.isSuper())
                size = clusteringSize + 1;
            ByteBuffer[] values = new ByteBuffer[size];
            for (int i = 0; i < clusteringSize; i++)
            {
                if (isStatic)
                {
                    values[i] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
                    continue;
                }

                ByteBuffer v = clustering.get(i);
                // we can have null (only for dense compound tables for backward compatibility reasons) but that
                // means we're done and should stop there as far as building the composite is concerned.
                if (v == null)
                    return CompositeType.build(Arrays.copyOfRange(values, 0, i));

                values[i] = v;
            }

            if (metadata.isSuper())
            {
                // We need to set the "column" (in thrift terms) name, i.e. the value corresponding to the subcomparator.
                // What it is depends if this a cell for a declared "static" column or a "dynamic" column part of the
                // super-column internal map.
                assert columnName != null; // This should never be null for supercolumns, see decodeForSuperColumn() above
                values[clusteringSize] = columnName.equals(CompactTables.SUPER_COLUMN_MAP_COLUMN)
                                         ? collectionElement
                                         : columnName;
            }
            else
            {
                if (!metadata.isDense())
                    values[clusteringSize] = columnName;
                if (collectionElement != null)
                    values[clusteringSize + 1] = collectionElement;
            }

            return CompositeType.build(isStatic, values);
        }

        private static Clustering decodeClustering(TableMetadata metadata, ByteBuffer value)
        {
            int csize = metadata.comparator.size();
            if (csize == 0)
                return Clustering.EMPTY;

            if (metadata.isCompound() && CompositeType.isStaticName(value))
                return Clustering.STATIC_CLUSTERING;

            List<ByteBuffer> components = metadata.isCompound()
                                          ? CompositeType.splitName(value)
                                          : Collections.singletonList(value);

            return Clustering.make(components.subList(0, Math.min(csize, components.size())).toArray(new ByteBuffer[csize]));
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
            return mark == null ? "null" : ByteBufferUtil.bytesToHex(mark);
        }
    }
}
