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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.*;


/**
 * Packs a CellName AND a Cell into one off-heap representation.
 * Layout is:
 *
 * Note we store the ColumnIdentifier in full as bytes. This seems an okay tradeoff for now, as we just
 * look it back up again when we need to, and in the near future we hope to switch to ints, longs or
 * UUIDs representing column identifiers on disk, at which point we can switch that here as well.
 *
 * [timestamp][value offset][name size]][name extra][name offset deltas][cell names][value][Descendants]
 * [   8b    ][     4b     ][    2b   ][     1b    ][     each 2b      ][ arb < 64k][ arb ][ arbitrary ]
 *
 * descendants: any overriding classes will put their state here
 * name offsets are deltas from their base offset, and don't include the first offset, or the end position of the final entry,
 * i.e. there will be size - 1 entries, and each is a delta that is added to the offset of the position of the first name
 * (which is always CELL_NAME_OFFSETS_OFFSET + (2 * (size - 1))). The length of the final name fills up any remaining
 * space upto the value offset
 * name extra:  lowest 2 bits indicate the clustering size delta (i.e. how many name items are NOT part of the clustering key)
 *              the next 2 bits indicate the CellNameType
 *              the next bit indicates if the column is a static or clustered/dynamic column
 */
public abstract class AbstractNativeCell extends AbstractCell implements CellName
{
    static final int TIMESTAMP_OFFSET = 4;
    private static final int VALUE_OFFSET_OFFSET = 12;
    private static final int CELL_NAME_SIZE_OFFSET = 16;
    private static final int CELL_NAME_EXTRA_OFFSET = 18;
    private static final int CELL_NAME_OFFSETS_OFFSET = 19;
    private static final int CELL_NAME_SIZE_DELTA_MASK = 3;
    private static final int CELL_NAME_TYPE_SHIFT = 2;
    private static final int CELL_NAME_TYPE_MASK = 7;

    private static enum NameType
    {
        COMPOUND_DENSE(0 << 2), COMPOUND_SPARSE(1 << 2), COMPOUND_SPARSE_STATIC(2 << 2), SIMPLE_DENSE(3 << 2), SIMPLE_SPARSE(4 << 2);
        static final NameType[] TYPES = NameType.values();
        final int bits;

        NameType(int bits)
        {
            this.bits = bits;
        }

        static NameType typeOf(CellName name)
        {
            if (name instanceof CompoundDenseCellName)
            {
                assert !name.isStatic();
                return COMPOUND_DENSE;
            }

            if (name instanceof CompoundSparseCellName)
                return name.isStatic() ? COMPOUND_SPARSE_STATIC : COMPOUND_SPARSE;

            if (name instanceof SimpleDenseCellName)
            {
                assert !name.isStatic();
                return SIMPLE_DENSE;
            }

            if (name instanceof SimpleSparseCellName)
            {
                assert !name.isStatic();
                return SIMPLE_SPARSE;
            }

            if (name instanceof NativeCell)
                return ((NativeCell) name).nametype();

            throw new AssertionError();
        }
    }

    private final long peer; // peer is assigned by peer updater in setPeer method

    AbstractNativeCell()
    {
        peer = -1;
    }

    public AbstractNativeCell(NativeAllocator allocator, OpOrder.Group writeOp, Cell copyOf)
    {
        int size = sizeOf(copyOf);
        peer = allocator.allocate(size, writeOp);

        MemoryUtil.setInt(peer, size);
        construct(copyOf);
    }

    protected int sizeOf(Cell cell)
    {
        int size = CELL_NAME_OFFSETS_OFFSET + Math.max(0, cell.name().size() - 1) * 2 + cell.value().remaining();
        CellName name = cell.name();
        for (int i = 0; i < name.size(); i++)
            size += name.get(i).remaining();
        return size;
    }

    protected void construct(Cell from)
    {
        setLong(TIMESTAMP_OFFSET, from.timestamp());
        CellName name = from.name();
        int nameSize = name.size();
        int offset = CELL_NAME_SIZE_OFFSET;
        setShort(offset, (short) nameSize);
        assert nameSize - name.clusteringSize() <= 2;
        byte cellNameExtraBits = (byte) ((nameSize - name.clusteringSize()) | NameType.typeOf(name).bits);
        setByte(offset += 2, cellNameExtraBits);
        offset += 1;
        short cellNameDelta = 0;
        for (int i = 1; i < nameSize; i++)
        {
            cellNameDelta += name.get(i - 1).remaining();
            setShort(offset, cellNameDelta);
            offset += 2;
        }
        for (int i = 0; i < nameSize; i++)
        {
            ByteBuffer bb = name.get(i);
            setBytes(offset, bb);
            offset += bb.remaining();
        }
        setInt(VALUE_OFFSET_OFFSET, offset);
        setBytes(offset, from.value());
    }

    // the offset at which to read the short that gives the names
    private int nameDeltaOffset(int i)
    {
        return CELL_NAME_OFFSETS_OFFSET + ((i - 1) * 2);
    }

    int valueStartOffset()
    {
        return getInt(VALUE_OFFSET_OFFSET);
    }

    private int valueEndOffset()
    {
        return (int) (internalSize() - postfixSize());
    }

    protected int postfixSize()
    {
        return 0;
    }

    @Override
    public ByteBuffer value()
    {
        long offset = valueStartOffset();
        return getByteBuffer(offset, (int) (internalSize() - (postfixSize() + offset))).order(ByteOrder.BIG_ENDIAN);
    }

    private int clusteringSizeDelta()
    {
        return getByte(CELL_NAME_EXTRA_OFFSET) & CELL_NAME_SIZE_DELTA_MASK;
    }

    public boolean isStatic()
    {
        return nametype() == NameType.COMPOUND_SPARSE_STATIC;
    }

    NameType nametype()
    {
        return NameType.TYPES[(((int) this.getByte(CELL_NAME_EXTRA_OFFSET)) >> CELL_NAME_TYPE_SHIFT) & CELL_NAME_TYPE_MASK];
    }

    public long minTimestamp()
    {
        return timestamp();
    }

    public long maxTimestamp()
    {
        return timestamp();
    }

    public int clusteringSize()
    {
        return size() - clusteringSizeDelta();
    }

    @Override
    public ColumnIdentifier cql3ColumnName(CFMetaData metadata)
    {
        switch (nametype())
        {
            case SIMPLE_SPARSE:
                return getIdentifier(metadata, get(clusteringSize()));
            case COMPOUND_SPARSE_STATIC:
            case COMPOUND_SPARSE:
                ByteBuffer buffer = get(clusteringSize());
                if (buffer.remaining() == 0)
                    return CompoundSparseCellNameType.rowMarkerId;

                return getIdentifier(metadata, buffer);
            case SIMPLE_DENSE:
            case COMPOUND_DENSE:
                return null;
            default:
                throw new AssertionError();
        }
    }

    public ByteBuffer collectionElement()
    {
        return isCollectionCell() ? get(size() - 1) : null;
    }

    // we always have a collection element if our clustering size is 2 less than our total size,
    // and we never have one otherwiss
    public boolean isCollectionCell()
    {
        return clusteringSizeDelta() == 2;
    }

    public boolean isSameCQL3RowAs(CellNameType type, CellName other)
    {
        switch (nametype())
        {
            case SIMPLE_DENSE:
            case COMPOUND_DENSE:
                return type.compare(this, other) == 0;
            case COMPOUND_SPARSE_STATIC:
            case COMPOUND_SPARSE:
                int clusteringSize = clusteringSize();
                if (clusteringSize != other.clusteringSize() || other.isStatic() != isStatic())
                    return false;
                for (int i = 0; i < clusteringSize; i++)
                    if (type.subtype(i).compare(get(i), other.get(i)) != 0)
                        return false;
                return true;
            case SIMPLE_SPARSE:
                return true;
            default:
                throw new AssertionError();
        }
    }

    public int size()
    {
        return getShort(CELL_NAME_SIZE_OFFSET);
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public ByteBuffer get(int i)
    {
        return get(i, null);
    }

    private ByteBuffer get(int i, AbstractAllocator copy)
    {
        // remember to take dense/sparse into account, and only return EOC when not dense
        int size = size();
        assert i >= 0 && i < size();
        int cellNamesOffset = nameDeltaOffset(size);
        int startDelta = i == 0 ? 0 : getShort(nameDeltaOffset(i));
        int endDelta = i < size - 1 ? getShort(nameDeltaOffset(i + 1)) : valueStartOffset() - cellNamesOffset;
        int length = endDelta - startDelta;
        if (copy == null)
            return getByteBuffer(cellNamesOffset + startDelta, length).order(ByteOrder.BIG_ENDIAN);
        ByteBuffer result = copy.allocate(length);
        FastByteOperations.UnsafeOperations.copy(null, peer + cellNamesOffset + startDelta, result, 0, length);
        return result;
    }

    private static final ThreadLocal<byte[]> BUFFER = new ThreadLocal<byte[]>()
    {
        protected byte[] initialValue()
        {
            return new byte[256];
        }
    };

    protected void writeComponentTo(MessageDigest digest, int i, boolean includeSize)
    {
        // remember to take dense/sparse into account, and only return EOC when not dense
        int size = size();
        assert i >= 0 && i < size();
        int cellNamesOffset = nameDeltaOffset(size);
        int startDelta = i == 0 ? 0 : getShort(nameDeltaOffset(i));
        int endDelta = i < size - 1 ? getShort(nameDeltaOffset(i + 1)) : valueStartOffset() - cellNamesOffset;

        int componentStart = cellNamesOffset + startDelta;
        int count = endDelta - startDelta;

        if (includeSize)
            FBUtilities.updateWithShort(digest, count);

        writeMemoryTo(digest, componentStart, count);
    }

    protected void writeMemoryTo(MessageDigest digest, int from, int count)
    {
        // only batch if we have more than 16 bytes remaining to transfer, otherwise fall-back to single-byte updates
        int i = 0, batchEnd = count - 16;
        if (i < batchEnd)
        {
            byte[] buffer = BUFFER.get();
            while (i < batchEnd)
            {
                int transfer = Math.min(count - i, 256);
                getBytes(from + i, buffer, 0, transfer);
                digest.update(buffer, 0, transfer);
                i += transfer;
            }
        }
        while (i < count)
            digest.update(getByte(from + i++));
    }

    public EOC eoc()
    {
        return EOC.NONE;
    }

    public Composite withEOC(EOC eoc)
    {
        throw new UnsupportedOperationException();
    }

    public Composite start()
    {
        throw new UnsupportedOperationException();
    }

    public Composite end()
    {
        throw new UnsupportedOperationException();
    }

    public ColumnSlice slice()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isPrefixOf(CType type, Composite c)
    {
        if (size() > c.size() || isStatic() != c.isStatic())
            return false;

        for (int i = 0; i < size(); i++)
        {
            if (type.subtype(i).compare(get(i), c.get(i)) != 0)
                return false;
        }
        return true;
    }

    public ByteBuffer toByteBuffer()
    {
        // for simple sparse we just return our one name buffer
        switch (nametype())
        {
            case SIMPLE_DENSE:
            case SIMPLE_SPARSE:
                return get(0);
            case COMPOUND_DENSE:
            case COMPOUND_SPARSE_STATIC:
            case COMPOUND_SPARSE:
                // This is the legacy format of composites.
                // See org.apache.cassandra.db.marshal.CompositeType for details.
                ByteBuffer result = ByteBuffer.allocate(cellDataSize());
                if (isStatic())
                    ByteBufferUtil.writeShortLength(result, CompositeType.STATIC_MARKER);

                for (int i = 0; i < size(); i++)
                {
                    ByteBuffer bb = get(i);
                    ByteBufferUtil.writeShortLength(result, bb.remaining());
                    result.put(bb);
                    result.put((byte) 0);
                }
                result.flip();
                return result;
            default:
                throw new AssertionError();
        }
    }

    protected void updateWithName(MessageDigest digest)
    {
        // for simple sparse we just return our one name buffer
        switch (nametype())
        {
            case SIMPLE_DENSE:
            case SIMPLE_SPARSE:
                writeComponentTo(digest, 0, false);
                break;

            case COMPOUND_DENSE:
            case COMPOUND_SPARSE_STATIC:
            case COMPOUND_SPARSE:
                // This is the legacy format of composites.
                // See org.apache.cassandra.db.marshal.CompositeType for details.
                if (isStatic())
                    FBUtilities.updateWithShort(digest, CompositeType.STATIC_MARKER);

                for (int i = 0; i < size(); i++)
                {
                    writeComponentTo(digest, i, true);
                    digest.update((byte) 0);
                }
                break;

            default:
                throw new AssertionError();
        }
    }

    protected void updateWithValue(MessageDigest digest)
    {
        int offset = valueStartOffset();
        int length = valueEndOffset() - offset;
        writeMemoryTo(digest, offset, length);
    }

    @Override // this is the NAME dataSize, only!
    public int dataSize()
    {
        switch (nametype())
        {
            case SIMPLE_DENSE:
            case SIMPLE_SPARSE:
                return valueStartOffset() - nameDeltaOffset(size());
            case COMPOUND_DENSE:
            case COMPOUND_SPARSE_STATIC:
            case COMPOUND_SPARSE:
                int size = size();
                return valueStartOffset() - nameDeltaOffset(size) + 3 * size + (isStatic() ? 2 : 0);
            default:
                throw new AssertionError();
        }
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;
        if (obj instanceof CellName)
            return equals((CellName) obj);
        if (obj instanceof Cell)
            return equals((Cell) obj);
        return false;
    }

    public boolean equals(CellName that)
    {
        int size = this.size();
        if (size != that.size())
            return false;

        for (int i = 0 ; i < size ; i++)
            if (!get(i).equals(that.get(i)))
                return false;
        return true;
    }

    private static final ByteBuffer[] EMPTY = new ByteBuffer[0];

    @Override
    public CellName copy(CFMetaData cfm, AbstractAllocator allocator)
    {
        ByteBuffer[] r;
        switch (nametype())
        {
            case SIMPLE_DENSE:
                return CellNames.simpleDense(get(0, allocator));

            case COMPOUND_DENSE:
                r = new ByteBuffer[size()];
                for (int i = 0; i < r.length; i++)
                    r[i] = get(i, allocator);
                return CellNames.compositeDense(r);

            case COMPOUND_SPARSE_STATIC:
            case COMPOUND_SPARSE:
                int clusteringSize = clusteringSize();
                r = clusteringSize == 0 ? EMPTY : new ByteBuffer[clusteringSize()];
                for (int i = 0; i < clusteringSize; i++)
                    r[i] = get(i, allocator);

                ByteBuffer nameBuffer = get(r.length);
                ColumnIdentifier name;

                if (nameBuffer.remaining() == 0)
                {
                    name = CompoundSparseCellNameType.rowMarkerId;
                }
                else
                {
                    name = getIdentifier(cfm, nameBuffer);
                }

                if (clusteringSizeDelta() == 2)
                {
                    ByteBuffer element = allocator.clone(get(size() - 1));
                    return CellNames.compositeSparseWithCollection(r, element, name, isStatic());
                }
                return CellNames.compositeSparse(r, name, isStatic());

            case SIMPLE_SPARSE:
                return CellNames.simpleSparse(getIdentifier(cfm, get(0)));
        }
        throw new IllegalStateException();
    }

    private static ColumnIdentifier getIdentifier(CFMetaData cfMetaData, ByteBuffer name)
    {
        ColumnDefinition def = cfMetaData.getColumnDefinition(name);
        if (def != null)
        {
            return def.name;
        }
        else
        {
            // it's safe to simply grab based on clusteringPrefixSize() as we are only called if not a dense type
            AbstractType<?> type = cfMetaData.comparator.subtype(cfMetaData.comparator.clusteringPrefixSize());
            return new ColumnIdentifier(HeapAllocator.instance.clone(name), type);
        }
    }

    @Override
    public Cell withUpdatedName(CellName newName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cell withUpdatedTimestamp(long newTimestamp)
    {
        throw new UnsupportedOperationException();
    }

    protected long internalSize()
    {
        return MemoryUtil.getInt(peer);
    }

    private void checkPosition(long offset, long size)
    {
        assert size >= 0;
        assert peer > 0 : "Memory was freed";
        assert offset >= 0 && offset + size <= internalSize() : String.format("Illegal range: [%d..%d), size: %s", offset, offset + size, internalSize());
    }

    protected final void setByte(long offset, byte b)
    {
        checkPosition(offset, 1);
        MemoryUtil.setByte(peer + offset, b);
    }

    protected final void setShort(long offset, short s)
    {
        checkPosition(offset, 1);
        MemoryUtil.setShort(peer + offset, s);
    }

    protected final void setInt(long offset, int l)
    {
        checkPosition(offset, 4);
        MemoryUtil.setInt(peer + offset, l);
    }

    protected final void setLong(long offset, long l)
    {
        checkPosition(offset, 8);
        MemoryUtil.setLong(peer + offset, l);
    }

    protected final void setBytes(long offset, ByteBuffer buffer)
    {
        int start = buffer.position();
        int count = buffer.limit() - start;
        if (count == 0)
            return;

        checkPosition(offset, count);
        MemoryUtil.setBytes(peer + offset, buffer);
    }

    protected final byte getByte(long offset)
    {
        checkPosition(offset, 1);
        return MemoryUtil.getByte(peer + offset);
    }

    protected final void getBytes(long offset, byte[] trg, int trgOffset, int count)
    {
        checkPosition(offset, count);
        MemoryUtil.getBytes(peer + offset, trg, trgOffset, count);
    }

    protected final int getShort(long offset)
    {
        checkPosition(offset, 2);
        return MemoryUtil.getShort(peer + offset);
    }

    protected final int getInt(long offset)
    {
        checkPosition(offset, 4);
        return MemoryUtil.getInt(peer + offset);
    }

    protected final long getLong(long offset)
    {
        checkPosition(offset, 8);
        return MemoryUtil.getLong(peer + offset);
    }

    protected final ByteBuffer getByteBuffer(long offset, int length)
    {
        checkPosition(offset, length);
        return MemoryUtil.getByteBuffer(peer + offset, length);
    }

    // requires isByteOrderComparable to be true. Compares the name components only; ; may need to compare EOC etc still
    @Inline
    public final int compareTo(final Composite that)
    {
        if (isStatic() != that.isStatic())
        {
            // Static sorts before non-static no matter what, except for empty which
            // always sort first
            if (isEmpty())
                return that.isEmpty() ? 0 : -1;
            if (that.isEmpty())
                return 1;
            return isStatic() ? -1 : 1;
        }

        int size = size();
        int size2 = that.size();
        int minSize = Math.min(size, size2);
        int startDelta = 0;
        int cellNamesOffset = nameDeltaOffset(size);
        for (int i = 0 ; i < minSize ; i++)
        {
            int endDelta = i < size - 1 ? getShort(nameDeltaOffset(i + 1)) : valueStartOffset() - cellNamesOffset;
            long offset = peer + cellNamesOffset + startDelta;
            int length = endDelta - startDelta;
            int cmp = FastByteOperations.UnsafeOperations.compareTo(null, offset, length, that.get(i));
            if (cmp != 0)
                return cmp;
            startDelta = endDelta;
        }

        EOC eoc = that.eoc();
        if (size == size2)
            return this.eoc().compareTo(eoc);

        return size < size2 ? this.eoc().prefixComparisonResult : -eoc.prefixComparisonResult;
    }

    public final int compareToSimple(final Composite that)
    {
        assert size() == 1 && that.size() == 1;
        int length = valueStartOffset() - nameDeltaOffset(1);
        long offset = peer + nameDeltaOffset(1);
        return FastByteOperations.UnsafeOperations.compareTo(null, offset, length, that.get(0));
    }
}
