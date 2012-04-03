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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Interval;

public class RangeTombstone extends Interval<ByteBuffer, DeletionTime> implements OnDiskAtom
{
    public static final Serializer serializer = new Serializer();

    public RangeTombstone(ByteBuffer start, ByteBuffer stop, long markedForDeleteAt, int localDeletionTime)
    {
        this(start, stop, new DeletionTime(markedForDeleteAt, localDeletionTime));
    }

    public RangeTombstone(ByteBuffer start, ByteBuffer stop, DeletionTime delTime)
    {
        super(start, stop, delTime);
    }

    public ByteBuffer name()
    {
        return min;
    }

    public int getLocalDeletionTime()
    {
        return data.localDeletionTime;
    }

    public long maxTimestamp()
    {
        return data.markedForDeleteAt;
    }

    public int serializedSize(TypeSizes typeSizes)
    {
        throw new UnsupportedOperationException();
    }

    public long serializedSizeForSSTable()
    {
        TypeSizes typeSizes = TypeSizes.NATIVE;
        return typeSizes.sizeof((short)min.remaining()) + min.remaining()
             + 1 // serialization flag
             + typeSizes.sizeof((short)max.remaining()) + max.remaining()
             + DeletionTime.serializer.serializedSize(data, typeSizes);
    }

    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        AbstractType<?> nameValidator = metadata.cfType == ColumnFamilyType.Super ? metadata.subcolumnComparator : metadata.comparator;
        nameValidator.validate(min);
        nameValidator.validate(max);
    }

    public void updateDigest(MessageDigest digest)
    {
        digest.update(min.duplicate());
        digest.update(max.duplicate());
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            buffer.writeLong(data.markedForDeleteAt);
            buffer.writeInt(data.localDeletionTime);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        digest.update(buffer.getData(), 0, buffer.getLength());
    }

    /**
     * This tombstone supersedes another one if it is more recent and cover a
     * bigger range than rt.
     */
    public boolean supersedes(RangeTombstone rt, Comparator<ByteBuffer> comparator)
    {
        if (rt.data.markedForDeleteAt > data.markedForDeleteAt)
            return false;

        return comparator.compare(min, rt.min) <= 0 && comparator.compare(max, rt.max) >= 0;
    }

    public static class Tracker
    {
        private final Comparator<ByteBuffer> comparator;
        private final Deque<RangeTombstone> ranges = new ArrayDeque<RangeTombstone>();
        private final SortedSet<RangeTombstone> maxOrderingSet = new TreeSet<RangeTombstone>(new Comparator<RangeTombstone>()
        {
            public int compare(RangeTombstone t1, RangeTombstone t2)
            {
                return comparator.compare(t1.max, t2.max);
            }
        });
        private int atomCount;

        public Tracker(Comparator<ByteBuffer> comparator)
        {
            this.comparator = comparator;
        }

        /**
         * Compute RangeTombstone that are needed at the beginning of an index
         * block starting with {@code firstColumn}.
         * Returns the total serialized size of said tombstones and write them
         * to {@code out} it if isn't null.
         */
        public long writeOpenedMarker(OnDiskAtom firstColumn, DataOutput out, OnDiskAtom.Serializer atomSerializer) throws IOException
        {
            long size = 0;
            if (ranges.isEmpty())
                return size;

            /*
             * Compute the marker that needs to be written at the beginning of
             * this block. We need to write one if it the more recent
             * (opened) tombstone for at least some part of its range.
             */
            List<RangeTombstone> toWrite = new LinkedList<RangeTombstone>();
            outer:
            for (RangeTombstone tombstone : ranges)
            {
                // If ever the first column is outside the range, skip it (in
                // case update() hasn't been called yet)
                if (comparator.compare(firstColumn.name(), tombstone.max) > 0)
                    continue;

                RangeTombstone updated = new RangeTombstone(firstColumn.name(), tombstone.max, tombstone.data);

                Iterator<RangeTombstone> iter = toWrite.iterator();
                while (iter.hasNext())
                {
                    RangeTombstone other = iter.next();
                    if (other.supersedes(updated, comparator))
                        break outer;
                    if (updated.supersedes(other, comparator))
                        iter.remove();
                }
                toWrite.add(tombstone);
            }

            TypeSizes typeSizes = TypeSizes.NATIVE;
            for (RangeTombstone tombstone : toWrite)
            {
                size += tombstone.serializedSize(typeSizes);
                atomCount++;
                if (out != null)
                    atomSerializer.serializeForSSTable(tombstone, out);
            }
            return size;
        }

        public int writtenAtom()
        {
            return atomCount;
        }

        /**
         * Update this tracker given an {@code atom}.
         * If column is a IColumn, check if any tracked range is useless and
         * can be removed. If it is a RangeTombstone, add it to this tracker.
         */
        public void update(OnDiskAtom atom)
        {
            if (atom instanceof RangeTombstone)
            {
                RangeTombstone t = (RangeTombstone)atom;
                // This could be a repeated marker already. If so, we already have a range in which it is
                // fully included. While keeping both would be ok functionaly, we could end up with a lot of
                // useless marker after a few compaction, so avoid this.
                for (RangeTombstone tombstone : maxOrderingSet.tailSet(t))
                {
                    // We only care about tombstone have the same max than t
                    if (comparator.compare(t.max, tombstone.max) > 0)
                        break;

                    // Since it is assume tombstones are passed to this method in growing min order, it's enough to
                    // check for the data to know is the current tombstone is included in a previous one
                    if (tombstone.data.equals(t.data))
                        return;
                }
                ranges.addLast(t);
                maxOrderingSet.add(t);
            }
            else
            {
                assert atom instanceof IColumn;
                Iterator<RangeTombstone> iter = maxOrderingSet.iterator();
                while (iter.hasNext())
                {
                    RangeTombstone tombstone = iter.next();
                    if (comparator.compare(atom.name(), tombstone.max) > 0)
                    {
                        // That tombstone is now useless
                        iter.remove();
                        ranges.remove(tombstone);
                    }
                    else
                    {
                        // Since we're iterating by growing end bound, if the current range
                        // includes the column, so does all the next ones
                        return;
                    }
                }
            }
        }

        public boolean isDeleted(IColumn column)
        {
            for (RangeTombstone tombstone : ranges)
            {
                if (comparator.compare(column.name(), tombstone.max) <= 0 && tombstone.data.isDeleted(column))
                    return true;
            }
            return false;
        }
    }

    public static class Serializer implements ISSTableSerializer<RangeTombstone>
    {
        public void serializeForSSTable(RangeTombstone t, DataOutput dos) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(t.min, dos);
            dos.writeByte(ColumnSerializer.RANGE_TOMBSTONE_MASK);
            ByteBufferUtil.writeWithShortLength(t.max, dos);
            DeletionTime.serializer.serialize(t.data, dos);
        }

        public RangeTombstone deserializeFromSSTable(DataInput dis, Descriptor.Version version) throws IOException
        {
            ByteBuffer min = ByteBufferUtil.readWithShortLength(dis);
            if (min.remaining() <= 0)
                throw ColumnSerializer.CorruptColumnException.create(dis, min);

            int b = dis.readUnsignedByte();
            assert (b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0;
            return deserializeBody(dis, min, version);
        }

        public RangeTombstone deserializeBody(DataInput dis, ByteBuffer min, Descriptor.Version version) throws IOException
        {
            ByteBuffer max = ByteBufferUtil.readWithShortLength(dis);
            if (max.remaining() <= 0)
                throw ColumnSerializer.CorruptColumnException.create(dis, max);

            DeletionTime dt = DeletionTime.serializer.deserialize(dis);
            return new RangeTombstone(min, max, dt);
        }
    }
}
