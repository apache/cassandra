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
import java.io.IOException;
import java.security.MessageDigest;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.OnDiskAtom.Serializer;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.Interval;

public class RangeTombstone extends Interval<Composite, DeletionTime> implements OnDiskAtom
{
    public RangeTombstone(Composite start, Composite stop, long markedForDeleteAt, int localDeletionTime)
    {
        this(start, stop, new DeletionTime(markedForDeleteAt, localDeletionTime));
    }

    public RangeTombstone(Composite start, Composite stop, DeletionTime delTime)
    {
        super(start, stop, delTime);
    }

    public Composite name()
    {
        return min;
    }

    public int getLocalDeletionTime()
    {
        return data.localDeletionTime;
    }

    public long timestamp()
    {
        return data.markedForDeleteAt;
    }

    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        metadata.comparator.validate(min);
        metadata.comparator.validate(max);
    }

    public void updateDigest(MessageDigest digest)
    {
        digest.update(min.toByteBuffer().duplicate());
        digest.update(max.toByteBuffer().duplicate());

        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            buffer.writeLong(data.markedForDeleteAt);
            digest.update(buffer.getData(), 0, buffer.getLength());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * This tombstone supersedes another one if it is more recent and cover a
     * bigger range than rt.
     */
    public boolean supersedes(RangeTombstone rt, Comparator<Composite> comparator)
    {
        if (rt.data.markedForDeleteAt > data.markedForDeleteAt)
            return false;

        return comparator.compare(min, rt.min) <= 0 && comparator.compare(max, rt.max) >= 0;
    }

    public boolean includes(Comparator<Composite> comparator, Composite name)
    {
        return comparator.compare(name, min) >= 0 && comparator.compare(name, max) <= 0;
    }

    /**
     * Tracks opened RangeTombstones when iterating over a partition.
     * <p>
     * This tracker must be provided all the atoms of a given partition in
     * order (to the {@code update} method). Given this, it keeps enough
     * information to be able to decide if one of an atom is deleted (shadowed)
     * by a previously open RT. One the tracker can prove a given range
     * tombstone cannot be useful anymore (that is, as soon as we've seen an
     * atom that is after the end of that RT), it discards this RT. In other
     * words, the maximum memory used by this object should be proportional to
     * the maximum number of RT that can be simultaneously open (and this
     * should fairly low in practice).
     */
    public static class Tracker
    {
        private final Comparator<Composite> comparator;

        // A list the currently open RTs. We keep the list sorted in order of growing end bounds as for a
        // new atom, this allows to efficiently find the RTs that are now useless (if any). Also note that because
        // atom are passed to the tracker in order, any RT that is tracked can be assumed as opened, i.e. we
        // never have to test the RTs start since it's always assumed to be less than what we have.
        // Also note that this will store expired RTs (#7810). Those will be of type ExpiredRangeTombstone and
        // will be ignored by writeOpenedMarker.
        private final List<RangeTombstone> openedTombstones = new LinkedList<>();

        // Holds tombstones that are processed but not yet written out. Delaying the write allows us to remove
        // duplicate / completely covered tombstones.
        // Sorted in open order (to be written in that order).
        private final Set<RangeTombstone> unwrittenTombstones = new LinkedHashSet<>();

        // Total number of atoms written by writeOpenedMarker().
        private int atomCount;

        /**
         * Creates a new tracker given the table comparator.
         *
         * @param comparator the comparator for the table this will track atoms
         * for. The tracker assumes that atoms will be later provided to the
         * tracker in {@code comparator} order.
         */
        public Tracker(Comparator<Composite> comparator)
        {
            this.comparator = comparator;
        }

        /**
         * Computes the RangeTombstone that are needed at the beginning of an index
         * block starting with {@code firstColumn}.
         *
         * @return the total serialized size of said tombstones and write them to
         * {@code out} it if isn't null.
         */
        public long writeOpenedMarkers(Composite startPos, DataOutputPlus out, OnDiskAtom.SerializerForWriting atomSerializer) throws IOException
        {
            long size = 0;

            for (RangeTombstone rt : openedTombstones)
            {
                if (rt instanceof ExpiredRangeTombstone || comparator.compare(rt.max, startPos) < 0)
                    continue;

                size += writeTombstone(rt, out, atomSerializer);
            }
            return size;
        }

        /**
         * Writes out all tombstones that have been accepted after the previous call of this method.
         * Tombstones are not written immediately to permit redundant ones to be skipped.
         *
         * @return the serialized size of written tombstones
         */
        public long writeUnwrittenTombstones(DataOutputPlus out, OnDiskAtom.SerializerForWriting atomSerializer) throws IOException
        {
            long size = 0;
            for (RangeTombstone rt : unwrittenTombstones)
            {
                size += writeTombstone(rt, out, atomSerializer);
            }
            unwrittenTombstones.clear();
            return size;
        }

        private long writeTombstone(RangeTombstone rt, DataOutputPlus out, OnDiskAtom.SerializerForWriting atomSerializer)
                throws IOException
        {
            long size = atomSerializer.serializedSizeForSSTable(rt);
            atomCount++;
            if (out != null)
                atomSerializer.serializeForSSTable(rt, out);
            return size;
        }

        /**
         * The total number of atoms written by calls to the above methods.
         */
        public int writtenAtom()
        {
            return atomCount;
        }

        /**
         * Update this tracker given an {@code atom}.
         * <p>
         * This method first test if some range tombstone can be discarded due
         * to the knowledge of that new atom. Then, if it's a range tombstone,
         * it adds it to the tracker.
         * <p>
         * Note that this method should be called on *every* atom of a partition for
         * the tracker to work as efficiently as possible (#9486).
         */
        public boolean update(OnDiskAtom atom, boolean isExpired)
        {
            // Get rid of now useless RTs
            ListIterator<RangeTombstone> iterator = openedTombstones.listIterator();
            while (iterator.hasNext())
            {
                // If this tombstone stops before the new atom, it is now useless since it cannot cover this or any future
                // atoms. Otherwise, if a RT ends after the new atom, then we know that's true of any following atom too
                // since maxOrderingSet is sorted by end bounds
                RangeTombstone t = iterator.next();
                if (comparator.compare(atom.name(), t.max) > 0)
                {
                    iterator.remove();
                    // The iterator may still be in the unwrittenTombstones list. That's ok, it still needs to be written
                    // but it can't influence anything else.
                }
                else
                {
                    // If the atom is a RT, we'll add it next and for that we want to start by looking at the atom we just
                    // returned, so rewind the iterator.
                    iterator.previous();
                    break;
                }
            }

            // If it's a RT, adds it.
            if (atom instanceof RangeTombstone)
            {
                RangeTombstone toAdd = (RangeTombstone)atom;

                // We want to maintain openedTombstones in end bounds order so we find where to insert the new element
                // and add it. While doing so, we also check if that new tombstone fully shadow or is fully shadowed
                // by an existing tombstone so we avoid tracking more tombstone than necessary (and we know this will
                // at least happend for start-of-index-block repeated range tombstones).
                while (iterator.hasNext())
                {
                    RangeTombstone existing = iterator.next();
                    int cmp = comparator.compare(toAdd.max, existing.max);
                    if (cmp > 0)
                    {
                        // the new one covers more than the existing one. If the new one happens to also supersedes
                        // the existing one, remove the existing one. In any case, we're not done yet.
                        if (!existing.data.supersedes(toAdd.data))
                        {
                            iterator.remove();
                            // If the existing one starts at the same position as the new, it does not need to be written
                            // (it won't have been yet).
                            if (comparator.compare(toAdd.min, existing.min) == 0)
                                unwrittenTombstones.remove(existing);
                        }
                    }
                    else
                    {
                        // the new one is included in the existing one. If the new one supersedes the existing one,
                        // then we add the new one (and if the new one ends like the existing one, we can actually remove
                        // the existing one), otherwise we can actually ignore it. In any case, we're done.
                        if (!toAdd.data.supersedes(existing.data))
                            return false;

                        if (cmp == 0)
                        {
                            iterator.remove();
                            // If the existing one starts at the same position as the new, it does not need to be written
                            // (it won't have been yet).
                            if (comparator.compare(toAdd.min, existing.min) == 0)
                                unwrittenTombstones.remove(existing);
                        }
                        else
                        {
                            iterator.previous();
                        }
                        // Found the insert position for the new tombstone
                        break;
                    }
                }

                if (isExpired)
                    iterator.add(new ExpiredRangeTombstone(toAdd));
                else
                {
                    iterator.add(toAdd);
                    unwrittenTombstones.add(toAdd);
                }
                return false;
            }
            // Caller should write cell.
            return true;
        }

        /**
         * Tests if the provided column is deleted by one of the tombstone
         * tracked by this tracker.
         * <p>
         * This method should be called on columns in the same order than for the update()
         * method. Note that this method does not update the tracker so the update() method
         * should still be called on {@code column} (it doesn't matter if update is called
         * before or after this call).
         */
        public boolean isDeleted(Cell cell)
        {
            // We know every tombstone kept are "open", start before the column. So the
            // column is deleted if any of the tracked tombstone ends after the column
            // (this will be the case of every RT if update() has been called before this
            // method, but we might have a few RT to skip otherwise) and the RT deletion is
            // actually more recent than the column timestamp.
            for (RangeTombstone tombstone : openedTombstones)
            {
                if (comparator.compare(cell.name(), tombstone.max) <= 0
                    && tombstone.timestamp() >= cell.timestamp())
                    return true;
            }
            return false;
        }

        public boolean hasUnwrittenTombstones()
        {
            return !unwrittenTombstones.isEmpty();
        }

        /**
         * The tracker needs to track expired range tombstone but keep tracks that they are
         * expired, so this is what this class is used for.
         */
        private static class ExpiredRangeTombstone extends RangeTombstone
        {
            private ExpiredRangeTombstone(RangeTombstone tombstone)
            {
                super(tombstone.min, tombstone.max, tombstone.data);
            }
        }
    }

    public static class Serializer implements ISSTableSerializer<RangeTombstone>
    {
        private final CType type;

        public Serializer(CType type)
        {
            this.type = type;
        }

        public void serializeForSSTable(RangeTombstone t, DataOutputPlus out) throws IOException
        {
            type.serializer().serialize(t.min, out);
            out.writeByte(ColumnSerializer.RANGE_TOMBSTONE_MASK);
            type.serializer().serialize(t.max, out);
            DeletionTime.serializer.serialize(t.data, out);
        }

        public RangeTombstone deserializeFromSSTable(DataInput in, Version version) throws IOException
        {
            Composite min = type.serializer().deserialize(in);

            int b = in.readUnsignedByte();
            assert (b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0;
            return deserializeBody(in, min, version);
        }

        public RangeTombstone deserializeBody(DataInput in, Composite min, Version version) throws IOException
        {
            Composite max = type.serializer().deserialize(in);
            DeletionTime dt = DeletionTime.serializer.deserialize(in);
            // If the max equals the min.end(), we can avoid keeping an extra ByteBuffer in memory by using
            // min.end() instead of max
            Composite minEnd = min.end();
            max = minEnd.equals(max) ? minEnd : max;
            return new RangeTombstone(min, max, dt);
        }

        public void skipBody(DataInput in, Version version) throws IOException
        {
            type.serializer().skip(in);
            DeletionTime.serializer.skip(in);
        }

        public long serializedSizeForSSTable(RangeTombstone t)
        {
            TypeSizes typeSizes = TypeSizes.NATIVE;
            return type.serializer().serializedSize(t.min, typeSizes)
                 + 1 // serialization flag
                 + type.serializer().serializedSize(t.max, typeSizes)
                 + DeletionTime.serializer.serializedSize(t.data, typeSizes);
        }
    }
}
