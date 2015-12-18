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

import java.io.IOException;
import java.io.IOError;
import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.PeekingIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.net.MessagingService;

/**
 * Helper class to deserialize Unfiltered object from disk efficiently.
 *
 * More precisely, this class is used by the low-level reader to ensure
 * we don't do more work than necessary (i.e. we don't allocate/deserialize
 * objects for things we don't care about).
 */
public abstract class UnfilteredDeserializer
{
    private static final Logger logger = LoggerFactory.getLogger(UnfilteredDeserializer.class);

    protected final CFMetaData metadata;
    protected final DataInputPlus in;
    protected final SerializationHelper helper;

    protected UnfilteredDeserializer(CFMetaData metadata,
                                     DataInputPlus in,
                                     SerializationHelper helper)
    {
        this.metadata = metadata;
        this.in = in;
        this.helper = helper;
    }

    public static UnfilteredDeserializer create(CFMetaData metadata,
                                                DataInputPlus in,
                                                SerializationHeader header,
                                                SerializationHelper helper,
                                                DeletionTime partitionDeletion,
                                                boolean readAllAsDynamic)
    {
        if (helper.version >= MessagingService.VERSION_30)
            return new CurrentDeserializer(metadata, in, header, helper);
        else
            return new OldFormatDeserializer(metadata, in, helper, partitionDeletion, readAllAsDynamic);
    }

    /**
     * Whether or not there is more atom to read.
     */
    public abstract boolean hasNext() throws IOException;

    /**
     * Compare the provided bound to the next atom to read on disk.
     *
     * This will not read/deserialize the whole atom but only what is necessary for the
     * comparison. Whenever we know what to do with this atom (read it or skip it),
     * readNext or skipNext should be called.
     */
    public abstract int compareNextTo(Slice.Bound bound) throws IOException;

    /**
     * Returns whether the next atom is a row or not.
     */
    public abstract boolean nextIsRow() throws IOException;

    /**
     * Returns whether the next atom is the static row or not.
     */
    public abstract boolean nextIsStatic() throws IOException;

    /**
     * Returns the next atom.
     */
    public abstract Unfiltered readNext() throws IOException;

    /**
     * Clears any state in this deserializer.
     */
    public abstract void clearState() throws IOException;

    /**
     * Skips the next atom.
     */
    public abstract void skipNext() throws IOException;


    /**
     * For the legacy layout deserializer, we have to deal with the fact that a row can span multiple index blocks and that
     * the call to hasNext() reads the next element upfront. We must take that into account when we check in AbstractSSTableIterator if
     * we're past the end of an index block boundary as that check expect to account for only consumed data (that is, if hasNext has
     * been called and made us cross an index boundary but neither readNext() or skipNext() as yet been called, we shouldn't consider
     * the index block boundary crossed yet).
     *
     * TODO: we don't care about this for the current file format because a row can never span multiple index blocks (further, hasNext()
     * only just basically read 2 bytes from disk in that case). So once we drop backward compatibility with pre-3.0 sstable, we should
     * remove this.
     */
    public abstract long bytesReadForUnconsumedData();

    private static class CurrentDeserializer extends UnfilteredDeserializer
    {
        private final ClusteringPrefix.Deserializer clusteringDeserializer;
        private final SerializationHeader header;

        private int nextFlags;
        private int nextExtendedFlags;
        private boolean isReady;
        private boolean isDone;

        private final Row.Builder builder;

        private CurrentDeserializer(CFMetaData metadata,
                                    DataInputPlus in,
                                    SerializationHeader header,
                                    SerializationHelper helper)
        {
            super(metadata, in, helper);
            this.header = header;
            this.clusteringDeserializer = new ClusteringPrefix.Deserializer(metadata.comparator, in, header);
            this.builder = BTreeRow.sortedBuilder();
        }

        public boolean hasNext() throws IOException
        {
            if (isReady)
                return true;

            prepareNext();
            return !isDone;
        }

        private void prepareNext() throws IOException
        {
            if (isDone)
                return;

            nextFlags = in.readUnsignedByte();
            if (UnfilteredSerializer.isEndOfPartition(nextFlags))
            {
                isDone = true;
                isReady = false;
                return;
            }

            nextExtendedFlags = UnfilteredSerializer.readExtendedFlags(in, nextFlags);

            clusteringDeserializer.prepare(nextFlags, nextExtendedFlags);
            isReady = true;
        }

        public int compareNextTo(Slice.Bound bound) throws IOException
        {
            if (!isReady)
                prepareNext();

            assert !isDone;

            return clusteringDeserializer.compareNextTo(bound);
        }

        public boolean nextIsRow() throws IOException
        {
            if (!isReady)
                prepareNext();

            return UnfilteredSerializer.kind(nextFlags) == Unfiltered.Kind.ROW;
        }

        public boolean nextIsStatic() throws IOException
        {
            // This exists only for the sake of the OldFormatDeserializer
            throw new UnsupportedOperationException();
        }

        public Unfiltered readNext() throws IOException
        {
            isReady = false;
            if (UnfilteredSerializer.kind(nextFlags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
            {
                RangeTombstone.Bound bound = clusteringDeserializer.deserializeNextBound();
                return UnfilteredSerializer.serializer.deserializeMarkerBody(in, header, bound);
            }
            else
            {
                builder.newRow(clusteringDeserializer.deserializeNextClustering());
                return UnfilteredSerializer.serializer.deserializeRowBody(in, header, helper, nextFlags, nextExtendedFlags, builder);
            }
        }

        public void skipNext() throws IOException
        {
            isReady = false;
            clusteringDeserializer.skipNext();
            if (UnfilteredSerializer.kind(nextFlags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
            {
                UnfilteredSerializer.serializer.skipMarkerBody(in);
            }
            else
            {
                UnfilteredSerializer.serializer.skipRowBody(in);
            }
        }

        public void clearState()
        {
            isReady = false;
            isDone = false;
        }

        public long bytesReadForUnconsumedData()
        {
            // In theory, hasNext() does consume 2-3 bytes, but we don't care about this for the current file format so returning
            // 0 to mean "do nothing".
            return 0;
        }
    }

    public static class OldFormatDeserializer extends UnfilteredDeserializer
    {
        private final boolean readAllAsDynamic;
        private boolean skipStatic;

        // The next Unfiltered to return, computed by hasNext()
        private Unfiltered next;
        // A temporary storage for an unfiltered that isn't returned next but should be looked at just afterwards
        private Unfiltered saved;

        private boolean isFirst = true;

        // The Unfiltered as read from the old format input
        private final UnfilteredIterator iterator;

        // The position in the input after the last data consumption (readNext/skipNext).
        private long lastConsumedPosition;

        private OldFormatDeserializer(CFMetaData metadata,
                                      DataInputPlus in,
                                      SerializationHelper helper,
                                      DeletionTime partitionDeletion,
                                      boolean readAllAsDynamic)
        {
            super(metadata, in, helper);
            this.iterator = new UnfilteredIterator(partitionDeletion);
            this.readAllAsDynamic = readAllAsDynamic;
            this.lastConsumedPosition = currentPosition();
        }

        public void setSkipStatic()
        {
            this.skipStatic = true;
        }

        private boolean isStatic(Unfiltered unfiltered)
        {
            return unfiltered.isRow() && ((Row)unfiltered).isStatic();
        }

        public boolean hasNext() throws IOException
        {
            try
            {
                while (next == null)
                {
                    if (saved == null && !iterator.hasNext())
                        return false;

                    next = saved == null ? iterator.next() : saved;
                    saved = null;

                    // The sstable iterators assume that if there is one, the static row is the first thing this deserializer will return.
                    // However, in the old format, a range tombstone with an empty start would sort before any static cell. So we should
                    // detect that case and return the static parts first if necessary.
                    if (isFirst && iterator.hasNext() && isStatic(iterator.peek()))
                    {
                        saved = next;
                        next = iterator.next();
                    }
                    isFirst = false;

                    // When reading old tables, we sometimes want to skip static data (due to how staticly defined column of compact
                    // tables are handled).
                    if (skipStatic && isStatic(next))
                        next = null;
                }
                return true;
            }
            catch (IOError e)
            {
                if (e.getCause() != null && e.getCause() instanceof IOException)
                    throw (IOException)e.getCause();
                throw e;
            }
        }

        private boolean isRow(LegacyLayout.LegacyAtom atom)
        {
            if (atom.isCell())
                return true;

            LegacyLayout.LegacyRangeTombstone tombstone = atom.asRangeTombstone();
            return tombstone.isCollectionTombstone() || tombstone.isRowDeletion(metadata);
        }

        public int compareNextTo(Slice.Bound bound) throws IOException
        {
            if (!hasNext())
                throw new IllegalStateException();
            return metadata.comparator.compare(next.clustering(), bound);
        }

        public boolean nextIsRow() throws IOException
        {
            if (!hasNext())
                throw new IllegalStateException();
            return next.isRow();
        }

        public boolean nextIsStatic() throws IOException
        {
            return nextIsRow() && ((Row)next).isStatic();
        }

        private long currentPosition()
        {
            // We return a bogus value if the input is not file based, but check we never rely
            // on that value in that case in bytesReadForUnconsumedData
            return in instanceof FileDataInput ? ((FileDataInput)in).getFilePointer() : 0;
        }

        public Unfiltered readNext() throws IOException
        {
            if (!hasNext())
                throw new IllegalStateException();
            Unfiltered toReturn = next;
            next = null;
            lastConsumedPosition = currentPosition();
            return toReturn;
        }

        public void skipNext() throws IOException
        {
            if (!hasNext())
                throw new UnsupportedOperationException();
            next = null;
            lastConsumedPosition = currentPosition();
        }

        public long bytesReadForUnconsumedData()
        {
            if (!(in instanceof FileDataInput))
                throw new AssertionError();

            return currentPosition() - lastConsumedPosition;
        }

        public void clearState()
        {
            next = null;
            saved = null;
            iterator.clearState();
            lastConsumedPosition = currentPosition();
        }

        // Groups atoms from the input into proper Unfiltered.
        // Note: this could use guava AbstractIterator except that we want to be able to clear
        // the internal state of the iterator so it's cleaner to do it ourselves.
        private class UnfilteredIterator implements PeekingIterator<Unfiltered>
        {
            private final AtomIterator atoms;
            private final LegacyLayout.CellGrouper grouper;
            private final TombstoneTracker tombstoneTracker;

            private Unfiltered next;

            private UnfilteredIterator(DeletionTime partitionDeletion)
            {
                this.grouper = new LegacyLayout.CellGrouper(metadata, helper);
                this.tombstoneTracker = new TombstoneTracker(partitionDeletion);
                this.atoms = new AtomIterator(tombstoneTracker);
            }

            public boolean hasNext()
            {
                // Note that we loop on next == null because TombstoneTracker.openNew() could return null below or the atom might be shadowed.
                while (next == null)
                {
                    if (atoms.hasNext())
                    {
                        // If a range tombstone closes strictly before the next row/RT, we need to return that close (or boundary) marker first.
                        if (tombstoneTracker.hasClosingMarkerBefore(atoms.peek()))
                        {
                            next = tombstoneTracker.popClosingMarker();
                        }
                        else
                        {
                            LegacyLayout.LegacyAtom atom = atoms.next();
                            if (!tombstoneTracker.isShadowed(atom))
                                next = isRow(atom) ? readRow(atom) : tombstoneTracker.openNew(atom.asRangeTombstone());
                        }
                    }
                    else if (tombstoneTracker.hasOpenTombstones())
                    {
                        next = tombstoneTracker.popClosingMarker();
                    }
                    else
                    {
                        return false;
                    }
                }
                return true;
            }

            private Unfiltered readRow(LegacyLayout.LegacyAtom first)
            {
                LegacyLayout.CellGrouper grouper = first.isStatic()
                                                 ? LegacyLayout.CellGrouper.staticGrouper(metadata, helper)
                                                 : this.grouper;
                grouper.reset();
                grouper.addAtom(first);
                // As long as atoms are part of the same row, consume them. Note that the call to addAtom() uses
                // atoms.peek() so that the atom is only consumed (by next) if it's part of the row (addAtom returns true)
                while (atoms.hasNext() && grouper.addAtom(atoms.peek()))
                {
                    atoms.next();
                }
                return grouper.getRow();
            }

            public Unfiltered next()
            {
                if (!hasNext())
                    throw new UnsupportedOperationException();
                Unfiltered toReturn = next;
                next = null;
                return toReturn;
            }

            public Unfiltered peek()
            {
                if (!hasNext())
                    throw new UnsupportedOperationException();
                return next;
            }

            public void clearState()
            {
                atoms.clearState();
                tombstoneTracker.clearState();
                next = null;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        }

        // Wraps the input of the deserializer to provide an iterator (and skip shadowed atoms).
        // Note: this could use guava AbstractIterator except that we want to be able to clear
        // the internal state of the iterator so it's cleaner to do it ourselves.
        private class AtomIterator implements PeekingIterator<LegacyLayout.LegacyAtom>
        {
            private final TombstoneTracker tombstoneTracker;
            private boolean isDone;
            private LegacyLayout.LegacyAtom next;

            private AtomIterator(TombstoneTracker tombstoneTracker)
            {
                this.tombstoneTracker = tombstoneTracker;
            }

            public boolean hasNext()
            {
                if (isDone)
                    return false;

                if (next == null)
                {
                    next = readAtom();
                    if (next == null)
                    {
                        isDone = true;
                        return false;
                    }
                }
                return true;
            }

            private LegacyLayout.LegacyAtom readAtom()
            {
                try
                {
                    return LegacyLayout.readLegacyAtom(metadata, in, readAllAsDynamic);
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }

            public LegacyLayout.LegacyAtom next()
            {
                if (!hasNext())
                    throw new UnsupportedOperationException();
                LegacyLayout.LegacyAtom toReturn = next;
                next = null;
                return toReturn;
            }

            public LegacyLayout.LegacyAtom peek()
            {
                if (!hasNext())
                    throw new UnsupportedOperationException();
                return next;
            }

            public void clearState()
            {
                this.next = null;
                this.isDone = false;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        }

        /**
         * Tracks which range tombstones are open when deserializing the old format.
         */
        private class TombstoneTracker
        {
            private final DeletionTime partitionDeletion;

            // Open tombstones sorted by their closing bound (i.e. first tombstone is the first to close).
            // As we only track non-fully-shadowed ranges, the first range is necessarily the currently
            // open tombstone (the one with the higher timestamp).
            private final SortedSet<LegacyLayout.LegacyRangeTombstone> openTombstones;

            public TombstoneTracker(DeletionTime partitionDeletion)
            {
                this.partitionDeletion = partitionDeletion;
                this.openTombstones = new TreeSet<>((rt1, rt2) -> metadata.comparator.compare(rt1.stop.bound, rt2.stop.bound));
            }

            /**
             * Checks if the provided atom is fully shadowed by the open tombstones of this tracker (or the partition deletion).
             */
            public boolean isShadowed(LegacyLayout.LegacyAtom atom)
            {
                assert !hasClosingMarkerBefore(atom);
                long timestamp = atom.isCell() ? atom.asCell().timestamp : atom.asRangeTombstone().deletionTime.markedForDeleteAt();

                if (partitionDeletion.deletes(timestamp))
                    return true;

                SortedSet<LegacyLayout.LegacyRangeTombstone> coveringTombstones = isRow(atom) ? openTombstones : openTombstones.tailSet(atom.asRangeTombstone());
                return Iterables.any(coveringTombstones, tombstone -> tombstone.deletionTime.deletes(timestamp));
            }

            /**
             * Whether the currently open marker closes stricly before the provided row/RT.
             */
            public boolean hasClosingMarkerBefore(LegacyLayout.LegacyAtom atom)
            {
                return !openTombstones.isEmpty()
                    && metadata.comparator.compare(openTombstones.first().stop.bound, atom.clustering()) < 0;
            }

            /**
             * Returns the unfiltered corresponding to closing the currently open marker (and update the tracker accordingly).
             */
            public Unfiltered popClosingMarker()
            {
                assert !openTombstones.isEmpty();

                Iterator<LegacyLayout.LegacyRangeTombstone> iter = openTombstones.iterator();
                LegacyLayout.LegacyRangeTombstone first = iter.next();
                iter.remove();

                // If that was the last open tombstone, we just want to close it. Otherwise, we have a boundary with the
                // next tombstone
                if (!iter.hasNext())
                    return new RangeTombstoneBoundMarker(first.stop.bound, first.deletionTime);

                LegacyLayout.LegacyRangeTombstone next = iter.next();
                return RangeTombstoneBoundaryMarker.makeBoundary(false, first.stop.bound, first.stop.bound.invert(), first.deletionTime, next.deletionTime);
            }

            /**
             * Update the tracker given the provided newly open tombstone. This return the Unfiltered corresponding to the opening
             * of said tombstone: this can be a simple open mark, a boundary (if there was an open tombstone superseded by this new one)
             * or even null (if the new tombston start is supersedes by the currently open tombstone).
             *
             * Note that this method assume the added tombstone is not fully shadowed, i.e. that !isShadowed(tombstone). It also
             * assumes no opened tombstone closes before that tombstone (so !hasClosingMarkerBefore(tombstone)).
             */
            public Unfiltered openNew(LegacyLayout.LegacyRangeTombstone tombstone)
            {
                if (openTombstones.isEmpty())
                {
                    openTombstones.add(tombstone);
                    return new RangeTombstoneBoundMarker(tombstone.start.bound, tombstone.deletionTime);
                }

                Iterator<LegacyLayout.LegacyRangeTombstone> iter = openTombstones.iterator();
                LegacyLayout.LegacyRangeTombstone first = iter.next();
                if (tombstone.deletionTime.supersedes(first.deletionTime))
                {
                    // We're supperseding the currently open tombstone, so we should produce a boundary that close the currently open
                    // one and open the new one. We should also add the tombstone, but if it stop after the first one, we should
                    // also remove that first tombstone as it won't be useful anymore.
                    if (metadata.comparator.compare(tombstone.stop.bound, first.stop.bound) >= 0)
                        iter.remove();

                    openTombstones.add(tombstone);
                    return RangeTombstoneBoundaryMarker.makeBoundary(false, tombstone.start.bound.invert(), tombstone.start.bound, first.deletionTime, tombstone.deletionTime);
                }
                else
                {
                    // If the new tombstone don't supersedes the currently open tombstone, we don't have anything to return, we
                    // just add the new tombstone (because we know tombstone is not fully shadowed, this imply the new tombstone
                    // simply extend after the first one and we'll deal with it later)
                    assert metadata.comparator.compare(tombstone.start.bound, first.stop.bound) > 0;
                    openTombstones.add(tombstone);
                    return null;
                }
            }

            public boolean hasOpenTombstones()
            {
                return !openTombstones.isEmpty();
            }

            private boolean formBoundary(LegacyLayout.LegacyRangeTombstone close, LegacyLayout.LegacyRangeTombstone open)
            {
                return metadata.comparator.compare(close.stop.bound, open.start.bound) == 0;
            }

            public void clearState()
            {
                openTombstones.clear();
            }
        }
    }
}
