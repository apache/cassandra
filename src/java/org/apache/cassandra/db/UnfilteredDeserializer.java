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
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.PeekingIterator;

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
    public abstract int compareNextTo(ClusteringBound bound) throws IOException;

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

        public int compareNextTo(ClusteringBound bound) throws IOException
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
                ClusteringBoundOrBoundary bound = clusteringDeserializer.deserializeNextBound();
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

        // Saved position in the input after the next Unfiltered that will be consumed
        private long nextConsumedPosition;

        // A temporary storage for an Unfiltered that isn't returned next but should be looked at just afterwards
        private Stash stash;

        private boolean couldBeStartOfPartition = true;

        // The Unfiltered as read from the old format input
        private final UnfilteredIterator iterator;

        // The position in the input after the last data consumption (readNext/skipNext).
        private long lastConsumedPosition;

        // Tracks the size of the last LegacyAtom read from disk, because this needs to be accounted
        // for when marking lastConsumedPosition after readNext/skipNext
        // Reading/skipping an Unfiltered consumes LegacyAtoms from the underlying legacy atom iterator
        // e.g. hasNext() -> iterator.hasNext() -> iterator.readRow() -> atoms.next()
        // The stop condition of the loop which groups legacy atoms into rows causes that AtomIterator
        // to read in the first atom which doesn't belong in the row. So by that point, our position
        // is actually past the end of the next Unfiltered. To compensate, we record the size of
        // the last LegacyAtom read and subtract it from the current position when we calculate lastConsumedPosition.
        // If we don't, then when reading an indexed block, we can over correct and may think that we've
        // exhausted the block before we actually have.
        private long bytesReadForNextAtom = 0L;

        private OldFormatDeserializer(CFMetaData metadata,
                                      DataInputPlus in,
                                      SerializationHelper helper,
                                      DeletionTime partitionDeletion,
                                      boolean readAllAsDynamic)
        {
            super(metadata, in, helper);
            this.iterator = new UnfilteredIterator(metadata, partitionDeletion, helper, this::readAtom);
            this.readAllAsDynamic = readAllAsDynamic;
            this.lastConsumedPosition = currentPosition();
        }

        private LegacyLayout.LegacyAtom readAtom()
        {
            while (true)
            {
                try
                {
                    long pos = currentPosition();
                    LegacyLayout.LegacyAtom atom = LegacyLayout.readLegacyAtom(metadata, in, readAllAsDynamic);
                    bytesReadForNextAtom = currentPosition() - pos;
                    return atom;
                }
                catch (UnknownColumnException e)
                {
                    // This is ok, see LegacyLayout.readLegacyAtom() for why this only happens in case were we're ok
                    // skipping the cell. We do want to catch this at this level however because when that happen,
                    // we should *not* count the byte of that discarded cell as part of the bytes for the atom
                    // we will eventually return, as doing so could throw the logic bytesReadForNextAtom participates in.
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
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
                    if (null != stash)
                    {
                        next = stash.unfiltered;
                        nextConsumedPosition = stash.consumedPosition;
                        stash = null;
                    }
                    else
                    {
                        if (!iterator.hasNext())
                            return false;
                        next = iterator.next();
                        nextConsumedPosition = currentPosition() - bytesReadForNextAtom;
                    }

                    /*
                     * The sstable iterators assume that if there is one, the static row is the first thing this deserializer will return.
                     * However, in the old format, a range tombstone with an empty start would sort before any static cell. So we should
                     * detect that case and return the static parts first if necessary.
                     */
                    if (couldBeStartOfPartition && next.isRangeTombstoneMarker() && next.clustering().size() == 0 && iterator.hasNext())
                    {
                        Unfiltered unfiltered = iterator.next();
                        long consumedPosition = currentPosition() - bytesReadForNextAtom;

                        stash = new Stash(unfiltered, consumedPosition);

                        /*
                         * reorder next and stash (see the comment above that explains why), but retain their positions
                         * it's ok to do so since consumedPosition value is only used to determine if we have gone past
                         * the end of the index ‘block’; since the edge case requires that the first value be the ‘bottom’
                         * RT bound (i.e. with no byte buffers), this has a small and well-defined size, and it must be
                         * the case that both unfiltered are in the same index ‘block’ if we began at the beginning of it.
                         * if we don't do this, however, we risk aborting early and not returning the BOTTOM rt bound,
                         * if the static row is large enough to cross block boundaries.
                         */
                        if (isStatic(unfiltered))
                        {
                            stash.unfiltered = next;
                            next = unfiltered;
                        }
                    }
                    couldBeStartOfPartition = false;

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

        public int compareNextTo(ClusteringBound bound) throws IOException
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
            lastConsumedPosition = nextConsumedPosition;
            return toReturn;
        }

        public void skipNext() throws IOException
        {
            readNext();
        }

        // in case we had to reorder an empty RT bound with a static row, this won't be returning the precise unconsumed size,
        // that corresponds to the last returned Unfiltered, but use the natural order in the sstable instead
        public long bytesReadForUnconsumedData()
        {
            if (!(in instanceof FileDataInput))
                throw new AssertionError();

            return currentPosition() - lastConsumedPosition;
        }

        public void clearState()
        {
            next = null;
            stash = null;
            couldBeStartOfPartition = true;
            iterator.clearState();
            lastConsumedPosition = currentPosition();
            bytesReadForNextAtom = 0L;
        }

        private static final class Stash
        {
            private Unfiltered unfiltered;
            long consumedPosition;

            private Stash(Unfiltered unfiltered, long consumedPosition)
            {
                this.unfiltered = unfiltered;
                this.consumedPosition = consumedPosition;
            }
        }

        // Groups atoms from the input into proper Unfiltered.
        // Note: this could use guava AbstractIterator except that we want to be able to clear
        // the internal state of the iterator so it's cleaner to do it ourselves.
        @VisibleForTesting
        static class UnfilteredIterator implements PeekingIterator<Unfiltered>
        {
            private final AtomIterator atoms;
            private final LegacyLayout.CellGrouper grouper;
            private final TombstoneTracker tombstoneTracker;
            private final CFMetaData metadata;
            private final SerializationHelper helper;

            private Unfiltered next;

            UnfilteredIterator(CFMetaData metadata,
                               DeletionTime partitionDeletion,
                               SerializationHelper helper,
                               Supplier<LegacyLayout.LegacyAtom> atomReader)
            {
                this.metadata = metadata;
                this.helper = helper;
                this.grouper = new LegacyLayout.CellGrouper(metadata, helper);
                this.tombstoneTracker = new TombstoneTracker(partitionDeletion);
                this.atoms = new AtomIterator(atomReader);
            }

            private boolean isRow(LegacyLayout.LegacyAtom atom)
            {
                if (atom.isCell())
                    return true;

                LegacyLayout.LegacyRangeTombstone tombstone = atom.asRangeTombstone();
                return tombstone.isCollectionTombstone() || tombstone.isRowDeletion(metadata);
            }


            public boolean hasNext()
            {
                // Note that we loop on next == null because TombstoneTracker.openNew() could return null below or the atom might be shadowed.
                while (next == null)
                {
                    if (atoms.hasNext())
                    {
                        // If there is a range tombstone to open strictly before the next row/RT, we need to return that open (or boundary) marker first.
                        if (tombstoneTracker.hasOpeningMarkerBefore(atoms.peek()))
                        {
                            next = tombstoneTracker.popOpeningMarker();
                        }
                        // If a range tombstone closes strictly before the next row/RT, we need to return that close (or boundary) marker first.
                        else if (tombstoneTracker.hasClosingMarkerBefore(atoms.peek()))
                        {
                            next = tombstoneTracker.popClosingMarker();
                        }
                        else
                        {
                            LegacyLayout.LegacyAtom atom = atoms.next();
                            if (tombstoneTracker.isShadowed(atom))
                                continue;

                            if (isRow(atom))
                                next = readRow(atom);
                            else
                                tombstoneTracker.openNew(atom.asRangeTombstone());
                        }
                    }
                    else if (tombstoneTracker.hasOpenTombstones())
                    {
                        next = tombstoneTracker.popMarker();
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

            // Wraps the input of the deserializer to provide an iterator (and skip shadowed atoms).
            // Note: this could use guava AbstractIterator except that we want to be able to clear
            // the internal state of the iterator so it's cleaner to do it ourselves.
            private static class AtomIterator implements PeekingIterator<LegacyLayout.LegacyAtom>
            {
                private final Supplier<LegacyLayout.LegacyAtom> atomReader;
                private boolean isDone;
                private LegacyLayout.LegacyAtom next;

                private AtomIterator(Supplier<LegacyLayout.LegacyAtom> atomReader)
                {
                    this.atomReader = atomReader;
                }

                public boolean hasNext()
                {
                    if (isDone)
                        return false;

                    if (next == null)
                    {
                        next = atomReader.get();
                        if (next == null)
                        {
                            isDone = true;
                            return false;
                        }
                    }
                    return true;
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
             * <p>
             * This is a bit tricky because in the old of format we could have duplicated tombstones, overlapping ones,
             * shadowed ones, etc.., but we should generate from that a "flat" output where at most one non-shadoowed
             * range is open at any given time and without empty range.
             * <p>
             * One consequence of that is that we have to be careful to not generate markers too soon. For instance,
             * we might get a range tombstone [1, 1]@3 followed by [1, 10]@5. So if we generate an opening marker on
             * the first tombstone (so INCL_START(1)@3), we're screwed when we get to the 2nd range tombstone: we really
             * should ignore the first tombstone in that that and generate INCL_START(1)@5 (assuming obviously we don't
             * have one more range tombstone starting at 1 in the stream). This is why we have the
             * {@link #hasOpeningMarkerBefore} method: in practice, we remember when a marker should be opened, but only
             * generate that opening marker when we're sure that we won't get anything shadowing that marker.
             * <p>
             * For closing marker, we also have a {@link #hasClosingMarkerBefore} because in the old format the closing
             * markers comes with the opening one, but we should generate them "in order" in the new format.
             */
            private class TombstoneTracker
            {
                private final DeletionTime partitionDeletion;

                // As explained in the javadoc, we need to wait to generate an opening marker until we're sure we have
                // seen anything that could shadow it. So this remember a marker that needs to be opened but hasn't
                // been yet. This is truly returned when hasOpeningMarkerBefore tells us it's safe to.
                private RangeTombstoneMarker openMarkerToReturn;

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
                 * Whether there is an outstanding opening marker that should be returned before we process the provided row/RT.
                 */
                public boolean hasOpeningMarkerBefore(LegacyLayout.LegacyAtom atom)
                {
                    return openMarkerToReturn != null
                           && metadata.comparator.compare(openMarkerToReturn.openBound(false), atom.clustering()) < 0;
                }

                public Unfiltered popOpeningMarker()
                {
                    assert openMarkerToReturn != null;
                    Unfiltered toReturn = openMarkerToReturn;
                    openMarkerToReturn = null;
                    return toReturn;
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
                  * Pop whatever next marker needs to be popped. This should be called as many time as necessary (until
                  * {@link #hasOpenTombstones} returns {@false}) when all atoms have been consumed to "empty" the tracker.
                  */
                 public Unfiltered popMarker()
                 {
                     assert hasOpenTombstones();
                     return openMarkerToReturn == null ? popClosingMarker() : popOpeningMarker();
                 }

                /**
                 * Update the tracker given the provided newly open tombstone. This potentially update openMarkerToReturn
                 * to account for th new opening.
                 *
                 * Note that this method assumes that:
                 +  1) the added tombstone is not fully shadowed: !isShadowed(tombstone).
                 +  2) there is no marker to open that open strictly before this new tombstone: !hasOpeningMarkerBefore(tombstone).
                 +  3) no opened tombstone closes before that tombstone: !hasClosingMarkerBefore(tombstone).
                 + One can check that this is only called after the condition above have been checked in UnfilteredIterator.hasNext above.
                 */
                public void openNew(LegacyLayout.LegacyRangeTombstone tombstone)
                {
                    if (openTombstones.isEmpty())
                    {
                        // If we have an openMarkerToReturn, the corresponding RT must be in openTombstones (or we wouldn't know when to close it)
                        assert openMarkerToReturn == null;
                        openTombstones.add(tombstone);
                        openMarkerToReturn = new RangeTombstoneBoundMarker(tombstone.start.bound, tombstone.deletionTime);
                        return;
                    }

                    if (openMarkerToReturn != null)
                    {
                        // If the new opening supersedes the one we're about to return, we need to update the one to return.
                        if (tombstone.deletionTime.supersedes(openMarkerToReturn.openDeletionTime(false)))
                            openMarkerToReturn = openMarkerToReturn.withNewOpeningDeletionTime(false, tombstone.deletionTime);
                    }
                    else
                    {
                        // We have no openMarkerToReturn set yet so set it now if needs be.
                        // Since openTombstones isn't empty, it means we have a currently ongoing deletion. And if the new tombstone
                        // supersedes that ongoing deletion, we need to close the opening  deletion and open with the new one.
                        DeletionTime currentOpenDeletion = openTombstones.first().deletionTime;
                        if (tombstone.deletionTime.supersedes(currentOpenDeletion))
                            openMarkerToReturn = RangeTombstoneBoundaryMarker.makeBoundary(false, tombstone.start.bound.invert(), tombstone.start.bound, currentOpenDeletion, tombstone.deletionTime);
                    }

                    // In all cases, we know !isShadowed(tombstone) so we need to add the tombstone (note however that we may not have set openMarkerToReturn if the
                    // new tombstone doesn't supersedes the current deletion _but_ extend past the marker currently open)
                    add(tombstone);
                }

                /**
                 * Adds a new tombstone to openTombstones, removing anything that would be shadowed by this new tombstone.
                 */
                private void add(LegacyLayout.LegacyRangeTombstone tombstone)
                {
                    // First, remove existing tombstone that is shadowed by this tombstone.
                    Iterator<LegacyLayout.LegacyRangeTombstone> iter = openTombstones.iterator();
                    while (iter.hasNext())
                    {

                        LegacyLayout.LegacyRangeTombstone existing = iter.next();
                        // openTombstones is ordered by stop bound and the new tombstone can't be shadowing anything that
                        // stop after it.
                        if (metadata.comparator.compare(tombstone.stop.bound, existing.stop.bound) < 0)
                            break;

                        // Note that we remove an existing tombstone even if it is equal to the new one because in that case,
                        // either the existing strictly stops before the new one and we don't want it, or it stops exactly
                        // like the new one but we're going to inconditionally add the new one anyway.
                        if (!existing.deletionTime.supersedes(tombstone.deletionTime))
                            iter.remove();
                    }
                    openTombstones.add(tombstone);
                }

                public boolean hasOpenTombstones()
                {
                    return openMarkerToReturn != null || !openTombstones.isEmpty();
                }

                public void clearState()
                {
                    openMarkerToReturn = null;
                    openTombstones.clear();
                }
            }
        }
    }
}
