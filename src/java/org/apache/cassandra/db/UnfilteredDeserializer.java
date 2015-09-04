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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.DataInputPlus;
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

    private static class CurrentDeserializer extends UnfilteredDeserializer
    {
        private final ClusteringPrefix.Deserializer clusteringDeserializer;
        private final SerializationHeader header;

        private int nextFlags;
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

            clusteringDeserializer.prepare(nextFlags);
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
                return UnfilteredSerializer.serializer.deserializeRowBody(in, header, helper, nextFlags, builder);
            }
        }

        public void skipNext() throws IOException
        {
            isReady = false;
            ClusteringPrefix.Kind kind = clusteringDeserializer.skipNext();
            if (UnfilteredSerializer.kind(nextFlags) == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
            {
                UnfilteredSerializer.serializer.skipMarkerBody(in, header, kind.isBoundary());
            }
            else
            {
                UnfilteredSerializer.serializer.skipRowBody(in, header, nextFlags);
            }
        }

        public void clearState()
        {
            isReady = false;
            isDone = false;
        }
    }

    public static class OldFormatDeserializer extends UnfilteredDeserializer
    {
        private final boolean readAllAsDynamic;
        private boolean skipStatic;

        private boolean isDone;
        private boolean isStart = true;

        private final LegacyLayout.CellGrouper grouper;
        private LegacyLayout.LegacyAtom nextAtom;

        private boolean staticFinished;
        private LegacyLayout.LegacyAtom savedAtom;

        private final LegacyLayout.TombstoneTracker tombstoneTracker;

        private RangeTombstoneMarker closingMarker;

        private OldFormatDeserializer(CFMetaData metadata,
                                      DataInputPlus in,
                                      SerializationHelper helper,
                                      DeletionTime partitionDeletion,
                                      boolean readAllAsDynamic)
        {
            super(metadata, in, helper);
            this.readAllAsDynamic = readAllAsDynamic;
            this.grouper = new LegacyLayout.CellGrouper(metadata, helper);
            this.tombstoneTracker = new LegacyLayout.TombstoneTracker(metadata, partitionDeletion);
        }

        public void setSkipStatic()
        {
            this.skipStatic = true;
        }

        public boolean hasNext() throws IOException
        {
            return nextAtom != null || (!isDone && deserializeNextAtom());
        }

        private boolean deserializeNextAtom() throws IOException
        {
            if (staticFinished && savedAtom != null)
            {
                nextAtom = savedAtom;
                savedAtom = null;
                return true;
            }

            while (true)
            {
                nextAtom = LegacyLayout.readLegacyAtom(metadata, in, readAllAsDynamic);
                if (nextAtom == null)
                {
                    isDone = true;
                    return false;
                }
                else if (tombstoneTracker.isShadowed(nextAtom))
                {
                    // We don't want to return shadowed data because that would fail the contract
                    // of UnfilteredRowIterator. However the old format could have shadowed data, so filter it here.
                    nextAtom = null;
                    continue;
                }

                tombstoneTracker.update(nextAtom);

                // For static compact tables, the "column_metadata" columns are supposed to be static, but in the old
                // format they are intermingled with other columns. We deal with that with 2 different strategy:
                //  1) for thrift queries, we basically consider everything as a "dynamic" cell. This is ok because
                //     that's basically what we end up with on ThriftResultsMerger has done its thing.
                //  2) otherwise, we make sure to extract the "static" columns first (see AbstractSSTableIterator.readStaticRow
                //     and SSTableSimpleIterator.readStaticRow) as a first pass. So, when we do a 2nd pass for dynamic columns
                //     (which in practice we only do for compactions), we want to ignore those extracted static columns.
                if (skipStatic && metadata.isStaticCompactTable() && nextAtom.isCell())
                {
                    LegacyLayout.LegacyCell cell = nextAtom.asCell();
                    if (cell.name.column.isStatic())
                    {
                        nextAtom = null;
                        continue;
                    }
                }

                // We want to fetch the static row as the first thing this deserializer return.
                // However, in practice, it's possible to have range tombstone before the static row cells
                // if that tombstone has an empty start. So if we do, we save it initially so we can get
                // to the static parts (if there is any).
                if (isStart)
                {
                    isStart = false;
                    if (!nextAtom.isCell())
                    {
                        LegacyLayout.LegacyRangeTombstone tombstone = nextAtom.asRangeTombstone();
                        if (tombstone.start.bound.size() == 0)
                        {
                            savedAtom = tombstone;
                            nextAtom = LegacyLayout.readLegacyAtom(metadata, in, readAllAsDynamic);
                            if (nextAtom == null)
                            {
                                // That was actually the only atom so use it after all
                                nextAtom = savedAtom;
                                savedAtom = null;
                            }
                            else if (!nextAtom.isStatic())
                            {
                                // We don't have anything static. So we do want to send first
                                // the saved atom, so switch
                                LegacyLayout.LegacyAtom atom = nextAtom;
                                nextAtom = savedAtom;
                                savedAtom = atom;
                            }
                        }
                    }
                }

                return true;
            }
        }

        private void checkReady() throws IOException
        {
            if (nextAtom == null)
                hasNext();
            assert !isDone;
        }

        public int compareNextTo(Slice.Bound bound) throws IOException
        {
            checkReady();
            int cmp = metadata.comparator.compare(nextAtom.clustering(), bound);
            if (cmp != 0 || nextAtom.isCell() || !nextIsRow())
                return cmp;

            // Comparing the clustering of the LegacyAtom to the bound work most of the time. There is the case
            // of LegacyRangeTombstone that are either a collectionTombstone or a rowDeletion. In those case, their
            // clustering will be the inclusive start of the row they are a tombstone for, which can be equal to
            // the slice bound. But we don't want to return equality because the LegacyTombstone should stand for
            // it's row and should sort accordingly. This matter particularly because SSTableIterator will skip
            // equal results for the start bound (see SSTableIterator.handlePreSliceData for details).
            return bound.isStart() ? 1 : -1;
        }

        public boolean nextIsRow() throws IOException
        {
            checkReady();
            if (nextAtom.isCell())
                return true;

            LegacyLayout.LegacyRangeTombstone tombstone = nextAtom.asRangeTombstone();
            return tombstone.isCollectionTombstone() || tombstone.isRowDeletion(metadata);
        }

        public boolean nextIsStatic() throws IOException
        {
            checkReady();
            return nextAtom.isStatic();
        }

        public Unfiltered readNext() throws IOException
        {
            if (!nextIsRow())
            {
                LegacyLayout.LegacyRangeTombstone tombstone = nextAtom.asRangeTombstone();
                // TODO: this is actually more complex, we can have repeated markers etc....
                if (closingMarker == null)
                    throw new UnsupportedOperationException();
                closingMarker = new RangeTombstoneBoundMarker(tombstone.stop.bound, tombstone.deletionTime);
                return new RangeTombstoneBoundMarker(tombstone.start.bound, tombstone.deletionTime);
            }

            LegacyLayout.CellGrouper grouper = nextAtom.isStatic()
                                             ? LegacyLayout.CellGrouper.staticGrouper(metadata, helper)
                                             : this.grouper;

            grouper.reset();
            grouper.addAtom(nextAtom);
            while (deserializeNextAtom() && grouper.addAtom(nextAtom))
            {
                // Nothing to do, deserializeNextAtom() changes nextAtom and it's then added to the grouper
            }

            // if this was the first static row, we're done with it. Otherwise, we're also done with static.
            staticFinished = true;
            return grouper.getRow();
        }

        public void skipNext() throws IOException
        {
            readNext();
        }

        public void clearState()
        {
            isDone = false;
            nextAtom = null;
        }
    }
}
