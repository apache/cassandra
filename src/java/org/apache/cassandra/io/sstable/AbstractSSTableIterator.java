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
package org.apache.cassandra.io.sstable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.UnfilteredDeserializer;
import org.apache.cassandra.db.UnfilteredValidation;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.vint.VIntCoding.VIntOutOfRangeException;


public abstract class AbstractSSTableIterator<RIE extends AbstractRowIndexEntry> implements UnfilteredRowIterator
{
    protected final SSTableReader sstable;
    // We could use sstable.metadata(), but that can change during execution so it's good hygiene to grab an immutable instance
    protected final TableMetadata metadata;

    protected final DecoratedKey key;
    protected final DeletionTime partitionLevelDeletion;
    protected final ColumnFilter columns;
    protected final DeserializationHelper helper;

    protected final Row staticRow;
    protected final Reader reader;

    protected final FileHandle ifile;

    private boolean isClosed;

    protected final Slices slices;

                                  // file on every path where we created it.
    protected AbstractSSTableIterator(SSTableReader sstable,
                                      FileDataInput file,
                                      DecoratedKey key,
                                      RIE indexEntry,
                                      Slices slices,
                                      ColumnFilter columnFilter,
                                      FileHandle ifile)
    {
        this.sstable = sstable;
        this.metadata = sstable.metadata();
        this.ifile = ifile;
        this.key = key;
        this.columns = columnFilter;
        this.slices = slices;
        this.helper = new DeserializationHelper(metadata, sstable.descriptor.version.correspondingMessagingVersion(), DeserializationHelper.Flag.LOCAL, columnFilter);

        if (indexEntry == null)
        {
            this.partitionLevelDeletion = DeletionTime.LIVE;
            this.reader = null;
            this.staticRow = Rows.EMPTY_STATIC_ROW;
        }
        else
        {
            boolean shouldCloseFile = file == null;
            try
            {
                // We seek to the beginning to the partition if either:
                //   - the partition is not indexed; we then have a single block to read anyway
                //     (and we need to read the partition deletion time).
                //   - we're querying static columns.
                boolean needSeekAtPartitionStart = !indexEntry.isIndexed() || !columns.fetchedColumns().statics.isEmpty();

                if (needSeekAtPartitionStart)
                {
                    // Not indexed (or is reading static), set to the beginning of the partition and read partition level deletion there
                    if (file == null)
                        file = sstable.getFileDataInput(indexEntry.position);
                    else
                        file.seek(indexEntry.position);

                    ByteBufferUtil.skipShortLength(file); // Skip partition key
                    this.partitionLevelDeletion = DeletionTime.getSerializer(sstable.descriptor.version).deserialize(file);

                    // Note that this needs to be called after file != null and after the partitionDeletion has been set, but before readStaticRow
                    // (since it uses it) so we can't move that up (but we'll be able to simplify as soon as we drop support for the old file format).
                    this.reader = createReader(indexEntry, file, shouldCloseFile);
                    this.staticRow = readStaticRow(sstable, file, helper, columns.fetchedColumns().statics);
                }
                else
                {
                    this.partitionLevelDeletion = indexEntry.deletionTime();
                    this.staticRow = Rows.EMPTY_STATIC_ROW;
                    this.reader = createReader(indexEntry, file, shouldCloseFile);
                }
                if (!partitionLevelDeletion.validate())
                    UnfilteredValidation.handleInvalid(metadata(), key, sstable, "partitionLevelDeletion="+partitionLevelDeletion.toString());

                if (reader != null && !slices.isEmpty())
                    reader.setForSlice(nextSlice());

                if (reader == null && file != null && shouldCloseFile)
                    file.close();
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                String filePath = file.getPath();
                if (shouldCloseFile)
                {
                    try
                    {
                        file.close();
                    }
                    catch (IOException suppressed)
                    {
                        e.addSuppressed(suppressed);
                    }
                }
                throw new CorruptSSTableException(e, filePath);
            }
        }
    }

    private Slice nextSlice()
    {
        return slices.get(nextSliceIndex());
    }

    /**
     * Returns the index of the next slice to process.
     * @return the index of the next slice to process
     */
    protected abstract int nextSliceIndex();

    /**
     * Checks if there are more slice to process.
     * @return {@code true} if there are more slice to process, {@code false} otherwise.
     */
    protected abstract boolean hasMoreSlices();

    private static Row readStaticRow(SSTableReader sstable,
                                     FileDataInput file,
                                     DeserializationHelper helper,
                                     Columns statics) throws IOException
    {
        if (!sstable.header.hasStatic())
            return Rows.EMPTY_STATIC_ROW;

        if (statics.isEmpty())
        {
            UnfilteredSerializer.serializer.skipStaticRow(file, sstable.header, helper);
            return Rows.EMPTY_STATIC_ROW;
        }
        else
        {
            return UnfilteredSerializer.serializer.deserializeStaticRow(file, sstable.header, helper);
        }
    }

    protected abstract Reader createReaderInternal(RIE indexEntry, FileDataInput file, boolean shouldCloseFile, Version version);

    private Reader createReader(RIE indexEntry, FileDataInput file, boolean shouldCloseFile)
    {
        return slices.isEmpty() ? new NoRowsReader(file, shouldCloseFile)
                                : createReaderInternal(indexEntry, file, shouldCloseFile, sstable.descriptor.version);
    };

    public TableMetadata metadata()
    {
        return metadata;
    }

    public RegularAndStaticColumns columns()
    {
        return columns.fetchedColumns();
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public EncodingStats stats()
    {
        return sstable.stats();
    }

    public boolean hasNext()
    {
        while (true)
        {
            if (reader == null)
                return false;

            if (reader.hasNext())
                return true;

            if (!hasMoreSlices())
                return false;

            slice(nextSlice());
        }
    }

    public Unfiltered next()
    {
        assert reader != null;
        return reader.next();
    }

    private void slice(Slice slice)
    {
        try
        {
            if (reader != null)
                reader.setForSlice(slice);
        }
        catch (IOException e)
        {
            try
            {
                closeInternal();
            }
            catch (IOException suppressed)
            {
                e.addSuppressed(suppressed);
            }
            sstable.markSuspect();
            throw new CorruptSSTableException(e, reader.toString());
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private void closeInternal() throws IOException
    {
        // It's important to make closing idempotent since it would bad to double-close 'file' as its a RandomAccessReader
        // and its close is not idemptotent in the case where we recycle it.
        if (isClosed)
            return;

        if (reader != null)
            reader.close();

        isClosed = true;
    }

    public void close()
    {
        try
        {
            closeInternal();
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, reader.toString());
        }
    }

    public interface Reader extends Iterator<Unfiltered>, Closeable {

        void setForSlice(Slice slice) throws IOException;

        void seekToPosition(long columnOffset) throws IOException;
    }

    public abstract class AbstractReader implements Reader
    {
        private final boolean shouldCloseFile;
        public FileDataInput file;

        public UnfilteredDeserializer deserializer;

        // Records the currently open range tombstone (if any)
        public DeletionTime openMarker;

        protected AbstractReader(FileDataInput file, boolean shouldCloseFile)
        {
            this.file = file;
            this.shouldCloseFile = shouldCloseFile;

            if (file != null)
                createDeserializer();
        }

        private void createDeserializer()
        {
            assert file != null && deserializer == null;
            deserializer = UnfilteredDeserializer.create(metadata, file, sstable.header, helper);
        }

        /**
         * Seek to the given position in the file. Initializes the file reader along with a deserializer if they are
         * not initialized yet. Otherwise, the deserializer is reset by calling its {@link UnfilteredDeserializer#clearState()}
         * method.
         * <p/>
         * Note that the only valid usage of this method is to seek to the beginning of the serialized record so that
         * the deserializer can read the next unfiltered from that position. Setting an arbitrary position will lead
         * to unexpected results and/or corrupted reads.
         */
        public void seekToPosition(long position) throws IOException
        {
            // This may be the first time we're actually looking into the file
            if (file == null)
            {
                file = sstable.getFileDataInput(position);
                createDeserializer();
            }
            else
            {
                file.seek(position);
                deserializer.clearState();
            }
        }

        protected void updateOpenMarker(RangeTombstoneMarker marker)
        {
            // Note that we always read index blocks in forward order so this method is always called in forward order
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
        }

        public boolean hasNext()
        {
            try
            {
                return hasNextInternal();
            }
            catch (IOException | IndexOutOfBoundsException | VIntOutOfRangeException e)
            {
                try
                {
                    closeInternal();
                }
                catch (IOException suppressed)
                {
                    e.addSuppressed(suppressed);
                }
                sstable.markSuspect();
                throw new CorruptSSTableException(e, toString());
            }
        }

        public Unfiltered next()
        {
            try
            {
                return nextInternal();
            }
            catch (IOException e)
            {
                try
                {
                    closeInternal();
                }
                catch (IOException suppressed)
                {
                    e.addSuppressed(suppressed);
                }
                sstable.markSuspect();
                throw new CorruptSSTableException(e, toString());
            }
        }

        // Set the reader so its hasNext/next methods return values within the provided slice
        public abstract void setForSlice(Slice slice) throws IOException;

        protected abstract boolean hasNextInternal() throws IOException;
        protected abstract Unfiltered nextInternal() throws IOException;

        @Override
        public void close() throws IOException
        {
            if (shouldCloseFile && file != null)
                file.close();
        }

        @Override
        public String toString()
        {
            return file != null ? file.toString() : "null";
        }
    }

    protected class ForwardReader extends AbstractReader
    {
        // The start of the current slice. This will be null as soon as we know we've passed that bound.
        protected ClusteringBound<?> start;
        // The end of the current slice. Will never be null.
        protected ClusteringBound<?> end = BufferClusteringBound.TOP;

        protected Unfiltered next; // the next element to return: this is computed by hasNextInternal().

        protected boolean sliceDone; // set to true once we know we have no more result for the slice. This is in particular
        // used by the indexed reader when we know we can't have results based on the index.

        public ForwardReader(FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
        }

        public void setForSlice(Slice slice) throws IOException
        {
            start = slice.start().isBottom() ? null : slice.start();
            end = slice.end();

            sliceDone = false;
            next = null;
        }

        // Skip all data that comes before the currently set slice.
        // Return what should be returned at the end of this, or null if nothing should.
        private Unfiltered handlePreSliceData() throws IOException
        {
            assert deserializer != null;

            // Note that the following comparison is not strict. The reason is that the only cases
            // where it can be == is if the "next" is a RT start marker (either a '[' of a ')[' boundary),
            // and if we had a strict inequality and an open RT marker before this, we would issue
            // the open marker first, and then return then next later, which would send in the
            // stream both '[' (or '(') and then ')[' for the same clustering value, which is wrong.
            // By using a non-strict inequality, we avoid that problem (if we do get ')[' for the same
            // clustering value than the slice, we'll simply record it in 'openMarker').
            while (deserializer.hasNext() && deserializer.compareNextTo(start) <= 0)
            {
                if (deserializer.nextIsRow())
                    deserializer.skipNext();
                else
                    updateOpenMarker((RangeTombstoneMarker)deserializer.readNext());
            }

            ClusteringBound<?> sliceStart = start;
            start = null;

            // We've reached the beginning of our queried slice. If we have an open marker
            // we should return that first.
            if (openMarker != null)
                return new RangeTombstoneBoundMarker(sliceStart, openMarker);

            return null;
        }

        // Compute the next element to return, assuming we're in the middle to the slice
        // and the next element is either in the slice, or just after it. Returns null
        // if we're done with the slice.
        protected Unfiltered computeNext() throws IOException
        {
            assert deserializer != null;

            while (true)
            {
                // We use a same reasoning as in handlePreSliceData regarding the strictness of the inequality below.
                // We want to exclude deserialized unfiltered equal to end, because 1) we won't miss any rows since those
                // woudn't be equal to a slice bound and 2) a end bound can be equal to a start bound
                // (EXCL_END(x) == INCL_START(x) for instance) and in that case we don't want to return start bound because
                // it's fundamentally excluded. And if the bound is a  end (for a range tombstone), it means it's exactly
                // our slice end, but in that  case we will properly close the range tombstone anyway as part of our "close
                // an open marker" code in hasNextInterna
                if (!deserializer.hasNext() || deserializer.compareNextTo(end) >= 0)
                    return null;

                Unfiltered next = deserializer.readNext();
                UnfilteredValidation.maybeValidateUnfiltered(next, metadata(), key, sstable);
                // We may get empty row for the same reason expressed on UnfilteredSerializer.deserializeOne.
                if (next.isEmpty())
                    continue;

                if (next.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                    updateOpenMarker((RangeTombstoneMarker) next);
                return next;
            }
        }

        protected boolean hasNextInternal() throws IOException
        {
            if (next != null)
                return true;

            if (sliceDone)
                return false;

            if (start != null)
            {
                Unfiltered unfiltered = handlePreSliceData();
                if (unfiltered != null)
                {
                    next = unfiltered;
                    return true;
                }
            }

            next = computeNext();
            if (next != null)
                return true;

            // for current slice, no data read from deserialization
            sliceDone = true;
            // If we have an open marker, we should not close it, there could be more slices
            if (openMarker != null)
            {
                next = new RangeTombstoneBoundMarker(end, openMarker);
                return true;
            }
            return false;
        }

        protected Unfiltered nextInternal() throws IOException
        {
            if (!hasNextInternal())
                throw new NoSuchElementException();

            Unfiltered toReturn = next;
            next = null;
            return toReturn;
        }
    }


    // Reader for when we have Slices.NONE but need to read static row or partition level deletion
    private class NoRowsReader extends AbstractReader
    {
        private NoRowsReader(FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
        }

        @Override
        public void setForSlice(Slice slice)
        {
            // no-op
        }

        @Override
        public boolean hasNextInternal()
        {
            return false;
        }

        @Override
        protected Unfiltered nextInternal() throws IOException
        {
            throw new NoSuchElementException();
        }
    }
}
