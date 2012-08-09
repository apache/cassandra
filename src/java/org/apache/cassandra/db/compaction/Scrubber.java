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
package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.io.*;
import java.util.*;

import com.google.common.base.Throwables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;

public class Scrubber implements Closeable
{
    public final ColumnFamilyStore cfs;
    public final SSTableReader sstable;
    public final File destination;

    private final CompactionController controller;
    private final boolean isCommutative;
    private final int expectedBloomFilterSize;

    private final RandomAccessReader dataFile;
    private final RandomAccessReader indexFile;
    private final ScrubInfo scrubInfo;

    private long rowsRead;

    private SSTableWriter writer;
    private SSTableReader newSstable;
    private SSTableReader newInOrderSstable;

    private int goodRows;
    private int badRows;
    private int emptyRows;

    private final OutputHandler outputHandler;

    private static final Comparator<AbstractCompactedRow> acrComparator = new Comparator<AbstractCompactedRow>()
    {
         public int compare(AbstractCompactedRow r1, AbstractCompactedRow r2)
         {
             return r1.key.compareTo(r2.key);
         }
    };
    private final Set<AbstractCompactedRow> outOfOrderRows = new TreeSet<AbstractCompactedRow>(acrComparator);

    public Scrubber(ColumnFamilyStore cfs, SSTableReader sstable) throws IOException
    {
        this(cfs, sstable, new OutputHandler.LogOutput(), false);
    }

    public Scrubber(ColumnFamilyStore cfs, SSTableReader sstable, OutputHandler outputHandler, boolean isOffline) throws IOException
    {
        this.cfs = cfs;
        this.sstable = sstable;
        this.outputHandler = outputHandler;

        // Calculate the expected compacted filesize
        this.destination = cfs.directories.getDirectoryForNewSSTables(sstable.onDiskLength());
        if (destination == null)
            throw new IOException("disk full");

        List<SSTableReader> toScrub = Collections.singletonList(sstable);
        // If we run scrub offline, we should never purge tombstone, as we cannot know if other sstable have data that the tombstone deletes.
        this.controller = isOffline
                        ? new ScrubController(cfs)
                        : new CompactionController(cfs, Collections.singletonList(sstable), CompactionManager.getDefaultGcBefore(cfs), true);
        this.isCommutative = cfs.metadata.getDefaultValidator().isCommutative();
        this.expectedBloomFilterSize = Math.max(DatabaseDescriptor.getIndexInterval(), (int)(SSTableReader.getApproximateKeyCount(toScrub)));

        // loop through each row, deserializing to check for damage.
        // we'll also loop through the index at the same time, using the position from the index to recover if the
        // row header (key or data size) is corrupt. (This means our position in the index file will be one row
        // "ahead" of the data file.)
        this.dataFile = sstable.openDataReader(true);
        this.indexFile = RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX)), true);
        this.scrubInfo = new ScrubInfo(dataFile, sstable);
    }

    public void scrub() throws IOException
    {
        outputHandler.output("Scrubbing " + sstable);
        try
        {
            ByteBuffer nextIndexKey = ByteBufferUtil.readWithShortLength(indexFile);
            {
                // throw away variable so we don't have a side effect in the assert
                long firstRowPositionFromIndex = RowIndexEntry.serializer.deserialize(indexFile, sstable.descriptor.version).position;
                assert firstRowPositionFromIndex == 0 : firstRowPositionFromIndex;
            }

            // TODO errors when creating the writer may leave empty temp files.
            writer = CompactionManager.maybeCreateWriter(cfs, destination, expectedBloomFilterSize, null, Collections.singletonList(sstable));

            AbstractCompactedRow prevRow = null;

            while (!dataFile.isEOF())
            {
                if (scrubInfo.isStopRequested())
                    throw new CompactionInterruptedException(scrubInfo.getCompactionInfo());
                long rowStart = dataFile.getFilePointer();
                outputHandler.debug("Reading row at " + rowStart);

                DecoratedKey key = null;
                long dataSize = -1;
                try
                {
                    key = SSTableReader.decodeKey(sstable.partitioner, sstable.descriptor, ByteBufferUtil.readWithShortLength(dataFile));
                    dataSize = sstable.descriptor.version.hasIntRowSize ? dataFile.readInt() : dataFile.readLong();
                    outputHandler.debug(String.format("row %s is %s bytes", ByteBufferUtil.bytesToHex(key.key), dataSize));
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    // check for null key below
                }

                ByteBuffer currentIndexKey = nextIndexKey;
                long nextRowPositionFromIndex;
                try
                {
                    nextIndexKey = indexFile.isEOF() ? null : ByteBufferUtil.readWithShortLength(indexFile);
                    nextRowPositionFromIndex = indexFile.isEOF()
                                             ? dataFile.length()
                                             : RowIndexEntry.serializer.deserialize(indexFile, sstable.descriptor.version).position;
                }
                catch (Throwable th)
                {
                    outputHandler.warn("Error reading index file", th);
                    nextIndexKey = null;
                    nextRowPositionFromIndex = dataFile.length();
                }

                long dataStart = dataFile.getFilePointer();
                long dataStartFromIndex = currentIndexKey == null
                                        ? -1
                                        : rowStart + 2 + currentIndexKey.remaining() + (sstable.descriptor.version.hasIntRowSize ? 4 : 8);
                long dataSizeFromIndex = nextRowPositionFromIndex - dataStartFromIndex;
                assert currentIndexKey != null || indexFile.isEOF();
                if (currentIndexKey != null)
                    outputHandler.debug(String.format("Index doublecheck: row %s is %s bytes", ByteBufferUtil.bytesToHex(currentIndexKey),  dataSizeFromIndex));

                writer.mark();
                try
                {
                    if (key == null)
                        throw new IOError(new IOException("Unable to read row key from data file"));
                    if (dataSize > dataFile.length())
                        throw new IOError(new IOException("Impossible row size " + dataSize));
                    SSTableIdentityIterator row = new SSTableIdentityIterator(sstable, dataFile, key, dataStart, dataSize, true);
                    AbstractCompactedRow compactedRow = controller.getCompactedRow(row);
                    if (compactedRow.isEmpty())
                    {
                        emptyRows++;
                    }
                    else
                    {
                        if (prevRow != null && acrComparator.compare(prevRow, compactedRow) >= 0)
                        {
                            outOfOrderRows.add(compactedRow);
                            outputHandler.warn(String.format("Out of order row detected (%s found after %s)", compactedRow.key, prevRow.key));
                            continue;
                        }

                        writer.append(compactedRow);
                        prevRow = compactedRow;
                        goodRows++;
                    }
                    if (!key.key.equals(currentIndexKey) || dataStart != dataStartFromIndex)
                        outputHandler.warn("Index file contained a different key or row size; using key from data file");
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    outputHandler.warn("Non-fatal error reading row (stacktrace follows)", th);
                    writer.resetAndTruncate();

                    if (currentIndexKey != null
                        && (key == null || !key.key.equals(currentIndexKey) || dataStart != dataStartFromIndex || dataSize != dataSizeFromIndex))
                    {
                        outputHandler.output(String.format("Retrying from row index; data is %s bytes starting at %s",
                                                  dataSizeFromIndex, dataStartFromIndex));
                        key = SSTableReader.decodeKey(sstable.partitioner, sstable.descriptor, currentIndexKey);
                        try
                        {
                            SSTableIdentityIterator row = new SSTableIdentityIterator(sstable, dataFile, key, dataStartFromIndex, dataSizeFromIndex, true);
                            AbstractCompactedRow compactedRow = controller.getCompactedRow(row);
                            if (compactedRow.isEmpty())
                            {
                                emptyRows++;
                            }
                            else
                            {
                                if (prevRow != null && acrComparator.compare(prevRow, compactedRow) >= 0)
                                {
                                    outOfOrderRows.add(compactedRow);
                                    outputHandler.warn(String.format("Out of order row detected (%s found after %s)", compactedRow.key, prevRow.key));
                                    continue;
                                }
                                writer.append(compactedRow);
                                prevRow = compactedRow;
                                goodRows++;
                            }
                        }
                        catch (Throwable th2)
                        {
                            throwIfFatal(th2);
                            // Skipping rows is dangerous for counters (see CASSANDRA-2759)
                            if (isCommutative)
                                throw new IOError(th2);

                            outputHandler.warn("Retry failed too. Skipping to next row (retry's stacktrace follows)", th2);
                            writer.resetAndTruncate();
                            dataFile.seek(nextRowPositionFromIndex);
                            badRows++;
                        }
                    }
                    else
                    {
                        // Skipping rows is dangerous for counters (see CASSANDRA-2759)
                        if (isCommutative)
                            throw new IOError(th);

                        outputHandler.warn("Row at " + dataStart + " is unreadable; skipping to next");
                        if (currentIndexKey != null)
                            dataFile.seek(nextRowPositionFromIndex);
                        badRows++;
                    }
                }
                if ((rowsRead++ % 1000) == 0)
                    controller.mayThrottle(dataFile.getFilePointer());
            }

            if (writer.getFilePointer() > 0)
                newSstable = writer.closeAndOpenReader(sstable.maxDataAge);
        }
        catch (Throwable t)
        {
            if (writer != null)
                writer.abort();
            throw Throwables.propagate(t);
        }

        if (!outOfOrderRows.isEmpty())
        {
            SSTableWriter inOrderWriter = CompactionManager.maybeCreateWriter(cfs, destination, expectedBloomFilterSize, null, Collections.singletonList(sstable));
            for (AbstractCompactedRow row : outOfOrderRows)
                inOrderWriter.append(row);
            newInOrderSstable = inOrderWriter.closeAndOpenReader(sstable.maxDataAge);
            outputHandler.warn(String.format("%d out of order rows found while scrubbing %s; Those have been written (in order) to a new sstable (%s)", outOfOrderRows.size(), sstable, newInOrderSstable));
        }

        if (newSstable == null)
        {
            if (badRows > 0)
                outputHandler.warn("No valid rows found while scrubbing " + sstable + "; it is marked for deletion now. If you want to attempt manual recovery, you can find a copy in the pre-scrub snapshot");
            else
                outputHandler.output("Scrub of " + sstable + " complete; looks like all " + emptyRows + " rows were tombstoned");
        }
        else
        {
            outputHandler.output("Scrub of " + sstable + " complete: " + goodRows + " rows in new sstable and " + emptyRows + " empty (tombstoned) rows dropped");
            if (badRows > 0)
                outputHandler.warn("Unable to recover " + badRows + " rows that were skipped.  You can attempt manual recovery from the pre-scrub snapshot.  You can also run nodetool repair to transfer the data from a healthy replica, if any");
        }
    }

    public SSTableReader getNewSSTable()
    {
        return newSstable;
    }

    public SSTableReader getNewInOrderSSTable()
    {
        return newInOrderSstable;
    }

    private void throwIfFatal(Throwable th)
    {
        if (th instanceof Error && !(th instanceof AssertionError || th instanceof IOError))
            throw (Error) th;
    }

    public void close()
    {
        FileUtils.closeQuietly(dataFile);
        FileUtils.closeQuietly(indexFile);
    }

    public CompactionInfo.Holder getScrubInfo()
    {
        return scrubInfo;
    }

    private static class ScrubInfo extends CompactionInfo.Holder
    {
        private final RandomAccessReader dataFile;
        private final SSTableReader sstable;

        public ScrubInfo(RandomAccessReader dataFile, SSTableReader sstable)
        {
            this.dataFile = dataFile;
            this.sstable = sstable;
        }

        public CompactionInfo getCompactionInfo()
        {
            try
            {
                return new CompactionInfo(sstable.metadata,
                                          OperationType.SCRUB,
                                          dataFile.getFilePointer(),
                                          dataFile.length());
            }
            catch (Exception e)
            {
                throw new RuntimeException();
            }
        }
    }

    private static class ScrubController extends CompactionController
    {
        public ScrubController(ColumnFamilyStore cfs)
        {
            super(cfs, Integer.MAX_VALUE, DataTracker.buildIntervalTree(Collections.<SSTableReader>emptyList()));
        }

        @Override
        public boolean shouldPurge(DecoratedKey key)
        {
            return false;
        }
    }
}
