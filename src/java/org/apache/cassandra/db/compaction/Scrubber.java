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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.UUIDGen;

public class Scrubber implements Closeable
{
    private final ColumnFamilyStore cfs;
    private final SSTableReader sstable;
    private final LifecycleTransaction transaction;
    private final File destination;
    private final boolean skipCorrupted;

    private final boolean isCommutative;
    private final boolean isIndex;
    private final boolean checkData;
    private final long expectedBloomFilterSize;

    private final RandomAccessReader dataFile;
    private final RandomAccessReader indexFile;
    private final ScrubInfo scrubInfo;
    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    private int goodRows;
    private int badRows;
    private int emptyRows;

    private ByteBuffer currentIndexKey;
    private ByteBuffer nextIndexKey;
    long currentRowPositionFromIndex;
    long nextRowPositionFromIndex;

    private final OutputHandler outputHandler;

    private static final Comparator<Partition> partitionComparator = new Comparator<Partition>()
    {
         public int compare(Partition r1, Partition r2)
         {
             return r1.partitionKey().compareTo(r2.partitionKey());
         }
    };
    private final SortedSet<Partition> outOfOrder = new TreeSet<>(partitionComparator);

    public Scrubber(ColumnFamilyStore cfs, LifecycleTransaction transaction, boolean skipCorrupted, boolean checkData) throws IOException
    {
        this(cfs, transaction, skipCorrupted, new OutputHandler.LogOutput(), checkData);
    }

    @SuppressWarnings("resource")
    public Scrubber(ColumnFamilyStore cfs,
                    LifecycleTransaction transaction,
                    boolean skipCorrupted,
                    OutputHandler outputHandler,
                    boolean checkData) throws IOException
    {
        this.cfs = cfs;
        this.transaction = transaction;
        this.sstable = transaction.onlyOne();
        this.outputHandler = outputHandler;
        this.skipCorrupted = skipCorrupted;
        this.rowIndexEntrySerializer = sstable.descriptor.version.getSSTableFormat().getIndexSerializer(sstable.metadata,
                                                                                                        sstable.descriptor.version,
                                                                                                        sstable.header);

        List<SSTableReader> toScrub = Collections.singletonList(sstable);

        int locIndex = CompactionStrategyManager.getCompactionStrategyIndex(cfs, cfs.getDirectories(), sstable);
        this.destination = cfs.getDirectories().getLocationForDisk(cfs.getDirectories().getWriteableLocations()[locIndex]);
        this.isCommutative = cfs.metadata.isCounter();

        boolean hasIndexFile = (new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX))).exists();
        this.isIndex = cfs.isIndex();
        if (!hasIndexFile)
        {
            // if there's any corruption in the -Data.db then rows can't be skipped over. but it's worth a shot.
            outputHandler.warn("Missing component: " + sstable.descriptor.filenameFor(Component.PRIMARY_INDEX));
        }
        this.checkData = checkData && !this.isIndex; //LocalByPartitionerType does not support validation
        this.expectedBloomFilterSize = Math.max(
            cfs.metadata.params.minIndexInterval,
            hasIndexFile ? SSTableReader.getApproximateKeyCount(toScrub) : 0);

        // loop through each row, deserializing to check for damage.
        // we'll also loop through the index at the same time, using the position from the index to recover if the
        // row header (key or data size) is corrupt. (This means our position in the index file will be one row
        // "ahead" of the data file.)
        this.dataFile = transaction.isOffline()
                        ? sstable.openDataReader()
                        : sstable.openDataReader(CompactionManager.instance.getRateLimiter());

        this.indexFile = hasIndexFile
                ? RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX)))
                : null;

        this.scrubInfo = new ScrubInfo(dataFile, sstable);

        this.currentRowPositionFromIndex = 0;
        this.nextRowPositionFromIndex = 0;
    }

    private UnfilteredRowIterator withValidation(UnfilteredRowIterator iter, String filename)
    {
        return checkData ? UnfilteredRowIterators.withValidation(iter, filename) : iter;
    }

    public void scrub()
    {
        List<SSTableReader> finished = new ArrayList<>();
        boolean completed = false;
        outputHandler.output(String.format("Scrubbing %s (%s)", sstable, FBUtilities.prettyPrintMemory(dataFile.length())));
        try (SSTableRewriter writer = SSTableRewriter.constructKeepingOriginals(transaction, false, sstable.maxDataAge))
        {
            nextIndexKey = indexAvailable() ? ByteBufferUtil.readWithShortLength(indexFile) : null;
            if (indexAvailable())
            {
                // throw away variable so we don't have a side effect in the assert
                long firstRowPositionFromIndex = rowIndexEntrySerializer.deserialize(indexFile).position;
                assert firstRowPositionFromIndex == 0 : firstRowPositionFromIndex;
            }

            writer.switchWriter(CompactionManager.createWriter(cfs, destination, expectedBloomFilterSize, sstable.getSSTableMetadata().repairedAt, sstable, transaction));

            DecoratedKey prevKey = null;

            while (!dataFile.isEOF())
            {
                if (scrubInfo.isStopRequested())
                    throw new CompactionInterruptedException(scrubInfo.getCompactionInfo());

                long rowStart = dataFile.getFilePointer();
                outputHandler.debug("Reading row at " + rowStart);

                DecoratedKey key = null;
                try
                {
                    key = sstable.decorateKey(ByteBufferUtil.readWithShortLength(dataFile));
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    // check for null key below
                }

                updateIndexKey();

                long dataStart = dataFile.getFilePointer();

                long dataStartFromIndex = -1;
                long dataSizeFromIndex = -1;
                if (currentIndexKey != null)
                {
                    dataStartFromIndex = currentRowPositionFromIndex + 2 + currentIndexKey.remaining();
                    dataSizeFromIndex = nextRowPositionFromIndex - dataStartFromIndex;
                }

                // avoid an NPE if key is null
                String keyName = key == null ? "(unreadable key)" : ByteBufferUtil.bytesToHex(key.getKey());
                outputHandler.debug(String.format("row %s is %s", keyName, FBUtilities.prettyPrintMemory(dataSizeFromIndex)));

                assert currentIndexKey != null || !indexAvailable();

                try
                {
                    if (key == null)
                        throw new IOError(new IOException("Unable to read row key from data file"));

                    if (currentIndexKey != null && !key.getKey().equals(currentIndexKey))
                    {
                        throw new IOError(new IOException(String.format("Key from data file (%s) does not match key from index file (%s)",
                                //ByteBufferUtil.bytesToHex(key.getKey()), ByteBufferUtil.bytesToHex(currentIndexKey))));
                                "_too big_", ByteBufferUtil.bytesToHex(currentIndexKey))));
                    }

                    if (indexFile != null && dataSizeFromIndex > dataFile.length())
                        throw new IOError(new IOException("Impossible row size (greater than file length): " + dataSizeFromIndex));

                    if (indexFile != null && dataStart != dataStartFromIndex)
                        outputHandler.warn(String.format("Data file row position %d differs from index file row position %d", dataStart, dataStartFromIndex));

                    try (UnfilteredRowIterator iterator = withValidation(new SSTableIdentityIterator(sstable, dataFile, key), dataFile.getPath()))
                    {
                        if (prevKey != null && prevKey.compareTo(key) > 0)
                        {
                            saveOutOfOrderRow(prevKey, key, iterator);
                            continue;
                        }

                        if (writer.tryAppend(iterator) == null)
                            emptyRows++;
                        else
                            goodRows++;
                    }

                    prevKey = key;
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    outputHandler.warn("Error reading row (stacktrace follows):", th);

                    if (currentIndexKey != null
                        && (key == null || !key.getKey().equals(currentIndexKey) || dataStart != dataStartFromIndex))
                    {
                        outputHandler.output(String.format("Retrying from row index; data is %s bytes starting at %s",
                                                  dataSizeFromIndex, dataStartFromIndex));
                        key = sstable.decorateKey(currentIndexKey);
                        try
                        {
                            dataFile.seek(dataStartFromIndex);

                            try (UnfilteredRowIterator iterator = withValidation(new SSTableIdentityIterator(sstable, dataFile, key), dataFile.getPath()))
                            {
                                if (prevKey != null && prevKey.compareTo(key) > 0)
                                {
                                    saveOutOfOrderRow(prevKey, key, iterator);
                                    continue;
                                }

                                if (writer.tryAppend(iterator) == null)
                                    emptyRows++;
                                else
                                    goodRows++;
                            }

                            prevKey = key;
                        }
                        catch (Throwable th2)
                        {
                            throwIfFatal(th2);
                            throwIfCannotContinue(key, th2);

                            outputHandler.warn("Retry failed too. Skipping to next row (retry's stacktrace follows)", th2);
                            badRows++;
                            seekToNextRow();
                        }
                    }
                    else
                    {
                        throwIfCannotContinue(key, th);

                        outputHandler.warn("Row starting at position " + dataStart + " is unreadable; skipping to next");
                        badRows++;
                        if (currentIndexKey != null)
                            seekToNextRow();
                    }
                }
            }

            if (!outOfOrder.isEmpty())
            {
                // out of order rows, but no bad rows found - we can keep our repairedAt time
                long repairedAt = badRows > 0 ? ActiveRepairService.UNREPAIRED_SSTABLE : sstable.getSSTableMetadata().repairedAt;
                SSTableReader newInOrderSstable;
                try (SSTableWriter inOrderWriter = CompactionManager.createWriter(cfs, destination, expectedBloomFilterSize, repairedAt, sstable, transaction))
                {
                    for (Partition partition : outOfOrder)
                        inOrderWriter.append(partition.unfilteredIterator());
                    newInOrderSstable = inOrderWriter.finish(-1, sstable.maxDataAge, true);
                }
                transaction.update(newInOrderSstable, false);
                finished.add(newInOrderSstable);
                outputHandler.warn(String.format("%d out of order rows found while scrubbing %s; Those have been written (in order) to a new sstable (%s)", outOfOrder.size(), sstable, newInOrderSstable));
            }

            // finish obsoletes the old sstable
            finished.addAll(writer.setRepairedAt(badRows > 0 ? ActiveRepairService.UNREPAIRED_SSTABLE : sstable.getSSTableMetadata().repairedAt).finish());
            completed = true;
        }
        catch (IOException e)
        {
            throw Throwables.propagate(e);
        }
        finally
        {
            if (transaction.isOffline())
                finished.forEach(sstable -> sstable.selfRef().release());
        }

        if (completed)
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

    private void updateIndexKey()
    {
        currentIndexKey = nextIndexKey;
        currentRowPositionFromIndex = nextRowPositionFromIndex;
        try
        {
            nextIndexKey = !indexAvailable() ? null : ByteBufferUtil.readWithShortLength(indexFile);

            nextRowPositionFromIndex = !indexAvailable()
                    ? dataFile.length()
                    : rowIndexEntrySerializer.deserialize(indexFile).position;
        }
        catch (Throwable th)
        {
            JVMStabilityInspector.inspectThrowable(th);
            outputHandler.warn("Error reading index file", th);
            nextIndexKey = null;
            nextRowPositionFromIndex = dataFile.length();
        }
    }

    private boolean indexAvailable()
    {
        return indexFile != null && !indexFile.isEOF();
    }

    private void seekToNextRow()
    {
        while(nextRowPositionFromIndex < dataFile.length())
        {
            try
            {
                dataFile.seek(nextRowPositionFromIndex);
                return;
            }
            catch (Throwable th)
            {
                throwIfFatal(th);
                outputHandler.warn(String.format("Failed to seek to next row position %d", nextRowPositionFromIndex), th);
                badRows++;
            }

            updateIndexKey();
        }
    }

    private void saveOutOfOrderRow(DecoratedKey prevKey, DecoratedKey key, UnfilteredRowIterator iterator)
    {
        // TODO bitch if the row is too large?  if it is there's not much we can do ...
        outputHandler.warn(String.format("Out of order row detected (%s found after %s)", key, prevKey));
        outOfOrder.add(ImmutableBTreePartition.create(iterator));
    }

    private void throwIfFatal(Throwable th)
    {
        if (th instanceof Error && !(th instanceof AssertionError || th instanceof IOError))
            throw (Error) th;
    }

    private void throwIfCannotContinue(DecoratedKey key, Throwable th)
    {
        if (isIndex)
        {
            outputHandler.warn(String.format("An error occurred while scrubbing the row with key '%s' for an index table. " +
                                             "Scrubbing will abort for this table and the index will be rebuilt.", key));
            throw new IOError(th);
        }

        if (isCommutative && !skipCorrupted)
        {
            outputHandler.warn(String.format("An error occurred while scrubbing the row with key '%s'.  Skipping corrupt " +
                                             "rows in counter tables will result in undercounts for the affected " +
                                             "counters (see CASSANDRA-2759 for more details), so by default the scrub will " +
                                             "stop at this point.  If you would like to skip the row anyway and continue " +
                                             "scrubbing, re-run the scrub with the --skip-corrupted option.", key));
            throw new IOError(th);
        }
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
        private final UUID scrubCompactionId;

        public ScrubInfo(RandomAccessReader dataFile, SSTableReader sstable)
        {
            this.dataFile = dataFile;
            this.sstable = sstable;
            scrubCompactionId = UUIDGen.getTimeUUID();
        }

        public CompactionInfo getCompactionInfo()
        {
            try
            {
                return new CompactionInfo(sstable.metadata,
                                          OperationType.SCRUB,
                                          dataFile.getFilePointer(),
                                          dataFile.length(),
                                          scrubCompactionId);
            }
            catch (Exception e)
            {
                throw new RuntimeException();
            }
        }
    }

    @VisibleForTesting
    public ScrubResult scrubWithResult()
    {
        scrub();
        return new ScrubResult(this);
    }

    public static final class ScrubResult
    {
        public final int goodRows;
        public final int badRows;
        public final int emptyRows;

        public ScrubResult(Scrubber scrubber)
        {
            this.goodRows = scrubber.goodRows;
            this.badRows = scrubber.badRows;
            this.emptyRows = scrubber.emptyRows;
        }
    }
}
