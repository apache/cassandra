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

import org.apache.cassandra.config.CFMetaData;
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
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.memory.HeapAllocator;

public class Scrubber implements Closeable
{
    private final ColumnFamilyStore cfs;
    private final SSTableReader sstable;
    private final LifecycleTransaction transaction;
    private final File destination;
    private final boolean skipCorrupted;
    private final boolean reinsertOverflowedTTLRows;

    private final boolean isCommutative;
    private final boolean isIndex;
    private final boolean checkData;
    private final long expectedBloomFilterSize;

    private final RandomAccessReader dataFile;
    private final RandomAccessReader indexFile;
    private final ScrubInfo scrubInfo;
    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    private int goodPartitions;
    private int badPartitions;
    private int emptyPartitions;

    private ByteBuffer currentIndexKey;
    private ByteBuffer nextIndexKey;
    private long currentPartitionPositionFromIndex;
    private long nextPartitionPositionFromIndex;

    private NegativeLocalDeletionInfoMetrics negativeLocalDeletionInfoMetrics = new NegativeLocalDeletionInfoMetrics();

    private final OutputHandler outputHandler;

    private static final Comparator<Partition> partitionComparator = Comparator.comparing(Partition::partitionKey);
    private final SortedSet<Partition> outOfOrder = new TreeSet<>(partitionComparator);

    public Scrubber(ColumnFamilyStore cfs, LifecycleTransaction transaction, boolean skipCorrupted, boolean checkData) throws IOException
    {
        this(cfs, transaction, skipCorrupted, checkData, false);
    }

    public Scrubber(ColumnFamilyStore cfs, LifecycleTransaction transaction, boolean skipCorrupted, boolean checkData,
                    boolean reinsertOverflowedTTLRows) throws IOException
    {
        this(cfs, transaction, skipCorrupted, new OutputHandler.LogOutput(), checkData, reinsertOverflowedTTLRows);
    }

    @SuppressWarnings("resource")
    public Scrubber(ColumnFamilyStore cfs,
                    LifecycleTransaction transaction,
                    boolean skipCorrupted,
                    OutputHandler outputHandler,
                    boolean checkData,
                    boolean reinsertOverflowedTTLRows) throws IOException
    {
        this.cfs = cfs;
        this.transaction = transaction;
        this.sstable = transaction.onlyOne();
        this.outputHandler = outputHandler;
        this.skipCorrupted = skipCorrupted;
        this.reinsertOverflowedTTLRows = reinsertOverflowedTTLRows;
        this.rowIndexEntrySerializer = sstable.descriptor.version.getSSTableFormat().getIndexSerializer(sstable.metadata,
                                                                                                        sstable.descriptor.version,
                                                                                                        sstable.header);

        List<SSTableReader> toScrub = Collections.singletonList(sstable);


        this.destination = cfs.getDirectories().getLocationForDisk(cfs.getDiskBoundaries().getCorrectDiskForSSTable(sstable));
        this.isCommutative = cfs.metadata.isCounter();

        boolean hasIndexFile = (new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX))).exists();
        this.isIndex = cfs.isIndex();
        if (!hasIndexFile)
        {
            // if there's any corruption in the -Data.db then partitions can't be skipped over. but it's worth a shot.
            outputHandler.warn("Missing component: " + sstable.descriptor.filenameFor(Component.PRIMARY_INDEX));
        }
        this.checkData = checkData && !this.isIndex; //LocalByPartitionerType does not support validation
        this.expectedBloomFilterSize = Math.max(
        cfs.metadata.params.minIndexInterval,
        hasIndexFile ? SSTableReader.getApproximateKeyCount(toScrub) : 0);

        // loop through each partition, deserializing to check for damage.
        // we'll also loop through the index at the same time, using the position from the index to recover if the
        // partition header (key or data size) is corrupt. (This means our position in the index file will be one
        // partition "ahead" of the data file.)
        this.dataFile = transaction.isOffline()
                        ? sstable.openDataReader()
                        : sstable.openDataReader(CompactionManager.instance.getRateLimiter());

        this.indexFile = hasIndexFile
                         ? RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX)))
                         : null;

        this.scrubInfo = new ScrubInfo(dataFile, sstable);

        this.currentPartitionPositionFromIndex = 0;
        this.nextPartitionPositionFromIndex = 0;

        if (reinsertOverflowedTTLRows)
            outputHandler.output("Starting scrub with reinsert overflowed TTL option");
    }

    private UnfilteredRowIterator withValidation(UnfilteredRowIterator iter, String filename)
    {
        return checkData ? UnfilteredRowIterators.withValidation(iter, filename) : iter;
    }

    private String keyString(DecoratedKey key)
    {
        try
        {
            return cfs.metadata.getKeyValidator().getString(key.getKey());
        }
        catch (Exception e)
        {
            return String.format("(corrupted; hex value: %s)", ByteBufferUtil.bytesToHex(key.getKey()));
        }
    }

    public void scrub()
    {
        List<SSTableReader> finished = new ArrayList<>();
        outputHandler.output(String.format("Scrubbing %s (%s)", sstable, FBUtilities.prettyPrintMemory(dataFile.length())));
        try (SSTableRewriter writer = SSTableRewriter.construct(cfs, transaction, false, sstable.maxDataAge);
             Refs<SSTableReader> refs = Refs.ref(Collections.singleton(sstable)))
        {
            nextIndexKey = indexAvailable() ? ByteBufferUtil.readWithShortLength(indexFile) : null;
            if (indexAvailable())
            {
                // throw away variable so we don't have a side effect in the assert
                long firstRowPositionFromIndex = rowIndexEntrySerializer.deserializePositionAndSkip(indexFile);
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
                    ByteBuffer raw = ByteBufferUtil.readWithShortLength(dataFile);
                    if (!cfs.metadata.isIndex())
                        cfs.metadata.getKeyValidator().validate(raw);
                    key = sstable.decorateKey(raw);
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
                    dataStartFromIndex = currentPartitionPositionFromIndex + 2 + currentIndexKey.remaining();
                    dataSizeFromIndex = nextPartitionPositionFromIndex - dataStartFromIndex;
                }

                String keyName = key == null ? "(unreadable key)" : keyString(key);
                outputHandler.debug(String.format("partition %s is %s", keyName, FBUtilities.prettyPrintMemory(dataSizeFromIndex)));
                assert currentIndexKey != null || !indexAvailable();

                try
                {
                    if (key == null)
                        throw new IOError(new IOException("Unable to read partition key from data file"));

                    if (currentIndexKey != null && !key.getKey().equals(currentIndexKey))
                    {
                        throw new IOError(new IOException(String.format("Key from data file (%s) does not match key from index file (%s)",
                                                                        //ByteBufferUtil.bytesToHex(key.getKey()), ByteBufferUtil.bytesToHex(currentIndexKey))));
                                                                        "_too big_", ByteBufferUtil.bytesToHex(currentIndexKey))));
                    }

                    if (indexFile != null && dataSizeFromIndex > dataFile.length())
                        throw new IOError(new IOException("Impossible partition size (greater than file length): " + dataSizeFromIndex));

                    if (indexFile != null && dataStart != dataStartFromIndex)
                        outputHandler.warn(String.format("Data file partition position %d differs from index file partition position %d", dataStart, dataStartFromIndex));

                    if (tryAppend(prevKey, key, writer))
                        prevKey = key;
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    outputHandler.warn(String.format("Error reading partition %s (stacktrace follows):", keyName), th);

                    if (currentIndexKey != null
                        && (key == null || !key.getKey().equals(currentIndexKey) || dataStart != dataStartFromIndex))
                    {
                        outputHandler.output(String.format("Retrying from index; data is %s bytes starting at %s",
                                                           dataSizeFromIndex, dataStartFromIndex));
                        key = sstable.decorateKey(currentIndexKey);
                        try
                        {
                            if (!cfs.metadata.isIndex())
                                cfs.metadata.getKeyValidator().validate(key.getKey());
                            dataFile.seek(dataStartFromIndex);

                            if (tryAppend(prevKey, key, writer))
                                prevKey = key;
                        }
                        catch (Throwable th2)
                        {
                            throwIfFatal(th2);
                            throwIfCannotContinue(key, th2);

                            outputHandler.warn("Retry failed too. Skipping to next partition (retry's stacktrace follows)", th2);
                            badPartitions++;
                            if (!seekToNextPartition())
                                break;
                        }
                    }
                    else
                    {
                        throwIfCannotContinue(key, th);

                        outputHandler.warn("Partition starting at position " + dataStart + " is unreadable; skipping to next");
                        badPartitions++;
                        if (currentIndexKey != null)
                            if (!seekToNextPartition())
                                break;
                    }
                }
            }

            if (!outOfOrder.isEmpty())
            {
                // out of order partitions/rows, but no bad partition found - we can keep our repairedAt time
                long repairedAt = badPartitions > 0 ? ActiveRepairService.UNREPAIRED_SSTABLE : sstable.getSSTableMetadata().repairedAt;
                SSTableReader newInOrderSstable;
                try (SSTableWriter inOrderWriter = CompactionManager.createWriter(cfs, destination, expectedBloomFilterSize, repairedAt, sstable, transaction))
                {
                    for (Partition partition : outOfOrder)
                        inOrderWriter.append(partition.unfilteredIterator());
                    newInOrderSstable = inOrderWriter.finish(-1, sstable.maxDataAge, true);
                }
                transaction.update(newInOrderSstable, false);
                finished.add(newInOrderSstable);
                outputHandler.warn(String.format("%d out of order partition (or partitions with out of order rows) found while scrubbing %s; " +
                                                 "Those have been written (in order) to a new sstable (%s)", outOfOrder.size(), sstable, newInOrderSstable));
            }

            // finish obsoletes the old sstable
            finished.addAll(writer.setRepairedAt(badPartitions > 0 ? ActiveRepairService.UNREPAIRED_SSTABLE : sstable.getSSTableMetadata().repairedAt).finish());
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

        if (!finished.isEmpty())
        {
            outputHandler.output("Scrub of " + sstable + " complete: " + goodPartitions + " partitionss in new sstable and " + emptyPartitions + " empty (tombstoned) partitions dropped");
            if (negativeLocalDeletionInfoMetrics.fixedRows > 0)
                outputHandler.output("Fixed " + negativeLocalDeletionInfoMetrics.fixedRows + " rows with overflowed local deletion time.");
            if (badPartitions > 0)
                outputHandler.warn("Unable to recover " + badPartitions + " partitions that were skipped.  You can attempt manual recovery from the pre-scrub snapshot.  You can also run nodetool repair to transfer the data from a healthy replica, if any");
        }
        else
        {
            if (badPartitions > 0)
                outputHandler.warn("No valid partitions found while scrubbing " + sstable + "; it is marked for deletion now. If you want to attempt manual recovery, you can find a copy in the pre-scrub snapshot");
            else
                outputHandler.output("Scrub of " + sstable + " complete; looks like all " + emptyPartitions + " partitions were tombstoned");
        }
    }

    @SuppressWarnings("resource")
    private boolean tryAppend(DecoratedKey prevKey, DecoratedKey key, SSTableRewriter writer)
    {
        // OrderCheckerIterator will check, at iteration time, that the rows are in the proper order. If it detects
        // that one row is out of order, it will stop returning them. The remaining rows will be sorted and added
        // to the outOfOrder set that will be later written to a new SSTable.
        OrderCheckerIterator sstableIterator = new OrderCheckerIterator(getIterator(key),
                                                                        cfs.metadata.comparator);

        try (UnfilteredRowIterator iterator = withValidation(sstableIterator, dataFile.getPath()))
        {
            if (prevKey != null && prevKey.compareTo(key) > 0)
            {
                saveOutOfOrderPartition(prevKey, key, iterator);
                return false;
            }

            if (writer.tryAppend(iterator) == null)
                emptyPartitions++;
            else
                goodPartitions++;
        }

        if (sstableIterator.hasRowsOutOfOrder())
        {
            outputHandler.warn(String.format("Out of order rows found in partition: %s", keyString(key)));
            outOfOrder.add(sstableIterator.getRowsOutOfOrder());
        }

        return true;
    }

    /**
     * Only wrap with {@link FixNegativeLocalDeletionTimeIterator} if {@link #reinsertOverflowedTTLRows} option
     * is specified
     */
    private UnfilteredRowIterator getIterator(DecoratedKey key)
    {
        RowMergingSSTableIterator rowMergingIterator = new RowMergingSSTableIterator(SSTableIdentityIterator.create(sstable, dataFile, key));
        return reinsertOverflowedTTLRows ? new FixNegativeLocalDeletionTimeIterator(rowMergingIterator,
                                                                                    outputHandler,
                                                                                    negativeLocalDeletionInfoMetrics) : rowMergingIterator;
    }

    private void updateIndexKey()
    {
        currentIndexKey = nextIndexKey;
        currentPartitionPositionFromIndex = nextPartitionPositionFromIndex;
        try
        {
            nextIndexKey = !indexAvailable() ? null : ByteBufferUtil.readWithShortLength(indexFile);

            nextPartitionPositionFromIndex = !indexAvailable()
                                       ? dataFile.length()
                                       : rowIndexEntrySerializer.deserializePositionAndSkip(indexFile);
        }
        catch (Throwable th)
        {
            JVMStabilityInspector.inspectThrowable(th);
            outputHandler.warn("Error reading index file", th);
            nextIndexKey = null;
            nextPartitionPositionFromIndex = dataFile.length();
        }
    }

    private boolean indexAvailable()
    {
        return indexFile != null && !indexFile.isEOF();
    }

    private boolean seekToNextPartition()
    {
        while(nextPartitionPositionFromIndex < dataFile.length())
        {
            try
            {
                dataFile.seek(nextPartitionPositionFromIndex);
                return true;
            }
            catch (Throwable th)
            {
                throwIfFatal(th);
                outputHandler.warn(String.format("Failed to seek to next partition position %d", nextPartitionPositionFromIndex), th);
                badPartitions++;
            }

            updateIndexKey();
        }

        return false;
    }

    private void saveOutOfOrderPartition(DecoratedKey prevKey, DecoratedKey key, UnfilteredRowIterator iterator)
    {
        // TODO bitch if the row is too large?  if it is there's not much we can do ...
        outputHandler.warn(String.format("Out of order partition detected (%s found after %s)",
                                         keyString(key), keyString(prevKey)));
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
            outputHandler.warn(String.format("An error occurred while scrubbing the partition with key '%s' for an index table. " +
                                             "Scrubbing will abort for this table and the index will be rebuilt.", keyString(key)));
            throw new IOError(th);
        }

        if (isCommutative && !skipCorrupted)
        {
            outputHandler.warn(String.format("An error occurred while scrubbing the partition with key '%s'.  Skipping corrupt " +
                                             "data in counter tables will result in undercounts for the affected " +
                                             "counters (see CASSANDRA-2759 for more details), so by default the scrub will " +
                                             "stop at this point.  If you would like to skip the partition anyway and continue " +
                                             "scrubbing, re-run the scrub with the --skip-corrupted option.",
                                             keyString(key)));
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
                throw new RuntimeException(e);
            }
        }

        public boolean isGlobal()
        {
            return false;
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
        public final int goodPartitions;
        public final int badPartitions;
        public final int emptyPartitions;

        public ScrubResult(Scrubber scrubber)
        {
            this.goodPartitions = scrubber.goodPartitions;
            this.badPartitions = scrubber.badPartitions;
            this.emptyPartitions = scrubber.emptyPartitions;
        }
    }

    public class NegativeLocalDeletionInfoMetrics
    {
        public volatile int fixedRows = 0;
    }

    /**
     * During 2.x migration, under some circumstances rows might have gotten duplicated.
     * Merging iterator merges rows with same clustering.
     *
     * For more details, refer to CASSANDRA-12144.
     */
    private static class RowMergingSSTableIterator extends WrappingUnfilteredRowIterator
    {
        Unfiltered nextToOffer = null;

        RowMergingSSTableIterator(UnfilteredRowIterator source)
        {
            super(source);
        }

        @Override
        public boolean hasNext()
        {
            return nextToOffer != null || wrapped.hasNext();
        }

        @Override
        public Unfiltered next()
        {
            Unfiltered next = nextToOffer != null ? nextToOffer : wrapped.next();

            if (next.isRow())
            {
                while (wrapped.hasNext())
                {
                    Unfiltered peek = wrapped.next();
                    if (!peek.isRow() || !next.clustering().equals(peek.clustering()))
                    {
                        nextToOffer = peek; // Offer peek in next call
                        return next;
                    }

                    // Duplicate row, merge it.
                    next = Rows.merge((Row) next, (Row) peek, FBUtilities.nowInSeconds());
                }
            }

            nextToOffer = null;
            return next;
        }
    }

    /**
     * In some case like CASSANDRA-12127 the cells might have been stored in the wrong order. This decorator check the
     * cells order and collect the out of order cells to correct the problem.
     */
    private static final class OrderCheckerIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
    {
        /**
         * The decorated iterator.
         */
        private final UnfilteredRowIterator iterator;

        private final ClusteringComparator comparator;

        private Unfiltered previous;

        /**
         * The partition containing the rows which are out of order.
         */
        private Partition rowsOutOfOrder;

        public OrderCheckerIterator(UnfilteredRowIterator iterator, ClusteringComparator comparator)
        {
            this.iterator = iterator;
            this.comparator = comparator;
        }

        public CFMetaData metadata()
        {
            return iterator.metadata();
        }

        public boolean isReverseOrder()
        {
            return iterator.isReverseOrder();
        }

        public PartitionColumns columns()
        {
            return iterator.columns();
        }

        public DecoratedKey partitionKey()
        {
            return iterator.partitionKey();
        }

        public Row staticRow()
        {
            return iterator.staticRow();
        }

        @Override
        public boolean isEmpty()
        {
            return iterator.isEmpty();
        }

        public void close()
        {
            iterator.close();
        }

        public DeletionTime partitionLevelDeletion()
        {
            return iterator.partitionLevelDeletion();
        }

        public EncodingStats stats()
        {
            return iterator.stats();
        }

        public boolean hasRowsOutOfOrder()
        {
            return rowsOutOfOrder != null;
        }

        public Partition getRowsOutOfOrder()
        {
            return rowsOutOfOrder;
        }

        protected Unfiltered computeNext()
        {
            if (!iterator.hasNext())
                return endOfData();

            Unfiltered next = iterator.next();

            // If we detect that some rows are out of order we will store and sort the remaining ones to insert them
            // in a separate SSTable.
            if (previous != null && comparator.compare(next, previous) < 0)
            {
                rowsOutOfOrder = ImmutableBTreePartition.create(UnfilteredRowIterators.concat(next, iterator), false);
                return endOfData();
            }
            previous = next;
            return next;
        }
    }

    /**
     * This iterator converts negative {@link AbstractCell#localDeletionTime()} into {@link AbstractCell#MAX_DELETION_TIME}
     *
     * This is to recover entries with overflowed localExpirationTime due to CASSANDRA-14092
     */
    private static final class FixNegativeLocalDeletionTimeIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
    {
        /**
         * The decorated iterator.
         */
        private final UnfilteredRowIterator iterator;

        private final OutputHandler outputHandler;
        private final NegativeLocalDeletionInfoMetrics negativeLocalExpirationTimeMetrics;

        public FixNegativeLocalDeletionTimeIterator(UnfilteredRowIterator iterator, OutputHandler outputHandler,
                                                    NegativeLocalDeletionInfoMetrics negativeLocalDeletionInfoMetrics)
        {
            this.iterator = iterator;
            this.outputHandler = outputHandler;
            this.negativeLocalExpirationTimeMetrics = negativeLocalDeletionInfoMetrics;
        }

        public CFMetaData metadata()
        {
            return iterator.metadata();
        }

        public boolean isReverseOrder()
        {
            return iterator.isReverseOrder();
        }

        public PartitionColumns columns()
        {
            return iterator.columns();
        }

        public DecoratedKey partitionKey()
        {
            return iterator.partitionKey();
        }

        public Row staticRow()
        {
            return iterator.staticRow();
        }

        @Override
        public boolean isEmpty()
        {
            return iterator.isEmpty();
        }

        public void close()
        {
            iterator.close();
        }

        public DeletionTime partitionLevelDeletion()
        {
            return iterator.partitionLevelDeletion();
        }

        public EncodingStats stats()
        {
            return iterator.stats();
        }

        protected Unfiltered computeNext()
        {
            if (!iterator.hasNext())
                return endOfData();

            Unfiltered next = iterator.next();
            if (!next.isRow())
                return next;

            if (hasNegativeLocalExpirationTime((Row) next))
            {
                outputHandler.debug(String.format("Found row with negative local expiration time: %s", next.toString(metadata(), false)));
                negativeLocalExpirationTimeMetrics.fixedRows++;
                return fixNegativeLocalExpirationTime((Row) next);
            }

            return next;
        }

        private boolean hasNegativeLocalExpirationTime(Row next)
        {
            Row row = next;
            if (row.primaryKeyLivenessInfo().isExpiring() && row.primaryKeyLivenessInfo().localExpirationTime() < 0)
            {
                return true;
            }

            for (ColumnData cd : row)
            {
                if (cd.column().isSimple())
                {
                    Cell cell = (Cell)cd;
                    if (cell.isExpiring() && cell.localDeletionTime() < 0)
                        return true;
                }
                else
                {
                    ComplexColumnData complexData = (ComplexColumnData)cd;
                    for (Cell cell : complexData)
                    {
                        if (cell.isExpiring() && cell.localDeletionTime() < 0)
                            return true;
                    }
                }
            }

            return false;
        }

        private Unfiltered fixNegativeLocalExpirationTime(Row row)
        {
            Row.Builder builder = HeapAllocator.instance.cloningBTreeRowBuilder();
            builder.newRow(row.clustering());
            builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo().isExpiring() && row.primaryKeyLivenessInfo().localExpirationTime() < 0 ?
                                              row.primaryKeyLivenessInfo().withUpdatedTimestampAndLocalDeletionTime(row.primaryKeyLivenessInfo().timestamp() + 1, AbstractCell.MAX_DELETION_TIME)
                                              :row.primaryKeyLivenessInfo());
            builder.addRowDeletion(row.deletion());
            for (ColumnData cd : row)
            {
                if (cd.column().isSimple())
                {
                    Cell cell = (Cell)cd;
                    builder.addCell(cell.isExpiring() && cell.localDeletionTime() < 0 ? cell.withUpdatedTimestampAndLocalDeletionTime(cell.timestamp() + 1, AbstractCell.MAX_DELETION_TIME) : cell);
                }
                else
                {
                    ComplexColumnData complexData = (ComplexColumnData)cd;
                    builder.addComplexDeletion(complexData.column(), complexData.complexDeletion());
                    for (Cell cell : complexData)
                    {
                        builder.addCell(cell.isExpiring() && cell.localDeletionTime() < 0 ? cell.withUpdatedTimestampAndLocalDeletionTime(cell.timestamp() + 1, AbstractCell.MAX_DELETION_TIME) : cell);
                    }
                }
            }
            return builder.build();
        }
    }

}
