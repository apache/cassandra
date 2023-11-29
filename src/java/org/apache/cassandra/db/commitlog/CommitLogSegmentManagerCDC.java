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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.io.util.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogSegment.CDCState;
import org.apache.cassandra.exceptions.CDCWriteException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.DirectorySizeCalculator;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class CommitLogSegmentManagerCDC extends AbstractCommitLogSegmentManager
{
    static final Logger logger = LoggerFactory.getLogger(CommitLogSegmentManagerCDC.class);
    private final CDCSizeTracker cdcSizeTracker;

    public CommitLogSegmentManagerCDC(final CommitLog commitLog, String storageDirectory)
    {
        super(commitLog, storageDirectory);
        cdcSizeTracker = new CDCSizeTracker(this, new File(DatabaseDescriptor.getCDCLogLocation()));
    }

    @Override
    void start()
    {
        cdcSizeTracker.start();
        super.start();
    }

    public void discard(CommitLogSegment segment, boolean delete)
    {
        segment.close();
        addSize(-segment.onDiskSize());

        cdcSizeTracker.processDiscardedSegment(segment);

        if (delete)
            segment.logFile.delete();

        if (segment.getCDCState() != CDCState.CONTAINS)
        {
            // Always delete hard-link from cdc folder if this segment didn't contain CDC data. Note: File may not exist
            // if processing discard during startup.
            File cdcLink = segment.getCDCFile();
            File cdcIndexFile = segment.getCDCIndexFile();
            deleteCDCFiles(cdcLink, cdcIndexFile);
        }
    }

    /**
     * Delete the oldest hard-linked CDC commit log segment to free up space.
     * @param bytesToFree the minimum space to free up
     * @return total size under the CDC folder in bytes after deletion
     */
    public long deleteOldLinkedCDCCommitLogSegment(long bytesToFree)
    {
        if (bytesToFree <= 0)
            return 0;

        File cdcDir = new File(DatabaseDescriptor.getCDCLogLocation());
        Preconditions.checkState(cdcDir.isDirectory(), "The CDC directory does not exist.");
        File[] files = cdcDir.tryList(f -> CommitLogDescriptor.isValid(f.name()));
        if (files == null || files.length == 0)
        {
            logger.warn("Skip deleting due to no CDC commit log segments found.");
            return 0;
        }
        List<File> sorted = Arrays.stream(files)
                                  // sort by the commmit log segment id
                                  .sorted(new CommitLogSegment.CommitLogSegmentFileComparator())
                                  .collect(Collectors.toList());
        long bytesDeleted = 0;
        long bytesRemaining = 0;
        boolean deletionCompleted = false;
        // keep deleting from old to new until it reaches to the goal or the current writing segment
        for (File linkedCdcFile : sorted)
        {
            // only evaluate/update when deletionCompleted is false
            if (!deletionCompleted)
            {
                deletionCompleted = bytesDeleted >= bytesToFree || linkedCdcFile.equals(allocatingFrom().getCDCFile());
            }

            if (deletionCompleted)
            {
                bytesRemaining += linkedCdcFile.length();
            }
            else
            {
                File cdcIndexFile = CommitLogDescriptor.inferCdcIndexFile(linkedCdcFile);
                bytesDeleted += deleteCDCFiles(linkedCdcFile, cdcIndexFile);
            }
        }
        return bytesRemaining;
    }

    private long deleteCDCFiles(File cdcLink, File cdcIndexFile)
    {
        long total = 0;
        if (cdcLink != null && cdcLink.exists())
        {
            total += cdcLink.length();
            cdcLink.delete();
        }

        if (cdcIndexFile != null && cdcIndexFile.exists())
        {
            total += cdcIndexFile.length();
            cdcIndexFile.delete();
        }
        return total;
    }

    /**
     * Initiates the shutdown process for the management thread. Also stops the cdc on-disk size calculator executor.
     */
    public void shutdown()
    {
        cdcSizeTracker.shutdown();
        super.shutdown();
    }

    /**
     * Reserve space in the current segment for the provided mutation or, if there isn't space available,
     * create a new segment. For CDC mutations, allocation is expected to throw WTE if the segment disallows CDC mutations.
     *
     * @param mutation Mutation to allocate in segment manager
     * @param size total size (overhead + serialized) of mutation
     * @return the created Allocation object
     * @throws CDCWriteException If segment disallows CDC mutations, we throw
     */
    @Override
    public CommitLogSegment.Allocation allocate(Mutation mutation, int size) throws CDCWriteException
    {
        CommitLogSegment segment = allocatingFrom();
        CommitLogSegment.Allocation alloc;

        permitSegmentMaybe(segment);
        throwIfForbidden(mutation, segment);
        while ( null == (alloc = segment.allocate(mutation, size)) )
        {
            // Failed to allocate, so move to a new segment with enough room if possible.
            advanceAllocatingFrom(segment);
            segment = allocatingFrom();

            permitSegmentMaybe(segment);
            throwIfForbidden(mutation, segment);
        }

        if (mutation.trackedByCDC())
            segment.setCDCState(CDCState.CONTAINS);

        return alloc;
    }

    // Permit a forbidden segment under the following conditions.
    // - Non-blocking mode has just recently been enabled for CDC.
    // - The CDC total space has droppped below the limit (e.g. CDC consumer cleans up).
    private void permitSegmentMaybe(CommitLogSegment segment)
    {
        if (segment.getCDCState() != CDCState.FORBIDDEN)
            return;

        if (!DatabaseDescriptor.getCDCBlockWrites()
            || cdcSizeTracker.sizeInProgress.get() + DatabaseDescriptor.getCommitLogSegmentSize() < DatabaseDescriptor.getCDCTotalSpace())
        {
            CDCState oldState = segment.setCDCState(CDCState.PERMITTED);
            if (oldState == CDCState.FORBIDDEN)
            {
                FileUtils.createHardLink(segment.logFile, segment.getCDCFile());
                cdcSizeTracker.addSize(DatabaseDescriptor.getCommitLogSegmentSize());
            }
        }
    }

    private void throwIfForbidden(Mutation mutation, CommitLogSegment segment) throws CDCWriteException
    {
        if (mutation.trackedByCDC() && segment.getCDCState() == CDCState.FORBIDDEN)
        {
            cdcSizeTracker.submitOverflowSizeRecalculation();
            String logMsg = String.format("Rejecting mutation to keyspace %s. Free up space in %s by processing CDC logs. " +
                                          "Total CDC bytes on disk is %s.",
                                          mutation.getKeyspaceName(), DatabaseDescriptor.getCDCLogLocation(),
                                          cdcSizeTracker.sizeInProgress.get());
            NoSpamLogger.log(logger,
                             NoSpamLogger.Level.WARN,
                             10,
                             TimeUnit.SECONDS,
                             logMsg);
            throw new CDCWriteException(logMsg);
        }
    }

    /**
     * On segment creation, flag whether the segment should accept CDC mutations or not based on the total currently
     * allocated unflushed CDC segments and the contents of cdc_raw
     *
     * Synchronized on this
     */
    @Override
    public CommitLogSegment createSegment()
    {
        CommitLogSegment segment = super.createSegment();
        segment.writeLogHeader();
        cdcSizeTracker.processNewSegment(segment);
        // After processing, the state of the segment can either be PERMITTED or FORBIDDEN
        if (segment.getCDCState() == CDCState.PERMITTED)
        {
            // Hard link file in cdc folder for realtime tracking
            FileUtils.createHardLink(segment.logFile, segment.getCDCFile());
        }
        return segment;
    }

    /**
     * Delete untracked segment files after replay
     *
     * @param file segment file that is no longer in use.
     */
    @Override
    void handleReplayedSegment(final File file)
    {
        super.handleReplayedSegment(file);

        // delete untracked cdc segment hard link files if their index files do not exist
        File cdcFile = new File(DatabaseDescriptor.getCDCLogLocation(), file.name());
        File cdcIndexFile = new File(DatabaseDescriptor.getCDCLogLocation(), CommitLogDescriptor.fromFileName(file.name()).cdcIndexFileName());
        if (cdcFile.exists() && !cdcIndexFile.exists())
        {
            logger.trace("(Unopened) CDC segment {} is no longer needed and will be deleted now", cdcFile);
            cdcFile.delete();
        }
    }

    /**
     * For use after replay when replayer hard-links / adds tracking of replayed segments
     */
    public void addCDCSize(long size)
    {
        cdcSizeTracker.addSize(size);
    }

    /**
     * Tracks total disk usage of CDC subsystem, defined by the summation of all unflushed CommitLogSegments with CDC
     * data in them and all segments archived into cdc_raw.
     *
     * Allows atomic increment/decrement of unflushed size, however only allows increment on flushed and requires a full
     * directory walk to determine any potential deletions by CDC consumer.
     */
    private static class CDCSizeTracker extends DirectorySizeCalculator
    {
        private final RateLimiter rateLimiter = RateLimiter.create(1000.0 / DatabaseDescriptor.getCDCDiskCheckInterval());
        private ExecutorService cdcSizeCalculationExecutor;
        private final CommitLogSegmentManagerCDC segmentManager;
        // track the total size between two dictionary size calculations
        private final AtomicLong sizeInProgress;

        private final File path;

        CDCSizeTracker(CommitLogSegmentManagerCDC segmentManager, File path)
        {
            this.path = path;
            this.segmentManager = segmentManager;
            this.sizeInProgress = new AtomicLong(0);
        }

        /**
         * Needed for stop/restart during unit tests
         */
        public void start()
        {
            sizeInProgress.getAndSet(0);
            cdcSizeCalculationExecutor = executorFactory().configureSequential("CDCSizeCalculationExecutor")
                                                          .withRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy())
                                                          .withQueueLimit(0)
                                                          .withKeepAlive(1000, TimeUnit.SECONDS)
                                                          .build();
        }

        /**
         * Synchronous size recalculation on each segment creation/deletion call could lead to very long delays in new
         * segment allocation, thus long delays in thread signaling to wake waiting allocation / writer threads.
         *
         * This can be reached either from the segment management thread in AbstractCommitLogSegmentManager or from the
         * size recalculation executor, so we synchronize on this object to reduce the race overlap window available for
         * size to get off.
         *
         * Reference DirectorySizerBench for more information about performance of the directory size recalc.
         */
        void processNewSegment(CommitLogSegment segment)
        {
            int segmentSize = defaultSegmentSize();
            long allowance = DatabaseDescriptor.getCDCTotalSpace();
            boolean blocking = DatabaseDescriptor.getCDCBlockWrites();

            // See synchronization in CommitLogSegment.setCDCState
            synchronized (segment.cdcStateLock)
            {
                segment.setCDCState(blocking && segmentSize + sizeInProgress.get() > allowance
                                    ? CDCState.FORBIDDEN
                                    : CDCState.PERMITTED);

                // Aggressively count in the (estimated) size of new segments.
                if (segment.getCDCState() == CDCState.PERMITTED)
                    addSize(segmentSize);
            }

            // Remove the oldest cdc segment file when exceeding the CDC storage allowance
            if (!blocking && sizeInProgress.get() > allowance)
            {
                long bytesToFree = sizeInProgress.get() - allowance;
                long remainingSize = segmentManager.deleteOldLinkedCDCCommitLogSegment(bytesToFree);
                long releasedSize = sizeInProgress.get() - remainingSize;
                sizeInProgress.getAndSet(remainingSize);
                logger.debug("Freed up {} ({}) bytes after deleting the oldest CDC commit log segments in non-blocking mode. " +
                             "Total on-disk CDC size: {}; allowed CDC size: {}",
                             releasedSize, bytesToFree, remainingSize, allowance);
            }

            // Take this opportunity to kick off a recalc to pick up any consumer file deletion.
            submitOverflowSizeRecalculation();
        }

        void processDiscardedSegment(CommitLogSegment segment)
        {
            if (!segment.getCDCFile().exists())
            {
                logger.debug("Not processing discarded CommitLogSegment {}; this segment appears to have been deleted already.", segment);
                return;
            }

            synchronized (segment.cdcStateLock)
            {
                // Add to flushed size before decrementing unflushed, so we don't have a window of false generosity
                if (segment.getCDCState() == CDCState.CONTAINS)
                    addSize(segment.onDiskSize());

                // Subtract the (estimated) size of the segment from processNewSegment.
                // For the segement that CONTAINS, we update with adding the actual onDiskSize and removing the estimated size.
                // For the segment that remains in PERMITTED, the file is to be deleted and the estimate should be returned.
                if (segment.getCDCState() != CDCState.FORBIDDEN)
                    addSize(-defaultSegmentSize());
            }

            // Take this opportunity to kick off a recalc to pick up any consumer file deletion.
            submitOverflowSizeRecalculation();
        }

        public void submitOverflowSizeRecalculation()
        {
            try
            {
                cdcSizeCalculationExecutor.submit(() -> {
                    rateLimiter.acquire();
                    calculateSize();
                });
            }
            catch (RejectedExecutionException e)
            {
                // Do nothing. Means we have one in flight so this req. should be satisfied when it completes.
            }
        }

        private int defaultSegmentSize()
        {
            // CommitLogSegmentSize is only loaded from yaml.
            // There is a setter but is used only for testing.
            return DatabaseDescriptor.getCommitLogSegmentSize();
        }

        private void calculateSize()
        {
            try
            {
                resetSize();
                Files.walkFileTree(path.toPath(), this);
                sizeInProgress.getAndSet(getAllocatedSize());
            }
            catch (IOException ie)
            {
                CommitLog.handleCommitError("Failed CDC Size Calculation", ie);
            }
        }

        public void shutdown()
        {
            if (cdcSizeCalculationExecutor != null && !cdcSizeCalculationExecutor.isShutdown())
            {
                cdcSizeCalculationExecutor.shutdown();
            }
        }

        private void addSize(long toAdd)
        {
            sizeInProgress.getAndAdd(toAdd);
        }
    }

    /**
     * Only use for testing / validation that size tracker is working. Not for production use.
     */
    @VisibleForTesting
    public long updateCDCTotalSize()
    {
        long sleepTime = DatabaseDescriptor.getCDCDiskCheckInterval() + 50L;
        // Give the update time to finish the last run if any. Therefore, avoid modifying production code only for testing purpose.
        Uninterruptibles.sleepUninterruptibly(sleepTime, TimeUnit.MILLISECONDS);
        cdcSizeTracker.submitOverflowSizeRecalculation();
        // Give the update time to run
        Uninterruptibles.sleepUninterruptibly(sleepTime, TimeUnit.MILLISECONDS);
        // then update the state of the segment it is allocating from. In produciton, the state is updated during "allocate"
        if (allocatingFrom().getCDCState() == CDCState.FORBIDDEN)
            cdcSizeTracker.processNewSegment(allocatingFrom());
        return cdcSizeTracker.getAllocatedSize();
    }
}
