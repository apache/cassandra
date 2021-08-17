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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogSegment.CDCState;
import org.apache.cassandra.exceptions.CDCWriteException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.DirectorySizeCalculator;
import org.apache.cassandra.utils.NoSpamLogger;

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
            FileUtils.deleteWithConfirm(segment.logFile);

        if (segment.getCDCState() != CDCState.CONTAINS)
        {
            // Always delete hard-link from cdc folder if this segment didn't contain CDC data. Note: File may not exist
            // if processing discard during startup.
            File cdcLink = segment.getCDCFile();
            if (cdcLink.exists())
                FileUtils.deleteWithConfirm(cdcLink);

            File cdcIndexFile = segment.getCDCIndexFile();
            if (cdcIndexFile.exists())
                FileUtils.deleteWithConfirm(cdcIndexFile);
        }
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

        throwIfForbidden(mutation, segment);
        while ( null == (alloc = segment.allocate(mutation, size)) )
        {
            // Failed to allocate, so move to a new segment with enough room if possible.
            advanceAllocatingFrom(segment);
            segment = allocatingFrom();

            throwIfForbidden(mutation, segment);
        }

        if (mutation.trackedByCDC())
            segment.setCDCState(CDCState.CONTAINS);

        return alloc;
    }

    private void throwIfForbidden(Mutation mutation, CommitLogSegment segment) throws CDCWriteException
    {
        if (mutation.trackedByCDC() && segment.getCDCState() == CDCState.FORBIDDEN)
        {
            cdcSizeTracker.submitOverflowSizeRecalculation();
            String logMsg = String.format("Rejecting mutation to keyspace %s. Free up space in %s by processing CDC logs.",
                mutation.getKeyspaceName(), DatabaseDescriptor.getCDCLogLocation());
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
     */
    public CommitLogSegment createSegment()
    {
        CommitLogSegment segment = CommitLogSegment.createSegment(commitLog, this);

        // Hard link file in cdc folder for realtime tracking
        FileUtils.createHardLink(segment.logFile, segment.getCDCFile());

        cdcSizeTracker.processNewSegment(segment);
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
        File cdcFile = new File(DatabaseDescriptor.getCDCLogLocation(), file.getName());
        File cdcIndexFile = new File(DatabaseDescriptor.getCDCLogLocation(), CommitLogDescriptor.fromFileName(file.getName()).cdcIndexFileName());
        if (cdcFile.exists() && !cdcIndexFile.exists())
        {
            logger.trace("(Unopened) CDC segment {} is no longer needed and will be deleted now", cdcFile);
            FileUtils.deleteWithConfirm(cdcFile);
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
        private CommitLogSegmentManagerCDC segmentManager;

        // Used instead of size during walk to remove chance of over-allocation
        private volatile long sizeInProgress = 0;

        CDCSizeTracker(CommitLogSegmentManagerCDC segmentManager, File path)
        {
            super(path);
            this.segmentManager = segmentManager;
        }

        /**
         * Needed for stop/restart during unit tests
         */
        public void start()
        {
            size = 0;
            cdcSizeCalculationExecutor = new ThreadPoolExecutor(1, 1, 1000, TimeUnit.SECONDS, new SynchronousQueue<>(), new ThreadPoolExecutor.DiscardPolicy());
        }

        /**
         * Synchronous size recalculation on each segment creation/deletion call could lead to very long delays in new
         * segment allocation, thus long delays in thread signaling to wake waiting allocation / writer threads.
         *
         * This can be reached either from the segment management thread in ABstractCommitLogSegmentManager or from the
         * size recalculation executor, so we synchronize on this object to reduce the race overlap window available for
         * size to get off.
         *
         * Reference DirectorySizerBench for more information about performance of the directory size recalc.
         */
        void processNewSegment(CommitLogSegment segment)
        {
            // See synchronization in CommitLogSegment.setCDCState
            synchronized(segment.cdcStateLock)
            {
                segment.setCDCState(defaultSegmentSize() + totalCDCSizeOnDisk() > allowableCDCBytes()
                                    ? CDCState.FORBIDDEN
                                    : CDCState.PERMITTED);
                if (segment.getCDCState() == CDCState.PERMITTED)
                    size += defaultSegmentSize();
            }

            // Take this opportunity to kick off a recalc to pick up any consumer file deletion.
            submitOverflowSizeRecalculation();
        }

        void processDiscardedSegment(CommitLogSegment segment)
        {
            // See synchronization in CommitLogSegment.setCDCState
            synchronized(segment.cdcStateLock)
            {
                // Add to flushed size before decrementing unflushed so we don't have a window of false generosity
                if (segment.getCDCState() == CDCState.CONTAINS)
                    size += segment.onDiskSize();
                if (segment.getCDCState() != CDCState.FORBIDDEN)
                    size -= defaultSegmentSize();
            }

            // Take this opportunity to kick off a recalc to pick up any consumer file deletion.
            submitOverflowSizeRecalculation();
        }

        private long allowableCDCBytes()
        {
            return (long)DatabaseDescriptor.getCDCSpaceInMB() * 1024 * 1024;
        }

        public void submitOverflowSizeRecalculation()
        {
            try
            {
                cdcSizeCalculationExecutor.submit(() -> recalculateOverflowSize());
            }
            catch (RejectedExecutionException e)
            {
                // Do nothing. Means we have one in flight so this req. should be satisfied when it completes.
            }
        }

        private void recalculateOverflowSize()
        {
            rateLimiter.acquire();
            calculateSize();
            CommitLogSegment allocatingFrom = segmentManager.allocatingFrom();
            if (allocatingFrom.getCDCState() == CDCState.FORBIDDEN)
                processNewSegment(allocatingFrom);
        }

        private int defaultSegmentSize()
        {
            return DatabaseDescriptor.getCommitLogSegmentSize();
        }

        private void calculateSize()
        {
            try
            {
                // The Arrays.stream approach is considerably slower on Windows than linux
                sizeInProgress = 0;
                Files.walkFileTree(path.toPath(), this);
                size = sizeInProgress;
            }
            catch (IOException ie)
            {
                CommitLog.instance.handleCommitError("Failed CDC Size Calculation", ie);
            }
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
        {
            sizeInProgress += attrs.size();
            return FileVisitResult.CONTINUE;
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
            size += toAdd;
        }

        private long totalCDCSizeOnDisk()
        {
            return size;
        }
    }

    /**
     * Only use for testing / validation that size tracker is working. Not for production use.
     */
    @VisibleForTesting
    public long updateCDCTotalSize()
    {
        cdcSizeTracker.submitOverflowSizeRecalculation();

        // Give the update time to run
        try
        {
            Thread.sleep(DatabaseDescriptor.getCDCDiskCheckInterval() + 10);
        }
        catch (InterruptedException e) {}

        return cdcSizeTracker.totalCDCSizeOnDisk();
    }
}
