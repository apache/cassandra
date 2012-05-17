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

import java.io.IOError;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * Performs the pre-allocation of commit log segments in a background thread. All the
 * public methods are thread safe.
 */
public class CommitLogAllocator
{
    static final Logger logger = LoggerFactory.getLogger(CommitLogAllocator.class);

    /** The (theoretical) max milliseconds between loop runs to perform janitorial tasks */
    public final static int TICK_CYCLE_TIME = 100;

    /** Segments that are ready to be used */
    private final BlockingQueue<CommitLogSegment> availableSegments = new LinkedBlockingQueue<CommitLogSegment>();

    /** Allocations to be run by the thread */
    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();

    /** Active segments, containing unflushed data */
    private final ConcurrentLinkedQueue<CommitLogSegment> activeSegments = new ConcurrentLinkedQueue<CommitLogSegment>();

    /**
     * Tracks commitlog size, in multiples of the segment size.  We need to do this so we can "promise" size
     * adjustments ahead of actually adding/freeing segments on disk, so that the "evict oldest segment" logic
     * can see the effect of recycling segments immediately (even though they're really happening asynchronously
     * on the allocator thread, which will take a ms or two).
     */
    private final AtomicLong size = new AtomicLong();

    /**
     * New segment creation is initially disabled because we'll typically get some "free" segments
     * recycled after log replay.
     */
    private volatile boolean createReserveSegments = false;

    private final Thread allocationThread;
    private volatile boolean run = true;

    public CommitLogAllocator()
    {
        // The run loop for the allocation thread
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                while (run)
                {
                    Runnable r = queue.poll(TICK_CYCLE_TIME, TimeUnit.MILLISECONDS);

                    if (r != null)
                    {
                        r.run();
                    }
                    else
                    {
                        // no job, so we're clear to check to see if we're out of segments
                        // and ready a new one if needed. has the effect of ensuring there's
                        // almost always a segment available when it's needed.
                        if (availableSegments.isEmpty() && (activeSegments.isEmpty() || createReserveSegments))
                        {
                            logger.debug("No segments in reserve; creating a fresh one");
                            createFreshSegment();
                        }
                    }
                }
            }
        };

        allocationThread = new Thread(runnable, "COMMIT-LOG-ALLOCATOR");
        allocationThread.start();
    }

    /**
     * Fetches an empty segment file.
     *
     * @return the next writable segment
     */
    public CommitLogSegment fetchSegment()
    {
        CommitLogSegment next;
        try
        {
            next = availableSegments.take();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }

        assert !activeSegments.contains(next);
        activeSegments.add(next);
        if (isCapExceeded())
            flushOldestTables();

        return next;
    }

    /**
     * Indicates that a segment is no longer in use and that it should be recycled.
     *
     * @param segment segment that is no longer in use
     */
    public void recycleSegment(final CommitLogSegment segment)
    {
        activeSegments.remove(segment);
        if (!CommitLog.instance.archiver.maybeWaitForArchiving(segment.getName()))
        {
            // if archiving (command) was not successful then leave the file alone. don't delete or recycle.
            discardSegment(segment, false);
            return;
        }
        if (isCapExceeded())
        {
            discardSegment(segment, true);
            return;
        }

        logger.debug("Recycling {}", segment);
        queue.add(new Runnable()
        {
            public void run()
            {
                CommitLogSegment recycled = segment.recycle();
                internalAddReadySegment(recycled);
            }
        });
    }

    /**
     * Differs from the above because it can work on any file instead of just existing
     * commit log segments managed by this allocator.
     *
     * @param file segment file that is no longer in use.
     */
    public void recycleSegment(final File file)
    {
        // check against SEGMENT_SIZE avoids recycling odd-sized or empty segments from old C* versions and unit tests
        if (isCapExceeded() || file.length() != DatabaseDescriptor.getCommitLogSegmentSize())
        {
            // (don't decrease managed size, since this was never a "live" segment)
            try
            {
                logger.debug("(Unopened) segment {} is no longer needed and will be deleted now", file);
                FileUtils.deleteWithConfirm(file);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            return;
        }

        logger.debug("Recycling {}", file);
        // this wasn't previously a live segment, so add it to the managed size when we make it live
        size.addAndGet(DatabaseDescriptor.getCommitLogSegmentSize());
        queue.add(new Runnable()
        {
            public void run()
            {
                CommitLogSegment segment = new CommitLogSegment(file.getPath());
                internalAddReadySegment(segment);
            }
        });
    }

    /**
     * Indicates that a segment file should be deleted.
     *
     * @param segment segment to be discarded
     */
    private void discardSegment(final CommitLogSegment segment, final boolean deleteFile)
    {
        logger.debug("Segment {} is no longer active and will be deleted {}", segment, deleteFile ? "now" : "by the archive script");
        size.addAndGet(-DatabaseDescriptor.getCommitLogSegmentSize());

        queue.add(new Runnable()
        {
            public void run()
            {
                segment.discard(deleteFile);
            }
        });
    }

    /**
     * @return the space (in bytes) used by all segment files.
     */
    public long bytesUsed()
    {
        return size.get();
    }

    /**
     * @param name the filename to check
     * @return true if file is managed by this allocator.
     */
    public boolean manages(String name)
    {
        for (CommitLogSegment segment : Iterables.concat(activeSegments, availableSegments))
            if (segment.getName().equals(name))
                return true;

        return false;
    }

    /**
     * Creates and readies a brand new segment.
     *
     * @return the newly minted segment
     */
    private CommitLogSegment createFreshSegment()
    {
        size.addAndGet(DatabaseDescriptor.getCommitLogSegmentSize());
        return internalAddReadySegment(CommitLogSegment.freshSegment());
    }

    /**
     * Adds a segment to our internal tracking list and makes it ready for consumption.
     *
     * @param   segment the segment to add
     * @return  the newly added segment
     */
    private CommitLogSegment internalAddReadySegment(CommitLogSegment segment)
    {
        assert !activeSegments.contains(segment);
        assert !availableSegments.contains(segment);
        availableSegments.add(segment);
        return segment;
    }

    /**
     * Check to see if the speculative current size exceeds the cap.
     *
     * @return true if cap is exceeded
     */
    private boolean isCapExceeded()
    {
        long currentSize = size.get();
        logger.debug("Total active commitlog segment space used is {}", currentSize);
        return currentSize > DatabaseDescriptor.getTotalCommitlogSpaceInMB() * 1024 * 1024;
    }

    /**
     * Throws a flag that enables the behavior of keeping at least one spare segment
     * available at all times.
     */
    public void enableReserveSegmentCreation()
    {
        createReserveSegments = true;
    }

    /**
     * Force a flush on all dirty CFs represented in the oldest commitlog segment
     */
    private void flushOldestTables()
    {
        CommitLogSegment oldestSegment = activeSegments.peek();

        if (oldestSegment != null)
        {
            for (UUID dirtyCFId : oldestSegment.getDirtyCFIDs())
            {
                String keypace = Schema.instance.getCF(dirtyCFId).left;
                final ColumnFamilyStore cfs = Table.open(keypace).getColumnFamilyStore(dirtyCFId);
                // flush shouldn't run on the commitlog executor, since it acquires Table.switchLock,
                // which may already be held by a thread waiting for the CL executor (via getContext),
                // causing deadlock
                Runnable runnable = new Runnable()
                {
                    public void run()
                    {
                        cfs.forceFlush();
                    }
                };
                StorageService.optionalTasks.execute(runnable);
            }
        }
    }

    /**
     * Resets all the segments, for testing purposes. DO NOT USE THIS OUTSIDE OF TESTS.
     */
    public void resetUnsafe()
    {
        logger.debug("Closing and clearing existing commit log segments...");

        while (!queue.isEmpty())
            Thread.yield();

        for (CommitLogSegment segment : activeSegments)
            segment.close();

        activeSegments.clear();
        availableSegments.clear();
    }

    /**
     * Initiates the shutdown process for the allocator thread.
     */
    public void shutdown()
    {
        run = false;
    }

    /**
     * Returns when the allocator thread terminates.
     */
    public void awaitTermination() throws InterruptedException
    {
        allocationThread.join();
    }

    /**
     * @return a read-only collection of the active commit log segments
     */
    public Collection<CommitLogSegment> getActiveSegments()
    {
        return Collections.unmodifiableCollection(activeSegments);
    }
}

