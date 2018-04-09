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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;

/**
 * Performs eager-creation of commit log segments in a background thread. All the
 * public methods are thread safe.
 */
public abstract class AbstractCommitLogSegmentManager
{
    static final Logger logger = LoggerFactory.getLogger(AbstractCommitLogSegmentManager.class);

    /**
     * Segment that is ready to be used. The management thread fills this and blocks until consumed.
     *
     * A single management thread produces this, and consumers are already synchronizing to make sure other work is
     * performed atomically with consuming this. Volatile to make sure writes by the management thread become
     * visible (ordered/lazySet would suffice). Consumers (advanceAllocatingFrom and discardAvailableSegment) must
     * synchronize on 'this'.
     */
    private volatile CommitLogSegment availableSegment = null;

    private final WaitQueue segmentPrepared = new WaitQueue();

    /** Active segments, containing unflushed data. The tail of this queue is the one we allocate writes to */
    private final ConcurrentLinkedQueue<CommitLogSegment> activeSegments = new ConcurrentLinkedQueue<>();

    /**
     * The segment we are currently allocating commit log records to.
     *
     * Written by advanceAllocatingFrom which synchronizes on 'this'. Volatile to ensure reads get current value.
     */
    private volatile CommitLogSegment allocatingFrom = null;

    final String storageDirectory;

    /**
     * Tracks commitlog size, in multiples of the segment size.  We need to do this so we can "promise" size
     * adjustments ahead of actually adding/freeing segments on disk, so that the "evict oldest segment" logic
     * can see the effect of recycling segments immediately (even though they're really happening asynchronously
     * on the manager thread, which will take a ms or two).
     */
    private final AtomicLong size = new AtomicLong();

    private Thread managerThread;
    protected final CommitLog commitLog;
    private volatile boolean shutdown;
    private final BooleanSupplier managerThreadWaitCondition = () -> (availableSegment == null && !atSegmentBufferLimit()) || shutdown;
    private final WaitQueue managerThreadWaitQueue = new WaitQueue();

    private static final SimpleCachedBufferPool bufferPool =
        new SimpleCachedBufferPool(DatabaseDescriptor.getCommitLogMaxCompressionBuffersInPool(), DatabaseDescriptor.getCommitLogSegmentSize());

    AbstractCommitLogSegmentManager(final CommitLog commitLog, String storageDirectory)
    {
        this.commitLog = commitLog;
        this.storageDirectory = storageDirectory;
    }

    void start()
    {
        // The run loop for the manager thread
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                while (!shutdown)
                {
                    try
                    {
                        assert availableSegment == null;
                        logger.trace("No segments in reserve; creating a fresh one");
                        availableSegment = createSegment();
                        if (shutdown)
                        {
                            // If shutdown() started and finished during segment creation, we are now left with a
                            // segment that no one will consume. Discard it.
                            discardAvailableSegment();
                            return;
                        }

                        segmentPrepared.signalAll();
                        Thread.yield();

                        if (availableSegment == null && !atSegmentBufferLimit())
                            // Writing threads need another segment now.
                            continue;

                        // Writing threads are not waiting for new segments, we can spend time on other tasks.
                        // flush old Cfs if we're full
                        maybeFlushToReclaim();
                    }
                    catch (Throwable t)
                    {
                        JVMStabilityInspector.inspectThrowable(t);
                        if (!CommitLog.handleCommitError("Failed managing commit log segments", t))
                            return;
                        // sleep some arbitrary period to avoid spamming CL
                        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

                        // If we offered a segment, wait for it to be taken before reentering the loop.
                        // There could be a new segment in next not offered, but only on failure to discard it while
                        // shutting down-- nothing more can or needs to be done in that case.
                    }

                    WaitQueue.waitOnCondition(managerThreadWaitCondition, managerThreadWaitQueue);
                }
            }
        };

        shutdown = false;
        managerThread = NamedThreadFactory.createThread(runnable, "COMMIT-LOG-ALLOCATOR");
        managerThread.start();

        // for simplicity, ensure the first segment is allocated before continuing
        advanceAllocatingFrom(null);
    }

    private boolean atSegmentBufferLimit()
    {
        return CommitLogSegment.usesBufferPool(commitLog) && bufferPool.atLimit();
    }

    private void maybeFlushToReclaim()
    {
        long unused = unusedCapacity();
        if (unused < 0)
        {
            long flushingSize = 0;
            List<CommitLogSegment> segmentsToRecycle = new ArrayList<>();
            for (CommitLogSegment segment : activeSegments)
            {
                if (segment == allocatingFrom)
                    break;
                flushingSize += segment.onDiskSize();
                segmentsToRecycle.add(segment);
                if (flushingSize + unused >= 0)
                    break;
            }
            flushDataFrom(segmentsToRecycle, false);
        }
    }


    /**
     * Allocate a segment within this CLSM. Should either succeed or throw.
     */
    public abstract Allocation allocate(Mutation mutation, int size);

    /**
     * The recovery and replay process replays mutations into memtables and flushes them to disk. Individual CLSM
     * decide what to do with those segments on disk after they've been replayed.
     */
    abstract void handleReplayedSegment(final File file);

    /**
     * Hook to allow segment managers to track state surrounding creation of new segments. Onl perform as task submit
     * to segment manager so it's performed on segment management thread.
     */
    abstract CommitLogSegment createSegment();

    /**
     * Indicates that a segment file has been flushed and is no longer needed. Only perform as task submit to segment
     * manager so it's performend on segment management thread, or perform while segment management thread is shutdown
     * during testing resets.
     *
     * @param segment segment to be discarded
     * @param delete  whether or not the segment is safe to be deleted.
     */
    abstract void discard(CommitLogSegment segment, boolean delete);

    /**
     * Advances the allocatingFrom pointer to the next prepared segment, but only if it is currently the segment provided.
     *
     * WARNING: Assumes segment management thread always succeeds in allocating a new segment or kills the JVM.
     */
    @DontInline
    void advanceAllocatingFrom(CommitLogSegment old)
    {
        while (true)
        {
            synchronized (this)
            {
                // do this in a critical section so we can maintain the order of segment construction when moving to allocatingFrom/activeSegments
                if (allocatingFrom != old)
                    return;

                // If a segment is ready, take it now, otherwise wait for the management thread to construct it.
                if (availableSegment != null)
                {
                    // Success! Change allocatingFrom and activeSegments (which must be kept in order) before leaving
                    // the critical section.
                    activeSegments.add(allocatingFrom = availableSegment);
                    availableSegment = null;
                    break;
                }
            }

            awaitAvailableSegment(old);
        }

        // Signal the management thread to prepare a new segment.
        wakeManager();

        if (old != null)
        {
            // Now we can run the user defined command just after switching to the new commit log.
            // (Do this here instead of in the recycle call so we can get a head start on the archive.)
            commitLog.archiver.maybeArchive(old);

            // ensure we don't continue to use the old file; not strictly necessary, but cleaner to enforce it
            old.discardUnusedTail();
        }

        // request that the CL be synced out-of-band, as we've finished a segment
        commitLog.requestExtraSync();
    }

    void awaitAvailableSegment(CommitLogSegment currentAllocatingFrom)
    {
        do
        {
            WaitQueue.Signal prepared = segmentPrepared.register(commitLog.metrics.waitingOnSegmentAllocation.time());
            if (availableSegment == null && allocatingFrom == currentAllocatingFrom)
                prepared.awaitUninterruptibly();
            else
                prepared.cancel();
        }
        while (availableSegment == null && allocatingFrom == currentAllocatingFrom);
    }

    /**
     * Switch to a new segment, regardless of how much is left in the current one.
     *
     * Flushes any dirty CFs for this segment and any older segments, and then discards the segments
     */
    void forceRecycleAll(Iterable<UUID> droppedCfs)
    {
        List<CommitLogSegment> segmentsToRecycle = new ArrayList<>(activeSegments);
        CommitLogSegment last = segmentsToRecycle.get(segmentsToRecycle.size() - 1);
        advanceAllocatingFrom(last);

        // wait for the commit log modifications
        last.waitForModifications();

        // make sure the writes have materialized inside of the memtables by waiting for all outstanding writes
        // to complete
        Keyspace.writeOrder.awaitNewBarrier();

        // flush and wait for all CFs that are dirty in segments up-to and including 'last'
        Future<?> future = flushDataFrom(segmentsToRecycle, true);
        try
        {
            future.get();

            for (CommitLogSegment segment : activeSegments)
                for (UUID cfId : droppedCfs)
                    segment.markClean(cfId, CommitLogPosition.NONE, segment.getCurrentCommitLogPosition());

            // now recycle segments that are unused, as we may not have triggered a discardCompletedSegments()
            // if the previous active segment was the only one to recycle (since an active segment isn't
            // necessarily dirty, and we only call dCS after a flush).
            for (CommitLogSegment segment : activeSegments)
            {
                if (segment.isUnused())
                    archiveAndDiscard(segment);
            }

            CommitLogSegment first;
            if ((first = activeSegments.peek()) != null && first.id <= last.id)
                logger.error("Failed to force-recycle all segments; at least one segment is still in use with dirty CFs.");
        }
        catch (Throwable t)
        {
            // for now just log the error
            logger.error("Failed waiting for a forced recycle of in-use commit log segments", t);
        }
    }

    /**
     * Indicates that a segment is no longer in use and that it should be discarded.
     *
     * @param segment segment that is no longer in use
     */
    void archiveAndDiscard(final CommitLogSegment segment)
    {
        boolean archiveSuccess = commitLog.archiver.maybeWaitForArchiving(segment.getName());
        if (!activeSegments.remove(segment))
            return; // already discarded
        // if archiving (command) was not successful then leave the file alone. don't delete or recycle.
        logger.debug("Segment {} is no longer active and will be deleted {}", segment, archiveSuccess ? "now" : "by the archive script");
        discard(segment, archiveSuccess);
    }

    /**
     * Adjust the tracked on-disk size. Called by individual segments to reflect writes, allocations and discards.
     * @param addedSize
     */
    void addSize(long addedSize)
    {
        size.addAndGet(addedSize);
    }

    /**
     * @return the space (in bytes) used by all segment files.
     */
    public long onDiskSize()
    {
        return size.get();
    }

    private long unusedCapacity()
    {
        long total = DatabaseDescriptor.getTotalCommitlogSpaceInMB() * 1024 * 1024;
        long currentSize = size.get();
        logger.trace("Total active commitlog segment space used is {} out of {}", currentSize, total);
        return total - currentSize;
    }

    /**
     * Force a flush on all CFs that are still dirty in @param segments.
     *
     * @return a Future that will finish when all the flushes are complete.
     */
    private Future<?> flushDataFrom(List<CommitLogSegment> segments, boolean force)
    {
        if (segments.isEmpty())
            return Futures.immediateFuture(null);
        final CommitLogPosition maxCommitLogPosition = segments.get(segments.size() - 1).getCurrentCommitLogPosition();

        // a map of CfId -> forceFlush() to ensure we only queue one flush per cf
        final Map<UUID, ListenableFuture<?>> flushes = new LinkedHashMap<>();

        for (CommitLogSegment segment : segments)
        {
            for (UUID dirtyCFId : segment.getDirtyCFIDs())
            {
                Pair<String,String> pair = Schema.instance.getCF(dirtyCFId);
                if (pair == null)
                {
                    // even though we remove the schema entry before a final flush when dropping a CF,
                    // it's still possible for a writer to race and finish his append after the flush.
                    logger.trace("Marking clean CF {} that doesn't exist anymore", dirtyCFId);
                    segment.markClean(dirtyCFId, CommitLogPosition.NONE, segment.getCurrentCommitLogPosition());
                }
                else if (!flushes.containsKey(dirtyCFId))
                {
                    String keyspace = pair.left;
                    final ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(dirtyCFId);
                    // can safely call forceFlush here as we will only ever block (briefly) for other attempts to flush,
                    // no deadlock possibility since switchLock removal
                    flushes.put(dirtyCFId, force ? cfs.forceFlush() : cfs.forceFlush(maxCommitLogPosition));
                }
            }
        }

        return Futures.allAsList(flushes.values());
    }

    /**
     * Stops CL, for testing purposes. DO NOT USE THIS OUTSIDE OF TESTS.
     * Only call this after the AbstractCommitLogService is shut down.
     */
    public void stopUnsafe(boolean deleteSegments)
    {
        logger.debug("CLSM closing and clearing existing commit log segments...");

        shutdown();
        try
        {
            awaitTermination();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        for (CommitLogSegment segment : activeSegments)
            closeAndDeleteSegmentUnsafe(segment, deleteSegments);
        activeSegments.clear();

        size.set(0L);

        logger.trace("CLSM done with closing and clearing existing commit log segments.");
    }

    /**
     * To be used by tests only. Not safe if mutation slots are being allocated concurrently.
     */
    void awaitManagementTasksCompletion()
    {
        if (availableSegment == null && !atSegmentBufferLimit())
        {
            awaitAvailableSegment(allocatingFrom);
        }
    }

    /**
     * Explicitly for use only during resets in unit testing.
     */
    private void closeAndDeleteSegmentUnsafe(CommitLogSegment segment, boolean delete)
    {
        try
        {
            discard(segment, delete);
        }
        catch (AssertionError ignored)
        {
            // segment file does not exist
        }
    }

    /**
     * Initiates the shutdown process for the management thread.
     */
    public void shutdown()
    {
        assert !shutdown;
        shutdown = true;

        // Release the management thread and delete prepared segment.
        // Do not block as another thread may claim the segment (this can happen during unit test initialization).
        discardAvailableSegment();
        wakeManager();
    }

    private void discardAvailableSegment()
    {
        CommitLogSegment next = null;
        synchronized (this)
        {
            next = availableSegment;
            availableSegment = null;
        }
        if (next != null)
            next.discard(true);
    }

    /**
     * Returns when the management thread terminates.
     */
    public void awaitTermination() throws InterruptedException
    {
        managerThread.join();
        managerThread = null;

        for (CommitLogSegment segment : activeSegments)
            segment.close();

        bufferPool.shutdown();
    }

    /**
     * @return a read-only collection of the active commit log segments
     */
    @VisibleForTesting
    public Collection<CommitLogSegment> getActiveSegments()
    {
        return Collections.unmodifiableCollection(activeSegments);
    }

    /**
     * @return the current CommitLogPosition of the active segment we're allocating from
     */
    CommitLogPosition getCurrentPosition()
    {
        return allocatingFrom.getCurrentCommitLogPosition();
    }

    /**
     * Requests commit log files sync themselves, if needed. This may or may not involve flushing to disk.
     *
     * @param flush Request that the sync operation flush the file to disk.
     */
    public void sync(boolean flush) throws IOException
    {
        CommitLogSegment current = allocatingFrom;
        for (CommitLogSegment segment : getActiveSegments())
        {
            // Do not sync segments that became active after sync started.
            if (segment.id > current.id)
                return;
            segment.sync(flush);
        }
    }

    /**
     * Used by compressed and encrypted segments to share a buffer pool across the CLSM.
     */
    SimpleCachedBufferPool getBufferPool()
    {
        return bufferPool;
    }

    void wakeManager()
    {
        managerThreadWaitQueue.signalAll();
    }

    /**
     * Called by commit log segments when a buffer is freed to wake the management thread, which may be waiting for
     * a buffer to become available.
     */
    void notifyBufferFreed()
    {
        wakeManager();
    }

    /** Read-only access to current segment for subclasses. */
    CommitLogSegment allocatingFrom()
    {
        return allocatingFrom;
    }
}

