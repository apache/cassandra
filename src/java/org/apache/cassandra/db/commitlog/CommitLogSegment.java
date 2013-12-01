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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.PureJavaCrc32;
import org.apache.cassandra.utils.WaitQueue;

/*
 * A single commit log file on disk. Manages creation of the file and writing row mutations to disk,
 * as well as tracking the last mutation position of any "dirty" CFs covered by the segment file. Segment
 * files are initially allocated to a fixed size and can grow to accomidate a larger value if necessary.
 */
public class CommitLogSegment
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogSegment.class);

    private final static long idBase = System.currentTimeMillis();
    private final static AtomicInteger nextId = new AtomicInteger(1);

    // The commit log entry overhead in bytes (int: length + long: head checksum + long: tail checksum)
    static final int ENTRY_OVERHEAD_SIZE = 4 + 8 + 8;

    // The commit log (chained) sync marker/header size in bytes (int: length + long: checksum [segmentId, position])
    static final int SYNC_MARKER_SIZE = 4 + 8;

    // The current AppendLock object - i.e. the one all threads adding new log records should use to synchronise
    private final AtomicReference<AppendLock> appendLock = new AtomicReference<>(new AppendLock());

    private final AtomicInteger allocatePosition = new AtomicInteger();

    // also the last synced position
    private volatile int nextSyncMarkerPosition;
    // the amount of the tail of the file we have allocated but not used - this is used when we discard a log segment
    // to ensure nobody writes to it after we've decided we're done with it
    private int discardedTailFrom;

    // a signal for writers to wait on to confirm the log message they provided has been written to disk
    private final WaitQueue syncComplete = new WaitQueue();

    // a map of Cf->dirty position; this is used to permit marking Cfs clean whilst the log is still in use
    private final NonBlockingHashMap<UUID, AtomicInteger> cfDirty = new NonBlockingHashMap<>(1024);

    // a map of Cf->clean position; this is used to permit marking Cfs clean whilst the log is still in use
    private final ConcurrentHashMap<UUID, AtomicInteger> cfClean = new ConcurrentHashMap<>();

    public final long id;

    private final File logFile;
    private final RandomAccessFile logFileAccessor;

    private final MappedByteBuffer buffer;

    public final CommitLogDescriptor descriptor;

    /**
     * @return a newly minted segment file
     */
    static CommitLogSegment freshSegment()
    {
        return new CommitLogSegment(null);
    }

    static long getNextId()
    {
        return idBase + nextId.getAndIncrement();
    }

    /**
     * Constructs a new segment file.
     *
     * @param filePath  if not null, recycles the existing file by renaming it and truncating it to CommitLog.SEGMENT_SIZE.
     */
    CommitLogSegment(String filePath)
    {
        id = getNextId();
        descriptor = new CommitLogDescriptor(id);
        logFile = new File(DatabaseDescriptor.getCommitLogLocation(), descriptor.fileName());
        boolean isCreating = true;

        try
        {
            if (filePath != null)
            {
                File oldFile = new File(filePath);

                if (oldFile.exists())
                {
                    logger.debug("Re-using discarded CommitLog segment for {} from {}", id, filePath);
                    if (!oldFile.renameTo(logFile))
                        throw new IOException("Rename from " + filePath + " to " + id + " failed");
                    isCreating = false;
                }
            }

            // Open the initial the segment file
            logFileAccessor = new RandomAccessFile(logFile, "rw");

            if (isCreating)
                logger.debug("Creating new commit log segment {}", logFile.getPath());

            // Map the segment, extending or truncating it to the standard segment size
            logFileAccessor.setLength(DatabaseDescriptor.getCommitLogSegmentSize());

            buffer = logFileAccessor.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, DatabaseDescriptor.getCommitLogSegmentSize());
            // mark the initial header as uninitialised
            buffer.putInt(0, 0);
            buffer.putLong(4, 0);
            allocatePosition.set(SYNC_MARKER_SIZE);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, logFile);
        }
    }

    /**
     * allocate space in this buffer for the provided row mutation, and populate the provided
     * Allocation object, returning true on success. False indicates there is not enough room in
     * this segment, and a new segment is needed
     */
    boolean allocate(RowMutation rowMutation, int size, Allocation alloc)
    {
        final AppendLock appendLock = lockForAppend();
        try
        {
            int position = allocate(size);
            if (position < 0)
            {
                appendLock.unlock();
                return false;
            }
            alloc.buffer = (ByteBuffer) buffer.duplicate().position(position).limit(position + size);
            alloc.position = position;
            alloc.segment = this;
            alloc.appendLock = appendLock;
            markDirty(rowMutation, position);
            return true;
        }
        catch (Throwable t)
        {
            appendLock.unlock();
            throw t;
        }
    }

    // obtain the current AppendLock and lock it for record appending
    private AppendLock lockForAppend()
    {
        while (true)
        {
            AppendLock appendLock = this.appendLock.get();
            if (appendLock.lock())
                return appendLock;
        }
    }

    // allocate bytes in the segment, or return -1 if not enough space
    private int allocate(int size)
    {
        while (true)
        {
            int prev = allocatePosition.get();
            int next = prev + size;
            if (next >= buffer.capacity())
                return -1;
            if (allocatePosition.compareAndSet(prev, next))
                return prev;
        }
    }

    // ensures no more of this segment is writeable, by allocating any unused section at the end and marking it discarded
    synchronized void discardUnusedTail()
    {
        if (discardedTailFrom > 0)
            return;
        while (true)
        {
            int prev = allocatePosition.get();
            int next = buffer.capacity();
            if (allocatePosition.compareAndSet(prev, next))
            {
                discardedTailFrom = prev;
                return;
            }
        }
    }

    /**
     * Forces a disk flush for this segment file.
     */
    synchronized void sync()
    {
        try
        {
            // check we have more work to do
            if (allocatePosition.get() <= nextSyncMarkerPosition + SYNC_MARKER_SIZE)
                return;

            // allocate a new sync marker; this is both necessary in itself, but also serves to demarcate
            // the point at which we can safely consider records to have been completely written to
            int nextMarker;
            nextMarker = allocate(SYNC_MARKER_SIZE);
            boolean close = false;
            if (nextMarker < 0)
            {
                // ensure no more of this CLS is writeable, and mark ourselves for closing
                discardUnusedTail();
                close = true;

                if (discardedTailFrom < buffer.capacity() - SYNC_MARKER_SIZE)
                    // if there's room in the discard section to write an empty header, use that as the nextMarker
                    nextMarker = discardedTailFrom;
                else
                    // not enough space left in the buffer, so mark the next sync marker as the EOF position
                    nextMarker = buffer.capacity();
            }

            // swap the append lock
            AppendLock curAppendLock = appendLock.get();
            appendLock.set(new AppendLock());
            curAppendLock.expireAndWaitForCompletion();

            // write previous sync marker to point to next sync marker
            // we don't chain the crcs here to ensure this method is idempotent if it fails
            int offset = nextSyncMarkerPosition;
            final PureJavaCrc32 crc = new PureJavaCrc32();
            crc.update((int) (id & 0xFFFFFFFFL));
            crc.update((int) (id >>> 32));
            crc.update(offset);
            buffer.putInt(offset, nextMarker);
            buffer.putLong(offset + 4, crc.getValue());

            // zero out the next sync marker so replayer can cleanly exit
            if (nextMarker < buffer.capacity())
            {
                buffer.putInt(nextMarker, 0);
                buffer.putLong(nextMarker + 4, 0);
            }

            // actually perform the sync and signal those waiting for it
            buffer.force();
            syncComplete.signalAll();

            if (close)
            {
                close();
                nextMarker = buffer.capacity();
            }

            nextSyncMarkerPosition = nextMarker;
        }
        catch (Exception e) // MappedByteBuffer.force() does not declare IOException but can actually throw it
        {
            throw new FSWriteError(e, getPath());
        }
    }

    public boolean isFullySynced()
    {
        return nextSyncMarkerPosition == buffer.capacity();
    }

    /**
     * Completely discards a segment file by deleting it. (Potentially blocking operation)
     */
    void delete()
    {
       FileUtils.deleteWithConfirm(logFile);
    }

    /**
     * Recycle processes an unneeded segment file for reuse.
     *
     * @return a new CommitLogSegment representing the newly reusable segment.
     */
    CommitLogSegment recycle()
    {
        try
        {
            sync();
        }
        catch (FSWriteError e)
        {
            logger.error("I/O error flushing {} {}", this, e.getMessage());
            throw e;
        }

        close();

        return new CommitLogSegment(getPath());
    }

    /**
     * @return the current ReplayPosition for this log segment
     */
    public ReplayPosition getContext()
    {
        return new ReplayPosition(id, allocatePosition.get());
    }

    /**
     * @return the file path to this segment
     */
    public String getPath()
    {
        return logFile.getPath();
    }

    /**
     * @return the file name of this segment
     */
    public String getName()
    {
        return logFile.getName();
    }

    /**
     * Close the segment file.
     */
    void close()
    {
        try
        {
            FileUtils.clean(buffer);
            logFileAccessor.close();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    void markDirty(RowMutation rowMutation, int allocatedPosition)
    {
        for (ColumnFamily columnFamily : rowMutation.getColumnFamilies())
        {
            // check for deleted CFS
            CFMetaData cfm = columnFamily.metadata();
            if (cfm.isPurged())
                logger.error("Attempted to write commit log entry for unrecognized column family: {}", columnFamily.id());
            else
                ensureAtleast(cfDirty, cfm.cfId, allocatedPosition);
        }
    }

    /**
     * Marks the ColumnFamily specified by cfId as clean for this log segment. If the
     * given context argument is contained in this file, it will only mark the CF as
     * clean if no newer writes have taken place.
     *
     * @param cfId    the column family ID that is now clean
     * @param context the optional clean offset
     */
    public void markClean(UUID cfId, ReplayPosition context)
    {
        if (!cfDirty.containsKey(cfId))
            return;
        if (context.segment == id)
            markClean(cfId, context.position);
        else if (context.segment > id)
            markClean(cfId, Integer.MAX_VALUE);
    }

    private void markClean(UUID cfId, int position)
    {
        ensureAtleast(cfClean, cfId, position);
        removeCleanFromDirty();
    }

    private static void ensureAtleast(ConcurrentMap<UUID, AtomicInteger> map, UUID cfId, int value)
    {
        AtomicInteger i = map.get(cfId);
        if (i == null)
        {
            AtomicInteger i2 = map.putIfAbsent(cfId, i = new AtomicInteger());
            if (i2 != null)
                i = i2;
        }
        while (true)
        {
            int cur = i.get();
            if (cur > value)
                break;
            if (i.compareAndSet(cur, value))
                break;
        }
    }

    private void removeCleanFromDirty()
    {
        if (!isFullySynced())
            return;
        Iterator<Map.Entry<UUID, AtomicInteger>> iter = cfClean.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<UUID, AtomicInteger> clean = iter.next();
            UUID cfId = clean.getKey();
            AtomicInteger cleanPos = clean.getValue();
            AtomicInteger dirtyPos = cfDirty.get(cfId);
            if (dirtyPos != null && dirtyPos.intValue() < cleanPos.intValue())
            {
                cfDirty.remove(cfId);
                iter.remove();
            }
        }
    }


    /**
     * @return a collection of dirty CFIDs for this segment file.
     */
    public Collection<UUID> getDirtyCFIDs()
    {
        removeCleanFromDirty();
        if (cfClean.isEmpty() || cfDirty.isEmpty())
            return cfDirty.keySet();
        List<UUID> r = new ArrayList<>(cfDirty.size());
        for (Map.Entry<UUID, AtomicInteger> dirty : cfDirty.entrySet())
        {
            UUID cfId = dirty.getKey();
            AtomicInteger dirtyPos = dirty.getValue();
            AtomicInteger cleanPos = cfClean.get(cfId);
            if (cleanPos == null || cleanPos.intValue() < dirtyPos.intValue())
                r.add(dirty.getKey());
        }
        return r;
    }

    /**
     * @return true if this segment is unused and safe to recycle or delete
     */
    public boolean isUnused()
    {
        if (!isFullySynced())
            return false;
        removeCleanFromDirty();
        return cfDirty.isEmpty();
    }

    /**
     * Check to see if a certain ReplayPosition is contained by this segment file.
     *
     * @param   context the replay position to be checked
     * @return  true if the replay position is contained by this segment file.
     */
    public boolean contains(ReplayPosition context)
    {
        return context.segment == id;
    }

    // For debugging, not fast
    public String dirtyString()
    {
        StringBuilder sb = new StringBuilder();
        for (UUID cfId : getDirtyCFIDs())
        {
            CFMetaData m = Schema.instance.getCFMetaData(cfId);
            sb.append(m == null ? "<deleted>" : m.cfName).append(" (").append(cfId).append("), ");
        }
        return sb.toString();
    }

    @Override
    public String toString()
    {
        return "CommitLogSegment(" + getPath() + ')';
    }

    public boolean equals(Object that)
    {
        return super.equals(that);
    }


    public static class CommitLogSegmentFileComparator implements Comparator<File>
    {
        public int compare(File f, File f2)
        {
            CommitLogDescriptor desc = CommitLogDescriptor.fromFileName(f.getName());
            CommitLogDescriptor desc2 = CommitLogDescriptor.fromFileName(f2.getName());
            return (int) (desc.id - desc2.id);
        }
    }

    /**
     * A relatively simple class for synchronising flushes() with log message writers:
     * Log writers take the readLock prior to allocating themselves space in the segment;
     * once they complete writing the record they release the read lock. A call to sync()
     * will first check the position we have allocated space up until, then allocate a new AppendLock object,
     * take the writeLock of the previous AppendLock, and invalidate it for further log writes. All appends are
     * redirected to the new Sync so they do not block, only the sync() blocks waiting to obtain the writeLock.
     * Once it obtains the lock it is guaranteed that all writes up to the allocation position it checked at
     * the start have been completely written to.
     */
    private static final class AppendLock
    {

        final ReadWriteLock syncLock = new ReentrantReadWriteLock();
        final Lock logLock = syncLock.readLock();
        // a map of Cfs with log records that have not been synced to disk, so cannot be marked clean yet

        boolean expired;

        // false if the lock could not be acquired for adding a log record;
        // a new AppendLock object will already be available, so fetch appendLock().get()
        // and retry
        boolean lock()
        {
            if (!logLock.tryLock())
                return false;
            if (expired)
            {
                logLock.unlock();
                return false;
            }
            return true;
        }

        // release the lock so that a appendLock() may complete
        void unlock()
        {
            logLock.unlock();
        }

        void expireAndWaitForCompletion()
        {
            // wait for log records to complete (take writeLock)
            syncLock.writeLock().lock();
            expired = true;
            // release lock immediately, though effectively a NOOP since we use tryLock() for log record appends
            syncLock.writeLock().unlock();
        }

    }

    /**
     * A simple class for tracking information about the portion of a segment that has been allocated to a log write.
     * The constructor leaves the fields uninitialized for population by CommitlogManager, so that it can be
     * stack-allocated by escape analysis in CommitLog.add.
     */
    static final class Allocation
    {
        private CommitLogSegment segment;
        private AppendLock appendLock;
        private int position;
        private ByteBuffer buffer;

        CommitLogSegment getSegment()
        {
            return segment;
        }

        ByteBuffer getBuffer()
        {
            return buffer;
        }

        // markWritten() MUST be called once we are done with the segment or the CL will never flush
        void markWritten()
        {
            appendLock.unlock();
        }

        void awaitDiskSync()
        {
            while (segment.nextSyncMarkerPosition < position)
            {
                WaitQueue.Signal signal = segment.syncComplete.register();
                if (segment.nextSyncMarkerPosition < position)
                    signal.awaitUninterruptibly();
            }
        }
    }
}
