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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.codahale.metrics.Timer;
import net.openhft.chronicle.core.util.ThrowingFunction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.FileWriter;
import org.apache.cassandra.io.util.SimpleCachedBufferPool;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.IntegerInterval;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;
import static org.apache.cassandra.utils.concurrent.WaitQueue.newWaitQueue;

/*
 * A single commit log file on disk. Manages creation of the file and writing mutations to disk,
 * as well as tracking the last mutation position of any "dirty" CFs covered by the segment file. Segment
 * files are initially allocated to a fixed size and can grow to accomidate a larger value if necessary.
 */
public abstract class CommitLogSegment
{
    private final static long idBase;

    private CDCState cdcState = CDCState.PERMITTED;
    public enum CDCState
    {
        PERMITTED,
        FORBIDDEN,
        CONTAINS
    }
    final Object cdcStateLock = new Object();

    private final static AtomicInteger nextId = new AtomicInteger(1);
    private static long replayLimitId;
    static
    {
        long maxId = Long.MIN_VALUE;
        for (File file : new File(DatabaseDescriptor.getCommitLogLocation()).tryList())
        {
            if (CommitLogDescriptor.isValid(file.name()))
                maxId = Math.max(CommitLogDescriptor.fromFileName(file.name()).id, maxId);
        }
        replayLimitId = idBase = Math.max(currentTimeMillis(), maxId + 1);
    }

    // The commit log entry overhead in bytes (int: length + int: head checksum + int: tail checksum)
    public static final int ENTRY_OVERHEAD_SIZE = 4 + 4 + 4;

    // The commit log (chained) sync marker/header size in bytes (int: length + int: checksum [segmentId, position])
    static final int SYNC_MARKER_SIZE = 4 + 4;

    // The OpOrder used to order appends wrt sync
    private final OpOrder appendOrder = new OpOrder();

    private final AtomicInteger allocatePosition = new AtomicInteger();

    // Everything before this offset has been synced and written.  The SYNC_MARKER_SIZE bytes after
    // each sync are reserved, and point forwards to the next such offset.  The final
    // sync marker in a segment will be zeroed out, or point to a position too close to the EOF to fit a marker.
    @VisibleForTesting
    volatile int lastSyncedOffset;

    /**
     * Everything before this offset has it's markers written into the {@link #buffer}, but has not necessarily
     * been flushed to disk. This value should be greater than or equal to {@link #lastSyncedOffset}.
     */
    private volatile int lastMarkerOffset;

    // The end position of the buffer. Initially set to its capacity and updated to point to the last written position
    // as the segment is being closed.
    // No need to be volatile as writes are protected by appendOrder barrier.
    private int endOfBuffer;

    // a signal for writers to wait on to confirm the log message they provided has been written to disk
    private final WaitQueue syncComplete = newWaitQueue();

    // a map of Cf->dirty interval in this segment; if interval is not covered by the clean set, the log contains unflushed data
    private final NonBlockingHashMap<TableId, IntegerInterval> tableDirty = new NonBlockingHashMap<>(1024);

    // a map of Cf->clean intervals; separate map from above to permit marking Cfs clean whilst the log is still in use
    private final ConcurrentHashMap<TableId, IntegerInterval.Set> tableClean = new ConcurrentHashMap<>();

    public final long id;

    final File logFile;
    final FileChannel channel;

    protected final AbstractCommitLogSegmentManager manager;

    ByteBuffer buffer;
    private volatile boolean headerWritten;

    public final CommitLogDescriptor descriptor;

    static long getNextId()
    {
        return idBase + nextId.getAndIncrement();
    }

    /**
     * Constructs a new segment file.
     */
    CommitLogSegment(AbstractCommitLogSegmentManager manager, ThrowingFunction<Path, FileChannel, IOException> channelFactory)
    {
        this.manager = manager;

        id = getNextId();
        descriptor = new CommitLogDescriptor(id,
                                             manager.getConfiguration().getCompressorClass(),
                                             manager.getConfiguration().getEncryptionContext());
        logFile = new File(manager.storageDirectory, descriptor.fileName());

        try
        {
            channel = channelFactory.apply(logFile.toPath());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, logFile);
        }

        this.buffer = createBuffer();
    }

    /**
     * Deferred writing of the commit log header until subclasses have had a chance to initialize
     */
    void writeLogHeader()
    {
        CommitLogDescriptor.writeHeader(buffer, descriptor, additionalHeaderParameters());
        endOfBuffer = buffer.capacity();

        lastSyncedOffset = lastMarkerOffset = buffer.position();
        allocatePosition.set(lastSyncedOffset + SYNC_MARKER_SIZE);
        headerWritten = true;
    }

    /**
     * Provide any additional header data that should be stored in the {@link CommitLogDescriptor}.
     */
    protected Map<String, String> additionalHeaderParameters()
    {
        return Collections.<String, String>emptyMap();
    }

    protected ByteBuffer createBuffer()
    {
        return manager.getBufferPool().createBuffer();
    }

    /**
     * Allocate space in this buffer for the provided mutation, and return the allocated Allocation object.
     * Returns null if there is not enough space in this segment, and a new segment is needed.
     */
    Allocation allocate(Mutation mutation, int size)
    {
        final OpOrder.Group opGroup = appendOrder.start();
        try
        {
            int position = allocate(size);
            if (position < 0)
            {
                opGroup.close();
                return null;
            }

            for (PartitionUpdate update : mutation.getPartitionUpdates())
                coverInMap(tableDirty, update.metadata().id, position);

            return new Allocation(this, opGroup, position, (ByteBuffer) buffer.duplicate().position(position).limit(position + size));
        }
        catch (Throwable t)
        {
            opGroup.close();
            throw t;
        }
    }

    static boolean shouldReplay(String name)
    {
        return CommitLogDescriptor.fromFileName(name).id < replayLimitId;
    }

    /**
     * FOR TESTING PURPOSES.
     */
    static void resetReplayLimit()
    {
        replayLimitId = getNextId();
    }

    // allocate bytes in the segment, or return -1 if not enough space
    private int allocate(int size)
    {
        while (true)
        {
            int prev = allocatePosition.get();
            int next = prev + size;
            if (next >= endOfBuffer)
                return -1;
            if (allocatePosition.compareAndSet(prev, next))
            {
                assert buffer != null;
                return prev;
            }
            LockSupport.parkNanos(1); // ConstantBackoffCAS Algorithm from https://arxiv.org/pdf/1305.5800.pdf
        }
    }

    // ensures no more of this segment is writeable, by allocating any unused section at the end and marking it discarded
    void discardUnusedTail()
    {
        // We guard this with the OpOrdering instead of synchronised due to potential dead-lock with ACLSM.advanceAllocatingFrom()
        // Ensures endOfBuffer update is reflected in the buffer end position picked up by sync().
        // This actually isn't strictly necessary, as currently all calls to discardUnusedTail are executed either by the thread
        // running sync or within a mutation already protected by this OpOrdering, but to prevent future potential mistakes,
        // we duplicate the protection here so that the contract between discardUnusedTail() and sync() is more explicit.
        try (OpOrder.Group group = appendOrder.start())
        {
            while (true)
            {
                int prev = allocatePosition.get();

                int next = endOfBuffer + 1;
                if (prev >= next)
                {
                    // Already stopped allocating, might also be closed.
                    assert buffer == null || prev == buffer.capacity() + 1;
                    return;
                }
                if (allocatePosition.compareAndSet(prev, next))
                {
                    // Stopped allocating now. Can only succeed once, no further allocation or discardUnusedTail can succeed.
                    endOfBuffer = prev;
                    assert buffer != null && next == buffer.capacity() + 1;
                    return;
                }
            }
        }
    }

    /**
     * Wait for any appends or discardUnusedTail() operations started before this method was called
     */
    void waitForModifications()
    {
        // issue a barrier and wait for it
        appendOrder.awaitNewBarrier();
    }

    /**
     * Update the chained markers in the commit log buffer and possibly force a disk flush for this segment file.
     *
     * @param flush true if the segment should flush to disk; else, false for just updating the chained markers.
     */
    synchronized void sync(boolean flush)
    {
        if (!headerWritten)
            throw new IllegalStateException("commit log header has not been written");
        assert lastMarkerOffset >= lastSyncedOffset : String.format("commit log segment positions are incorrect: last marked = %d, last synced = %d",
                                                                    lastMarkerOffset, lastSyncedOffset);
        // check we have more work to do
        final boolean needToMarkData = allocatePosition.get() > lastMarkerOffset + SYNC_MARKER_SIZE;
        final boolean hasDataToFlush = lastSyncedOffset != lastMarkerOffset;
        if (!(needToMarkData || hasDataToFlush))
            return;
        // Note: Even if the very first allocation of this sync section failed, we still want to enter this
        // to ensure the segment is closed. As allocatePosition is set to 1 beyond the capacity of the buffer,
        // this will always be entered when a mutation allocation has been attempted after the marker allocation
        // succeeded in the previous sync.
        assert buffer != null;  // Only close once.

        boolean close = false;
        int startMarker = lastMarkerOffset;
        int nextMarker, sectionEnd;
        if (needToMarkData)
        {
            // Allocate a new sync marker; this is both necessary in itself, but also serves to demarcate
            // the point at which we can safely consider records to have been completely written to.
            nextMarker = allocate(SYNC_MARKER_SIZE);
            if (nextMarker < 0)
            {
                // Ensure no more of this CLS is writeable, and mark ourselves for closing.
                discardUnusedTail();
                close = true;

                // We use the buffer size as the synced position after a close instead of the end of the actual data
                // to make sure we only close the buffer once.
                // The endOfBuffer position may be incorrect at this point (to be written by another stalled thread).
                nextMarker = buffer.capacity();
            }
            // Wait for mutations to complete as well as endOfBuffer to have been written.
            waitForModifications();
            sectionEnd = close ? endOfBuffer : nextMarker;

            // Possibly perform compression or encryption and update the chained markers
            write(startMarker, sectionEnd);
            lastMarkerOffset = sectionEnd;
        }
        else
        {
            // note: we don't need to waitForModifications() as, once we get to this block, we are only doing the flush
            // and any mutations have already been fully written into the segment (as we wait for it in the previous block).
            nextMarker = lastMarkerOffset;
            sectionEnd = nextMarker;
        }


        if (flush || close)
        {
            try (Timer.Context ignored = CommitLog.instance.metrics.waitingOnFlush.time())
            {
                flush(startMarker, sectionEnd);
            }
            
            if (cdcState == CDCState.CONTAINS)
                writeCDCIndexFile(descriptor, sectionEnd, close);
            lastSyncedOffset = lastMarkerOffset = nextMarker;

            if (close)
                internalClose();

            syncComplete.signalAll();
        }
    }

    /**
     * We persist the offset of the last data synced to disk so clients can parse only durable data if they choose. Data
     * in shared / memory-mapped buffers reflects un-synced data so we need an external sentinel for clients to read to
     * determine actual durable data persisted.
     */
    public static void writeCDCIndexFile(CommitLogDescriptor desc, int offset, boolean complete)
    {
        try(FileWriter writer = new FileWriter(new File(DatabaseDescriptor.getCDCLogLocation(), desc.cdcIndexFileName())))
        {
            writer.write(String.valueOf(offset));
            if (complete)
                writer.write("\nCOMPLETED");
            writer.flush();
        }
        catch (IOException e)
        {
            if (!CommitLog.instance.handleCommitError("Failed to sync CDC Index: " + desc.cdcIndexFileName(), e))
                throw new RuntimeException(e);
        }
    }

    /**
     * Create a sync marker to delineate sections of the commit log, typically created on each sync of the file.
     * The sync marker consists of a file pointer to where the next sync marker should be (effectively declaring the length
     * of this section), as well as a CRC value.
     *
     * @param buffer buffer in which to write out the sync marker.
     * @param offset Offset into the {@code buffer} at which to write the sync marker.
     * @param filePos The current position in the target file where the sync marker will be written (most likely different from the buffer position).
     * @param nextMarker The file position of where the next sync marker should be.
     */
    protected static void writeSyncMarker(long id, ByteBuffer buffer, int offset, int filePos, int nextMarker)
    {
        if (filePos > nextMarker)
            throw new IllegalArgumentException(String.format("commit log sync marker's current file position %d is greater than next file position %d", filePos, nextMarker));
        CRC32 crc = new CRC32();
        updateChecksumInt(crc, (int) (id & 0xFFFFFFFFL));
        updateChecksumInt(crc, (int) (id >>> 32));
        updateChecksumInt(crc, filePos);
        buffer.putInt(offset, nextMarker);
        buffer.putInt(offset + 4, (int) crc.getValue());
    }

    abstract void write(int lastSyncedOffset, int nextMarker);

    abstract void flush(int startMarker, int nextMarker);

    public boolean isStillAllocating()
    {
        return allocatePosition.get() < endOfBuffer;
    }

    /**
     * Discards a segment file when the log no longer requires it. The file may be left on disk if the archive script
     * requires it. (Potentially blocking operation)
     */
    void discard(boolean deleteFile)
    {
        close();
        if (deleteFile)
            FileUtils.deleteWithConfirm(logFile);
        manager.addSize(-onDiskSize());
    }

    /**
     * @return the current CommitLogPosition for this log segment
     */
    public CommitLogPosition getCurrentCommitLogPosition()
    {
        return new CommitLogPosition(id, allocatePosition.get());
    }

    /**
     * @return the file path to this segment
     */
    public String getPath()
    {
        return logFile.path();
    }

    /**
     * @return the file name of this segment
     */
    public String getName()
    {
        return logFile.name();
    }

    /**
     * @return a File object representing the CDC directory and this file name for hard-linking
     */
    public File getCDCFile()
    {
        return new File(DatabaseDescriptor.getCDCLogLocation(), logFile.name());
    }

    /**
     * @return a File object representing the CDC Index file holding the offset and completion status of this segment
     */
    public File getCDCIndexFile()
    {
        return new File(DatabaseDescriptor.getCDCLogLocation(), descriptor.cdcIndexFileName());
    }

    void waitForFinalSync()
    {
        while (true)
        {
            WaitQueue.Signal signal = syncComplete.register();
            if (lastSyncedOffset < endOfBuffer)
            {
                signal.awaitUninterruptibly();
            }
            else
            {
                signal.cancel();
                break;
            }
        }
    }

    void waitForSync(int position)
    {
        while (lastSyncedOffset < position)
        {
            WaitQueue.Signal signal = syncComplete.register();
            if (lastSyncedOffset < position)
                signal.awaitThrowUncheckedOnInterrupt();
            else
                signal.cancel();
        }
    }

    /**
     * Stop writing to this file, sync and close it. Does nothing if the file is already closed.
     */
    synchronized void close()
    {
        discardUnusedTail();
        sync(true);
        assert buffer == null;
    }

    /**
     * Close the segment file. Do not call from outside this class, use syncAndClose() instead.
     */
    protected void internalClose()
    {
        try
        {
            channel.close();
            buffer = null;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    public static<K> void coverInMap(ConcurrentMap<K, IntegerInterval> map, K key, int value)
    {
        IntegerInterval i = map.get(key);
        if (i == null)
        {
            i = map.putIfAbsent(key, new IntegerInterval(value, value));
            if (i == null)
                // success
                return;
        }
        i.expandToCover(value);
    }

    /**
     * Marks the ColumnFamily specified by id as clean for this log segment. If the
     * given context argument is contained in this file, it will only mark the CF as
     * clean if no newer writes have taken place.
     *
     * @param tableId        the table that is now clean
     * @param startPosition  the start of the range that is clean
     * @param endPosition    the end of the range that is clean
     */
    public synchronized void markClean(TableId tableId, CommitLogPosition startPosition, CommitLogPosition endPosition)
    {
        if (startPosition.segmentId > id || endPosition.segmentId < id)
            return;
        if (!tableDirty.containsKey(tableId))
            return;
        int start = startPosition.segmentId == id ? startPosition.position : 0;
        int end = endPosition.segmentId == id ? endPosition.position : Integer.MAX_VALUE;
        tableClean.computeIfAbsent(tableId, k -> new IntegerInterval.Set()).add(start, end);
        removeCleanFromDirty();
    }

    private void removeCleanFromDirty()
    {
        // if we're still allocating from this segment, don't touch anything since it can't be done thread-safely
        if (isStillAllocating())
            return;

        Iterator<Map.Entry<TableId, IntegerInterval.Set>> iter = tableClean.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<TableId, IntegerInterval.Set> clean = iter.next();
            TableId tableId = clean.getKey();
            IntegerInterval.Set cleanSet = clean.getValue();
            IntegerInterval dirtyInterval = tableDirty.get(tableId);
            if (dirtyInterval != null && cleanSet.covers(dirtyInterval))
            {
                tableDirty.remove(tableId);
                iter.remove();
            }
        }
    }

    /**
     * @return a collection of dirty CFIDs for this segment file.
     */
    public synchronized Collection<TableId> getDirtyTableIds()
    {
        if (tableClean.isEmpty() || tableDirty.isEmpty())
            return tableDirty.keySet();

        List<TableId> r = new ArrayList<>(tableDirty.size());
        for (Map.Entry<TableId, IntegerInterval> dirty : tableDirty.entrySet())
        {
            TableId tableId = dirty.getKey();
            IntegerInterval dirtyInterval = dirty.getValue();
            IntegerInterval.Set cleanSet = tableClean.get(tableId);
            if (cleanSet == null || !cleanSet.covers(dirtyInterval))
                r.add(dirty.getKey());
        }
        return r;
    }

    /**
     * @return true if this segment is unused and safe to recycle or delete
     */
    public synchronized boolean isUnused()
    {
        // if room to allocate, we're still in use as the active allocatingFrom,
        // so we don't want to race with updates to tableClean with removeCleanFromDirty
        if (isStillAllocating())
            return false;

        removeCleanFromDirty();
        return tableDirty.isEmpty();
    }

    /**
     * Check to see if a certain CommitLogPosition is contained by this segment file.
     *
     * @param   context the commit log segment position to be checked
     * @return  true if the commit log segment position is contained by this segment file.
     */
    public boolean contains(CommitLogPosition context)
    {
        return context.segmentId == id;
    }

    // For debugging, not fast
    public String dirtyString()
    {
        StringBuilder sb = new StringBuilder();
        for (TableId tableId : getDirtyTableIds())
        {
            TableMetadata m = Schema.instance.getTableMetadata(tableId);
            sb.append(m == null ? "<deleted>" : m.name).append(" (").append(tableId)
              .append(", dirty: ").append(tableDirty.get(tableId))
              .append(", clean: ").append(tableClean.get(tableId))
              .append("), ");
        }
        return sb.toString();
    }

    abstract public long onDiskSize();

    public long contentSize()
    {
        return lastSyncedOffset;
    }

    @Override
    public String toString()
    {
        return "CommitLogSegment(" + getPath() + ')';
    }

    public static class CommitLogSegmentFileComparator implements Comparator<File>
    {
        public int compare(File f, File f2)
        {
            return Long.compare(CommitLogDescriptor.idFromFileName(f.name()),
                                CommitLogDescriptor.idFromFileName(f2.name()));
        }
    }

    public CDCState getCDCState()
    {
        return cdcState;
    }

    /**
     * Change the current cdcState on this CommitLogSegment. There are some restrictions on state transitions and this
     * method is idempotent.
     *
     * @return the old cdc state
     */
    public CDCState setCDCState(CDCState newState)
    {
        if (newState == cdcState)
            return cdcState;

        // Also synchronized in CDCSizeTracker.processNewSegment and .processDiscardedSegment
        synchronized(cdcStateLock)
        {
            // Need duplicate CONTAINS to be idempotent since 2 threads can race on this lock
            if (cdcState == CDCState.CONTAINS && newState != CDCState.CONTAINS)
                throw new IllegalArgumentException("Cannot transition from CONTAINS to any other state.");

            if (cdcState == CDCState.FORBIDDEN && newState != CDCState.PERMITTED)
                throw new IllegalArgumentException("Only transition from FORBIDDEN to PERMITTED is allowed.");

            CDCState oldState = cdcState;
            cdcState = newState;
            return oldState;
        }
    }

    /**
     * A simple class for tracking information about the portion of a segment that has been allocated to a log write.
     */
    protected static class Allocation
    {
        private final CommitLogSegment segment;
        private final OpOrder.Group appendOp;
        private final int position;
        private final ByteBuffer buffer;

        Allocation(CommitLogSegment segment, OpOrder.Group appendOp, int position, ByteBuffer buffer)
        {
            this.segment = segment;
            this.appendOp = appendOp;
            this.position = position;
            this.buffer = buffer;
        }

        CommitLogSegment getSegment()
        {
            return segment;
        }

        ByteBuffer getBuffer()
        {
            return buffer;
        }

        // markWritten() MUST be called once we are done with the segment or the CL will never flush
        // but must not be called more than once
        void markWritten()
        {
            appendOp.close();
        }

        void awaitDiskSync(Timer waitingOnCommit)
        {
            try (Timer.Context ignored = waitingOnCommit.time())
            {
                segment.waitForSync(position);
            }
        }

        /**
         * Returns the position in the CommitLogSegment at the end of this allocation.
         */
        public CommitLogPosition getCommitLogPosition()
        {
            return new CommitLogPosition(segment.id, buffer.limit());
        }
    }

    protected abstract static class Builder
    {
        protected final AbstractCommitLogSegmentManager segmentManager;

        public Builder(AbstractCommitLogSegmentManager segmentManager)
        {
            this.segmentManager = segmentManager;
        }

        public abstract CommitLogSegment build();

        public abstract SimpleCachedBufferPool createBufferPool();
    }
}
