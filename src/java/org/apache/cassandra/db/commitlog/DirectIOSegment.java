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
import java.nio.LongBuffer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;

/*
 * Direct-IO segment. Allocates ByteBuffer using ByteBuffer.allocateDirect and align
 * ByteBuffer.position, ByteBuffer.limit and FileChannel.position to page size (4K).
 * Java-11 forces minimum page size to be written to disk with Direct-IO.
 */
public class DirectIOSegment extends CommitLogSegment
{
    private ByteBuffer original;

    // Needed to track number of bytes written to disk in multiple of page size.
    long lastWritten = 0;

    /**
     * Constructs a new segment file.
     *
     * @param commitLog the commit log it will be used with.
     */
    DirectIOSegment(CommitLog commitLog, AbstractCommitLogSegmentManager manager)
    {
        super(commitLog, manager);

        // mark the initial sync marker as uninitialised
        int firstSync = buffer.position();
        buffer.putInt(firstSync + 0, 0);
        buffer.putInt(firstSync + 4, 0);
    }

    ByteBuffer createBuffer(CommitLog commitLog)
    {
        int segmentSize = DatabaseDescriptor.getCommitLogSegmentSize();

        original =  manager.getBufferPool().createBuffer();
        assert original != null : String.format("Direct ByteBuffer allocation failed for DirectIOSegment");

        // May get previously used buffer and zero it out to now. Direct I/O writes additional bytes during flush
        // operation.
        LongBuffer arrayBuffer = original.asLongBuffer();
        for(int i = 0 ; i < arrayBuffer.limit() ; i++)
            arrayBuffer.put(i, 0);

        ByteBuffer alignedBuffer = original.alignedSlice(minimumDirectIOAlignement);
        assert alignedBuffer.limit() >= segmentSize : String.format("Bytebuffer slicing failed to get required buffer size (required=%d,current size=%d", segmentSize, alignedBuffer.limit());

        assert alignedBuffer.alignmentOffset(0, minimumDirectIOAlignement) == 0 : String.format("Index 0 should be aligned to %d page size.", minimumDirectIOAlignement);
        assert alignedBuffer.alignmentOffset(alignedBuffer.limit(), minimumDirectIOAlignement) == 0 : String.format("Limit should be aligned to %d page size", minimumDirectIOAlignement);

        return alignedBuffer;
    }

    @Override
    void writeLogHeader()
    {
        super.writeLogHeader();
        // Testing shows writing initial bytes takes some time for Direct I/O. During peak load,
        // it is better to make "COMMIT-LOG-ALLOCATOR" thread to write these few bytes of each
        // file and this helps syncer thread to speedup the flush activity.
        flush(0, lastSyncedOffset);
    }

    @Override
    void write(int startMarker, int nextMarker)
    {
        // if there's room in the discard section to write an empty header,
        // zero out the next sync marker so replayer can cleanly exit
        if (nextMarker <= buffer.capacity() - SYNC_MARKER_SIZE)
        {
            buffer.putInt(nextMarker, 0);
            buffer.putInt(nextMarker + 4, 0);
        }

        // write previous sync marker to point to next sync marker
        // we don't chain the crcs here to ensure this method is idempotent if it fails
        writeSyncMarker(id, buffer, startMarker, startMarker, nextMarker);
    }

    @Override
    protected void flush(int startMarker, int nextMarker)
    {
        try
        {
            // lastSyncedOffset is synced to disk. Align lastSyncedOffset to start of its block
            // and nextMarker to end of its block to avoid write errors.
            int flushPosition = lastSyncedOffset;
            ByteBuffer duplicate = buffer.duplicate();

            // Aligned file position if not aligned to start of 4K page.
            if (flushPosition % minimumDirectIOAlignement != 0)
            {
                flushPosition = flushPosition & ~(minimumDirectIOAlignement -1);
                channel.position(flushPosition);
            }
            duplicate.position(flushPosition);

            int flushLimit = nextMarker;

            // Align last byte to end of block.
            if (flushLimit % minimumDirectIOAlignement != 0)
                flushLimit = (flushLimit + minimumDirectIOAlignement) & ~(minimumDirectIOAlignement -1);

            duplicate.limit(flushLimit);

            channel.write(duplicate);

            // Direct I/O always writes flushes in block size and writes more than the flush size.
            // File size on disk will always multiple of block size and taking this into account
            // helps testcases to pass. Avoid counting same block more than once.
            if (flushLimit > lastWritten)
            {
                manager.addSize(flushLimit - lastWritten);
                lastWritten = flushLimit;
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    @Override
    public long onDiskSize()
    {
        return lastWritten;
    }

    @Override
    protected void internalClose()
    {
        try
        {
            manager.getBufferPool().releaseBuffer(original);
            super.internalClose();
        }
        finally
        {
            manager.notifyBufferFreed();
        }
    }
}
