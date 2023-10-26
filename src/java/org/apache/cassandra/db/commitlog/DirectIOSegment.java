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
import java.nio.file.Files;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;

/*
 * Direct-IO segment. Allocates ByteBuffer using ByteBuffer.allocateDirect and align
 * ByteBuffer.position, ByteBuffer.limit and FileChannel.position to page size (4K).
 * Java-11 forces minimum page size to be written to disk with Direct-IO.
 */
public class DirectIOSegment extends CommitLogSegment
{
    ByteBuffer original;
    static int minimumAllowedAlign;

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

        // Testing shows writing initial bytes takes some time. During peak load, it helps
        // lot and Syncer thread can actually do flush activity. Making this initial
        // slow operation to be executed by Allocator thread here.
        int oldLastSyncedOffset = lastSyncedOffset;
        // 8 bytes are written above.
        lastSyncedOffset = 8;
        flush(0, lastSyncedOffset);
        lastSyncedOffset = oldLastSyncedOffset;
    }

    ByteBuffer createBuffer(CommitLog commitLog)
    {
        if (minimumAllowedAlign == 0)
        {
            try
            {
                minimumAllowedAlign = (int)Files.getFileStore(logFile.toPath()).getBlockSize();
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, logFile);
            }
        }

        int segmentSize = DatabaseDescriptor.getCommitLogSegmentSize();
        original = ByteBuffer.allocateDirect(segmentSize + minimumAllowedAlign);

        ByteBuffer alignedBuffer = original.alignedSlice(minimumAllowedAlign);
        assert alignedBuffer.limit() >= segmentSize : String.format("Bytebuffer slicing failed to get required buffer size (required=%d,current size=%d", segmentSize, alignedBuffer.limit());

        assert alignedBuffer.alignmentOffset(0, minimumAllowedAlign) == 0 : "Index 0 should be aligned to 4K page size";
        assert alignedBuffer.alignmentOffset(alignedBuffer.limit(), minimumAllowedAlign) == 0 : "Limit should be aligned to 4K page size" ;

        return alignedBuffer;
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
            // lastSyncedOffset is synced to disk. Align lastSyncedOffset to start of 4K page
            // and nextMarker to end of 4K page to avoid write errors.
            int filePosition = lastSyncedOffset;
            ByteBuffer duplicate = buffer.duplicate();

            // Aligned file position if not aligned to start of 4K page.
            if (filePosition % minimumAllowedAlign != 0 )
            {
                filePosition = filePosition & ~(minimumAllowedAlign -1);
                channel.position(filePosition);
            }
            duplicate.position(filePosition);

            int flushSizeInBytes = nextMarker;

            // Align last byte to end of 4K page.
            if (flushSizeInBytes % minimumAllowedAlign !=0)
                flushSizeInBytes = (flushSizeInBytes + minimumAllowedAlign) & ~(minimumAllowedAlign -1);

            duplicate.limit(flushSizeInBytes);

            channel.write(duplicate);

            // Direct I/O always writes flushes in block size and writes more than the flush size.
            // File size on disk will always multiple of page size and taking this into account
            // helps testcases to pass.
            if (flushSizeInBytes > lastWritten) {
                manager.addSize(flushSizeInBytes - lastWritten);
                lastWritten = flushSizeInBytes;
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
        FileUtils.clean(original);
        super.internalClose();
    }
}
