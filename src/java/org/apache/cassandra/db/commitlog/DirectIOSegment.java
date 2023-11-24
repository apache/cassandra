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
import java.nio.file.StandardOpenOption;

import com.google.common.annotations.VisibleForTesting;

import com.sun.nio.file.ExtendedOpenOption;
import net.openhft.chronicle.core.util.ThrowingFunction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SimpleCachedBufferPool;
import org.apache.cassandra.utils.ByteBufferUtil;
import sun.nio.ch.DirectBuffer;

/*
 * Direct-IO segment. Allocates ByteBuffer using ByteBuffer.allocateDirect and align
 * ByteBuffer.position, ByteBuffer.limit and FileChannel.position to page size (4K).
 * Java-11 forces minimum page size to be written to disk with Direct-IO.
 */
public class DirectIOSegment extends CommitLogSegment
{
    private final int fsBlockSize;
    private final int fsBlockRemainderMask;

    // Needed to track number of bytes written to disk in multiple of page size.
    long lastWritten = 0;

    /**
     * Constructs a new segment file.
     */
    DirectIOSegment(AbstractCommitLogSegmentManager manager, ThrowingFunction<Path, FileChannel, IOException> channelFactory, int fsBlockSize)
    {
        super(manager, channelFactory);

        assert Integer.highestOneBit(fsBlockSize) == fsBlockSize : "fsBlockSize must be a power of 2";

        // mark the initial sync marker as uninitialised
        int firstSync = buffer.position();
        buffer.putInt(firstSync + 0, 0);
        buffer.putInt(firstSync + 4, 0);

        this.fsBlockSize = fsBlockSize;
        this.fsBlockRemainderMask = fsBlockSize - 1;
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
            // TODO move the alignment calculations to PageAware

            // lastSyncedOffset is synced to disk. Align lastSyncedOffset to start of its block
            // and nextMarker to end of its block to avoid write errors.
            int flushPosition = lastSyncedOffset;
            ByteBuffer duplicate = buffer.duplicate();

            // Aligned file position if not aligned to start of a block.
            if ((flushPosition & fsBlockRemainderMask) != 0)
            {
                flushPosition = flushPosition & -fsBlockSize;
                channel.position(flushPosition);
            }
            duplicate.position(flushPosition);

            int flushLimit = nextMarker;

            // Align last byte to end of block
            flushLimit = (flushLimit + fsBlockSize - 1) & -fsBlockSize;

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
            manager.getBufferPool().releaseBuffer(buffer);
            super.internalClose();
        }
        finally
        {
            manager.notifyBufferFreed();
        }
    }

    protected static class DirectIOSegmentBuilder extends CommitLogSegment.Builder
    {
        public final int fsBlockSize;

        public DirectIOSegmentBuilder(AbstractCommitLogSegmentManager segmentManager)
        {
            this(segmentManager, FileUtils.getBlockSize(new File(segmentManager.storageDirectory)));
        }

        @VisibleForTesting
        public DirectIOSegmentBuilder(AbstractCommitLogSegmentManager segmentManager, int fsBlockSize)
        {
            super(segmentManager);
            this.fsBlockSize = fsBlockSize;
        }

        @Override
        public DirectIOSegment build()
        {
            return new DirectIOSegment(segmentManager,
                                       path -> FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE, ExtendedOpenOption.DIRECT),
                                       fsBlockSize);
        }

        @Override
        public SimpleCachedBufferPool createBufferPool()
        {
            // The direct buffer must be aligned with the file system block size. We cannot enforce that during
            // allocation, but we can get an aligned slice from the allocated buffer. The buffer must be oversized by the
            // alignment unit to make it possible.
            return new SimpleCachedBufferPool(DatabaseDescriptor.getCommitLogMaxCompressionBuffersInPool(),
                                              DatabaseDescriptor.getCommitLogSegmentSize() + fsBlockSize,
                                              BufferType.OFF_HEAP) {
                @Override
                public ByteBuffer createBuffer()
                {
                    int segmentSize = DatabaseDescriptor.getCommitLogSegmentSize();

                    ByteBuffer original = super.createBuffer();

                    // May get previously used buffer and zero it out to now. Direct I/O writes additional bytes during
                    // flush operation
                    ByteBufferUtil.writeZeroes(original.duplicate(), original.limit());

                    ByteBuffer alignedBuffer;
                    if (original.alignmentOffset(0, fsBlockSize) > 0)
                        alignedBuffer = original.alignedSlice(fsBlockSize);
                    else
                        alignedBuffer = original.slice().limit(segmentSize);

                    assert alignedBuffer.limit() >= segmentSize : String.format("Bytebuffer slicing failed to get required buffer size (required=%d, current size=%d", segmentSize, alignedBuffer.limit());

                    assert alignedBuffer.alignmentOffset(0, fsBlockSize) == 0 : String.format("Index 0 should be aligned to %d page size.", fsBlockSize);
                    assert alignedBuffer.alignmentOffset(alignedBuffer.limit(), fsBlockSize) == 0 : String.format("Limit should be aligned to %d page size", fsBlockSize);

                    return alignedBuffer;
                }

                @Override
                public void releaseBuffer(ByteBuffer buffer)
                {
                    ByteBuffer original = (ByteBuffer) ((DirectBuffer) buffer).attachment();
                    assert original != null;
                    super.releaseBuffer(original);
                }
            };
        }
    }
}
