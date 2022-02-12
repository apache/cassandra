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

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.SyncUtil;

/**
 * Uncompressed commit log segment. Provides an in-memory buffer for the mutation threads. On sync writes anything
 * unwritten to disk and waits for the writes to materialize.
 *
 * The format of the uncompressed commit log is as follows:
 * - standard commit log header (as written by {@link CommitLogDescriptor#writeHeader(ByteBuffer, CommitLogDescriptor)})
 * - a series of 'sync segments' that are written every time the commit log is sync()'ed
 * -- a sync section header, see {@link CommitLogSegment#writeSyncMarker(long, ByteBuffer, int, int, int)}
 * -- a block of uncompressed data
 */
public class UncompressedSegment extends FileDirectSegment
{
    /**
     * Constructs a new segment file.
     */
    UncompressedSegment(CommitLog commitLog, AbstractCommitLogSegmentManager manager)
    {
        super(commitLog, manager);
        Preconditions.checkState(BufferType.OFF_HEAP == manager.getBufferPool().getPreferredReusableBufferType(),
                                 "Invalid BufferType %s, expected OFF_HEAP", manager.getBufferPool().getPreferredReusableBufferType());
    }

    ByteBuffer createBuffer(CommitLog commitLog)
    {
        return manager.getBufferPool().createBuffer();
    }

    @Override
    synchronized void write(int startMarker, int nextMarker)
    {
        int contentStart = startMarker + SYNC_MARKER_SIZE;
        int length = nextMarker - contentStart;
        // The length may be 0 when the segment is being closed.
        assert length > 0 || length == 0 && !isStillAllocating();

        try
        {
            writeSyncMarker(id, buffer, startMarker, startMarker, nextMarker);

            ByteBuffer inputBuffer = buffer.duplicate();
            inputBuffer.limit(nextMarker).position(startMarker);

            // Only one thread can be here at a given time.
            // Protected by synchronization on CommitLogSegment.sync().
            manager.addSize(inputBuffer.remaining());
            channel.write(inputBuffer);
            lastWrittenPos = nextMarker;
            assert channel.position() == nextMarker;
            SyncUtil.force(channel, true);
        }
        catch (Exception e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    @Override
    public long onDiskSize()
    {
        return lastWrittenPos;
    }
}
