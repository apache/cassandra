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

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.utils.SyncUtil;

/**
 * Writes to the backing commit log file only on sync, allowing transformations of the mutations,
 * such as compression or encryption, before writing out to disk.
 */
public abstract class FileDirectSegment extends CommitLogSegment
{
    volatile long lastWrittenPos = 0;

    FileDirectSegment(CommitLog commitLog, AbstractCommitLogSegmentManager manager)
    {
        super(commitLog, manager);
    }

    @Override
    void writeLogHeader()
    {
        super.writeLogHeader();
        try
        {
            channel.write((ByteBuffer) buffer.duplicate().flip());
            manager.addSize(lastWrittenPos = buffer.position());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
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

    @Override
    protected void flush(int startMarker, int nextMarker)
    {
        try
        {
            SyncUtil.force(channel, true);
        }
        catch (Exception e)
        {
            throw new FSWriteError(e, getPath());
        }
    }
}
