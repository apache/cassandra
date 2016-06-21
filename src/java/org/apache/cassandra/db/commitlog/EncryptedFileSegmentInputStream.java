/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.commitlog;

import java.io.DataInput;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileSegmentInputStream;

/**
 * Each segment of an encrypted file may contain many encrypted chunks, and each chunk needs to be individually decrypted
 * to reconstruct the full segment.
 */
public class EncryptedFileSegmentInputStream extends FileSegmentInputStream implements FileDataInput, DataInput
{
    private final long segmentOffset;
    private final int expectedLength;
    private final ChunkProvider chunkProvider;

    /**
     * Offset representing the decrypted chunks already processed in this segment.
     */
    private int totalChunkOffset;

    public EncryptedFileSegmentInputStream(String filePath, long segmentOffset, int position, int expectedLength, ChunkProvider chunkProvider)
    {
        super(chunkProvider.nextChunk(), filePath, position);
        this.segmentOffset = segmentOffset;
        this.expectedLength = expectedLength;
        this.chunkProvider = chunkProvider;
    }

    public interface ChunkProvider
    {
        /**
         * Get the next chunk from the backing provider, if any chunks remain.
         * @return Next chunk, else null if no more chunks remain.
         */
        ByteBuffer nextChunk();
    }

    public long getFilePointer()
    {
        return segmentOffset + totalChunkOffset + buffer.position();
    }

    public boolean isEOF()
    {
        return totalChunkOffset + buffer.position() >= expectedLength;
    }

    public long bytesRemaining()
    {
        return expectedLength - (totalChunkOffset + buffer.position());
    }

    public void seek(long position)
    {
        long bufferPos = position - totalChunkOffset - segmentOffset;
        while (buffer != null && bufferPos > buffer.capacity())
        {
            // rebuffer repeatedly until we have reached desired position
            buffer.position(buffer.limit());

            // increases totalChunkOffset
            reBuffer();
            bufferPos = position - totalChunkOffset - segmentOffset;
        }
        if (buffer == null || bufferPos < 0 || bufferPos > buffer.capacity())
            throw new IllegalArgumentException(
                    String.format("Unable to seek to position %d in %s (%d bytes) in partial mode",
                            position,
                            getPath(),
                            segmentOffset + expectedLength));
        buffer.position((int) bufferPos);
    }

    public long bytesPastMark(DataPosition mark)
    {
        throw new UnsupportedOperationException();
    }

    public void reBuffer()
    {
        totalChunkOffset += buffer.position();
        buffer = chunkProvider.nextChunk();
    }
}
