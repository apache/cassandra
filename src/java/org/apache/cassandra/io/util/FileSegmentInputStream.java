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

package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

/**
 * This is the same as DataInputBuffer, i.e. a stream for a fixed byte buffer,
 * except that we also implement FileDataInput by using an offset and a file path.
 */
public class FileSegmentInputStream extends DataInputBuffer implements FileDataInput
{
    private final String filePath;
    private final long offset;

    public FileSegmentInputStream(ByteBuffer buffer, String filePath, long offset)
    {
        super(buffer, false);
        this.filePath = filePath;
        this.offset = offset;
    }

    public String getPath()
    {
        return filePath;
    }

    private long size()
    {
        return offset + buffer.capacity();
    }

    public boolean isEOF()
    {
        return !buffer.hasRemaining();
    }

    public long bytesRemaining()
    {
        return buffer.remaining();
    }

    public void seek(long pos)
    {
        if (pos < 0 || pos > size())
            throw new IllegalArgumentException(String.format("Unable to seek to position %d in %s (%d bytes) in partial mode",
                                                             pos,
                                                             getPath(),
                                                             size()));


        buffer.position((int) (pos - offset));
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    public DataPosition mark()
    {
        throw new UnsupportedOperationException();
    }

    public void reset(DataPosition mark)
    {
        throw new UnsupportedOperationException();
    }

    public long bytesPastMark(DataPosition mark)
    {
        return 0;
    }

    public long getFilePointer()
    {
        return offset + buffer.position();
    }
}
