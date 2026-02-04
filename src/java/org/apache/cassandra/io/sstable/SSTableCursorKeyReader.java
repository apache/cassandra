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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.ResizableByteBuffer;
import org.apache.cassandra.utils.Throwables;

@NotThreadSafe
public class SSTableCursorKeyReader implements AutoCloseable
{
    private final FileHandle indexFile;
    private final RandomAccessReader indexFileReader;
    private final long initialPosition;

    public static class Entry extends ResizableByteBuffer
    {
        private long dataPosition = -1;
        private long keyPosition = -1;

        public void load(RandomAccessReader indexReader) throws IOException
        {
            keyPosition = indexReader.getFilePointer();
            super.loadShortLength(indexReader);
            if (length() != 0)
            {
                dataPosition = indexReader.readUnsignedVInt();
                // skip row index entries
                int size = indexReader.readUnsignedVInt32();
                if (size > 0)
                    indexReader.skipBytesFully(size);
            }
            else
            {
                dataPosition = -1;
            }
        }

        public long dataPosition()
        {
            return dataPosition;
        }

        public long keyPosition()
        {
            return keyPosition;
        }

        public ByteBuffer getKey()
        {
            return buffer();
        }
    }

    private SSTableCursorKeyReader(FileHandle indexFile,
                                   RandomAccessReader indexFileReader)
    {
        this.indexFile = indexFile;
        this.indexFileReader = indexFileReader;
        this.initialPosition = indexFileReader.getFilePointer();
    }

    public static SSTableCursorKeyReader create(RandomAccessReader indexFileReader) throws IOException
    {
        return new SSTableCursorKeyReader(null, indexFileReader);
    }

    @SuppressWarnings({ "resource", "RedundantSuppression" }) // iFile and reader are closed in the BigTableKeyReader#close method
    public static SSTableCursorKeyReader create(FileHandle indexFile) throws IOException
    {
        FileHandle iFile = null;
        RandomAccessReader reader = null;
        try
        {
            iFile = indexFile.sharedCopy();
            reader = iFile.createReader();
            return new SSTableCursorKeyReader(iFile, reader);
        }
        catch (RuntimeException ex)
        {
            Throwables.closeNonNullAndAddSuppressed(ex, reader, iFile);
            throw ex;
        }
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(indexFileReader);
        FileUtils.closeQuietly(indexFile);
    }

    public boolean advance(Entry entry) throws IOException
    {
        if (indexFileReader.isEOF())
        {
            return false;
        }
        entry.load(indexFileReader);
        return true;
    }

    public boolean isExhausted()
    {
        return indexFileReader.isEOF();
    }

    public long indexPosition()
    {
        return indexFileReader.getFilePointer();
    }

    public void seek(long position) throws IOException
    {
        if (position > indexLength())
            throw new IndexOutOfBoundsException("The requested position exceeds the index length");
        indexFileReader.seek(position);
    }

    public long indexLength()
    {
        return indexFileReader.length();
    }

    public void reset() throws IOException
    {
        indexFileReader.seek(initialPosition);
    }

    @Override
    public String toString()
    {
        return String.format("BigTable-SSTableCursorKeyReader(%s), indexPosition=%d", indexFile.path(), indexFileReader.getFilePointer());
    }
}
