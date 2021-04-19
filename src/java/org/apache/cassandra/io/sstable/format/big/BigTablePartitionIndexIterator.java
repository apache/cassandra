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
package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.big.BigTableRowIndexEntry.IndexSerializer;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

@NotThreadSafe
// TODO STAR-247: implement unit test
public class BigTablePartitionIndexIterator implements PartitionIndexIterator
{
    private final FileHandle indexFile;
    private final RandomAccessReader reader;
    private final IndexSerializer<IndexInfo> rowIndexEntrySerializer;
    private final long initialPosition;

    private ByteBuffer key;
    private long dataPosition;
    private long keyPosition;

    private BigTablePartitionIndexIterator(FileHandle indexFile,
                                           RandomAccessReader reader,
                                           IndexSerializer<IndexInfo> rowIndexEntrySerializer)
    {
        this.indexFile = indexFile;
        this.reader = reader;
        this.rowIndexEntrySerializer = rowIndexEntrySerializer;
        this.initialPosition = reader.getFilePointer();
    }

    public static BigTablePartitionIndexIterator create(RandomAccessReader reader, IndexSerializer<IndexInfo> serializer)
    throws IOException
    {
        BigTablePartitionIndexIterator iterator = new BigTablePartitionIndexIterator(null, reader, serializer);
        try
        {
            iterator.advance();
            return iterator;
        }
        catch (IOException | RuntimeException ex)
        {
            iterator.close();
            throw ex;
        }
    }

    @SuppressWarnings({ "resource" })
    public static BigTablePartitionIndexIterator create(FileHandle indexFile, IndexSerializer<IndexInfo> serializer)
    throws IOException
    {
        FileHandle iFile = null;
        RandomAccessReader reader = null;
        BigTablePartitionIndexIterator iterator = null;
        try
        {
            iFile = indexFile.sharedCopy();
            reader = iFile.createReader();
            iterator = new BigTablePartitionIndexIterator(iFile, reader, serializer);
            iterator.advance();
            return iterator;
        }
        catch (IOException | RuntimeException ex)
        {
            if (iterator != null)
            {
                iterator.close();
            }
            else
            {
                FileUtils.closeQuietly(reader);
                FileUtils.closeQuietly(iFile);
            }
            throw ex;
        }
    }

    @Override
    public void close()
    {
        key = null;
        dataPosition = -1;
        keyPosition = -1;
        FileUtils.closeQuietly(reader);
        FileUtils.closeQuietly(indexFile);
    }

    @Override
    public boolean advance() throws IOException
    {
        if (!reader.isEOF())
        {
            keyPosition = reader.getFilePointer();
            key = ByteBufferUtil.readWithShortLength(reader);
            dataPosition = rowIndexEntrySerializer.deserializePositionAndSkip(reader);
            return true;
        }
        else
        {
            keyPosition = -1;
            dataPosition = -1;
            key = null;
            return false;
        }
    }

    @Override
    public boolean isExhausted()
    {
        return key == null && dataPosition < 0;
    }

    @Override
    public ByteBuffer key()
    {
        return key;
    }

    @Override
    public long keyPosition()
    {
        return keyPosition;
    }

    @Override
    public long dataPosition()
    {
        return dataPosition;
    }

    @Override
    public long indexPosition()
    {
        return reader.getFilePointer();
    }

    @Override
    public void indexPosition(long position) throws IOException
    {
        if (position > indexLength())
            throw new IndexOutOfBoundsException("The requested position exceeds the index length");
        reader.seek(position);
        key = null;
        keyPosition = 0;
        dataPosition = 0;
        advance();
    }

    @Override
    public long indexLength()
    {
        return reader.length();
    }

    @Override
    public void reset() throws IOException
    {
        reader.seek(initialPosition);
        key = null;
        keyPosition = 0;
        dataPosition = 0;
        advance();
    }

    @Override
    public String toString()
    {
        return String.format("BigTable-PartitionIndexIterator(%s)", indexFile.path());
    }
}
