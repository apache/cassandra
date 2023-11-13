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

import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry.IndexSerializer;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;

@NotThreadSafe
public class BigTableKeyReader implements KeyReader
{
    private final FileHandle indexFile;
    private final RandomAccessReader indexFileReader;
    private final IndexSerializer rowIndexEntrySerializer;
    private final long initialPosition;

    private ByteBuffer key;
    private long dataPosition;
    private long keyPosition;

    private BigTableKeyReader(FileHandle indexFile,
                              RandomAccessReader indexFileReader,
                              IndexSerializer rowIndexEntrySerializer)
    {
        this.indexFile = indexFile;
        this.indexFileReader = indexFileReader;
        this.rowIndexEntrySerializer = rowIndexEntrySerializer;
        this.initialPosition = indexFileReader.getFilePointer();
    }

    public static BigTableKeyReader create(RandomAccessReader indexFileReader, IndexSerializer serializer) throws IOException
    {
        BigTableKeyReader iterator = new BigTableKeyReader(null, indexFileReader, serializer);
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

    @SuppressWarnings({ "resource", "RedundantSuppression" }) // iFile and reader are closed in the BigTableKeyReader#close method
    public static BigTableKeyReader create(FileHandle indexFile, IndexSerializer serializer) throws IOException
    {
        FileHandle iFile = null;
        RandomAccessReader reader = null;
        BigTableKeyReader iterator = null;
        try
        {
            iFile = indexFile.sharedCopy();
            reader = iFile.createReader();
            iterator = new BigTableKeyReader(iFile, reader, serializer);
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
                Throwables.closeNonNullAndAddSuppressed(ex, reader, iFile);
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
        FileUtils.closeQuietly(indexFileReader);
        FileUtils.closeQuietly(indexFile);
    }

    @Override
    public boolean advance() throws IOException
    {
        if (!indexFileReader.isEOF())
        {
            keyPosition = indexFileReader.getFilePointer();
            key = ByteBufferUtil.readWithShortLength(indexFileReader);
            dataPosition = rowIndexEntrySerializer.deserializePositionAndSkip(indexFileReader);
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
    public long keyPositionForSecondaryIndex()
    {
        return keyPosition;
    }

    @Override
    public long dataPosition()
    {
        return dataPosition;
    }

    public long indexPosition()
    {
        return indexFileReader.getFilePointer();
    }

    public void indexPosition(long position) throws IOException
    {
        if (position > indexLength())
            throw new IndexOutOfBoundsException("The requested position exceeds the index length");
        indexFileReader.seek(position);
        key = null;
        keyPosition = 0;
        dataPosition = 0;
        advance();
    }

    public long indexLength()
    {
        return indexFileReader.length();
    }

    @Override
    public void reset() throws IOException
    {
        indexFileReader.seek(initialPosition);
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
