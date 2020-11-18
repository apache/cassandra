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
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.db.RowIndexEntry.IndexSerializer;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

@NotThreadSafe
public class BigTablePartitionIndexIterator implements PartitionIndexIterator
{
    private final FileHandle indexFile;
    private final RandomAccessReader reader;
    private final IndexSerializer<IndexInfo> rowIndexEntrySerializer;

    private ByteBuffer key;
    private long dataPosition;

    private BigTablePartitionIndexIterator(FileHandle indexFile,
                                           RandomAccessReader reader,
                                           IndexSerializer<IndexInfo> rowIndexEntrySerializer)
    {
        this.indexFile = indexFile;
        this.reader = reader;
        this.rowIndexEntrySerializer = rowIndexEntrySerializer;
    }

    public static BigTablePartitionIndexIterator create(FileHandle indexFile, IndexSerializer<IndexInfo> indexSerializer)
    throws IOException
    {
        FileHandle iFile = null;
        RandomAccessReader reader = null;
        try
        {
            iFile = indexFile.sharedCopy();
            reader = iFile.createReader();

            BigTablePartitionIndexIterator iterator = new BigTablePartitionIndexIterator(iFile,
                                                                                         reader,
                                                                                         Objects.requireNonNull(indexSerializer));

            iterator.advance();
            return iterator;
        }
        catch (IOException | RuntimeException ex)
        {
            if (reader != null)
                FileUtils.closeQuietly(reader);
            if (iFile != null)
                FileUtils.closeQuietly(iFile);
            throw ex;
        }
    }

    @Override
    public void close()
    {
        key = null;
        dataPosition = -1;
        FileUtils.closeQuietly(reader);
        FileUtils.closeQuietly(indexFile);
    }

    @Override
    public boolean advance() throws IOException
    {
        if (!reader.isEOF())
        {
            key = ByteBufferUtil.readWithShortLength(reader);
            dataPosition = rowIndexEntrySerializer.deserializePositionAndSkip(reader);
            System.err.println("ADVANCED INDEX ITERATOR TO: " + ByteBufferUtil.bytesToHex(key) + " / " + dataPosition);
            return true;
        }
        else
        {
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
    public long dataPosition()
    {
        return dataPosition;
    }
}
