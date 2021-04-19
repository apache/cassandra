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

import org.apache.cassandra.io.sstable.format.ScrubPartitionIterator;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ScrubIterator implements ScrubPartitionIterator
{
    private final FileHandle ifile;
    private final RandomAccessReader reader;
    private final BigTableRowIndexEntry.IndexSerializer<IndexInfo> rowIndexEntrySerializer;

    private ByteBuffer key;
    private long dataPosition;

    public ScrubIterator(FileHandle ifile, BigTableRowIndexEntry.IndexSerializer<IndexInfo> rowIndexEntrySerializer) throws IOException
    {
        this.ifile = ifile.sharedCopy();
        this.reader = this.ifile.createReader();
        this.rowIndexEntrySerializer = rowIndexEntrySerializer;
        advance();
    }

    @Override
    public void close()
    {
        reader.close();
        ifile.close();
    }

    @Override
    public void advance() throws IOException
    {
        if (!reader.isEOF())
        {
            key = ByteBufferUtil.readWithShortLength(reader);
            dataPosition = rowIndexEntrySerializer.deserializePositionAndSkip(reader);
        }
        else
        {
            dataPosition = -1;
            key = null;
        }
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

    @Override
    public boolean isExhausted()
    {
        return dataPosition == -1;
    }
}
