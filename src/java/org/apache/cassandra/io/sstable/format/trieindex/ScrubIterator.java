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
package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.sstable.format.ScrubPartitionIterator;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.ByteBufferUtil;

// TODO STAR-247: implement unit test
public class ScrubIterator extends PartitionIndex.IndexPosIterator implements ScrubPartitionIterator
{
    ByteBuffer key;
    long dataPosition;
    final FileHandle rowIndexFile;

    ScrubIterator(PartitionIndex partitionIndex, FileHandle rowIndexFile) throws IOException
    {
        super(partitionIndex);
        this.rowIndexFile = rowIndexFile.sharedCopy();
        advance();
    }

    @Override
    public void close()
    {
        super.close();
        rowIndexFile.close();
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
    public void advance() throws IOException
    {
        long pos = nextIndexPos();
        if (pos != PartitionIndex.NOT_FOUND)
        {
            if (pos >= 0) // row index position
            {
                try (FileDataInput in = rowIndexFile.createReader(pos))
                {
                    key = ByteBufferUtil.readWithShortLength(in);
                    dataPosition = TrieIndexEntry.deserialize(in, in.getFilePointer()).position;
                }
            }
            else
            {
                key = null;
                dataPosition = ~pos;
            }
        }
        else
        {
            key = null;
            dataPosition = -1;
        }
    }

    @Override
    public boolean isExhausted()
    {
        return dataPosition == -1;
    }
}
