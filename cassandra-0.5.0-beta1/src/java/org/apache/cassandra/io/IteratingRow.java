package org.apache.cassandra.io;
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


import java.io.*;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.service.StorageService;
import com.google.common.collect.AbstractIterator;

public class IteratingRow extends AbstractIterator<IColumn> implements Comparable<IteratingRow>
{
    private final DecoratedKey key;
    private final long finishedAt;
    private final BufferedRandomAccessFile file;
    private SSTableReader sstable;
    private long dataStart;
    private final IPartitioner partitioner;

    public IteratingRow(BufferedRandomAccessFile file, SSTableReader sstable) throws IOException
    {
        this.file = file;
        this.sstable = sstable;
        this.partitioner = StorageService.getPartitioner();

        key = partitioner.convertFromDiskFormat(file.readUTF());
        int dataSize = file.readInt();
        dataStart = file.getFilePointer();
        finishedAt = dataStart + dataSize;
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public String getPath()
    {
        return file.getPath();
    }

    public void echoData(DataOutput out) throws IOException
    {
        file.seek(dataStart);
        while (file.getFilePointer() < finishedAt)
        {
            out.write(file.read());
        }
    }

    // TODO r/m this and make compaction merge columns iteratively for CASSSANDRA-16
    public ColumnFamily getColumnFamily() throws IOException
    {
        file.seek(dataStart);
        IndexHelper.skipBloomFilter(file);
        IndexHelper.skipIndex(file);
        return ColumnFamily.serializer().deserializeFromSSTable(sstable, file);
    }

    public void skipRemaining() throws IOException
    {
        file.seek(finishedAt);
    }

    public long getEndPosition()
    {
        return finishedAt;
    }

    protected IColumn computeNext()
    {
        try
        {
            assert file.getFilePointer() <= finishedAt;
            if (file.getFilePointer() == finishedAt)
            {
                return endOfData();
            }

            return sstable.getColumnSerializer().deserialize(file);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public int compareTo(IteratingRow o)
    {
        return partitioner.getDecoratedKeyComparator().compare(key, o.key);
    }
}
