package org.apache.cassandra.io.sstable;
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
import org.apache.cassandra.db.filter.IColumnIterator;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;

import com.google.common.collect.AbstractIterator;

public class SSTableIdentityIterator extends AbstractIterator<IColumn> implements Comparable<SSTableIdentityIterator>, IColumnIterator
{
    private final DecoratedKey key;
    private final long finishedAt;
    private final BufferedRandomAccessFile file;
    private SSTableReader sstable;
    private long dataStart;

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param key Key of this row.
     * @param dataStart Data for this row starts at this pos.
     * @param finishedAt Data for this row ends at this pos.
     * @throws IOException
     */
    public SSTableIdentityIterator(SSTableReader sstable, BufferedRandomAccessFile file, DecoratedKey key, long dataStart, long finishedAt)
    throws IOException
    {
        this.sstable = sstable;
        this.file = file;
        this.key = key;
        this.dataStart = dataStart;
        this.finishedAt = finishedAt;
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
            out.write(file.readByte());
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

    public int compareTo(SSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }

    public void close() throws IOException
    {
    }
}