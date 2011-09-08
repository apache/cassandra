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


import java.io.File;
import java.io.IOError;
import java.io.IOException;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;

public class KeyIterator extends AbstractIterator<DecoratedKey> implements CloseableIterator<DecoratedKey>
{
    private final RandomAccessReader in;
    private final Descriptor desc;

    public KeyIterator(Descriptor desc)
    {
        this.desc = desc;
        try
        {
            in = RandomAccessReader.open(new File(desc.filenameFor(SSTable.COMPONENT_INDEX)), true);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    protected DecoratedKey<?> computeNext()
    {
        try
        {
            if (in.isEOF())
                return endOfData();
            DecoratedKey<?> key = SSTableReader.decodeKey(StorageService.getPartitioner(), desc, ByteBufferUtil.readWithShortLength(in));
            in.readLong(); // skip data position
            return key;
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public void close() throws IOException
    {
        in.close();
    }

    public long getBytesRead()
    {
        return in.getFilePointer();
    }

    public long getTotalBytes()
    {
        try
        {
            return in.length();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }
}
