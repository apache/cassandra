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


import java.io.IOException;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.config.DatabaseDescriptor;
import com.google.common.collect.AbstractIterator;

public class IteratingRow extends AbstractIterator<IColumn>
{
    private final String key;
    private final long finishedAt;
    private final ColumnFamily emptyColumnFamily;
    private final BufferedRandomAccessFile file;

    public IteratingRow(BufferedRandomAccessFile file, SSTableReader sstable) throws IOException
    {
        this.file = file;

        key = file.readUTF();
        long dataSize = file.readInt();
        long dataStart = file.getFilePointer();
        finishedAt = dataStart + dataSize;
        IndexHelper.skipBloomFilterAndIndex(file);
        emptyColumnFamily = ColumnFamily.serializer().deserializeFromSSTableNoColumns(sstable.makeColumnFamily(), file);
        file.readInt(); // column count. breaking serializer encapsulation is less fugly than adding a wrapper class to allow deserializeEmpty to return both values
    }

    public String getKey()
    {
        return key;
    }

    public ColumnFamily getEmptyColumnFamily()
    {
        return emptyColumnFamily;
    }

    public void skipRemaining() throws IOException
    {
        file.seek(finishedAt);
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

            return emptyColumnFamily.getColumnSerializer().deserialize(file);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
