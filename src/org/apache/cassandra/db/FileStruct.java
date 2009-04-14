/**
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

package org.apache.cassandra.db;

import java.io.IOException;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.service.IPartitioner;


public class FileStruct implements Comparable<FileStruct>
{
    private String key = null; // decorated!
    private boolean exhausted = false;
    private IFileReader reader;
    private DataInputBuffer bufIn;
    private DataOutputBuffer bufOut;
    private IPartitioner partitioner;

    public FileStruct(IFileReader reader, IPartitioner partitioner)
    {
        this.reader = reader;
        this.partitioner = partitioner;
        bufIn = new DataInputBuffer();
        bufOut = new DataOutputBuffer();
    }

    public String getFileName()
    {
        return reader.getFileName();
    }

    public void close() throws IOException
    {
        reader.close();
    }

    public boolean isExhausted()
    {
        return exhausted;
    }

    public DataInputBuffer getBufIn()
    {
        return bufIn;
    }

    public String getKey()
    {
        return key;
    }

    public int compareTo(FileStruct f)
    {
        return partitioner.getDecoratedKeyComparator().compare(key, f.key);
    }

    /*
     * Read the next key from the data file, skipping block indexes.
     * Caller must check isExhausted after each call to see if further
     * reads are valid.
     */
    public void advance() throws IOException
    {
        if (exhausted)
        {
            throw new IndexOutOfBoundsException();
        }

        bufOut.reset();
        if (reader.isEOF())
        {
            reader.close();
            exhausted = true;
            return;
        }

        long bytesread = reader.next(bufOut);
        if (bytesread == -1)
        {
            reader.close();
            exhausted = true;
            return;
        }

        bufIn.reset(bufOut.getData(), bufOut.getLength());
        key = bufIn.readUTF();
        /* If the key we read is the Block Index Key then omit and read the next key. */
        if (key.equals(SSTable.blockIndexKey_))
        {
            reader.close();
            exhausted = true;
        }
    }

}
