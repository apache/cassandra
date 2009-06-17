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
import java.util.Iterator;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.io.Coordinate;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.log4j.Logger;


public class FileStruct implements Comparable<FileStruct>, Iterator<String>
{
    private static Logger logger = Logger.getLogger(FileStruct.class);

    private String key = null; // decorated!
    private boolean exhausted = false;
    private IFileReader reader;
    private DataInputBuffer bufIn;
    private DataOutputBuffer bufOut;
    private IPartitioner partitioner;
    private FileStructIterator iterator;

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

    public void seekTo(String seekKey)
    {
        try
        {
            Coordinate range = SSTable.getCoordinates(seekKey, reader, partitioner);
            reader.seek(range.end_);
            long position = reader.getPositionFromBlockIndex(seekKey);
            if (position == -1)
            {
                reader.seek(range.start_);
            }
            else
            {
                reader.seek(position);
            }

            while (!exhausted)
            {
                advance();
                if (key.compareTo(seekKey) >= 0)
                {
                    break;
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("corrupt sstable", e);
        }
    }

    /*
     * Read the next key from the data file, skipping block indexes.
     * Caller must check isExhausted after each call to see if further
     * reads are valid.
     * Do not mix with calls to the iterator interface (next/hasnext).
     * @deprecated -- prefer the iterator interface.
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

    public boolean hasNext()
    {
        if (iterator == null)
            iterator = new FileStructIterator();
        return iterator.hasNext();
    }

    /** do not mix with manual calls to advance(). */
    public String next()
    {
        if (iterator == null)
            iterator = new FileStructIterator();
        return iterator.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private class FileStructIterator
    {
        String saved;

        public FileStructIterator()
        {
            if (key == null)
            {
                if (!isExhausted())
                {
                    forward();
                }
            }
            if (key.equals(SSTable.blockIndexKey_))
            {
                saved = null;
            }
            else
            {
                saved = key;
            }
        }

        private void forward()
        {
            try
            {
                advance();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            saved = isExhausted() ? null : key;
        }

        public boolean hasNext()
        {
            return saved != null;
        }

        public String next()
        {
            if (saved == null)
            {
                throw new IndexOutOfBoundsException();
            }
            String key = saved;
            forward();
            return key;
        }
    }
}
