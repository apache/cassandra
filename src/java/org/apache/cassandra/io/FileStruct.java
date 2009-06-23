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

package org.apache.cassandra.io;

import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.log4j.Logger;
import com.google.common.collect.AbstractIterator;


public class FileStruct implements Comparable<FileStruct>, Iterator<String>
{
    private static Logger logger = Logger.getLogger(FileStruct.class);

    private String key = null; // decorated!
    private boolean exhausted = false;
    private IFileReader reader;
    private DataInputBuffer bufIn;
    private DataOutputBuffer bufOut;
    private SSTable sstable;
    private FileStructIterator iterator;

    FileStruct(SSTable sstable) throws IOException
    {
        this.reader = SequenceFile.bufferedReader(sstable.getFilename(), 1024 * 1024);
        this.sstable = sstable;
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
        return sstable.getPartitioner().getDecoratedKeyComparator().compare(key, f.key);
    }    

    public void seekTo(String seekKey)
    {
        try
        {
            long position = sstable.getNearestPosition(seekKey);
            if (position < 0)
            {
                exhausted = true;
                return;
            }
            reader.seek(position);
            advance();
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

    private class FileStructIterator extends AbstractIterator<String>
    {
        public FileStructIterator()
        {
            if (key == null)
            {
                if (!isExhausted())
                {
                    forward();
                }
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
        }

        protected String computeNext()
        {
            if (isExhausted())
            {
                return endOfData();
            }
            String oldKey = key;
            forward();
            return oldKey;
        }
    }
}
