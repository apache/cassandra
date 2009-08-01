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

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ColumnFamily;

import org.apache.log4j.Logger;
import com.google.common.collect.AbstractIterator;


public class FileStruct implements Comparable<FileStruct>, Iterator<String>
{
    private static Logger logger = Logger.getLogger(FileStruct.class);

    private IteratingRow row;
    private boolean exhausted = false;
    private BufferedRandomAccessFile file;
    private SSTableReader sstable;
    private FileStructIterator iterator;

    FileStruct(SSTableReader sstable) throws IOException
    {
        this.file = new BufferedRandomAccessFile(sstable.getFilename(), "r", 1024 * 1024);
        this.sstable = sstable;
    }

    public String getFileName()
    {
        return file.getPath();
    }

    public void close() throws IOException
    {
        file.close();
    }

    public boolean isExhausted()
    {
        return exhausted;
    }

    public String getKey()
    {
        return row.getKey();
    }

    public ColumnFamily getColumnFamily()
    {
        return row.getEmptyColumnFamily();
    }

    public int compareTo(FileStruct f)
    {
        return sstable.getPartitioner().getDecoratedKeyComparator().compare(getKey(), f.getKey());
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
            file.seek(position);
            advance(false);
        }
        catch (IOException e)
        {
            throw new RuntimeException("corrupt sstable", e);
        }
    }

    /*
     * Read the next key from the data file.
     * Caller must check isExhausted after each call to see if further
     * reads are valid.
     * Do not mix with calls to the iterator interface (next/hasnext).
     * @deprecated -- prefer the iterator interface.
     */
    public void advance(boolean materialize) throws IOException
    {
        // TODO r/m materialize option -- use iterableness!
        if (exhausted)
        {
            throw new IndexOutOfBoundsException();
        }

        if (file.isEOF())
        {
            file.close();
            exhausted = true;
            return;
        }

        row = new IteratingRow(file);
        if (materialize)
        {
            while (row.hasNext())
            {
                IColumn column = row.next();
                row.getEmptyColumnFamily().addColumn(column);
            }
        }
        else
        {
            row.skipRemaining();
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

    private class FileStructIterator extends AbstractIterator<String>
    {
        public FileStructIterator()
        {
            if (row == null)
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
                advance(false);
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
            String oldKey = getKey();
            forward();
            return oldKey;
        }
    }
}
