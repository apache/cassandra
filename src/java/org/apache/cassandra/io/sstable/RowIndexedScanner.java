/**
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
 */

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.io.IOError;
import java.util.Iterator;
import java.util.Arrays;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.IColumnIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.service.StorageService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RowIndexedScanner extends SSTableScanner
{
    private static Logger logger = LoggerFactory.getLogger(RowIndexedScanner.class);

    private final BufferedRandomAccessFile file;
    private final SSTableReader sstable;
    private IColumnIterator row;
    private boolean exhausted = false;
    private Iterator<IColumnIterator> iterator;
    private QueryFilter filter;

    /**
     * @param sstable SSTable to scan.
     */
    RowIndexedScanner(SSTableReader sstable, int bufferSize)
    {
        try
        {
            this.file = new BufferedRandomAccessFile(sstable.getFilename(), "r", bufferSize);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        this.sstable = sstable;
    }
    
    /**
     * @param sstable SSTable to scan.
     * @param filter filter to use when scanning the columns
     */
    RowIndexedScanner(SSTableReader sstable, QueryFilter filter, int bufferSize)
    {
        try
        {
            this.file = new BufferedRandomAccessFile(sstable.getFilename(), "r", bufferSize);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        this.sstable = sstable;
        this.filter = filter;
    }

    public void close() throws IOException
    {
        file.close();
    }

    public void seekTo(DecoratedKey seekKey)
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
            row = null;
        }
        catch (IOException e)
        {
            throw new RuntimeException("corrupt sstable", e);
        }
    }

    public long getFileLength()
    {
        try
        {
            return file.length();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public long getFilePointer()
    {
        return file.getFilePointer();
    }

    public boolean hasNext()
    {
        if (iterator == null)
            iterator = exhausted ? Arrays.asList(new IColumnIterator[0]).iterator() : new KeyScanningIterator();
        return iterator.hasNext();
    }

    public IColumnIterator next()
    {
        if (iterator == null)
            iterator = exhausted ? Arrays.asList(new IColumnIterator[0]).iterator() : new KeyScanningIterator();
        return iterator.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private class KeyScanningIterator implements Iterator<IColumnIterator>
    {
        private long dataStart;
        private long finishedAt;
        
        public boolean hasNext()
        {
            try
            {
                if (row == null)
                    return !file.isEOF();
                return finishedAt < file.length();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public IColumnIterator next()
        {
            try
            {
                if (row != null)
                    file.seek(finishedAt);
                assert !file.isEOF();

                DecoratedKey key = StorageService.getPartitioner().convertFromDiskFormat(file.readUTF());
                int dataSize = file.readInt();
                dataStart = file.getFilePointer();
                finishedAt = dataStart + dataSize;

                if (filter == null)
                {
                    return row = new SSTableIdentityIterator(sstable, file, key, dataStart, finishedAt);
                }
                else
                {
                    return row = filter.getSSTableColumnIterator(sstable, file, key, dataStart);
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
