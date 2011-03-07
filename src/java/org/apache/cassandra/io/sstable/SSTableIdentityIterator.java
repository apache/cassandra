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


import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.PageCacheInformer;
import org.apache.cassandra.utils.PageCacheMetrics;

public class SSTableIdentityIterator implements Comparable<SSTableIdentityIterator>, IColumnIterator
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableIdentityIterator.class);

    private final DecoratedKey key;
    private final long finishedAt;
    private final BufferedRandomAccessFile file;
    public final SSTableReader sstable;
    private final long dataStart;
    public final long dataSize;
    public final PageCacheMetrics pageCacheMetrics;
    private boolean hasRowsInPageCache;
    private boolean hasColumnsInPageCache;

    private final ColumnFamily columnFamily;
    public final int columnCount;
    private final long columnPosition;

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param key Key of this row.
     * @param dataStart Data for this row starts at this pos.
     * @param dataSize length of row data
     * @throws IOException
     */
    public SSTableIdentityIterator(SSTableReader sstable, BufferedRandomAccessFile file, DecoratedKey key, long dataStart, long dataSize, PageCacheMetrics pageCacheMetrics)
    throws IOException
    {
        this(sstable, file, key, dataStart, dataSize, pageCacheMetrics, false);
    }

    public SSTableIdentityIterator(SSTableReader sstable, BufferedRandomAccessFile file, DecoratedKey key, long dataStart, long dataSize, PageCacheMetrics pageCacheMetrics, boolean deserializeRowHeader)
    throws IOException
    {
        this.sstable = sstable;
        this.file = file;
        this.key = key;
        this.dataStart = dataStart;
        this.dataSize = dataSize;
        finishedAt = dataStart + dataSize;
        this.pageCacheMetrics = pageCacheMetrics;

        //Mark if any rows are in the pageCache
        hasRowsInPageCache = (pageCacheMetrics != null) && pageCacheMetrics.isRangeInCache(dataStart, finishedAt);

        try
        {
            file.seek(this.dataStart);
            if (deserializeRowHeader)
            {
                try
                {
                    IndexHelper.defreezeBloomFilter(file, dataSize, sstable.descriptor.usesOldBloomFilter);
                }
                catch (Exception e)
                {
                    if (e instanceof EOFException)
                        throw (EOFException) e;

                    logger.debug("Invalid bloom filter in {}; will rebuild it", sstable);
                    // deFreeze should have left the file position ready to deserialize index
                }
                try
                {
                    IndexHelper.deserializeIndex(file);
                }
                catch (Exception e)
                {
                    logger.debug("Invalid row summary in {}; will rebuild it", sstable);
                }
                file.seek(this.dataStart);
            }

            IndexHelper.skipBloomFilter(file);
            IndexHelper.skipIndex(file);
            columnFamily = sstable.createColumnFamily();
            ColumnFamily.serializer().deserializeFromSSTableNoColumns(columnFamily, file);
            columnCount = file.readInt();
            columnPosition = file.getFilePointer();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return columnFamily;
    }

    public boolean hasNext()
    {
        return file.getFilePointer() < finishedAt;
    }

    public IColumn next()
    {
        try
        {
            long columnStartAt = file.getFilePointer();

            IColumn col = sstable.getColumnSerializer().deserialize(file);

            long columnEndAt = file.getFilePointer();

            if (pageCacheMetrics != null)
            {
                col.setIsInPageCache(pageCacheMetrics.isRangeInCache(columnStartAt, columnEndAt));
            }

            return col;

        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException
    {
        // creator is responsible for closing file when finished
    }

    public String getPath()
    {
        return file.getPath();
    }

    public void echoData(PageCacheInformer out) throws IOException
    {
        file.seek(dataStart);

        if (pageCacheMetrics == null)
        {
            while (file.getFilePointer() < finishedAt)
            {
                out.write(file.readByte());
            }
        }
        else
        {
            // Since this is just a big opaque block of data we
            // Split into chunks >= pageSize
            int chunkSize = (int) (finishedAt - dataStart) / 128;

            chunkSize = chunkSize >= pageCacheMetrics.pageSize ? chunkSize : pageCacheMetrics.pageSize;

            long chunkStart = 0;
            long chunkEnd = 0;
            boolean isChunkInPageCache = false;

            while (file.getFilePointer() < finishedAt)
            {

                // Mark chunks that have cached pages
                // So we can migrate them
                if (file.getFilePointer() >= chunkEnd)
                {
                    if (isChunkInPageCache)
                    {
                        out.keepCacheWindow(out.getCurrentPosition() - chunkSize);
                    }

                    chunkStart = file.getFilePointer();
                    chunkEnd = chunkStart + chunkSize;

                    if(chunkEnd > finishedAt)
                        chunkEnd = finishedAt;

                    isChunkInPageCache = pageCacheMetrics.isRangeInCache(chunkStart, chunkEnd);
                }


                out.write(file.readByte());
            }

            if (isChunkInPageCache)
                out.keepCacheWindow(out.getCurrentPosition() - (file.getFilePointer() - chunkStart));
        }
    }

    public ColumnFamily getColumnFamilyWithColumns() throws IOException
    {
        file.seek(columnPosition - 4); // seek to before column count int
        ColumnFamily cf = columnFamily.cloneMeShallow();

        hasColumnsInPageCache = ColumnFamily.serializer().deserializeColumns(file, cf, pageCacheMetrics);

        return cf;
    }

    public int compareTo(SSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }

    public void reset()
    {
        try
        {
            file.seek(columnPosition);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public boolean hasRowsInPageCache()
    {
        return hasRowsInPageCache;
    }

    public boolean hasColumnsInPageCache()
    {
        return hasColumnsInPageCache;
    }

}
