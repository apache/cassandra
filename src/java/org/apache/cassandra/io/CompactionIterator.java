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


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.iterators.CollatingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ReducingIterator;

public class CompactionIterator extends ReducingIterator<SSTableIdentityIterator, AbstractCompactedRow>
implements Closeable, ICompactionInfo
{
    private static Logger logger = LoggerFactory.getLogger(CompactionIterator.class);

    public static final int FILE_BUFFER_SIZE = 1024 * 1024;

    protected final List<SSTableIdentityIterator> rows = new ArrayList<SSTableIdentityIterator>();
    private final ColumnFamilyStore cfs;
    private final int gcBefore;
    private final boolean major;

    private long totalBytes;
    private long bytesRead;
    private long row;

    public CompactionIterator(ColumnFamilyStore cfs, Iterable<SSTableReader> sstables, int gcBefore, boolean major) throws IOException
    {
        this(cfs, getCollatingIterator(sstables), gcBefore, major);
    }

    @SuppressWarnings("unchecked")
    protected CompactionIterator(ColumnFamilyStore cfs, Iterator iter, int gcBefore, boolean major)
    {
        super(iter);
        row = 0;
        totalBytes = bytesRead = 0;
        for (SSTableScanner scanner : getScanners())
        {
            totalBytes += scanner.getFileLength();
        }
        this.cfs = cfs;
        this.gcBefore = gcBefore;
        this.major = major;
    }

    @SuppressWarnings("unchecked")
    protected static CollatingIterator getCollatingIterator(Iterable<SSTableReader> sstables) throws IOException
    {
        // TODO CollatingIterator iter = FBUtilities.<SSTableIdentityIterator>getCollatingIterator();
        CollatingIterator iter = FBUtilities.getCollatingIterator();
        for (SSTableReader sstable : sstables)
        {
            iter.addIterator(sstable.getDirectScanner(FILE_BUFFER_SIZE));
        }
        return iter;
    }

    @Override
    protected boolean isEqual(SSTableIdentityIterator o1, SSTableIdentityIterator o2)
    {
        return o1.getKey().equals(o2.getKey());
    }

    public void reduce(SSTableIdentityIterator current)
    {
        rows.add(current);
    }

    protected AbstractCompactedRow getReduced()
    {
        assert rows.size() > 0;

        try
        {
            AbstractCompactedRow compactedRow = getCompactedRow();
            if (compactedRow.isEmpty())
            {
                cfs.invalidateCachedRow(compactedRow.key);
                return null;
            }
            else
            {
                return compactedRow;
            }
        }
        finally
        {
            rows.clear();
            if ((row++ % 1000) == 0)
            {
                bytesRead = 0;
                for (SSTableScanner scanner : getScanners())
                {
                    bytesRead += scanner.getFilePointer();
                }
            }
        }
    }

    protected AbstractCompactedRow getCompactedRow()
    {
        long rowSize = 0;
        for (SSTableIdentityIterator row : rows)
        {
            rowSize += row.dataSize;
        }

        if (rowSize > DatabaseDescriptor.getInMemoryCompactionLimit())
        {
            logger.info(String.format("Compacting large row %s (%d bytes) incrementally",
                                      ByteBufferUtil.bytesToHex(rows.get(0).getKey().key), rowSize));
            return new LazilyCompactedRow(cfs, rows, major, gcBefore, false);
        }
        return new PrecompactedRow(cfs, rows, major, gcBefore, false);
    }

    public void close() throws IOException
    {
        for (SSTableScanner scanner : getScanners())
        {
            scanner.close();
        }
    }

    protected Iterable<SSTableScanner> getScanners()
    {
        return ((CollatingIterator)source).getIterators();
    }

    public long getTotalBytes()
    {
        return totalBytes;
    }

    public long getBytesComplete()
    {
        return bytesRead;
    }

    public String getTaskType()
    {
        return major ? "Major" : "Minor";
    }
}
