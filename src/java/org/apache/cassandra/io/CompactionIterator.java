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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.iterators.CollatingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ReducingIterator;

public class CompactionIterator extends ReducingIterator<SSTableIdentityIterator, AbstractCompactedRow>
implements Closeable, CompactionInfo.Holder
{
    private static Logger logger = LoggerFactory.getLogger(CompactionIterator.class);

    public static final int FILE_BUFFER_SIZE = 1024 * 1024;

    protected final List<SSTableIdentityIterator> rows = new ArrayList<SSTableIdentityIterator>();
    protected final String type;
    protected final CompactionController controller;

    private long totalBytes;
    private long bytesRead;
    private long row;

    public CompactionIterator(String type, Iterable<SSTableReader> sstables, CompactionController controller) throws IOException
    {
        this(type, getCollatingIterator(sstables), controller);
    }

    @SuppressWarnings("unchecked")
    protected CompactionIterator(String type, Iterator iter, CompactionController controller)
    {
        super(iter);
        this.type = type;
        this.controller = controller;
        row = 0;
        totalBytes = bytesRead = 0;
        for (SSTableScanner scanner : getScanners())
        {
            totalBytes += scanner.getFileLength();
        }
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

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(controller.getKeyspace(),
                                  controller.getColumnFamily(),
                                  type,
                                  bytesRead,
                                  totalBytes);
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
                controller.invalidateCachedRow(compactedRow.key);
                return null;
            }

            // If the raw is cached, we call removeDeleted on it to have/ coherent query returns. However it would look
            // like some deleted columns lived longer than gc_grace + compaction. This can also free up big amount of
            // memory on long running instances
            controller.removeDeletedInCache(compactedRow.key);

            return compactedRow;
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
            return new LazilyCompactedRow(controller, rows);
        }
        return new PrecompactedRow(controller, rows);
    }

    public void close() throws IOException
    {
        FileUtils.close(getScanners());
    }

    protected Iterable<SSTableScanner> getScanners()
    {
        return ((CollatingIterator)source).getIterators();
    }

    public String toString()
    {
        return this.getCompactionInfo().toString();
    }
}
