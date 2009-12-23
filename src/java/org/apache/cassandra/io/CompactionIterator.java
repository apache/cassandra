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
import java.io.IOError;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.commons.collections.iterators.CollatingIterator;

import org.apache.cassandra.utils.ReducingIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;

public class CompactionIterator extends ReducingIterator<IteratingRow, CompactionIterator.CompactedRow> implements Closeable
{
    private static Logger logger = Logger.getLogger(CompactionIterator.class);

    protected static final int FILE_BUFFER_SIZE = 1024 * 1024;

    private final List<IteratingRow> rows = new ArrayList<IteratingRow>();
    private final int gcBefore;
    private final boolean major;

    private long totalBytes;
    private long bytesRead;
    private long row;

    public CompactionIterator(Iterable<SSTableReader> sstables, int gcBefore, boolean major) throws IOException
    {
<<<<<<< HEAD
        this(getCollatingIterator(sstables), gcBefore, major);
    }

    @SuppressWarnings("unchecked")
    protected CompactionIterator(Iterator iter, int gcBefore, boolean major)
    {
        super(iter);
=======
        super(getCollatingIterator(sstables));
        row = 0;
        totalBytes = bytesRead = 0;
        for (SSTableScanner iter : (List<SSTableScanner>)((CollatingIterator)source).getIterators())
        {
            totalBytes += iter.getFileLength();
        }
>>>>>>> make estimation of pendingtasks for CompactionManager sane
        this.gcBefore = gcBefore;
        this.major = major;
    }

    @SuppressWarnings("unchecked")
    protected static CollatingIterator getCollatingIterator(Iterable<SSTableReader> sstables) throws IOException
    {
        CollatingIterator iter = FBUtilities.<IteratingRow>getCollatingIterator();
        for (SSTableReader sstable : sstables)
        {
            iter.addIterator(sstable.getScanner(FILE_BUFFER_SIZE));
        }
        return iter;
    }

    @Override
    protected boolean isEqual(IteratingRow o1, IteratingRow o2)
    {
        return o1.getKey().equals(o2.getKey());
    }

    public void reduce(IteratingRow current)
    {
        rows.add(current);
    }

    protected CompactedRow getReduced()
    {
        assert rows.size() > 0;
        DataOutputBuffer buffer = new DataOutputBuffer();
        DecoratedKey key = rows.get(0).getKey();

        try
        {
            if (rows.size() > 1 || major)
            {
                ColumnFamily cf = null;
                for (IteratingRow row : rows)
                {
                    ColumnFamily thisCF;
                    try
                    {
                        thisCF = row.getColumnFamily();
                    }
                    catch (IOException e)
                    {
                        logger.error("Skipping row " + key + " in " + row.getPath(), e);
                        continue;
                    }
                    if (cf == null)
                    {
                        cf = thisCF;
                    }
                    else
                    {
                        cf.addAll(thisCF);
                    }
                }
                ColumnFamily cfPurged = major ? ColumnFamilyStore.removeDeleted(cf, gcBefore) : cf;
                if (cfPurged == null)
                    return null;
                ColumnFamily.serializer().serializeWithIndexes(cfPurged, buffer);
            }
            else
            {
                assert rows.size() == 1;
                try
                {
                    rows.get(0).echoData(buffer);
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        }
        finally
        {
            rows.clear();
            if ((row++ % 1000) == 0)
            {
                bytesRead = 0;
                for (SSTableScanner iter : (List<SSTableScanner>)((CollatingIterator)source).getIterators())
                {
                    bytesRead += iter.getFilePointer();
                }
            }
        }
        return new CompactedRow(key, buffer);
    }

    public void close() throws IOException
    {
        for (Object o : ((CollatingIterator)source).getIterators())
        {
            ((SSTableScanner)o).close();
        }
    }

    public long getTotalBytes()
    {
        return totalBytes;
    }

    public long getBytesRead()
    {
        return bytesRead;
    }

    public static class CompactedRow
    {
        public final DecoratedKey key;
        public final DataOutputBuffer buffer;

        public CompactedRow(DecoratedKey key, DataOutputBuffer buffer)
        {
            this.key = key;
            this.buffer = buffer;
        }
    }
}
