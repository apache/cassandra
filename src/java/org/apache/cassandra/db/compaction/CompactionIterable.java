package org.apache.cassandra.db.compaction;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;

public class CompactionIterable extends AbstractCompactionIterable
{
    private static Logger logger = LoggerFactory.getLogger(CompactionIterable.class);

    private long row;
    private final List<SSTableScanner> scanners;

    private static final Comparator<IColumnIterator> comparator = new Comparator<IColumnIterator>()
    {
        public int compare(IColumnIterator i1, IColumnIterator i2)
        {
            return i1.getKey().compareTo(i2.getKey());
        }
    };

    public CompactionIterable(OperationType type, Iterable<SSTableReader> sstables, CompactionController controller) throws IOException
    {
        this(type, getScanners(sstables), controller);
    }

    protected CompactionIterable(OperationType type, List<SSTableScanner> scanners, CompactionController controller)
    {
        super(controller, type);
        this.scanners = scanners;
        row = 0;
        totalBytes = bytesRead = 0;
        for (SSTableScanner scanner : scanners)
            totalBytes += scanner.getFileLength();
    }

    protected static List<SSTableScanner> getScanners(Iterable<SSTableReader> sstables) throws IOException
    {
        ArrayList<SSTableScanner> scanners = new ArrayList<SSTableScanner>();
        for (SSTableReader sstable : sstables)
            scanners.add(sstable.getDirectScanner());
        return scanners;
    }

    public CloseableIterator<AbstractCompactedRow> iterator()
    {
        return MergeIterator.get(scanners, comparator, new Reducer());
    }

    public String toString()
    {
        return this.getCompactionInfo().toString();
    }

    protected class Reducer extends MergeIterator.Reducer<IColumnIterator, AbstractCompactedRow>
    {
        protected final List<SSTableIdentityIterator> rows = new ArrayList<SSTableIdentityIterator>();

        public void reduce(IColumnIterator current)
        {
            rows.add((SSTableIdentityIterator) current);
        }

        protected AbstractCompactedRow getReduced()
        {
            assert rows.size() > 0;

            try
            {
                AbstractCompactedRow compactedRow = controller.getCompactedRow(new ArrayList<SSTableIdentityIterator>(rows));
                if (compactedRow.isEmpty())
                {
                    controller.invalidateCachedRow(compactedRow.key);
                    return null;
                }
                else
                {
                    // If the raw is cached, we call removeDeleted on it to have/ coherent query returns. However it would look
                    // like some deleted columns lived longer than gc_grace + compaction. This can also free up big amount of
                    // memory on long running instances
                    controller.removeDeletedInCache(compactedRow.key);
                }

                return compactedRow;
            }
            finally
            {
                rows.clear();
                if ((row++ % 1000) == 0)
                {
                    long n = 0;
                    for (SSTableScanner scanner : scanners)
                        n += scanner.getFilePointer();
                    bytesRead = n;
                    throttle.throttle(bytesRead);
                }
            }
        }
    }
}
