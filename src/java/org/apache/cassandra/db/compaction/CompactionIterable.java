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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Throttle;

public class CompactionIterable implements Iterable<AbstractCompactedRow>, CompactionInfo.Holder
{
    private static Logger logger = LoggerFactory.getLogger(CompactionIterable.class);

    public static final int FILE_BUFFER_SIZE = 1024 * 1024;

    private MergeIterator<IColumnIterator, AbstractCompactedRow> source;
    protected final CompactionType type;
    private final List<SSTableScanner> scanners;
    protected final CompactionController controller;
    private final Throttle throttle;

    private long totalBytes;
    private long bytesRead;
    private long row;

    public CompactionIterable(CompactionType type, Iterable<SSTableReader> sstables, CompactionController controller) throws IOException
    {
        this(type, getScanners(sstables), controller);
    }

    protected CompactionIterable(CompactionType type, List<SSTableScanner> scanners, CompactionController controller)
    {
        this.type = type;
        this.scanners = scanners;
        this.controller = controller;
        row = 0;
        totalBytes = bytesRead = 0;
        for (SSTableScanner scanner : scanners)
            totalBytes += scanner.getFileLength();
        this.throttle = new Throttle(toString(), new Throttle.ThroughputFunction()
        {
            /** @return Instantaneous throughput target in bytes per millisecond. */
            public int targetThroughput()
            {
                if (DatabaseDescriptor.getCompactionThroughputMbPerSec() < 1 || StorageService.instance.isBootstrapMode())
                    // throttling disabled
                    return 0;
                // total throughput
                int totalBytesPerMS = DatabaseDescriptor.getCompactionThroughputMbPerSec() * 1024 * 1024 / 1000;
                // per stream throughput (target bytes per MS)
                return totalBytesPerMS / Math.max(1, CompactionManager.instance.getActiveCompactions());
            }
        });
    }

    protected static List<SSTableScanner> getScanners(Iterable<SSTableReader> sstables) throws IOException
    {
        ArrayList<SSTableScanner> scanners = new ArrayList<SSTableScanner>();
        for (SSTableReader sstable : sstables)
            scanners.add(sstable.getDirectScanner(FILE_BUFFER_SIZE));
        return scanners;
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(this.hashCode(),
                                  controller.getKeyspace(),
                                  controller.getColumnFamily(),
                                  type,
                                  bytesRead,
                                  totalBytes);
    }

    public CloseableIterator<AbstractCompactedRow> iterator()
    {
        return MergeIterator.get(scanners, ICOMP, new Reducer());
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
            rows.add((SSTableIdentityIterator)current);
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
                    for (SSTableScanner scanner : scanners)
                    {
                        bytesRead += scanner.getFilePointer();
                    }
                    throttle.throttle(bytesRead);
                }
            }
        }
    }

    public final static Comparator<IColumnIterator> ICOMP = new Comparator<IColumnIterator>()
    {
        public int compare(IColumnIterator i1, IColumnIterator i2)
        {
            return i1.getKey().compareTo(i2.getKey());
        }
    };
}
