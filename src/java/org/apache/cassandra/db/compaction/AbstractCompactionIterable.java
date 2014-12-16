/*
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
package org.apache.cassandra.db.compaction;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.utils.CloseableIterator;

public abstract class AbstractCompactionIterable extends CompactionInfo.Holder implements Iterable<AbstractCompactedRow>
{
    protected final OperationType type;
    protected final CompactionController controller;
    protected final long totalBytes;
    protected volatile long bytesRead;
    protected final List<ISSTableScanner> scanners;
    /*
     * counters for merged rows.
     * array index represents (number of merged rows - 1), so index 0 is counter for no merge (1 row),
     * index 1 is counter for 2 rows merged, and so on.
     */
    protected final AtomicLong[] mergeCounters;

    public AbstractCompactionIterable(CompactionController controller, OperationType type, List<ISSTableScanner> scanners)
    {
        this.controller = controller;
        this.type = type;
        this.scanners = scanners;
        this.bytesRead = 0;

        long bytes = 0;
        for (ISSTableScanner scanner : scanners)
            bytes += scanner.getLengthInBytes();
        this.totalBytes = bytes;
        mergeCounters = new AtomicLong[scanners.size()];
        for (int i = 0; i < mergeCounters.length; i++)
            mergeCounters[i] = new AtomicLong();
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(controller.cfs.metadata,
                                  type,
                                  bytesRead,
                                  totalBytes);
    }

    protected void updateCounterFor(int rows)
    {
        assert rows > 0 && rows - 1 < mergeCounters.length;
        mergeCounters[rows - 1].incrementAndGet();
    }

    public long[] getMergedRowCounts()
    {
        long[] counters = new long[mergeCounters.length];
        for (int i = 0; i < counters.length; i++)
            counters[i] = mergeCounters[i].get();
        return counters;
    }

    public abstract CloseableIterator<AbstractCompactedRow> iterator();
}
