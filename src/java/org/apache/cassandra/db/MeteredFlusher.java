package org.apache.cassandra.db;
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


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

class MeteredFlusher implements Runnable
{
    private static Logger logger = LoggerFactory.getLogger(MeteredFlusher.class);

    public void run()
    {
        // first, find how much memory non-active memtables are using
        Memtable activelyMeasuring = Memtable.activelyMeasuring;
        long flushingBytes = activelyMeasuring == null ? 0 : activelyMeasuring.getLiveSize();
        flushingBytes += countFlushingBytes();

        // next, flush CFs using more than 1 / (maximum number of memtables it could have in the pipeline)
        // of the total size allotted.  Then, flush other CFs in order of size if necessary.
        long liveBytes = 0;
        try
        {
            for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            {
                long size = cfs.getTotalMemtableLiveSize();
                int maxInFlight = (int) Math.ceil((double) (1 // live memtable
                                                            + 1 // potentially a flushed memtable being counted by jamm
                                                            + DatabaseDescriptor.getFlushWriters()
                                                            + DatabaseDescriptor.getFlushQueueSize())
                                                  / (1 + cfs.indexManager.getIndexedColumns().size()));
                if (size > (DatabaseDescriptor.getTotalMemtableSpaceInMB() * 1048576L - flushingBytes) / maxInFlight)
                {
                    logger.info("flushing high-traffic column family {} (estimated {} bytes)", cfs, size);
                    cfs.forceFlush();
                }
                else
                {
                    liveBytes += size;
                }
            }

            if (flushingBytes + liveBytes <= DatabaseDescriptor.getTotalMemtableSpaceInMB() * 1048576L)
                return;

            logger.info("estimated {} bytes used by all memtables pre-flush", liveBytes);

            // sort memtables by size
            List<ColumnFamilyStore> sorted = new ArrayList<ColumnFamilyStore>();
            Iterables.addAll(sorted, ColumnFamilyStore.all());
            Collections.sort(sorted, new Comparator<ColumnFamilyStore>()
            {
                public int compare(ColumnFamilyStore o1, ColumnFamilyStore o2)
                {
                    long size1 = o1.getTotalMemtableLiveSize();
                    long size2 = o2.getTotalMemtableLiveSize();
                    if (size1 < size2)
                        return -1;
                    if (size1 > size2)
                        return 1;
                    return 0;
                }
            });

            // flush largest first until we get below our threshold.
            // although it looks like liveBytes + flushingBytes will stay a constant, it will not if flushes finish
            // while we loop, which is especially likely to happen if the flush queue fills up (so further forceFlush calls block)
            while (true)
            {
                flushingBytes = countFlushingBytes();
                if (liveBytes + flushingBytes <= DatabaseDescriptor.getTotalMemtableSpaceInMB() * 1048576L || sorted.isEmpty())
                    break;

                ColumnFamilyStore cfs = sorted.remove(sorted.size() - 1);
                long size = cfs.getTotalMemtableLiveSize();
                logger.info("flushing {} to free up {} bytes", cfs, size);
                liveBytes -= size;
                cfs.forceFlush();
            }
        }
        finally
        {
            logger.trace("memtable memory usage is {} bytes with {} live", liveBytes + flushingBytes, liveBytes);
        }
    }

    private long countFlushingBytes()
    {
        long flushingBytes = 0;
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            for (Memtable memtable : cfs.getMemtablesPendingFlush())
                flushingBytes += memtable.getLiveSize();
        }
        return flushingBytes;
    }
}
