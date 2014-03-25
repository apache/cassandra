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
package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

public class MeteredFlusher implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(MeteredFlusher.class);

    public void run()
    {
        long allowedSize = calculateAllowedSize();

        // find how much memory non-active memtables are using
        long flushingSize = calculateFlushingSize();
        if (flushingSize > 0)
            logger.debug("Currently flushing {} bytes of {} max", flushingSize, allowedSize);

        List<ColumnFamilyStore> affectedCFs = affectedColumnFamilies();
        long liveSize = 0;

        // flush CFs using more than 1 / (maximum number of memtables it could have in the pipeline)
        // of the total size allotted. Then, flush other CFs in order of size if necessary.
        for (ColumnFamilyStore cfs : affectedCFs)
        {
            int maxInFlight = (int) Math.ceil((double) (1 // live memtable
                                                        + 1 // potentially a flushed memtable being counted by jamm
                                                        + DatabaseDescriptor.getFlushWriters()
                                                        + DatabaseDescriptor.getFlushQueueSize())
                                              / (1 + cfs.indexManager.getIndexesBackedByCfs().size()));
            long size = cfs.getTotalMemtableLiveSize();
            if (allowedSize > flushingSize && size > (allowedSize - flushingSize) / maxInFlight)
            {
                logger.info("flushing high-traffic column family {} (estimated {} bytes)", cfs, size);
                cfs.forceFlush();
            }
            else
            {
                liveSize += size;
            }
        }

        if (liveSize + flushingSize <= allowedSize)
            return;
        logger.info("estimated {} live and {} flushing bytes used by all memtables", liveSize, flushingSize);

        Collections.sort(affectedCFs, new Comparator<ColumnFamilyStore>()
        {
            public int compare(ColumnFamilyStore lhs, ColumnFamilyStore rhs)
            {
                return Long.compare(lhs.getTotalMemtableLiveSize(), rhs.getTotalMemtableLiveSize());
            }
        });

        // flush largest first until we get below our threshold.
        // although it looks like liveSize + flushingSize will stay a constant, it will not if flushes finish
        // while we loop, which is especially likely to happen if the flush queue fills up (so further forceFlush calls block)
        while (!affectedCFs.isEmpty())
        {
            flushingSize = calculateFlushingSize();
            if (liveSize + flushingSize <= allowedSize)
                break;

            ColumnFamilyStore cfs = affectedCFs.remove(affectedCFs.size() - 1);
            long size = cfs.getTotalMemtableLiveSize();
            if (size > 0)
            {
                logger.info("flushing {} to free up {} bytes", cfs, size);
                liveSize -= size;
                cfs.forceFlush();
            }
        }

        logger.trace("memtable memory usage is {} bytes with {} live", liveSize + flushingSize, liveSize);
    }

    private static List<ColumnFamilyStore> affectedColumnFamilies()
    {
        List<ColumnFamilyStore> affected = new ArrayList<>();
        // filter out column families that aren't affected by MeteredFlusher
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            if (cfs.getCompactionStrategy().isAffectedByMeteredFlusher())
                affected.add(cfs);
        return affected;
    }

    private static long calculateAllowedSize()
    {
        long allowed = DatabaseDescriptor.getTotalMemtableSpaceInMB() * 1048576L;
        // deduct the combined memory limit of the tables unaffected by the metered flusher (we don't flush them, we
        // should not count their limits to the total limit either).
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            if (!cfs.getCompactionStrategy().isAffectedByMeteredFlusher())
                allowed -= cfs.getCompactionStrategy().getMemtableReservedSize();
        return allowed;
    }

    private static long calculateFlushingSize()
    {
        ColumnFamilyStore measuredCFS = Memtable.activelyMeasuring;
        long flushing = measuredCFS != null && measuredCFS.getCompactionStrategy().isAffectedByMeteredFlusher()
                      ? measuredCFS.getMemtableThreadSafe().getLiveSize()
                      : 0;
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            if (cfs.getCompactionStrategy().isAffectedByMeteredFlusher())
                for (Memtable memtable : cfs.getMemtablesPendingFlush())
                    flushing += memtable.getLiveSize();
        return flushing;
    }
}
