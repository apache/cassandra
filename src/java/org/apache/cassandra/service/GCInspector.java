package org.apache.cassandra.service;
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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.SSTableDeletingTask;
import org.apache.cassandra.utils.StatusLogger;

public class GCInspector
{
    private static final Logger logger = LoggerFactory.getLogger(GCInspector.class);
    final static long INTERVAL_IN_MS = 1000;
    final static long MIN_DURATION = 200;
    final static long MIN_DURATION_TPSTATS = 1000;
    
    public static final GCInspector instance = new GCInspector();

    private HashMap<String, Long> gctimes = new HashMap<String, Long>();
    private HashMap<String, Long> gccounts = new HashMap<String, Long>();

    List<GarbageCollectorMXBean> beans = new ArrayList<GarbageCollectorMXBean>();
    MemoryMXBean membean = ManagementFactory.getMemoryMXBean();

    private volatile boolean cacheSizesReduced;

    public GCInspector()
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try
        {
            ObjectName gcName = new ObjectName(ManagementFactory.GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE + ",*");
            for (ObjectName name : server.queryNames(gcName, null))
            {
                GarbageCollectorMXBean gc = ManagementFactory.newPlatformMXBeanProxy(server, name.getCanonicalName(), GarbageCollectorMXBean.class);
                beans.add(gc);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void start()
    {
        // don't bother starting a thread that will do nothing.
        if (beans.size() == 0)
            return;         
        Runnable t = new Runnable()
        {
            public void run()
            {
                logGCResults();
            }
        };
        StorageService.scheduledTasks.scheduleWithFixedDelay(t, INTERVAL_IN_MS, INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
    }

    private void logGCResults()
    {
        for (GarbageCollectorMXBean gc : beans)
        {
            Long previousTotal = gctimes.get(gc.getName());
            Long total = gc.getCollectionTime();
            if (previousTotal == null)
                previousTotal = 0L;
            if (previousTotal.equals(total))
                continue;
            gctimes.put(gc.getName(), total);
            Long duration = total - previousTotal;
            assert duration > 0;

            Long previousCount = gccounts.get(gc.getName());
            Long count = gc.getCollectionCount();
            
            if (previousCount == null)
                previousCount = 0L;           
            if (count.equals(previousCount))
                continue;
            
            gccounts.put(gc.getName(), count);

            MemoryUsage mu = membean.getHeapMemoryUsage();
            long memoryUsed = mu.getUsed();
            long memoryMax = mu.getMax();

            String st = String.format("GC for %s: %s ms for %s collections, %s used; max is %s",
                                      gc.getName(), duration, count - previousCount, memoryUsed, memoryMax);
            long durationPerCollection = duration / (count - previousCount);
            if (durationPerCollection > MIN_DURATION)
                logger.info(st);
            else if (logger.isDebugEnabled())
                logger.debug(st);

            if (durationPerCollection > MIN_DURATION_TPSTATS)
                StatusLogger.log();

            // if we just finished a full collection and we're still using a lot of memory, try to reduce the pressure
            if (gc.getName().equals("ConcurrentMarkSweep"))
            {
                SSTableDeletingTask.rescheduleFailedTasks();

                double usage = (double) memoryUsed / memoryMax;

                if (memoryUsed > DatabaseDescriptor.getReduceCacheSizesAt() * memoryMax && !cacheSizesReduced)
                {
                    cacheSizesReduced = true;
                    logger.warn("Heap is " + usage + " full.  You may need to reduce memtable and/or cache sizes.  Cassandra is now reducing cache sizes to free up memory.  Adjust reduce_cache_sizes_at threshold in cassandra.yaml if you don't want Cassandra to do this automatically");
                    StorageService.instance.reduceCacheSizes();
                }

                if (memoryUsed > DatabaseDescriptor.getFlushLargestMemtablesAt() * memoryMax)
                {
                    logger.warn("Heap is " + usage + " full.  You may need to reduce memtable and/or cache sizes.  Cassandra will now flush up to the two largest memtables to free up memory.  Adjust flush_largest_memtables_at threshold in cassandra.yaml if you don't want Cassandra to do this automatically");
                    StorageService.instance.flushLargestMemtables();
                }
            }
        }
    }
}
