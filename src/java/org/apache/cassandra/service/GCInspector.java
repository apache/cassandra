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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import org.apache.cassandra.concurrent.IExecutorMBean;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.net.MessagingService;

import java.lang.management.MemoryUsage;
import java.lang.management.ManagementFactory;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.collect.Iterables;

public class GCInspector
{
    public static final GCInspector instance = new GCInspector();

    private static final Logger logger = LoggerFactory.getLogger(GCInspector.class);
    final static long INTERVAL_IN_MS = 1000;
    final static long MIN_DURATION = 200;
    final static long MIN_DURATION_TPSTATS = 1000;

    private HashMap<String, Long> gctimes = new HashMap<String, Long>();
    private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

    List<Object> beans = new ArrayList<Object>(); // these are instances of com.sun.management.GarbageCollectorMXBean

    public GCInspector()
    {
        // we only want this class to do its thing on sun jdks, or when the sun classes are present.
        Class gcBeanClass = null;
        try
        {
            gcBeanClass = Class.forName("com.sun.management.GarbageCollectorMXBean");
            Class.forName("com.sun.management.GcInfo");
        }
        catch (ClassNotFoundException ex)
        {
            // this happens when using a non-sun jdk.
            logger.warn("Cannot load sun GC monitoring classes. GCInspector is disabled.");
        }
        
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try
        {
            ObjectName gcName = new ObjectName(ManagementFactory.GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE + ",*");
            for (ObjectName name : server.queryNames(gcName, null))
            {
                Object gc = ManagementFactory.newPlatformMXBeanProxy(server, name.getCanonicalName(), gcBeanClass);
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
        TimerTask t = new TimerTask()
        {
            public void run()
            {
                logIntervalGCStats();
            }
        };
        new Timer("GC inspection").schedule(t, INTERVAL_IN_MS, INTERVAL_IN_MS);
    }

    private void logIntervalGCStats()
    {
        for (Object gc : beans)
        {
            SunGcWrapper gcw = new SunGcWrapper(gc);
            if (gcw.isLastGcInfoNull())
                continue;

            Long previous = gctimes.get(gcw.getName());
            if (previous != null && previous.longValue() == gcw.getCollectionTime().longValue())            
                continue;
            gctimes.put(gcw.getName(), gcw.getCollectionTime());

            long previousMemoryUsed = 0;
            long memoryUsed = 0;
            long memoryMax = 0;
            for (Map.Entry<String, MemoryUsage> entry : gcw.getMemoryUsageBeforeGc().entrySet())
            {
                previousMemoryUsed += entry.getValue().getUsed();
            }
            for (Map.Entry<String, MemoryUsage> entry : gcw.getMemoryUsageAfterGc().entrySet())
            {
                MemoryUsage mu = entry.getValue();
                memoryUsed += mu.getUsed();
                memoryMax += mu.getMax();
            }

            String st = String.format("GC for %s: %s ms, %s reclaimed leaving %s used; max is %s",
                    gcw.getName(), gcw.getDuration(), previousMemoryUsed - memoryUsed, memoryUsed, memoryMax);
            if (gcw.getDuration() > MIN_DURATION)                          
                logger.info(st);
            else if (logger.isDebugEnabled())
                logger.debug(st);
            if (gcw.getDuration() > MIN_DURATION_TPSTATS)
            {
                try
                {
                    logStats();
                }
                catch (MalformedObjectNameException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void logStats() throws MalformedObjectNameException
    {
        // everything from o.a.c.concurrent
        logger.info(String.format("%-25s%10s%10s", "Pool Name", "Active", "Pending"));
        Set<ObjectName> requests = server.queryNames(new ObjectName("org.apache.cassandra.request:type=*"), null);
        Set<ObjectName> internal = server.queryNames(new ObjectName("org.apache.cassandra.internal:type=*"), null);
        for (ObjectName objectName : Iterables.concat(requests, internal))
        {
            String poolName = objectName.getKeyProperty("type");
            IExecutorMBean threadPoolProxy = JMX.newMBeanProxy(server, objectName, IExecutorMBean.class);
            logger.info(String.format("%-25s%10s%10s",
                                      poolName, threadPoolProxy.getActiveCount(), threadPoolProxy.getPendingTasks()));
        }
        // one offs
        logger.info(String.format("%-25s%10s%10s",
                                  "CompactionManager", "n/a", CompactionManager.instance.getPendingTasks()));
        int pendingCommands = 0;
        for (int n : MessagingService.instance.getCommandPendingTasks().values())
        {
            pendingCommands += n;
        }
        int pendingResponses = 0;
        for (int n : MessagingService.instance.getResponsePendingTasks().values())
        {
            pendingResponses += n;
        }
        logger.info(String.format("%-25s%10s%10s",
                                  "MessagingService", "n/a", pendingCommands + "," + pendingResponses));

        // per-CF stats
        logger.info(String.format("%-25s%20s%20s%20s", "ColumnFamily", "Memtable ops,data", "Row cache size/cap", "Key cache size/cap"));
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            logger.info(String.format("%-25s%20s%20s%20s",
                                      cfs.table.name + "." + cfs.columnFamily,
                                      cfs.getMemtableColumnsCount() + "," + cfs.getMemtableDataSize(),
                                      cfs.getRowCacheSize() + "/" + cfs.getRowCacheCapacity(),
                                      cfs.getKeyCacheSize() + "/" + cfs.getKeyCacheCapacity()));
        }
    }
    
    
    // wrapper for sun class. this enables other jdks to compile this class.
    private final class SunGcWrapper
    {
        
        private Map<String, MemoryUsage> usageBeforeGc = null;
        private Map<String, MemoryUsage> usageAfterGc = null;
        private String name;
        private Long collectionTime;
        private Long duration;
        
        SunGcWrapper(Object gcMxBean)
        {
            // if we've gotten this far, we've already verified that the right classes are in the CP. Now we just
            // need to check for boneheadedness.
            // grab everything we need here so that we don't have to deal with try/catch everywhere.
            try
            {
                assert Class.forName("com.sun.management.GarbageCollectorMXBean").isAssignableFrom(gcMxBean.getClass());
                Method getGcInfo = gcMxBean.getClass().getDeclaredMethod("getLastGcInfo");
                Object lastGcInfo = getGcInfo.invoke(gcMxBean);
                if (lastGcInfo != null)
                {
                    usageBeforeGc = (Map<String, MemoryUsage>)lastGcInfo.getClass().getDeclaredMethod("getMemoryUsageBeforeGc").invoke(lastGcInfo);
                    usageAfterGc = (Map<String, MemoryUsage>)lastGcInfo.getClass().getDeclaredMethod("getMemoryUsageAfterGc").invoke(lastGcInfo);
                    duration = (Long)lastGcInfo.getClass().getDeclaredMethod("getDuration").invoke(lastGcInfo);
                    name = (String)gcMxBean.getClass().getDeclaredMethod("getName").invoke(gcMxBean);
                    collectionTime = (Long)gcMxBean.getClass().getDeclaredMethod("getCollectionTime").invoke(gcMxBean);
                }
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }
            catch (NoSuchMethodException e)
            {
                throw new RuntimeException(e);
            }
            catch (IllegalAccessException e)
            {
                throw new RuntimeException(e);
            }
            catch (InvocationTargetException e)
            {
                throw new RuntimeException(e);
            }
        }
        
        String getName()
        {
            return name;
        }
        
        Long getCollectionTime()
        {
            return collectionTime;
        }
        
        Long getDuration()
        {
            return duration;
        }
        
        Map<String, MemoryUsage> getMemoryUsageAfterGc()
        {
            return usageAfterGc;
        }
        
        Map<String, MemoryUsage> getMemoryUsageBeforeGc()
        {
            return usageBeforeGc;
        }
        
        boolean isLastGcInfoNull()
        {
            return usageBeforeGc == null;
        }
    }
    
}
