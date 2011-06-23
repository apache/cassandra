package org.apache.cassandra.utils;
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


import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.net.MessagingService;

public class StatusLogger
{
    private static final Logger logger = LoggerFactory.getLogger(StatusLogger.class);

    public static void log()
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        
        // everything from o.a.c.concurrent
        logger.info(String.format("%-25s%10s%10s%10s", "Pool Name", "Active", "Pending", "Blocked"));
        Set<ObjectName> request, internal;
        try
        {
            request = server.queryNames(new ObjectName("org.apache.cassandra.request:type=*"), null);
            internal = server.queryNames(new ObjectName("org.apache.cassandra.internal:type=*"), null);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
        for (ObjectName objectName : Iterables.concat(request, internal))
        {
            String poolName = objectName.getKeyProperty("type");
            JMXEnabledThreadPoolExecutorMBean threadPoolProxy = JMX.newMBeanProxy(server, objectName, JMXEnabledThreadPoolExecutorMBean.class);
            logger.info(String.format("%-25s%10s%10s%10s",
                                      poolName, threadPoolProxy.getActiveCount(), threadPoolProxy.getPendingTasks(), threadPoolProxy.getCurrentlyBlockedTasks()));
        }
        // one offs
        logger.info(String.format("%-25s%10s%10s",
                                  "CompactionManager", "n/a", CompactionManager.instance.getPendingTasks()));
        int pendingCommands = 0;
        for (int n : MessagingService.instance().getCommandPendingTasks().values())
        {
            pendingCommands += n;
        }
        int pendingResponses = 0;
        for (int n : MessagingService.instance().getResponsePendingTasks().values())
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
}
