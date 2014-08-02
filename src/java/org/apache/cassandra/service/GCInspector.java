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
package org.apache.cassandra.service;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.management.GarbageCollectionNotificationInfo;
import org.apache.cassandra.io.sstable.SSTableDeletingTask;
import org.apache.cassandra.utils.StatusLogger;

public class GCInspector implements NotificationListener
{
    private static final Logger logger = LoggerFactory.getLogger(GCInspector.class);
    final static long MIN_DURATION = 200;
    final static long MIN_DURATION_TPSTATS = 1000;

    public static void register() throws Exception
    {
        GCInspector inspector = new GCInspector();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName gcName = new ObjectName(ManagementFactory.GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE + ",*");
        for (ObjectName name : server.queryNames(gcName, null))
        {
            server.addNotificationListener(name, inspector, null, null);
        }
    }

    public void handleNotification(Notification notification, Object handback)
    {
        String type = notification.getType();
        if (type.equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION))
        {
            // retrieve the garbage collection notification information
            CompositeData cd = (CompositeData) notification.getUserData();
            GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(cd);

            long duration = info.getGcInfo().getDuration();

            StringBuilder sb = new StringBuilder();
            sb.append(info.getGcName()).append(" GC in ").append(duration).append("ms.  ");

            List<String> keys = new ArrayList<>(info.getGcInfo().getMemoryUsageBeforeGc().keySet());
            Collections.sort(keys);
            for (String key : keys)
            {
                MemoryUsage before = info.getGcInfo().getMemoryUsageBeforeGc().get(key);
                MemoryUsage after = info.getGcInfo().getMemoryUsageAfterGc().get(key);
                if (after != null && after.getUsed() != before.getUsed())
                {
                    sb.append(key).append(": ").append(before.getUsed());
                    sb.append(" -> ");
                    sb.append(after.getUsed());
                    if (!key.equals(keys.get(keys.size() - 1)))
                        sb.append("; ");
                }
            }

            String st = sb.toString();
            if (duration > MIN_DURATION)
                logger.info(st);
            else if (logger.isDebugEnabled())
                logger.debug(st);

            if (duration > MIN_DURATION_TPSTATS)
                StatusLogger.log();

            // if we just finished a full collection and we're still using a lot of memory, try to reduce the pressure
            if (info.getGcName().equals("ConcurrentMarkSweep"))
                SSTableDeletingTask.rescheduleFailedTasks();
        }
    }
}
