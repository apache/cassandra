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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.StatusLogger;

public class GCInspector implements NotificationListener, GCInspectorMXBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.service:type=GCInspector";
    private static final Logger logger = LoggerFactory.getLogger(GCInspector.class);
    final static long MIN_LOG_DURATION = DatabaseDescriptor.getGCLogThreshold();
    final static long GC_WARN_THRESHOLD_IN_MS = DatabaseDescriptor.getGCWarnThreshold();
    final static long STAT_THRESHOLD = GC_WARN_THRESHOLD_IN_MS != 0 ? GC_WARN_THRESHOLD_IN_MS : MIN_LOG_DURATION;

    /*
     * The field from java.nio.Bits that tracks the total number of allocated
     * bytes of direct memory requires via ByteBuffer.allocateDirect that have not been GCed.
     */
    final static Field BITS_TOTAL_CAPACITY;

    static
    {
        Field temp = null;
        try
        {
            Class<?> bitsClass = Class.forName("java.nio.Bits");
            Field f = bitsClass.getDeclaredField("totalCapacity");
            f.setAccessible(true);
            temp = f;
        }
        catch (Throwable t)
        {
            logger.debug("Error accessing field of java.nio.Bits", t);
            //Don't care, will just return the dummy value -1 if we can't get at the field in this JVM
        }
        BITS_TOTAL_CAPACITY = temp;
    }

    static final class State
    {
        final double maxRealTimeElapsed;
        final double totalRealTimeElapsed;
        final double sumSquaresRealTimeElapsed;
        final double totalBytesReclaimed;
        final double count;
        final long startNanos;

        State(double extraElapsed, double extraBytes, State prev)
        {
            this.totalRealTimeElapsed = prev.totalRealTimeElapsed + extraElapsed;
            this.totalBytesReclaimed = prev.totalBytesReclaimed + extraBytes;
            this.sumSquaresRealTimeElapsed = prev.sumSquaresRealTimeElapsed + (extraElapsed * extraElapsed);
            this.startNanos = prev.startNanos;
            this.count = prev.count + 1;
            this.maxRealTimeElapsed = Math.max(prev.maxRealTimeElapsed, extraElapsed);
        }

        State()
        {
            count = maxRealTimeElapsed = sumSquaresRealTimeElapsed = totalRealTimeElapsed = totalBytesReclaimed = 0;
            startNanos = System.nanoTime();
        }
    }

    static final class GCState
    {
        final GarbageCollectorMXBean gcBean;
        final boolean assumeGCIsPartiallyConcurrent;
        final boolean assumeGCIsOldGen;
        private String[] keys;
        long lastGcTotalDuration = 0;


        GCState(GarbageCollectorMXBean gcBean, boolean assumeGCIsPartiallyConcurrent, boolean assumeGCIsOldGen)
        {
            this.gcBean = gcBean;
            this.assumeGCIsPartiallyConcurrent = assumeGCIsPartiallyConcurrent;
            this.assumeGCIsOldGen = assumeGCIsOldGen;
        }

        String[] keys(GarbageCollectionNotificationInfo info)
        {
            if (keys != null)
                return keys;

            keys = info.getGcInfo().getMemoryUsageBeforeGc().keySet().toArray(new String[0]);
            Arrays.sort(keys);

            return keys;
        }
    }

    final AtomicReference<State> state = new AtomicReference<>(new State());

    final Map<String, GCState> gcStates = new HashMap<>();

    public GCInspector()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try
        {
            ObjectName gcName = new ObjectName(ManagementFactory.GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE + ",*");
            for (ObjectName name : mbs.queryNames(gcName, null))
            {
                GarbageCollectorMXBean gc = ManagementFactory.newPlatformMXBeanProxy(mbs, name.getCanonicalName(), GarbageCollectorMXBean.class);
                gcStates.put(gc.getName(), new GCState(gc, assumeGCIsPartiallyConcurrent(gc), assumeGCIsOldGen(gc)));
            }

            MBeanWrapper.instance.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

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

    /*
     * Assume that a GC type is at least partially concurrent and so a side channel method
     * should be used to calculate application stopped time due to the GC.
     *
     * If the GC isn't recognized then assume that is concurrent and we need to do our own calculation
     * via the the side channel.
     */
    private static boolean assumeGCIsPartiallyConcurrent(GarbageCollectorMXBean gc)
    {
        switch (gc.getName())
        {
                //First two are from the serial collector
            case "Copy":
            case "MarkSweepCompact":
                //Parallel collector
            case "PS MarkSweep":
            case "PS Scavenge":
            case "G1 Young Generation":
                //CMS young generation collector
            case "ParNew":
                return false;
            case "ConcurrentMarkSweep":
            case "G1 Old Generation":
                return true;
            default:
                //Assume possibly concurrent if unsure
                return true;
        }
    }

    /*
     * Assume that a GC type is an old generation collection so TransactionLogs.rescheduleFailedTasks()
     * should be invoked.
     *
     * Defaults to not invoking TransactionLogs.rescheduleFailedTasks() on unrecognized GC names
     */
    private static boolean assumeGCIsOldGen(GarbageCollectorMXBean gc)
    {
        switch (gc.getName())
        {
            case "Copy":
            case "PS Scavenge":
            case "G1 Young Generation":
            case "ParNew":
                return false;
            case "MarkSweepCompact":
            case "PS MarkSweep":
            case "ConcurrentMarkSweep":
            case "G1 Old Generation":
                return true;
            default:
                //Assume not old gen otherwise, don't call
                //TransactionLogs.rescheduleFailedTasks()
                return false;
        }
    }

    public void handleNotification(final Notification notification, final Object handback)
    {
        String type = notification.getType();
        if (type.equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION))
        {
            // retrieve the garbage collection notification information
            CompositeData cd = (CompositeData) notification.getUserData();
            GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(cd);
            String gcName = info.getGcName();
            GcInfo gcInfo = info.getGcInfo();

            long duration = gcInfo.getDuration();

            /*
             * The duration supplied in the notification info includes more than just
             * application stopped time for concurrent GCs. Try and do a better job coming up with a good stopped time
             * value by asking for and tracking cumulative time spent blocked in GC.
             */
            GCState gcState = gcStates.get(gcName);
            if (gcState.assumeGCIsPartiallyConcurrent)
            {
                long previousTotal = gcState.lastGcTotalDuration;
                long total = gcState.gcBean.getCollectionTime();
                gcState.lastGcTotalDuration = total;
                duration = total - previousTotal; // may be zero for a really fast collection
            }

            StringBuilder sb = new StringBuilder();
            sb.append(info.getGcName()).append(" GC in ").append(duration).append("ms.  ");
            long bytes = 0;
            Map<String, MemoryUsage> beforeMemoryUsage = gcInfo.getMemoryUsageBeforeGc();
            Map<String, MemoryUsage> afterMemoryUsage = gcInfo.getMemoryUsageAfterGc();
            for (String key : gcState.keys(info))
            {
                MemoryUsage before = beforeMemoryUsage.get(key);
                MemoryUsage after = afterMemoryUsage.get(key);
                if (after != null && after.getUsed() != before.getUsed())
                {
                    sb.append(key).append(": ").append(before.getUsed());
                    sb.append(" -> ");
                    sb.append(after.getUsed());
                    if (!key.equals(gcState.keys[gcState.keys.length - 1]))
                        sb.append("; ");
                    bytes += before.getUsed() - after.getUsed();
                }
            }

            while (true)
            {
                State prev = state.get();
                if (state.compareAndSet(prev, new State(duration, bytes, prev)))
                    break;
            }

            String st = sb.toString();
            if (GC_WARN_THRESHOLD_IN_MS != 0 && duration > GC_WARN_THRESHOLD_IN_MS)
                logger.warn(st);
            else if (duration > MIN_LOG_DURATION)
                logger.info(st);
            else if (logger.isTraceEnabled())
                logger.trace(st);

            if (duration > STAT_THRESHOLD)
                StatusLogger.log();

            // if we just finished an old gen collection and we're still using a lot of memory, try to reduce the pressure
            if (gcState.assumeGCIsOldGen)
                LifecycleTransaction.rescheduleFailedDeletions();
        }
    }

    public State getTotalSinceLastCheck()
    {
        return state.getAndSet(new State());
    }

    public double[] getAndResetStats()
    {
        State state = getTotalSinceLastCheck();
        double[] r = new double[7];
        r[0] = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - state.startNanos);
        r[1] = state.maxRealTimeElapsed;
        r[2] = state.totalRealTimeElapsed;
        r[3] = state.sumSquaresRealTimeElapsed;
        r[4] = state.totalBytesReclaimed;
        r[5] = state.count;
        r[6] = getAllocatedDirectMemory();

        return r;
    }

    private static long getAllocatedDirectMemory()
    {
        if (BITS_TOTAL_CAPACITY == null) return -1;
        try
        {
            return BITS_TOTAL_CAPACITY.getLong(null);
        }
        catch (Throwable t)
        {
            logger.trace("Error accessing field of java.nio.Bits", t);
            //Don't care how or why we failed to get the value in this JVM. Return -1 to indicate failure
            return -1;
        }
    }
}
