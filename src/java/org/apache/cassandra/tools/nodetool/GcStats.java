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
package org.apache.cassandra.tools.nodetool;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;

import com.sun.management.OperatingSystemMXBean;

import io.airlift.airline.Command;

@Command(name = "gcstats", description = "Print GC Statistics")
public class GcStats extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        double[] stats = probe.getAndResetGCStats();
        double mean = stats[2] / stats[5];
        double stdev = Math.sqrt((stats[3] / stats[5]) - (mean * mean));

        probe.output().out.println("GC Threads: " + probe.getNumberOfGCThreads());
        probe.output().out.println("Duration: " + probe.getYoungGenDuration() + " ms");
        probe.output().out.println("MemLock: " + NativeLibrary.jnaMemoryLockable() + "\n");
        probe.output().out.println("G1HeapRegionSize: " + FBUtilities.prettyPrintMemory(getRegionSize(probe), " ") + "\n");

        try {

            MBeanServerConnection mbeanServerConn = probe.getMbeanServerConn();

            Set<ObjectName> osMBeans = mbeanServerConn.queryNames(new ObjectName("java.lang:type=OperatingSystem"), null);
            if (!osMBeans.isEmpty()) {
                ObjectName osMBean = osMBeans.iterator().next();

            long freePhysicalMemorySize = (long) mbeanServerConn.getAttribute(osMBean, "FreePhysicalMemorySize");
            long totalPhysicalMemorySize = (long) mbeanServerConn.getAttribute(osMBean, "TotalPhysicalMemorySize");

            long freeSwapSpaceSize = (long) mbeanServerConn.getAttribute(osMBean, "FreeSwapSpaceSize");
            long totalSwapSpaceSize = (long) mbeanServerConn.getAttribute(osMBean, "TotalSwapSpaceSize");

            long osMemoryInUse = totalPhysicalMemorySize - freePhysicalMemorySize;
            double ospercent = ((double) osMemoryInUse / totalPhysicalMemorySize) * 100;

            long swapInUse = totalSwapSpaceSize - freeSwapSpaceSize;
            double swapPercent = ((double) swapInUse / totalSwapSpaceSize) * 100;

            probe.output().out.println("OS Free Memory Bytes: " + FBUtilities.prettyPrintMemory(freePhysicalMemorySize, " "));
            probe.output().out.println("OS In Use: " + FBUtilities.prettyPrintMemory(osMemoryInUse, " ") + " / " + FBUtilities.prettyPrintMemory(totalPhysicalMemorySize, " ") + " (" + String.format("%.1f", ospercent) + "%)\n");

            probe.output().out.println("OS Swap In Use: " + FBUtilities.prettyPrintMemory(swapInUse, " ") + " / " + FBUtilities.prettyPrintMemory(totalSwapSpaceSize, " ") + " (" + String.format("%.1f", swapPercent) + "%)\n");
            } else {
                probe.output().out.println("Operating System MBean not found.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        probe.output().out.printf("%20s%20s%20s%20s%20s%20s%25s%n", "Interval (ms)", "Max GC Elapsed (ms)", "Total GC Elapsed (ms)", "Stdev GC Elapsed (ms)", "GC Reclaimed (MB)", "Collections", "Direct Memory Bytes");
        probe.output().out.printf("%20.0f%20.0f%20.0f%20.0f%20.0f%20.0f%25d%n", stats[0], stats[1], stats[2], stdev, stats[4], stats[5], -1);
        MemoryUsage heapMemoryUsage = probe.getHeapMemoryUsage();

        probe.output().out.print("\nHeap memory used: " + FBUtilities.prettyPrintMemory(heapMemoryUsage.getUsed(), " "));
        probe.output().out.println(" (" + String.format("%.1f", ((double)heapMemoryUsage.getUsed()/(double)heapMemoryUsage.getCommitted())*100) + "%)");

        try {
            MBeanServerConnection mbeanServer = probe.getMbeanServerConn();
            Set<ObjectName> mbeans = mbeanServer.queryNames(new ObjectName("java.lang:type=MemoryPool,*"), null);

            for (ObjectName mbean : mbeans) {
                String name = mbean.getKeyProperty("name");

                if (name.contains("Eden") || name.contains("Old") || name.contains("Survivor")) {
                    MemoryUsage usage = MemoryUsage.from((CompositeData) mbeanServer.getAttribute(mbean, "Usage"));
                    probe.output().out.print("  " + name + " memory used: " + FBUtilities.prettyPrintMemory(usage.getUsed(), " "));
                    probe.output().out.println(" (" + String.format("%.1f", ((double) usage.getUsed() / (double) usage.getCommitted()) * 100) + "%)");
                }
            }
            probe.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static long getRegionSize(NodeProbe probe) {
        try {

        MBeanServerConnection mbeanServerConn = probe.getMbeanServerConn();
        ObjectName hotSpotDiagnostic = new ObjectName("com.sun.management:type=HotSpotDiagnostic");

        CompositeDataSupport g1HeapRegionSizeData = (CompositeDataSupport) mbeanServerConn.invoke(
            hotSpotDiagnostic,
            "getVMOption",
            new Object[]{"G1HeapRegionSize"},
            new String[]{"java.lang.String"}
        );

        String g1HeapRegionSizeValue = (String) g1HeapRegionSizeData.get("value");
        return Long.parseLong(g1HeapRegionSizeValue);

    } catch (Exception e) {
        throw new RuntimeException("Failed to get G1 heap region size", e);
    }
    }
}