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
import org.apache.cassandra.utils.NativeLibrary; // Import the NativeLibrary class
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.List;
import com.sun.management.OperatingSystemMXBean; // Import the OperatingSystemMXBean class

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
        long[] thread = probe.numberOfGCThreads();

        //value of jnaLockable
        boolean jnaLockable = NativeLibrary.jnaMemoryLockable();

        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        long freeMemoryBytes = osBean.getFreeMemorySize();
        long swapMemoryBytes = osBean.getFreeSwapSpaceSize();
        long totalMemoryBytes = osBean.getTotalMemorySize();
        long totalswapMemoryBytes = osBean.getTotalSwapSpaceSize();

        String PM_freeMemoryBytes = FBUtilities.prettyPrintMemory(freeMemoryBytes, " ");
        String PM_totalMemoryBytes = FBUtilities.prettyPrintMemory(totalMemoryBytes, " ");
        String PM_totalswapMemoryBytes = FBUtilities.prettyPrintMemory(totalswapMemoryBytes, " ");

        String PM_OSInUseMemoryBytes = FBUtilities.prettyPrintMemory(totalswapMemoryBytes - freeMemoryBytes, " ");
        String PM_SWAPInUseMemoryBytes = FBUtilities.prettyPrintMemory(totalswapMemoryBytes - swapMemoryBytes, " ");

        probe.output().out.println("GC Threads: " + thread[1]);
        probe.output().out.println("Duration: " + thread[0] + " ms");
        probe.output().out.println("MemLock: " + jnaLockable + "\n");

        long osInUse = totalMemoryBytes - freeMemoryBytes;
        double ospercent = ((double)osInUse/totalMemoryBytes) * 100;
        probe.output().out.println("OS Free Memory Bytes: " + PM_freeMemoryBytes);
        probe.output().out.println("OS Total Memory Bytes: " + PM_totalMemoryBytes);
        probe.output().out.println("OS In Use: " +  PM_OSInUseMemoryBytes + " / " + PM_totalMemoryBytes + " (" + String.format("%.1f", ospercent) + "%)\n");

        long swapInUse = (totalswapMemoryBytes - swapMemoryBytes);
        double swapPercent = ((double)swapInUse/totalswapMemoryBytes) * 100;

        probe.output().out.println("Swap in Use: " + PM_SWAPInUseMemoryBytes + " / " + PM_totalswapMemoryBytes + " (" + String.format("%.1f", swapPercent) + "%)\n");
        probe.output().out.printf("%20s%20s%20s%20s%20s%20s%25s%n", "Interval (ms)", "Max GC Elapsed (ms)", "Total GC Elapsed (ms)", "Stdev GC Elapsed (ms)", "GC Reclaimed (MB)", "Collections", "Direct Memory Bytes");
        probe.output().out.printf("%20.0f%20.0f%20.0f%20.0f%20.0f%20.0f%25d%n", stats[0], stats[1], stats[2], stdev, stats[4], stats[5], -1);
        probe.output().out.printf("\n");

        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

        // Get the heap memory usage
        MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
        long heapUsed = heapMemoryUsage.getUsed();
        long heapCommitted = heapMemoryUsage.getCommitted();

        String PM_heapUsed = FBUtilities.prettyPrintMemory(heapUsed, " ");

        probe.output().out.print("Heap memory used: " + PM_heapUsed);
        probe.output().out.println(" (" + String.format("%.1f", ((double)heapUsed/(double)heapCommitted)*100) + "%)");


        // Get the MemoryPoolMXBeans
        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();

        for (MemoryPoolMXBean memoryPoolMXBean : memoryPoolMXBeans) {
            String name = memoryPoolMXBean.getName();
            MemoryUsage usage = memoryPoolMXBean.getUsage();

            String PM_usage = FBUtilities.prettyPrintMemory(usage.getUsed(), " ");

            if(name.contains("Eden") || name.contains("Old") || name.contains("Survivor")){
                probe.output().out.print("  " + name + " memory used: " + PM_usage);
                probe.output().out.println(" (" + String.format("%.1f", ((double)usage.getUsed()/(double)usage.getCommitted())*100) + "%)");
            }
        }
    }
}
