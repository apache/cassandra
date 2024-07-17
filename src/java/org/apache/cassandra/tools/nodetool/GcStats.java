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
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import com.sun.management.OperatingSystemMXBean;
import org.apache.commons.lang3.StringUtils;

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

        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        String freeMemoryBytes = FBUtilities.prettyPrintMemory(osBean.getFreeMemorySize(), " ");
        String totalMemoryBytes = FBUtilities.prettyPrintMemory(osBean.getTotalMemorySize(), " ");
        String totalswapMemoryBytes = FBUtilities.prettyPrintMemory(osBean.getTotalSwapSpaceSize(), " ");

        String OSInUseMemoryBytes = FBUtilities.prettyPrintMemory(osBean.getTotalMemorySize() - osBean.getFreeMemorySize(), " ");
        String SWAPInUseMemoryBytes = FBUtilities.prettyPrintMemory(osBean.getTotalSwapSpaceSize() - osBean.getFreeSwapSpaceSize(), " ");

        probe.output().out.println("GC Threads: " + probe.getNumberOfGCThreads());
        probe.output().out.println("Duration: " + probe.getYoungGenDuration() + " ms");
        probe.output().out.println("MemLock: " + NativeLibrary.jnaMemoryLockable() + "\n");

        long osMemoryInUse = osBean.getTotalMemorySize() - osBean.getFreeMemorySize();
        double ospercent = ((double)osMemoryInUse/osBean.getTotalMemorySize()) * 100;
        probe.output().out.println("OS Free Memory Bytes: " + freeMemoryBytes);
        probe.output().out.println("OS Total Memory Bytes: " + totalMemoryBytes);
        probe.output().out.println("OS In Use: " +  OSInUseMemoryBytes + " / " + totalMemoryBytes + " (" + String.format("%.1f", ospercent) + "%)\n");

        long swapInUse;
        double swapPercent;

        if(osBean.getTotalSwapSpaceSize() == 0){
            swapInUse = 0;
            swapPercent = 0;
        }
        else{
            swapInUse = osBean.getTotalSwapSpaceSize() - osBean.getFreeSwapSpaceSize();
            swapPercent = ((double)swapInUse/osBean.getTotalSwapSpaceSize()) * 100;
        }

        probe.output().out.println("Swap in Use: " + SWAPInUseMemoryBytes + " / " + totalswapMemoryBytes + " (" + String.format("%.1f", swapPercent) + "%)\n");
        probe.output().out.printf("%20s%20s%20s%20s%20s%20s%25s%n", "Interval (ms)", "Max GC Elapsed (ms)", "Total GC Elapsed (ms)", "Stdev GC Elapsed (ms)", "GC Reclaimed (MB)", "Collections", "Direct Memory Bytes");
        probe.output().out.printf("%20.0f%20.0f%20.0f%20.0f%20.0f%20.0f%25d%n", stats[0], stats[1], stats[2], stdev, stats[4], stats[5], -1);
        MemoryUsage heapMemoryUsage = probe.getHeapMemoryUsage();

        probe.output().out.print("\nHeap memory used: " + FBUtilities.prettyPrintMemory(heapMemoryUsage.getUsed(), " "));
        probe.output().out.println(" (" + String.format("%.1f", ((double)heapMemoryUsage.getUsed()/(double)heapMemoryUsage.getCommitted())*100) + "%)");

        for (MemoryPoolMXBean memoryPoolMXBean : ManagementFactory.getMemoryPoolMXBeans()) {
            String name = memoryPoolMXBean.getName();
            MemoryUsage usage = memoryPoolMXBean.getUsage();

            if(StringUtils.containsAny(name,"Eden", "Old","Survivor")){
                probe.output().out.print("  " + name + " memory used: " + FBUtilities.prettyPrintMemory(usage.getUsed(), " "));
                probe.output().out.println(" (" + String.format("%.1f", ((double)usage.getUsed()/(double)usage.getCommitted())*100) + "%)");
            }
        }
    }
}

