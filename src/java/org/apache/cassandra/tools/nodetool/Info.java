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

import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.io.PrintStream;
import java.lang.management.MemoryUsage;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.InstanceNotFoundException;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "info", description = "Print node information (uptime, load, ...)")
public class Info extends NodeToolCmd
{
    @Option(name = {"-T", "--tokens"}, description = "Display all tokens")
    private boolean tokens = false;

    @Override
    public void execute(NodeProbe probe)
    {
        boolean gossipInitialized = probe.isGossipRunning();

        PrintStream out = probe.output().out;
        out.printf("%-23s: %s%n", "ID", probe.getLocalHostId());
        out.printf("%-23s: %s%n", "Gossip active", gossipInitialized);
        out.printf("%-23s: %s%n", "Native Transport active", probe.isNativeTransportRunning());
        out.printf("%-23s: %s%n", "Load", probe.getLoadString());
        out.printf("%-23s: %s%n", "Uncompressed load", probe.getUncompressedLoadString());

        if (gossipInitialized)
            out.printf("%-23s: %s%n", "Generation No", probe.getCurrentGenerationNumber());
        else
            out.printf("%-23s: %s%n", "Generation No", 0);

        // Uptime
        long secondsUp = probe.getUptime() / 1000;
        out.printf("%-23s: %d%n", "Uptime (seconds)", secondsUp);

        // Memory usage
        MemoryUsage heapUsage = probe.getHeapMemoryUsage();
        double memUsed = (double) heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double) heapUsage.getMax() / (1024 * 1024);
        out.printf("%-23s: %.2f / %.2f%n", "Heap Memory (MB)", memUsed, memMax);
        try
        {
            out.printf("%-23s: %.2f%n", "Off Heap Memory (MB)", getOffHeapMemoryUsed(probe));
        }
        catch (RuntimeException e)
        {
            // offheap-metrics introduced in 2.1.3 - older versions do not have the appropriate mbeans
            if (!(e.getCause() instanceof InstanceNotFoundException))
                throw e;
        }

        // Data Center/Rack
        out.printf("%-23s: %s%n", "Data Center", probe.getDataCenter());
        out.printf("%-23s: %s%n", "Rack", probe.getRack());

        // Exceptions
        out.printf("%-23s: %s%n", "Exceptions", probe.getStorageMetric("Exceptions"));

        CacheServiceMBean cacheService = probe.getCacheServiceMBean();

        // Key Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
        out.printf("%-23s: entries %d, size %s, capacity %s, %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n",
                "Key Cache",
                probe.getCacheMetric("KeyCache", "Entries"),
                FileUtils.stringifyFileSize((long) probe.getCacheMetric("KeyCache", "Size")),
                FileUtils.stringifyFileSize((long) probe.getCacheMetric("KeyCache", "Capacity")),
                probe.getCacheMetric("KeyCache", "Hits"),
                probe.getCacheMetric("KeyCache", "Requests"),
                probe.getCacheMetric("KeyCache", "HitRate"),
                cacheService.getKeyCacheSavePeriodInSeconds());

        // Row Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
        out.printf("%-23s: entries %d, size %s, capacity %s, %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n",
                "Row Cache",
                probe.getCacheMetric("RowCache", "Entries"),
                FileUtils.stringifyFileSize((long) probe.getCacheMetric("RowCache", "Size")),
                FileUtils.stringifyFileSize((long) probe.getCacheMetric("RowCache", "Capacity")),
                probe.getCacheMetric("RowCache", "Hits"),
                probe.getCacheMetric("RowCache", "Requests"),
                probe.getCacheMetric("RowCache", "HitRate"),
                cacheService.getRowCacheSavePeriodInSeconds());

        // Counter Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
        out.printf("%-23s: entries %d, size %s, capacity %s, %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n",
                "Counter Cache",
                probe.getCacheMetric("CounterCache", "Entries"),
                FileUtils.stringifyFileSize((long) probe.getCacheMetric("CounterCache", "Size")),
                FileUtils.stringifyFileSize((long) probe.getCacheMetric("CounterCache", "Capacity")),
                probe.getCacheMetric("CounterCache", "Hits"),
                probe.getCacheMetric("CounterCache", "Requests"),
                probe.getCacheMetric("CounterCache", "HitRate"),
                cacheService.getCounterCacheSavePeriodInSeconds());

        // Chunk Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
        try
        {
            out.printf("%-23s: entries %d, size %s, capacity %s, %d misses, %d requests, %.3f recent hit rate, %.3f %s miss latency%n",
                    "Chunk Cache",
                    probe.getCacheMetric("ChunkCache", "Entries"),
                    FileUtils.stringifyFileSize((long) probe.getCacheMetric("ChunkCache", "Size")),
                    FileUtils.stringifyFileSize((long) probe.getCacheMetric("ChunkCache", "Capacity")),
                    probe.getCacheMetric("ChunkCache", "Misses"),
                    probe.getCacheMetric("ChunkCache", "Requests"),
                    probe.getCacheMetric("ChunkCache", "HitRate"),
                    probe.getCacheMetric("ChunkCache", "MissLatency"),
                    probe.getCacheMetric("ChunkCache", "MissLatencyUnit"));
        }
        catch (RuntimeException e)
        {
            if (!(e.getCause() instanceof InstanceNotFoundException))
                throw e;

            // Chunk cache is not on.
        }

        // network Cache: capacity, size
        try
        {
            out.printf("%-23s: size %s, overflow size: %s, capacity %s%n", "Network Cache",
                       FileUtils.stringifyFileSize((long) probe.getBufferPoolMetric("networking", "Size")),
                       FileUtils.stringifyFileSize((long) probe.getBufferPoolMetric("networking", "OverflowSize")),
                       FileUtils.stringifyFileSize((long) probe.getBufferPoolMetric("networking", "Capacity")));
        }
        catch (RuntimeException e)
        {
            if (!(e.getCause() instanceof InstanceNotFoundException))
                throw e;

            // network cache is not on.
        }

        // Global table stats
        out.printf("%-23s: %s%%%n", "Percent Repaired", probe.getColumnFamilyMetric(null, null, "PercentRepaired"));

        // check if node is already joined, before getting tokens, since it throws exception if not.
        if (probe.isJoined())
        {
            // Tokens
            List<String> tokens = probe.getTokens();
            if (tokens.size() == 1 || this.tokens)
                for (String token : tokens)
                    out.printf("%-23s: %s%n", "Token", token);
            else
                out.printf("%-23s: (invoke with -T/--tokens to see all %d tokens)%n", "Token",
                                  tokens.size());
        }
        else
        {
            out.printf("%-23s: (node is not joined to the cluster)%n", "Token");
        }

        out.printf("%-23s: %s%n", "Bootstrap state", probe.getStorageService().getBootstrapState());
        out.printf("%-23s: %s%n", "Bootstrap failed", probe.getStorageService().isBootstrapFailed());
        out.printf("%-23s: %s%n", "Decommissioning", probe.getStorageService().isDecommissioning());
        out.printf("%-23s: %s%n", "Decommission failed", probe.getStorageService().isDecommissionFailed());
    }

    /**
     * Returns the total off heap memory used in MiB.
     * @return the total off heap memory used in MiB.
     */
    private static double getOffHeapMemoryUsed(NodeProbe probe)
    {
        long offHeapMemUsedInBytes = 0;
        // get a list of column family stores
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe.getColumnFamilyStoreMBeanProxies();

        while (cfamilies.hasNext())
        {
            Entry<String, ColumnFamilyStoreMBean> entry = cfamilies.next();
            String keyspaceName = entry.getKey();
            String cfName = entry.getValue().getTableName();

            offHeapMemUsedInBytes += (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableOffHeapSize");
            offHeapMemUsedInBytes += (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "BloomFilterOffHeapMemoryUsed");
            offHeapMemUsedInBytes += (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "IndexSummaryOffHeapMemoryUsed");
            offHeapMemUsedInBytes += (Long) probe.getColumnFamilyMetric(keyspaceName, cfName, "CompressionMetadataOffHeapMemoryUsed");
        }

        return offHeapMemUsedInBytes / (1024d * 1024);
    }
}
