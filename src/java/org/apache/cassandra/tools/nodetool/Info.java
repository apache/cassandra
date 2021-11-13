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

import org.apache.cassandra.auth.NetworkPermissionsCacheMBean;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.PermissionsCacheMBean;
import org.apache.cassandra.auth.RolesCacheMBean;
import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CacheService;
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
        out.printf("%-25s: %s%n", "ID", probe.getLocalHostId());
        out.printf("%-25s: %s%n", "Gossip active", gossipInitialized);
        out.printf("%-25s: %s%n", "Native Transport active", probe.isNativeTransportRunning());
        out.printf("%-25s: %s%n", "Load", probe.getLoadString());
        if (gossipInitialized)
            out.printf("%-25s: %s%n", "Generation No", probe.getCurrentGenerationNumber());
        else
            out.printf("%-25s: %s%n", "Generation No", 0);

        // Uptime
        long secondsUp = probe.getUptime() / 1000;
        out.printf("%-25s: %d%n", "Uptime (seconds)", secondsUp);

        // Memory usage
        MemoryUsage heapUsage = probe.getHeapMemoryUsage();
        double memUsed = (double) heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double) heapUsage.getMax() / (1024 * 1024);
        out.printf("%-25s: %.2f / %.2f%n", "Heap Memory (MB)", memUsed, memMax);
        try
        {
            out.printf("%-25s: %.2f%n", "Off Heap Memory (MB)", getOffHeapMemoryUsed(probe));
        }
        catch (RuntimeException e)
        {
            // offheap-metrics introduced in 2.1.3 - older versions do not have the appropriate mbeans
            if (!(e.getCause() instanceof InstanceNotFoundException))
                throw e;
        }

        // Data Center/Rack
        out.printf("%-25s: %s%n", "Data Center", probe.getDataCenter());
        out.printf("%-25s: %s%n", "Rack", probe.getRack());

        // Exceptions
        out.printf("%-25s: %s%n", "Exceptions", probe.getStorageMetric("Exceptions"));

        CacheServiceMBean cacheService = probe.getCacheServiceMBean();

        printAutoSavingCacheStats(probe, out, "Key Cache", CacheService.CacheType.KEY_CACHE, cacheService.getKeyCacheSavePeriodInSeconds());
        printAutoSavingCacheStats(probe, out, "Row Cache", CacheService.CacheType.ROW_CACHE, cacheService.getRowCacheSavePeriodInSeconds());
        printAutoSavingCacheStats(probe, out, "Counter Cache", CacheService.CacheType.COUNTER_CACHE, cacheService.getCounterCacheSavePeriodInSeconds());

        // Chunk Cache: Hits, Requests, RecentHitRate, SavePeriodInSeconds
        try
        {
            out.printf("%-25s: entries %d, size %s, capacity %s, %d misses, %d requests, %.3f recent hit rate, %.3f %s miss latency%n",
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

        printAuthCacheStats(probe, out, "Credentials Cache", PasswordAuthenticator.CredentialsCacheMBean.CACHE_NAME);
        printAuthCacheStats(probe, out, "JMX Permissions Cache", AuthorizationProxy.JmxPermissionsCacheMBean.CACHE_NAME);
        printAuthCacheStats(probe, out, "Network Permissions Cache", NetworkPermissionsCacheMBean.CACHE_NAME);
        printAuthCacheStats(probe, out, "Permissions Cache", PermissionsCacheMBean.CACHE_NAME);
        printAuthCacheStats(probe, out, "Roles Cache", RolesCacheMBean.CACHE_NAME);

        // Global table stats
        out.printf("%-25s: %s%%%n", "Percent Repaired", probe.getColumnFamilyMetric(null, null, "PercentRepaired"));

        // check if node is already joined, before getting tokens, since it throws exception if not.
        if (probe.isJoined())
        {
            // Tokens
            List<String> tokens = probe.getTokens();
            if (tokens.size() == 1 || this.tokens)
                for (String token : tokens)
                    out.printf("%-25s: %s%n", "Token", token);
            else
                out.printf("%-25s: (invoke with -T/--tokens to see all %d tokens)%n", "Token",
                                  tokens.size());
        }
        else
        {
            out.printf("%-25s: (node is not joined to the cluster)%n", "Token");
        }
    }

    private void printAutoSavingCacheStats(NodeProbe probe, PrintStream out, String cacheName, CacheService.CacheType cacheType, int savePeriodInSeconds)
    {
        out.printf("%-25s: entries %d, size %s, capacity %s, %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n",
                   cacheName,
                   probe.getCacheMetric(cacheType.toString(), "Entries"),
                   FileUtils.stringifyFileSize((long) probe.getCacheMetric(cacheType.toString(), "Size")),
                   FileUtils.stringifyFileSize((long) probe.getCacheMetric(cacheType.toString(), "Capacity")),
                   probe.getCacheMetric(cacheType.toString(), "Hits"),
                   probe.getCacheMetric(cacheType.toString(), "Requests"),
                   probe.getCacheMetric(cacheType.toString(), "HitRate"),
                   savePeriodInSeconds);
    }

    private void printAuthCacheStats(NodeProbe probe, PrintStream out, String cacheName, String cacheType)
    {
        try
        {
            out.printf("%-25s: entries %d, size %s, capacity %s, %d hits, %d requests, %.3f recent hit rate%n",
                       cacheName,
                       probe.getCacheMetric(cacheType, "Entries"),
                       FileUtils.stringifyFileSize((long) probe.getCacheMetric(cacheType, "Size")),
                       FileUtils.stringifyFileSize((long) probe.getCacheMetric(cacheType, "Capacity")),
                       probe.getCacheMetric(cacheType, "Hits"),
                       probe.getCacheMetric(cacheType, "Requests"),
                       probe.getCacheMetric(cacheType, "HitRate"));
        }
        catch (RuntimeException e)
        {
            if (!(e.getCause() instanceof InstanceNotFoundException))
                throw e;

            // cache is not on
        }
    }

    /**
     * Returns the total off heap memory used in MB.
     * @return the total off heap memory used in MB.
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
