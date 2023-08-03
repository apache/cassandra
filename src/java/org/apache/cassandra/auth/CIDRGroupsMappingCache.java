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

package org.apache.cassandra.auth;

import java.net.InetAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.CIDRAuthorizerMetrics;
import org.apache.cassandra.utils.MonotonicClock;

/**
 * CIDR groups mapping cache. Periodically reloads the cache with the latest entries
 * in the table {@link AuthKeyspace#CIDR_GROUPS}
 */
public class CIDRGroupsMappingCache
{
    // References to the latest mapping, changes everytime cache is reloaded
    private volatile CIDRGroupsMappingLoader currentCidrGroupsMappingLoader = null;

    // IP to CIDR groups cache, to reuse CIDR groups lookup result for an IP
    private LoadingCache<InetAddress, Set<String>> ipToCidrGroupsCache;

    private volatile boolean cacheInitialized = false;

    private CIDRGroupsMappingManager cidrGroupsMappingManager;
    private final CIDRAuthorizerMetrics cidrAuthorizerMetrics;

    public CIDRGroupsMappingCache(CIDRGroupsMappingManager cidrGroupsMappingManager,
                                  CIDRAuthorizerMetrics cidrAuthorizerMetrics)
    {
        this.cidrGroupsMappingManager = cidrGroupsMappingManager;
        this.cidrAuthorizerMetrics = cidrAuthorizerMetrics;

        // IP to CIDR groups cache
        ipToCidrGroupsCache = Caffeine.newBuilder()
                                      .maximumSize(DatabaseDescriptor.getIpCacheMaxSize())
                                      .executor(ImmediateExecutor.INSTANCE)
                                      .build(this::lookupCidrGroupsCacheForIp);

        // Interval to reload the cache periodically
        int cidrGroupsCacheRefreshInterval = DatabaseDescriptor.getCidrGroupsCacheRefreshInterval();
        // Schedule a task to reload cache periodically
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::reloadCidrGroupsCache,
                                                                 cidrGroupsCacheRefreshInterval,
                                                                 cidrGroupsCacheRefreshInterval,
                                                                 TimeUnit.MINUTES);
    }

    /**
     * Builds CIDR groups cache from entries in the table {@link AuthKeyspace#CIDR_GROUPS}
     * and points to the latest cache
     */
    private void loadCidrGroupsCacheInternal()
    {
        long startTimeNanos = MonotonicClock.Global.approxTime.now();

        currentCidrGroupsMappingLoader = new CIDRGroupsMappingLoader(this.cidrGroupsMappingManager);
        // Clear IP to CIDR groups cache entries as we reloaded CIDR groups cache
        ipToCidrGroupsCache.invalidateAll();

        cidrAuthorizerMetrics.cacheReloadCount.inc();
        cidrAuthorizerMetrics.cacheReloadLatency.update(MonotonicClock.Global.approxTime.now() - startTimeNanos,
                                                        TimeUnit.NANOSECONDS);
    }

    /**
     * Reload CIDR groups cache with the latest state of the table {@link AuthKeyspace#CIDR_GROUPS}
     */
    private void reloadCidrGroupsCache()
    {
        // Wait until CIDR authorizer calls init cache during the setup
        // Otherwise scheduled task may trigger this before startup is complete
        // and fails to find system tables
        if (cacheInitialized)
            loadCidrGroupsCacheInternal();
    }

    /**
     * Builds CIDR groups cache the first time. Should be called before
     * auth starts sending lookup IP requests
     */
    public void loadCidrGroupsCache()
    {
        loadCidrGroupsCacheInternal();
        cacheInitialized = true;
    }

    private Set<String> lookupCidrGroupsCacheForIp(InetAddress ipAddr)
    {
        long startTimeNanos = MonotonicClock.Global.approxTime.now();

        Preconditions.checkNotNull(currentCidrGroupsMappingLoader);
        Set<String> cidrGroups = currentCidrGroupsMappingLoader.lookupCidrGroupsCacheForIp(ipAddr);

        cidrAuthorizerMetrics.lookupCidrGroupsForIpLatency.update(MonotonicClock.Global.approxTime.now() - startTimeNanos,
                                                                  TimeUnit.NANOSECONDS);

        return cidrGroups;
    }

    /**
     * Lookup best matching CIDR group for the given IP
     * @param ipAddr IP address
     * @return returns set of CIDR group(s)
     */
    public Set<String> lookupCidrGroupsForIp(InetAddress ipAddr)
    {
        Preconditions.checkState(cacheInitialized, "CIDR groups cache not inited");
        return ipToCidrGroupsCache.get(ipAddr);
    }
}
