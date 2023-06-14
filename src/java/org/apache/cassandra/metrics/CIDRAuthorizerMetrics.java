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
package org.apache.cassandra.metrics;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Captures metrics of CIDR authorizer
 */
public class CIDRAuthorizerMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("CIDRAuthorizer");

    // Number of user accesses rejected by CIDR authorization
    public static final String CIDR_ACCESSES_REJECTED_COUNT_PREFIX = "CIDRAccessesRejectedCount - ";
    public static final String CIDR_ACCESSES_ACCEPTED_COUNT_PREFIX = "CIDRAccessesAcceptedCount - ";
    // Time taken by CIDR authorization checks
    public static final String CIDR_CHECKS_LATENCY = "CIDRChecksLatency";

    // Number of times CIDR groups cache was (re)loaded
    public static final String CIDR_GROUPS_CACHE_RELOAD_COUNT = "CIDRGroupsMappingCacheReloadCount";
    // Time taken to reload CIDR groups cache
    public static final String CIDR_GROUPS_CACHE_RELOAD_LATENCY = "CIDRGroupsMappingCacheReloadLatency";
    // Time taken to look up an IP in CIDR groups cache, to find best matching CIDR group
    public static final String LOOKUP_CIDR_GROUPS_FOR_IP_LATENCY = "LookupCIDRGroupsForIPLatency";

    public final Timer cidrChecksLatency;
    public final Counter cacheReloadCount;
    public final Timer cacheReloadLatency;
    public final Timer lookupCidrGroupsForIpLatency;

    public final ConcurrentHashMap<String, Counter> rejectedCidrAccessCount = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, Counter> acceptedCidrAccessCount = new ConcurrentHashMap<>();

    public CIDRAuthorizerMetrics()
    {
        cidrChecksLatency = Metrics.timer(factory.createMetricName(CIDR_CHECKS_LATENCY));
        cacheReloadCount = Metrics.counter(factory.createMetricName(CIDR_GROUPS_CACHE_RELOAD_COUNT));
        cacheReloadLatency = Metrics.timer(factory.createMetricName(CIDR_GROUPS_CACHE_RELOAD_LATENCY));
        lookupCidrGroupsForIpLatency = Metrics.timer(factory.createMetricName(LOOKUP_CIDR_GROUPS_FOR_IP_LATENCY));
    }

    private void incCounter(ConcurrentHashMap<String, Counter> metric, String metricPrefix, String cidrGroup)
    {
        metric.computeIfAbsent(cidrGroup,
                               k -> Metrics.counter(factory.createMetricName(metricPrefix + cidrGroup))).inc();
    }

    public void incrRejectedAccessCount(Set<String> cidrGroups)
    {
        if (cidrGroups == null || cidrGroups.isEmpty())
        {
            incCounter(rejectedCidrAccessCount, CIDR_ACCESSES_REJECTED_COUNT_PREFIX, "Undefined");
            return;
        }

        for (String cidrGroup : cidrGroups)
        {
            incCounter(rejectedCidrAccessCount, CIDR_ACCESSES_REJECTED_COUNT_PREFIX, cidrGroup);
        }
    }

    public void incrAcceptedAccessCount(Set<String> cidrGroups)
    {
        if (cidrGroups == null || cidrGroups.isEmpty())
        {
            incCounter(acceptedCidrAccessCount, CIDR_ACCESSES_ACCEPTED_COUNT_PREFIX, "Undefined");
            return;
        }

        for (String cidrGroup : cidrGroups)
        {
            incCounter(acceptedCidrAccessCount, CIDR_ACCESSES_ACCEPTED_COUNT_PREFIX, cidrGroup);
        }
    }
}
