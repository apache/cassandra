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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * CassandraCIDRAuthorizer is backend for CIDR authorization checks
 * 1, Provides functionality to populate Role to CIDR permisssions cache and
 * 2, Uses CIDR groups mapping cache as backend to lookup CIDR groups of an IP
 */
public class CassandraCIDRAuthorizer extends AbstractCIDRAuthorizer
{
    private static final Logger logger = LoggerFactory.getLogger(AuthenticatedUser.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    protected static CIDRPermissionsCache cidrPermissionsCache;
    protected static CIDRGroupsMappingCache cidrGroupsMappingCache;

    @Override
    public void setup()
    {
        commonSetup();

        // Create <Role to CIDR permissions> cache
        cidrPermissionsCache = new CIDRPermissionsCache(this::getCidrPermissionsForRole,
                                                        this.bulkLoadCidrPermsCache(),
                                                        this::requireAuthorization);

        // Create CIDR groups cache
        cidrGroupsMappingCache = new CIDRGroupsMappingCache(cidrGroupsMappingManager, cidrAuthorizerMetrics);
    }

    @Override
    public void initCaches()
    {
        AuthCacheService.instance.register(cidrPermissionsCache);

        // Load CIDR groups cache during the startup, to avoid increased latency during the first CIDR check
        loadCidrGroupsCache();
    }

    private CIDRPermissions getCidrPermissionsForRole(RoleResource role)
    {
        return cidrPermissionsManager.getCidrPermissionsForRole(role);
    }

    private Supplier<Map<RoleResource, CIDRPermissions>> bulkLoadCidrPermsCache()
    {
        return cidrPermissionsManager.bulkLoader();
    }

    @Override
    public boolean invalidateCidrPermissionsCache(String roleName)
    {
        if (roleName == null || roleName.isEmpty())
        {
            cidrPermissionsCache.invalidate();
            return true;
        }

        return cidrPermissionsCache.invalidateCidrPermissions(roleName);
    }

    @Override
    public void loadCidrGroupsCache()
    {
        cidrGroupsMappingCache.loadCidrGroupsCache();
    }

    @Override
    public Set<String> lookupCidrGroupsForIp(InetAddress ip)
    {
        return cidrGroupsMappingCache.lookupCidrGroupsForIp(ip);
    }

    @VisibleForTesting
    protected boolean isMonitorMode()
    {
        return DatabaseDescriptor.getCidrAuthorizerMode() == CIDRAuthorizerMode.MONITOR;
    }

    private boolean hasCidrAccess(RoleResource role, InetAddress ipAddress)
    {
        CIDRPermissions cidrPermissions = cidrPermissionsCache.get(role);
        // Superusers and Roles without CIDR restrictions, should be able to access even when
        // CIDR authorization is enabled and CIDR groups mapping table is not populated yet
        if (!cidrPermissions.restrictsAccess() && !isMonitorMode())
            return true;

        Set<String> cidrGroups = lookupCidrGroupsForIp(ipAddress);

        if (isMonitorMode())
        {
            if (cidrGroups != null && !cidrPermissions.canAccessFrom(cidrGroups))
                noSpamLogger.warn("Role {} accessed from unauthorized IP {}, CIDR group {}", role.getRoleName(),
                                  ipAddress.getHostAddress(), cidrGroups);
            else
                noSpamLogger.info("Role {} accessed from IP {}, CIDR group {}", role.getRoleName(),
                                  ipAddress.getHostAddress(), cidrGroups);

            cidrAuthorizerMetrics.incrAcceptedAccessCount(cidrGroups);
            return true;
        }

        // Reach here only for enforce mode
        if (cidrGroups == null || cidrGroups.isEmpty() ||  // No CIDR group found for this IP
            !cidrPermissions.canAccessFrom(cidrGroups))
        {
            cidrAuthorizerMetrics.incrRejectedAccessCount(cidrGroups);
            return false;
        }

        cidrAuthorizerMetrics.incrAcceptedAccessCount(cidrGroups);
        return true;
    }

    @Override
    public boolean hasAccessFromIp(RoleResource role, InetAddress ipAddress)
    {
        long startTimeNanos = MonotonicClock.Global.approxTime.now();

        boolean hasAccess = hasCidrAccess(role, ipAddress);

        cidrAuthorizerMetrics.cidrChecksLatency.update(MonotonicClock.Global.approxTime.now() - startTimeNanos,
                                                          TimeUnit.NANOSECONDS);
        return hasAccess;
    }
}
