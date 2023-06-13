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

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.CIDRAuthorizerMetrics;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Backend for CIDR authorization feature
 */
public interface ICIDRAuthorizer
{
    /**
     * Supported modes by CIDR authorizer
     */
    public enum CIDRAuthorizerMode
    {
        MONITOR,
        ENFORCE
    }

    public static ICIDRAuthorizer newCIDRAuthorizer(ParameterizedClass cidrAuthorizer)
    {
        if (cidrAuthorizer == null || cidrAuthorizer.class_name == null)
        {
            return new AllowAllCIDRAuthorizer();
        }

        String className = cidrAuthorizer.class_name;
        if (!className.contains("."))
        {
            className = "org.apache.cassandra.auth." + className;
        }
        return FBUtilities.construct(className, "cidr authorizer");
    }

    public void setup();

    /**
     * Init caches held by CIDR authorizer
     */
    public void initCaches();

    public CIDRGroupsMappingManager getCidrGroupsMappingManager();

    public CIDRAuthorizerMetrics getCidrAuthorizerMetrics();

    public boolean requireAuthorization();

    /**
     * Set CIDR permissions for a given role
     * @param role role for which to set CIDR permissions
     * @param cidrPermissions CIR permissions to set for the role
     */
    public void setCidrGroupsForRole(RoleResource role, CIDRPermissions cidrPermissions);

    /**
     * Drop CIDR permissions of a role
     * @param role for which to drop cidr permissions
     */
    public void dropCidrPermissionsForRole(RoleResource role);

    /**
     * Invalidate given role from CIDR permissions cache
     * @param roleName role to invalidate
     * @return returns true if given role found in the cache and invalidated, false otherwise
     */
    public boolean invalidateCidrPermissionsCache(String roleName);

    public void validateConfiguration() throws ConfigurationException;

    /**
     * Load CIDR groups mapping cache
     */
    public void loadCidrGroupsCache();

    /**
     * Lookup IP in CIDR groups mapping cache
     * @param ip input IP to lookup CIDR group
     * @return returns best matching CIDR group for this IP
     */
    public Set<String> lookupCidrGroupsForIp(InetAddress ip);

    /**
     * Determines does the given role has access from CIDR groups associated with given IP
     * @param role role to check access
     * @param ipAddress IP of the client
     * @return returns true if role has access from given IP, false otherwise
     */
    public boolean hasAccessFromIp(RoleResource role, InetAddress ipAddress);
}
