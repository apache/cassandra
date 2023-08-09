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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.CIDRAuthorizerMetrics;

/**
 * Abstract CIDR authorizer, contains code common to all implementations of ICIDRAuthorizer
 */
public abstract class AbstractCIDRAuthorizer implements ICIDRAuthorizer
{
    protected static CIDRPermissionsManager cidrPermissionsManager;
    protected static CIDRGroupsMappingManager cidrGroupsMappingManager;

    protected static CIDRAuthorizerMetrics cidrAuthorizerMetrics;

    @VisibleForTesting
    void createManagers()
    {
        // CIDRPermissionsManager provides functionality to retrieve or update CIDR permissions for a role
        cidrPermissionsManager = new CIDRPermissionsManager();

        // CIDRGroupsMappingManager provides functionality to retrieve or update CIDR groups mapping table
        cidrGroupsMappingManager = new CIDRGroupsMappingManager();
    }

    protected void commonSetup()
    {
        createManagers();

        cidrPermissionsManager.setup();
        cidrGroupsMappingManager.setup();

        cidrAuthorizerMetrics = new CIDRAuthorizerMetrics();
    }

    @Override
    public CIDRGroupsMappingManager getCidrGroupsMappingManager()
    {
        return cidrGroupsMappingManager;
    }

    @Override
    public CIDRAuthorizerMetrics getCidrAuthorizerMetrics() { return cidrAuthorizerMetrics; }

    @Override
    public boolean requireAuthorization()
    {
        return true;
    }

    @Override
    public void setCidrGroupsForRole(RoleResource role, CIDRPermissions cidrPermissions)
    {
        // Allow 'create role' and 'alter role' CQL commands with ACCESS FROM CIDRS clause in both cases,
        // when feature is enabled and also disabled
        cidrPermissionsManager.setCidrGroupsForRole(role, cidrPermissions);
    }

    @Override
    public void dropCidrPermissionsForRole(RoleResource role)
    {
        // Allow dropping of CIDR permissions for roles in both cases,
        // when feature is enabled and also disabled
        cidrPermissionsManager.drop(role);
    }

    @Override
    public void validateConfiguration() throws ConfigurationException
    {
        // do nothing
    }
}
