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

import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * AllowAllCIDRAuthorizer allows any user to access from any CIDR
 * i.e, disables CIDR authorization
 */
public class AllowAllCIDRAuthorizer extends AbstractCIDRAuthorizer
{
    public void setup()
    {
        commonSetup();
    }

    @Override
    public void initCaches()
    {
        // Caches not created when CIDR authorization is disabled
    }

    @Override
    public boolean requireAuthorization()
    {
        return false;
    }

    @Override
    public boolean invalidateCidrPermissionsCache(String roleName)
    {
        throw new InvalidRequestException("Invalidate CIDR permissions cache operation not supported by " + getClass().getSimpleName());
    }

    @Override
    public void loadCidrGroupsCache()
    {
        throw new InvalidRequestException("Load CIDR groups cache operation not supported by " + getClass().getSimpleName());
    }

    @Override
    public Set<String> lookupCidrGroupsForIp(InetAddress ip)
    {
        throw new InvalidRequestException("'Get CIDR groups for IP' operation not supported by " + getClass().getSimpleName());
    }

    @Override
    public boolean hasAccessFromIp(RoleResource role, InetAddress ipAddress)
    {
        // Allow all accesses
        return true;
    }
}
