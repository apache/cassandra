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

import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.config.DatabaseDescriptor;

public class CIDRPermissionsCache extends AuthCache<RoleResource, CIDRPermissions>
{
    public static final String CACHE_NAME = "CidrPermissionsCache";

    public CIDRPermissionsCache(Function<RoleResource, CIDRPermissions> loadFunction,
                                Supplier<Map<RoleResource, CIDRPermissions>> bulkLoadFunction,
                                BooleanSupplier cacheEnabledDelegate)
    {
        super(CACHE_NAME,
              DatabaseDescriptor::setRolesValidity,
              DatabaseDescriptor::getRolesValidity,
              DatabaseDescriptor::setRolesUpdateInterval,
              DatabaseDescriptor::getRolesUpdateInterval,
              DatabaseDescriptor::setRolesCacheMaxEntries,
              DatabaseDescriptor::getRolesCacheMaxEntries,
              DatabaseDescriptor::setRolesCacheActiveUpdate,
              DatabaseDescriptor::getRolesCacheActiveUpdate,
              loadFunction,
              bulkLoadFunction,
              cacheEnabledDelegate);
    }

    /**
     * Invalidate a role from CIDR permissions cache
     * @param roleName role for which to invalidate the cache
     * @return boolean returns true if given role found in the cache and invalidated, otherwise returns false
     */
    public boolean invalidateCidrPermissions(String roleName)
    {
        RoleResource role = RoleResource.role(roleName);
        if (cache.getIfPresent(role) == null)
            return false;

        invalidate(role);
        return true;
    }

    @Override
    protected void unregisterMBean()
    {
        super.unregisterMBean();
    }
}
