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

import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;

public class RolesCache extends AuthCache<RoleResource, Set<Role>> implements RolesCacheMBean
{
    private final IRoleManager roleManager;

    public RolesCache(IRoleManager roleManager, BooleanSupplier enableCache)
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
              roleManager::getRoleDetails,
              roleManager.bulkLoader(),
              enableCache);
        this.roleManager = roleManager;
    }

    /**
     * Read or return from the cache the Set of the RoleResources identifying the roles granted to the primary resource
     * @see Roles#getRoles(RoleResource)
     * @param primaryRole identifier for the primary role
     * @return the set of identifiers of all the roles granted to (directly or through inheritance) the primary role
     */
    Set<RoleResource> getRoleResources(RoleResource primaryRole)
    {
        return get(primaryRole).stream()
                               .map(r -> r.resource)
                               .collect(Collectors.toSet());
    }

    /**
     * Read or return from cache the set of Role objects representing the roles granted to the primary resource
     * @see Roles#getRoleDetails(RoleResource)
     * @param primaryRole identifier for the primary role
     * @return the set of Role objects containing info of all roles granted to (directly or through inheritance)
     * the primary role.
     */
    Set<Role> getRoles(RoleResource primaryRole)
    {
        return get(primaryRole);
    }

    Set<RoleResource> getAllRoles()
    {
        // This method seems kind of unnecessary as it is only called from Roles::getAllRoles,
        // but we are able to inject the RoleManager to this class, making testing possible. If
        // we lose this method and did everything in Roles, we'd be dependent on the IRM impl
        // supplied by DatabaseDescriptor
        return roleManager.getAllRoles();
    }

    public void invalidateRoles(String roleName)
    {
        invalidate(RoleResource.role(roleName));
    }
}
