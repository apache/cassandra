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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;

public class Roles
{
    private static final Role NO_ROLE = new Role("", false, false, Collections.emptyMap(), Collections.emptySet());

    private static RolesCache cache;
    static
    {
        initRolesCache(DatabaseDescriptor.getRoleManager(),
                       () -> DatabaseDescriptor.getAuthenticator().requireAuthentication());
    }

    @VisibleForTesting
    public static void initRolesCache(IRoleManager roleManager, BooleanSupplier enableCache)
    {
        if (cache != null)
            cache.unregisterMBean();
        cache = new RolesCache(roleManager, enableCache);
    }

    @VisibleForTesting
    public static void clearCache()
    {
        cache.invalidate();
    }

    /**
     * Identify all roles granted to the supplied Role, including both directly granted
     * and inherited roles.
     * This method is used where we mainly just care about *which* roles are granted to a given role,
     * including when looking up or listing permissions for a role on a given resource.
     *
     * @param primaryRole the Role
     * @return set of all granted Roles for the primary Role
     */
    public static Set<RoleResource> getRoles(RoleResource primaryRole)
    {
        return cache.getRoleResources(primaryRole);
    }

    /**
     * Get detailed info on all the roles granted to the role identified by the supplied RoleResource.
     * This includes superuser status and login privileges for the primary role and all roles granted directly
     * to it or inherited.
     * The returnred roles may be cached if roles_validity_in_ms > 0
     * This method is used where we need to know specific attributes of the collection of granted roles, i.e.
     * when checking for superuser status which may be inherited from *any* granted role.
     *
     * @param primaryRole identifies the role
     * @return set of detailed info for all of the roles granted to the primary
     */
    public static Set<Role> getRoleDetails(RoleResource primaryRole)
    {
        return cache.getRoles(primaryRole);
    }

    /**
     * Returns true if the supplied role or any other role granted to it
     * (directly or indirectly) has superuser status.
     *
     * @param role the primary role
     * @return true if the role has superuser status, false otherwise
     */
    public static boolean hasSuperuserStatus(RoleResource role)
    {
        for (Role r : getRoleDetails(role))
            if (r.isSuper)
                return true;

        return false;
    }

    /**
     * Returns true if the supplied role has the login privilege. This cannot be inherited, so
     * returns true iff the named role has that bit set.
     * @param role the role identifier
     * @return true if the role has the canLogin privilege, false otherwise
     */
    public static boolean canLogin(final RoleResource role)
    {
        for (Role r : getRoleDetails(role))
            if (r.resource.equals(role))
                return r.canLogin;

        return false;
    }

    /**
     * Returns the map of custom options for the named role. These options are not inherited from granted roles, but
     * are set directly.
     * @param role the role identifier
     * @return map of option_name -> value. If no options are set for the named role, the map will be empty
     * but never null.
     */
    public static Map<String, String> getOptions(RoleResource role)
    {
        for (Role r : getRoleDetails(role))
            if (r.resource.equals(role))
                return r.options;

        return NO_ROLE.options;
    }

   /**
    * Return the NullObject Role instance which can be safely used to indicate no information is available
    * when querying for a specific named role.
    * @return singleton null role object
    */
   public static Role nullRole()
   {
       return NO_ROLE;
   }

   /**
    * Just a convenience method which compares a role instance with the null object version, indicating if the
    * return from some query/lookup method was a valid Role or indicates that the role does not exist.
    * @param role
    * @return true if the supplied role is the null role instance, false otherwise.
    */
   public static boolean isNullRole(Role role)
   {
       return NO_ROLE.equals(role);
   }


   /**
    * Constructs a Role object from a RoleResource, using the methods of the supplied IRoleManager.
    * This is used by the default implementation of IRoleManager#getRoleDetails so that IRoleManager impls
    * which don't implement an optimized getRoleDetails remain compatible. Depending on the IRoleManager
    * implementation this could be quite heavyweight, so should not be used on any hot path.
    *
    * @param resource identifies the role
    * @param roleManager provides lookup functions to retrieve role info
    * @return Role object including superuser status, login privilege, custom options and the set of roles
    * granted to identified role.
    */
   public static Role fromRoleResource(RoleResource resource, IRoleManager roleManager)
   {
       return new Role(resource.getName(),
                       roleManager.isSuper(resource),
                       roleManager.canLogin(resource),
                       roleManager.getCustomOptions(resource),
                       roleManager.getRoles(resource, false)
                                  .stream()
                                  .map(RoleResource::getRoleName)
                                  .collect(Collectors.toSet()));
   }
}
