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

import org.apache.cassandra.config.DatabaseDescriptor;

public class Roles
{
    private static final RolesCache cache = new RolesCache(DatabaseDescriptor.getRoleManager());

    /**
     * Get all roles granted to the supplied Role, including both directly granted
     * and inherited roles.
     * The returned roles may be cached if {@code roles_validity_in_ms > 0}
     *
     * @param primaryRole the Role
     * @return set of all granted Roles for the primary Role
     */
    public static Set<RoleResource> getRoles(RoleResource primaryRole)
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
        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        for (RoleResource r : cache.getRoles(role))
            if (roleManager.isSuper(r))
                return true;
        return false;
    }
}
