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

import java.net.InetSocketAddress;
import java.util.Set;
import com.google.common.base.Objects;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Datacenters;

/**
 * Returned from IAuthenticator#authenticate(), represents an authenticated user everywhere internally.
 *
 * Holds the name of the user and the roles that have been granted to the user. The roles will be cached
 * for roles_validity.
 */
public class AuthenticatedUser
{
    public static final String SYSTEM_USERNAME = "system";
    public static final AuthenticatedUser SYSTEM_USER = new AuthenticatedUser(SYSTEM_USERNAME);

    public static final String ANONYMOUS_USERNAME = "anonymous";
    public static final AuthenticatedUser ANONYMOUS_USER = new AuthenticatedUser(ANONYMOUS_USERNAME);

    // User-level permissions cache.
    public static final PermissionsCache permissionsCache = new PermissionsCache(DatabaseDescriptor.getAuthorizer());
    public static final NetworkPermissionsCache networkPermissionsCache = new NetworkPermissionsCache(DatabaseDescriptor.getNetworkAuthorizer());

    private static final ICIDRAuthorizer cidrAuthorizer = DatabaseDescriptor.getCIDRAuthorizer();

    /** Use {@link AuthCacheService#initializeAndRegisterCaches} rather than calling this directly */
    public static void init()
    {
        AuthCacheService.instance.register(permissionsCache);
        AuthCacheService.instance.register(networkPermissionsCache);

        cidrAuthorizer.initCaches();
    }

    private final String name;

    // Primary Role of the logged in user
    private final RoleResource role;

    public AuthenticatedUser(String name)
    {
        this.name = name;
        this.role = RoleResource.role(name);
    }

    public String getName()
    {
        return name;
    }

    public RoleResource getPrimaryRole()
    {
        return role;
    }

    /**
     * Checks the user's superuser status.
     * Only a superuser is allowed to perform CREATE USER and DROP USER queries.
     * Im most cased, though not necessarily, a superuser will have Permission.ALL on every resource
     * (depends on IAuthorizer implementation).
     */
    public boolean isSuper()
    {
        return !isAnonymous() && Roles.hasSuperuserStatus(role);
    }

    /**
     * If IAuthenticator doesn't require authentication, this method may return true.
     */
    public boolean isAnonymous()
    {
        return this == ANONYMOUS_USER;
    }

    /**
     * Some internal operations are performed on behalf of Cassandra itself, in those cases
     * the system user should be used where an identity is required
     * see CreateRoleStatement#execute() and overrides of AlterSchemaStatement#createdResources()
     */
    public boolean isSystem()
    {
        return this == SYSTEM_USER;
    }

    /**
     * Get the roles that have been granted to the user via the IRoleManager
     *
     * @return a set of identifiers for the roles that have been granted to the user
     */
    public Set<RoleResource> getRoles()
    {
        return Roles.getRoles(role);
    }

    /**
     * Get the detailed info on roles granted to the user via IRoleManager
     *
     * @return a set of Role objects detailing the roles granted to the user
     */
    public Set<Role> getRoleDetails()
    {
       return Roles.getRoleDetails(role);
    }

    public Set<Permission> getPermissions(IResource resource)
    {
        return permissionsCache.getPermissions(this, resource);
    }

    /**
     * Check whether this user has login privileges.
     * LOGIN is not inherited from granted roles, so must be directly granted to the primary role for this user
     *
     * @return true if the user is permitted to login, false otherwise.
     */
    public boolean canLogin()
    {
        return Roles.canLogin(getPrimaryRole());
    }

    /**
     * Verify that there is not DC level restriction on this user accessing this node.
     * Further extends the login privilege check by verifying that the primary role for this user is permitted
     * to perform operations in the local (to this node) datacenter. Like LOGIN, this is not inherited from
     * granted roles.
     * @return true if the user is permitted to access nodes in this node's datacenter, false otherwise
     */
    public boolean hasLocalAccess()
    {
        return networkPermissionsCache.get(this.getPrimaryRole()).canAccess(Datacenters.thisDatacenter());
    }

    public boolean hasAccessFromIp(InetSocketAddress remoteAddress)
    {
        return cidrAuthorizer.hasAccessFromIp(role, remoteAddress.getAddress());
    }

    @Override
    public String toString()
    {
        return String.format("#<User %s>", name);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof AuthenticatedUser))
            return false;

        AuthenticatedUser u = (AuthenticatedUser) o;

        return Objects.equal(name, u.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name);
    }
}
