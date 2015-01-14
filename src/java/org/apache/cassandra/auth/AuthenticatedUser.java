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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;

/**
 * Returned from IAuthenticator#authenticate(), represents an authenticated user everywhere internally.
 *
 * Holds the name of the user and the roles that have been granted to the user. The roles will be cached
 * for roles_validity_in_ms.
 */
public class AuthenticatedUser
{
    private static final Logger logger = LoggerFactory.getLogger(AuthenticatedUser.class);

    public static final String ANONYMOUS_USERNAME = "anonymous";
    public static final AuthenticatedUser ANONYMOUS_USER = new AuthenticatedUser(ANONYMOUS_USERNAME);

    // User-level roles cache
    private static final LoadingCache<String, Set<String>> rolesCache = initRolesCache();

    // User-level permissions cache.
    private static final PermissionsCache permissionsCache = new PermissionsCache(DatabaseDescriptor.getPermissionsValidity(),
                                                                                  DatabaseDescriptor.getPermissionsUpdateInterval(),
                                                                                  DatabaseDescriptor.getPermissionsCacheMaxEntries(),
                                                                                  DatabaseDescriptor.getAuthorizer());

    private final String name;

    public AuthenticatedUser(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    /**
     * Checks the user's superuser status.
     * Only a superuser is allowed to perform CREATE USER and DROP USER queries.
     * Im most cased, though not necessarily, a superuser will have Permission.ALL on every resource
     * (depends on IAuthorizer implementation).
     */
    public boolean isSuper()
    {
        return !isAnonymous() && hasSuperuserRole();
    }

    private boolean hasSuperuserRole()
    {
        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        for (String role : getRoles())
            if (roleManager.isSuper(role))
                return true;
        return false;
    }

    /**
     * If IAuthenticator doesn't require authentication, this method may return true.
     */
    public boolean isAnonymous()
    {
        return this == ANONYMOUS_USER;
    }

    /**
     * Get the roles that have been granted to the user via the IRoleManager
     *
     * @return a list of roles that have been granted to the user
     */
    public Set<String> getRoles()
    {
        if (rolesCache == null)
            return loadRoles(name);

        try
        {
            return rolesCache.get(name);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Set<Permission> getPermissions(AuthenticatedUser user, IResource resource)
    {
        return permissionsCache.getPermissions(user, resource);
    }

    private static Set<String> loadRoles(String name)
    {
        try
        {
            return DatabaseDescriptor.getRoleManager().getRoles(name, true);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    private static LoadingCache<String, Set<String>> initRolesCache()
    {
        if (DatabaseDescriptor.getAuthenticator() instanceof AllowAllAuthenticator)
            return null;

        int validityPeriod = DatabaseDescriptor.getRolesValidity();
        if (validityPeriod <= 0)
            return null;

        return CacheBuilder.newBuilder()
                           .refreshAfterWrite(validityPeriod, TimeUnit.MILLISECONDS)
                           .build(new CacheLoader<String, Set<String>>()
                           {
                               public Set<String> load(String name)
                               {
                                   return loadRoles(name);
                               }

                               public ListenableFuture<Set<String>> reload(final String name, Set<String> oldValue)
                               {
                                   ListenableFutureTask<Set<String>> task = ListenableFutureTask.create(new Callable<Set<String>>()
                                   {
                                       public Set<String> call()
                                       {
                                           return loadRoles(name);
                                       }
                                   });
                                   ScheduledExecutors.optionalTasks.execute(task);
                                   return task;
                               }
                           });
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
