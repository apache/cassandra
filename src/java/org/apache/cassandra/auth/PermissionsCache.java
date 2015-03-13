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

import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.utils.Pair;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public class PermissionsCache implements PermissionsCacheMBean
{
    private static final Logger logger = LoggerFactory.getLogger(PermissionsCache.class);

    private final String MBEAN_NAME = "org.apache.cassandra.auth:type=PermissionsCache";

    private final ThreadPoolExecutor cacheRefreshExecutor = new DebuggableThreadPoolExecutor("PermissionsCacheRefresh",
                                                                                             Thread.NORM_PRIORITY);
    private final IAuthorizer authorizer;
    private volatile LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> cache;

    public PermissionsCache(IAuthorizer authorizer)
    {
        this.authorizer = authorizer;
        this.cache = initCache(null);
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public Set<Permission> getPermissions(AuthenticatedUser user, IResource resource)
    {
        if (cache == null)
            return authorizer.authorize(user, resource);

        try
        {
            return cache.get(Pair.create(user, resource));
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void invalidate()
    {
        cache = initCache(null);
    }

    public void setValidity(int validityPeriod)
    {
        DatabaseDescriptor.setPermissionsValidity(validityPeriod);
        cache = initCache(cache);
    }

    public int getValidity()
    {
        return DatabaseDescriptor.getPermissionsValidity();
    }

    public void setUpdateInterval(int updateInterval)
    {
        DatabaseDescriptor.setPermissionsUpdateInterval(updateInterval);
        cache = initCache(cache);
    }

    public int getUpdateInterval()
    {
        return DatabaseDescriptor.getPermissionsUpdateInterval();
    }

    private LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> initCache(
                                                             LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> existing)
    {
        if (authorizer instanceof AllowAllAuthorizer)
            return null;

        if (DatabaseDescriptor.getPermissionsValidity() <= 0)
            return null;

        LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> newcache = CacheBuilder.newBuilder()
                           .refreshAfterWrite(DatabaseDescriptor.getPermissionsUpdateInterval(), TimeUnit.MILLISECONDS)
                           .expireAfterWrite(DatabaseDescriptor.getPermissionsValidity(), TimeUnit.MILLISECONDS)
                           .maximumSize(DatabaseDescriptor.getPermissionsCacheMaxEntries())
                           .build(new CacheLoader<Pair<AuthenticatedUser, IResource>, Set<Permission>>()
                           {
                               public Set<Permission> load(Pair<AuthenticatedUser, IResource> userResource)
                               {
                                   return authorizer.authorize(userResource.left, userResource.right);
                               }

                               public ListenableFuture<Set<Permission>> reload(final Pair<AuthenticatedUser, IResource> userResource,
                                                                               final Set<Permission> oldValue)
                               {
                                   ListenableFutureTask<Set<Permission>> task = ListenableFutureTask.create(new Callable<Set<Permission>>()
                                   {
                                       public Set<Permission>call() throws Exception
                                       {
                                           try
                                           {
                                               return authorizer.authorize(userResource.left, userResource.right);
                                           }
                                           catch (Exception e)
                                           {
                                               logger.debug("Error performing async refresh of user permissions", e);
                                               throw e;
                                           }
                                       }
                                   });
                                   cacheRefreshExecutor.execute(task);
                                   return task;
                               }
                           });
        if (existing != null)
            newcache.putAll(existing.asMap());
        return newcache;
    }
}
