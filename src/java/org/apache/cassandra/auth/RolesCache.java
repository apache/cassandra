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
import java.util.concurrent.*;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;

public class RolesCache
{
    private static final Logger logger = LoggerFactory.getLogger(RolesCache.class);

    private final ThreadPoolExecutor cacheRefreshExecutor = new DebuggableThreadPoolExecutor("RolesCacheRefresh",
                                                                                             Thread.NORM_PRIORITY);
    private final IRoleManager roleManager;
    private final LoadingCache<RoleResource, Set<RoleResource>> cache;

    public RolesCache(int validityPeriod, int updateInterval, int maxEntries, IRoleManager roleManager)
    {
        this.roleManager = roleManager;
        this.cache = initCache(validityPeriod, updateInterval, maxEntries);
    }

    public Set<RoleResource> getRoles(RoleResource role)
    {
        if (cache == null)
            return roleManager.getRoles(role, true);

        try
        {
            return cache.get(role);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private LoadingCache<RoleResource, Set<RoleResource>> initCache(int validityPeriod,
                                                                    int updateInterval,
                                                                    int maxEntries)
    {
        if (DatabaseDescriptor.getAuthenticator() instanceof AllowAllAuthenticator)
            return null;

        if (validityPeriod <= 0)
            return null;

        return CacheBuilder.newBuilder()
                           .refreshAfterWrite(updateInterval, TimeUnit.MILLISECONDS)
                           .expireAfterWrite(validityPeriod, TimeUnit.MILLISECONDS)
                           .maximumSize(maxEntries)
                           .build(new CacheLoader<RoleResource, Set<RoleResource>>()
                           {
                               public Set<RoleResource> load(RoleResource primaryRole)
                               {
                                   return roleManager.getRoles(primaryRole, true);
                               }

                               public ListenableFuture<Set<RoleResource>> reload(final RoleResource primaryRole,
                                                                                 final Set<RoleResource> oldValue)
                               {
                                   ListenableFutureTask<Set<RoleResource>> task;
                                   task = ListenableFutureTask.create(new Callable<Set<RoleResource>>()
                                   {
                                       public Set<RoleResource> call() throws Exception
                                       {
                                           try
                                           {
                                               return roleManager.getRoles(primaryRole, true);
                                           }
                                           catch (Exception e)
                                           {
                                               logger.debug("Error performing async refresh of user roles", e);
                                               throw e;
                                           }
                                       }
                                   });
                                   cacheRefreshExecutor.execute(task);
                                   return task;
                               }
                           });
    }
}
