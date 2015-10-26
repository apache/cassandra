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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public class RolesCache implements RolesCacheMBean
{
    private static final Logger logger = LoggerFactory.getLogger(RolesCache.class);

    private final String MBEAN_NAME = "org.apache.cassandra.auth:type=RolesCache";
    private final ThreadPoolExecutor cacheRefreshExecutor = new DebuggableThreadPoolExecutor("RolesCacheRefresh",
                                                                                             Thread.NORM_PRIORITY);
    private final IRoleManager roleManager;
    private volatile LoadingCache<RoleResource, Set<RoleResource>> cache;

    public RolesCache(IRoleManager roleManager)
    {
        this.roleManager = roleManager;
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

    public void invalidate()
    {
        cache = initCache(null);
    }

    public void setValidity(int validityPeriod)
    {
        DatabaseDescriptor.setRolesValidity(validityPeriod);
        cache = initCache(cache);
    }

    public int getValidity()
    {
        return DatabaseDescriptor.getRolesValidity();
    }

    public void setUpdateInterval(int updateInterval)
    {
        DatabaseDescriptor.setRolesUpdateInterval(updateInterval);
        cache = initCache(cache);
    }

    public int getUpdateInterval()
    {
        return DatabaseDescriptor.getRolesUpdateInterval();
    }


    private LoadingCache<RoleResource, Set<RoleResource>> initCache(LoadingCache<RoleResource, Set<RoleResource>> existing)
    {
        if (!DatabaseDescriptor.getAuthenticator().requireAuthentication())
            return null;

        if (DatabaseDescriptor.getRolesValidity() <= 0)
            return null;

        LoadingCache<RoleResource, Set<RoleResource>> newcache = CacheBuilder.newBuilder()
                .refreshAfterWrite(DatabaseDescriptor.getRolesUpdateInterval(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(DatabaseDescriptor.getRolesValidity(), TimeUnit.MILLISECONDS)
                .maximumSize(DatabaseDescriptor.getRolesCacheMaxEntries())
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
                                } catch (Exception e)
                                {
                                    logger.trace("Error performing async refresh of user roles", e);
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
