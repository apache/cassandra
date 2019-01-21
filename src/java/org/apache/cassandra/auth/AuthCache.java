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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.utils.MBeanWrapper;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;

public class AuthCache<K, V> implements AuthCacheMBean
{
    private static final Logger logger = LoggerFactory.getLogger(AuthCache.class);

    private static final String MBEAN_NAME_BASE = "org.apache.cassandra.auth:type=";

    private volatile LoadingCache<K, V> cache;
    private ThreadPoolExecutor cacheRefreshExecutor;

    private final String name;
    private final Consumer<Integer> setValidityDelegate;
    private final Supplier<Integer> getValidityDelegate;
    private final Consumer<Integer> setUpdateIntervalDelegate;
    private final Supplier<Integer> getUpdateIntervalDelegate;
    private final Consumer<Integer> setMaxEntriesDelegate;
    private final Supplier<Integer> getMaxEntriesDelegate;
    private final Function<K, V> loadFunction;
    private final Supplier<Boolean> enableCache;

    protected AuthCache(String name,
                        Consumer<Integer> setValidityDelegate,
                        Supplier<Integer> getValidityDelegate,
                        Consumer<Integer> setUpdateIntervalDelegate,
                        Supplier<Integer> getUpdateIntervalDelegate,
                        Consumer<Integer> setMaxEntriesDelegate,
                        Supplier<Integer> getMaxEntriesDelegate,
                        Function<K, V> loadFunction,
                        Supplier<Boolean> enableCache)
    {
        this.name = name;
        this.setValidityDelegate = setValidityDelegate;
        this.getValidityDelegate = getValidityDelegate;
        this.setUpdateIntervalDelegate = setUpdateIntervalDelegate;
        this.getUpdateIntervalDelegate = getUpdateIntervalDelegate;
        this.setMaxEntriesDelegate = setMaxEntriesDelegate;
        this.getMaxEntriesDelegate = getMaxEntriesDelegate;
        this.loadFunction = loadFunction;
        this.enableCache = enableCache;
        init();
    }

    protected void init()
    {
        this.cacheRefreshExecutor = new DebuggableThreadPoolExecutor(name + "Refresh", Thread.NORM_PRIORITY);
        this.cache = initCache(null);
        MBeanWrapper.instance.registerMBean(this, getObjectName());
    }

    protected String getObjectName()
    {
        return MBEAN_NAME_BASE + name;
    }

    public V get(K k) throws ExecutionException
    {
        if (cache == null)
            return loadFunction.apply(k);

        return cache.get(k);
    }

    public void invalidate()
    {
        cache = initCache(null);
    }

    public void invalidate(K k)
    {
        if (cache != null)
            cache.invalidate(k);
    }

    public void setValidity(int validityPeriod)
    {
        if (Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration"))
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setValidityDelegate.accept(validityPeriod);
        cache = initCache(cache);
    }

    public int getValidity()
    {
        return getValidityDelegate.get();
    }

    public void setUpdateInterval(int updateInterval)
    {
        if (Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration"))
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setUpdateIntervalDelegate.accept(updateInterval);
        cache = initCache(cache);
    }

    public int getUpdateInterval()
    {
        return getUpdateIntervalDelegate.get();
    }

    public void setMaxEntries(int maxEntries)
    {
        if (Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration"))
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setMaxEntriesDelegate.accept(maxEntries);
        cache = initCache(cache);
    }

    public int getMaxEntries()
    {
        return getMaxEntriesDelegate.get();
    }

    private LoadingCache<K, V> initCache(LoadingCache<K, V> existing)
    {
        if (!enableCache.get())
            return null;

        if (getValidity() <= 0)
            return null;

        logger.info("(Re)initializing {} (validity period/update interval/max entries) ({}/{}/{})",
                    name, getValidity(), getUpdateInterval(), getMaxEntries());

        LoadingCache<K, V> newcache = CacheBuilder.newBuilder()
                           .refreshAfterWrite(getUpdateInterval(), TimeUnit.MILLISECONDS)
                           .expireAfterWrite(getValidity(), TimeUnit.MILLISECONDS)
                           .maximumSize(getMaxEntries())
                           .build(new CacheLoader<K, V>()
                           {
                               public V load(K k)
                               {
                                   return loadFunction.apply(k);
                               }

                               public ListenableFuture<V> reload(final K k, final V oldV)
                               {
                                   ListenableFutureTask<V> task = ListenableFutureTask.create(() -> {
                                       try
                                       {
                                           return loadFunction.apply(k);
                                       }
                                       catch (Exception e)
                                       {
                                           logger.trace("Error performing async refresh of auth data in {}", name, e);
                                           throw e;
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
