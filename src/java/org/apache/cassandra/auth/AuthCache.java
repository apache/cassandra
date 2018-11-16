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

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.cassandra.utils.MBeanWrapper;

import static com.google.common.base.Preconditions.checkNotNull;

public class AuthCache<K, V> implements AuthCacheMBean
{
    private static final Logger logger = LoggerFactory.getLogger(AuthCache.class);

    private static final String MBEAN_NAME_BASE = "org.apache.cassandra.auth:type=";

    /**
     * Underlying cache. LoadingCache will call underlying load function on {@link #get} if key is not present
     */
    protected volatile LoadingCache<K, V> cache;

    private String name;
    private IntConsumer setValidityDelegate;
    private IntSupplier getValidityDelegate;
    private IntConsumer setUpdateIntervalDelegate;
    private IntSupplier getUpdateIntervalDelegate;
    private IntConsumer setMaxEntriesDelegate;
    private IntSupplier getMaxEntriesDelegate;
    private Function<K, V> loadFunction;
    private BooleanSupplier enableCache;

    /**
     * @param name Used for MBean
     * @param setValidityDelegate Used to set cache validity period. See {@link Policy#expireAfterWrite()}
     * @param getValidityDelegate Getter for validity period
     * @param setUpdateIntervalDelegate Used to set cache update interval. See {@link Policy#refreshAfterWrite()}
     * @param getUpdateIntervalDelegate Getter for update interval
     * @param setMaxEntriesDelegate Used to set max # entries in cache. See {@link com.github.benmanes.caffeine.cache.Policy.Eviction#setMaximum(long)}
     * @param getMaxEntriesDelegate Getter for max entries.
     * @param loadFunction Function to load the cache. Called on {@link #get(Object)}
     * @param cacheEnabledDelegate Used to determine if cache is enabled.
     */
    protected AuthCache(String name,
                        IntConsumer setValidityDelegate,
                        IntSupplier getValidityDelegate,
                        IntConsumer setUpdateIntervalDelegate,
                        IntSupplier getUpdateIntervalDelegate,
                        IntConsumer setMaxEntriesDelegate,
                        IntSupplier getMaxEntriesDelegate,
                        Function<K, V> loadFunction,
                        BooleanSupplier cacheEnabledDelegate)
    {
        this.name = checkNotNull(name);
        this.setValidityDelegate = checkNotNull(setValidityDelegate);
        this.getValidityDelegate = checkNotNull(getValidityDelegate);
        this.setUpdateIntervalDelegate = checkNotNull(setUpdateIntervalDelegate);
        this.getUpdateIntervalDelegate = checkNotNull(getUpdateIntervalDelegate);
        this.setMaxEntriesDelegate = checkNotNull(setMaxEntriesDelegate);
        this.getMaxEntriesDelegate = checkNotNull(getMaxEntriesDelegate);
        this.loadFunction = checkNotNull(loadFunction);
        this.enableCache = checkNotNull(cacheEnabledDelegate);
        init();
    }

    /**
     * Do setup for the cache and MBean.
     */
    protected void init()
    {
        cache = initCache(null);
        MBeanWrapper.instance.registerMBean(this, getObjectName());
    }

    protected void unregisterMBean()
    {
        MBeanWrapper.instance.unregisterMBean(getObjectName(), MBeanWrapper.OnException.LOG);
    }

    protected String getObjectName()
    {
        return MBEAN_NAME_BASE + name;
    }

    /**
     * Retrieve a value from the cache. Will call {@link LoadingCache#get(Object)} which will
     * "load" the value if it's not present, thus populating the key.
     * @param k
     * @return The current value of {@code K} if cached or loaded.
     *
     * See {@link LoadingCache#get(Object)} for possible exceptions.
     */
    public V get(K k)
    {
        if (cache == null)
            return loadFunction.apply(k);

        return cache.get(k);
    }

    /**
     * Invalidate the entire cache.
     */
    public void invalidate()
    {
        cache = initCache(null);
    }

    /**
     * Invalidate a key
     * @param k key to invalidate
     */
    public void invalidate(K k)
    {
        if (cache != null)
            cache.invalidate(k);
    }

    /**
     * Time in milliseconds that a value in the cache will expire after.
     * @param validityPeriod in milliseconds
     */
    public void setValidity(int validityPeriod)
    {
        if (Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration"))
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setValidityDelegate.accept(validityPeriod);
        cache = initCache(cache);
    }

    public int getValidity()
    {
        return getValidityDelegate.getAsInt();
    }

    /**
     * Time in milliseconds after which an entry in the cache should be refreshed (it's load function called again)
     * @param updateInterval in milliseconds
     */
    public void setUpdateInterval(int updateInterval)
    {
        if (Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration"))
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setUpdateIntervalDelegate.accept(updateInterval);
        cache = initCache(cache);
    }

    public int getUpdateInterval()
    {
        return getUpdateIntervalDelegate.getAsInt();
    }

    /**
     * Set maximum number of entries in the cache.
     * @param maxEntries
     */
    public void setMaxEntries(int maxEntries)
    {
        if (Boolean.getBoolean("cassandra.disable_auth_caches_remote_configuration"))
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setMaxEntriesDelegate.accept(maxEntries);
        cache = initCache(cache);
    }

    public int getMaxEntries()
    {
        return getMaxEntriesDelegate.getAsInt();
    }

    /**
     * (Re-)initialise the underlying cache. Will update validity, max entries, and update interval if
     * any have changed. The underlying {@link LoadingCache} will be initiated based on the provided {@code loadFunction}.
     * Note: If you need some unhandled cache setting to be set you should extend {@link AuthCache} and override this method.
     * @param existing If not null will only update cache update validity, max entries, and update interval.
     * @return New {@link LoadingCache} if existing was null, otherwise the existing {@code cache}
     */
    protected LoadingCache<K, V> initCache(LoadingCache<K, V> existing)
    {
        if (!enableCache.getAsBoolean())
            return null;

        if (getValidity() <= 0)
            return null;

        logger.info("(Re)initializing {} (validity period/update interval/max entries) ({}/{}/{})",
                    name, getValidity(), getUpdateInterval(), getMaxEntries());

        if (existing == null) {
          return Caffeine.newBuilder()
                         .refreshAfterWrite(getUpdateInterval(), TimeUnit.MILLISECONDS)
                         .expireAfterWrite(getValidity(), TimeUnit.MILLISECONDS)
                         .maximumSize(getMaxEntries())
                         .executor(MoreExecutors.directExecutor())
                         .build(loadFunction::apply);
        }

        // Always set as mandatory
        cache.policy().refreshAfterWrite().ifPresent(policy ->
            policy.setExpiresAfter(getUpdateInterval(), TimeUnit.MILLISECONDS));
        cache.policy().expireAfterWrite().ifPresent(policy ->
            policy.setExpiresAfter(getValidity(), TimeUnit.MILLISECONDS));
        cache.policy().eviction().ifPresent(policy ->
            policy.setMaximum(getMaxEntries()));
        return cache;
    }
}
