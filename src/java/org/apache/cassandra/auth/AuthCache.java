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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Policy;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.MBeanWrapper;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.AUTH_CACHE_WARMING_MAX_RETRIES;
import static org.apache.cassandra.config.CassandraRelevantProperties.AUTH_CACHE_WARMING_RETRY_INTERVAL_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_AUTH_CACHES_REMOTE_CONFIGURATION;

public class AuthCache<K, V> implements AuthCacheMBean, Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(AuthCache.class);

    public static final String MBEAN_NAME_BASE = "org.apache.cassandra.auth:type=";

    private volatile ScheduledFuture cacheRefresher = null;

    // Keep a handle on created instances so their executors can be terminated cleanly
    private static final Set<Shutdownable> REGISTRY = new HashSet<>(4);

    public static void shutdownAllAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, REGISTRY);
    }

    /**
     * Underlying cache. LoadingCache will call underlying load function on {@link #get} if key is not present
     */
    protected volatile LoadingCache<K, V> cache;
    private ExecutorPlus cacheRefreshExecutor;

    private final String name;
    private final IntConsumer setValidityDelegate;
    private final IntSupplier getValidityDelegate;
    private final IntConsumer setUpdateIntervalDelegate;
    private final IntSupplier getUpdateIntervalDelegate;
    private final IntConsumer setMaxEntriesDelegate;
    private final IntSupplier getMaxEntriesDelegate;
    private final Consumer<Boolean> setActiveUpdate;
    private final BooleanSupplier getActiveUpdate;
    private final Function<K, V> loadFunction;
    private final Supplier<Map<K, V>> bulkLoadFunction;
    private final BooleanSupplier enableCache;

    // Determines whether the presence of a specific value should trigger the invalidation of
    // the supplied key. Used by CredentialsCache & CacheRefresher to identify when the
    // credentials for a role couldn't be loaded without throwing an exception or serving stale
    // values until the natural expiry time.
    private final BiPredicate<K, V> invalidateCondition;

    /**
     * @param name Used for MBean
     * @param setValidityDelegate Used to set cache validity period. See {@link Policy#expireAfterWrite()}
     * @param getValidityDelegate Getter for validity period
     * @param setUpdateIntervalDelegate Used to set cache update interval. See {@link Policy#refreshAfterWrite()}
     * @param getUpdateIntervalDelegate Getter for update interval
     * @param setMaxEntriesDelegate Used to set max # entries in cache. See {@link com.github.benmanes.caffeine.cache.Policy.Eviction#setMaximum(long)}
     * @param getMaxEntriesDelegate Getter for max entries.
     * @param setActiveUpdate Method to process config to actively update the auth cache prior to configured cache expiration
     * @param getActiveUpdate Getter for active update
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
                        Consumer<Boolean> setActiveUpdate,
                        BooleanSupplier getActiveUpdate,
                        Function<K, V> loadFunction,
                        Supplier<Map<K, V>> bulkLoadFunction,
                        BooleanSupplier cacheEnabledDelegate)
    {
        this(name,
             setValidityDelegate,
             getValidityDelegate,
             setUpdateIntervalDelegate,
             getUpdateIntervalDelegate,
             setMaxEntriesDelegate,
             getMaxEntriesDelegate,
             setActiveUpdate,
             getActiveUpdate,
             loadFunction,
             bulkLoadFunction,
             cacheEnabledDelegate,
             (k, v) -> false);
    }

    /**
     * @param name Used for MBean
     * @param setValidityDelegate Used to set cache validity period. See {@link Policy#expireAfterWrite()}
     * @param getValidityDelegate Getter for validity period
     * @param setUpdateIntervalDelegate Used to set cache update interval. See {@link Policy#refreshAfterWrite()}
     * @param getUpdateIntervalDelegate Getter for update interval
     * @param setMaxEntriesDelegate Used to set max # entries in cache. See {@link com.github.benmanes.caffeine.cache.Policy.Eviction#setMaximum(long)}
     * @param getMaxEntriesDelegate Getter for max entries.
     * @param setActiveUpdate Actively update the cache before expiry
     * @param getActiveUpdate Getter for active update
     * @param loadFunction Function to load the cache. Called on {@link #get(Object)}
     * @param cacheEnabledDelegate Used to determine if cache is enabled.
     * @param invalidationCondition Used during active updates to determine if a refreshed value indicates a missing
     *                              entry in the underlying table. If satisfied, the key will be invalidated.
     */
    protected AuthCache(String name,
                        IntConsumer setValidityDelegate,
                        IntSupplier getValidityDelegate,
                        IntConsumer setUpdateIntervalDelegate,
                        IntSupplier getUpdateIntervalDelegate,
                        IntConsumer setMaxEntriesDelegate,
                        IntSupplier getMaxEntriesDelegate,
                        Consumer<Boolean> setActiveUpdate,
                        BooleanSupplier getActiveUpdate,
                        Function<K, V> loadFunction,
                        Supplier<Map<K, V>> bulkLoadFunction,
                        BooleanSupplier cacheEnabledDelegate,
                        BiPredicate<K, V> invalidationCondition)
    {
        this.name = checkNotNull(name);
        this.setValidityDelegate = checkNotNull(setValidityDelegate);
        this.getValidityDelegate = checkNotNull(getValidityDelegate);
        this.setUpdateIntervalDelegate = checkNotNull(setUpdateIntervalDelegate);
        this.getUpdateIntervalDelegate = checkNotNull(getUpdateIntervalDelegate);
        this.setMaxEntriesDelegate = checkNotNull(setMaxEntriesDelegate);
        this.getMaxEntriesDelegate = checkNotNull(getMaxEntriesDelegate);
        this.setActiveUpdate = checkNotNull(setActiveUpdate);
        this.getActiveUpdate = checkNotNull(getActiveUpdate);
        this.loadFunction = checkNotNull(loadFunction);
        this.bulkLoadFunction = checkNotNull(bulkLoadFunction);
        this.enableCache = checkNotNull(cacheEnabledDelegate);
        this.invalidateCondition = checkNotNull(invalidationCondition);
        init();
    }

    /**
     * Do setup for the cache and MBean.
     */
    protected void init()
    {
        this.cacheRefreshExecutor = executorFactory().sequential(name + "Refresh");
        cache = initCache(null);
        MBeanWrapper.instance.registerMBean(this, getObjectName());
        REGISTRY.add(this);
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
     * Retrieve all cached entries. Will call {@link LoadingCache#asMap()} which does not trigger "load".
     * @return a map of cached key-value pairs
     */
    public Map<K, V> getAll()
    {
        if (cache == null)
            return Collections.emptyMap();

        return Collections.unmodifiableMap(cache.asMap());
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
    public synchronized void invalidate()
    {
        cache = initCache(null);
    }

    /**
     * Invalidate a key.
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
    public synchronized void setValidity(int validityPeriod)
    {
        if (DISABLE_AUTH_CACHES_REMOTE_CONFIGURATION.getBoolean())
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
    public synchronized void setUpdateInterval(int updateInterval)
    {
        if (DISABLE_AUTH_CACHES_REMOTE_CONFIGURATION.getBoolean())
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
    public synchronized void setMaxEntries(int maxEntries)
    {
        if (DISABLE_AUTH_CACHES_REMOTE_CONFIGURATION.getBoolean())
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setMaxEntriesDelegate.accept(maxEntries);
        cache = initCache(cache);
    }

    public int getMaxEntries()
    {
        return getMaxEntriesDelegate.getAsInt();
    }

    public boolean getActiveUpdate()
    {
        return getActiveUpdate.getAsBoolean();
    }

    public synchronized void setActiveUpdate(boolean update)
    {
        if (DISABLE_AUTH_CACHES_REMOTE_CONFIGURATION.getBoolean())
            throw new UnsupportedOperationException("Remote configuration of auth caches is disabled");

        setActiveUpdate.accept(update);
        cache = initCache(cache);
    }

    public long getEstimatedSize()
    {
        return cache == null ? 0L : cache.estimatedSize();
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

        boolean activeUpdate = getActiveUpdate();
        logger.info("(Re)initializing {} (validity period/update interval/max entries/active update) ({}/{}/{}/{})",
                    name, getValidity(), getUpdateInterval(), getMaxEntries(), activeUpdate);
        LoadingCache<K, V> updatedCache;

        if (existing == null)
        {
            updatedCache = Caffeine.newBuilder().refreshAfterWrite(activeUpdate ? getValidity() : getUpdateInterval(), TimeUnit.MILLISECONDS)
                                   .expireAfterWrite(getValidity(), TimeUnit.MILLISECONDS)
                                   .maximumSize(getMaxEntries())
                                   .executor(cacheRefreshExecutor)
                                   .build(loadFunction::apply);
        }
        else
        {
            updatedCache = cache;
            // Always set as mandatory
            cache.policy().refreshAfterWrite().ifPresent(policy ->
                policy.setRefreshesAfter(activeUpdate ? getValidity() : getUpdateInterval(), TimeUnit.MILLISECONDS));
            cache.policy().expireAfterWrite().ifPresent(policy -> policy.setExpiresAfter(getValidity(), TimeUnit.MILLISECONDS));
            cache.policy().eviction().ifPresent(policy -> policy.setMaximum(getMaxEntries()));
        }

        if (cacheRefresher != null)
        {
            cacheRefresher.cancel(false); // permit the two refreshers to race until the old one dies, should be harmless.
            cacheRefresher = null;
        }

        if (activeUpdate)
        {
            cacheRefresher = ScheduledExecutors.optionalTasks.scheduleAtFixedRate(CacheRefresher.create(name,
                                                                                                        updatedCache,
                                                                                                        invalidateCondition),
                                                                                  getUpdateInterval(),
                                                                                  getUpdateInterval(),
                                                                                  TimeUnit.MILLISECONDS);
        }
        return updatedCache;
    }

    @Override
    public boolean isTerminated()
    {
        return cacheRefreshExecutor.isTerminated();
    }

    @Override
    public void shutdown()
    {
        cacheRefreshExecutor.shutdown();
    }

    @Override
    public Object shutdownNow()
    {
        return cacheRefreshExecutor.shutdownNow();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        return cacheRefreshExecutor.awaitTermination(timeout, units);
    }

    public void warm()
    {
        if (cache == null)
        {
            logger.info("{} cache not enabled, skipping pre-warming", name);
            return;
        }

        int retries = AUTH_CACHE_WARMING_MAX_RETRIES.getInt(10);
        long retryInterval = AUTH_CACHE_WARMING_RETRY_INTERVAL_MS.getLong(1000);

        while (retries-- > 0)
        {
            try
            {
                Map<K, V> entries = bulkLoadFunction.get();
                cache.putAll(entries);
                break;
            }
            catch (Exception e)
            {
                Uninterruptibles.sleepUninterruptibly(retryInterval, TimeUnit.MILLISECONDS);
            }
        }
    }

    /*
     * Implemented when we can provide an efficient way to bulk load all entries for a cache. This isn't a
     * @FunctionalInterface due to the default impl, which is for IRoleManager, IAuthorizer, and INetworkAuthorizer.
     * They all extend this interface so that implementations only need to provide an override if it's useful.
     * IAuthenticator doesn't implement this interface because CredentialsCache is more tightly coupled to
     * PasswordAuthenticator, which does expose a bulk loader.
     */
    public interface BulkLoader<K, V>
    {
        default Supplier<Map<K, V>> bulkLoader()
        {
            return Collections::emptyMap;
        }
    }
}
