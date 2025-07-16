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

package org.apache.cassandra.metrics;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.FBUtilities.getAvailableProcessors;

/**
 * Abstract class for managing metrics.
 * This class provides a thread-safe way to create and retrieve metrics.
 * It supports both synchronous and asynchronous registration of metrics.
 *
 * @param <K> the type of key used to identify metrics
 * @param <V> the type of metric instance being managed
 */
public abstract class AbstractMetricsManager<K, V>
{
    private final Map<K, V> metrics = new ConcurrentHashMap<>();

    // Flag to indicate if metrics registration (first time call) should be done asynchronously
    private final boolean asyncRegistration;
    private final ExecutorPlus executor;
    private static final int MAX_THREAD = getAvailableProcessors();

    protected AbstractMetricsManager(boolean asyncRegistration) {
        this.asyncRegistration = asyncRegistration;
        this.executor = asyncRegistration ? executorFactory()
                                             .withJmxInternal()
                                             .configurePooled(this.getClass().getSimpleName(), MAX_THREAD)
                                             .withThreadPriority(Thread.MIN_PRIORITY)
                                             .build()
                                          : null;
    }

    /**
     * Creates a new metric instance for the given key.
     *
     * @param key the key for which the metric is created
     * @return a new metric instance of type V
     */
    protected abstract V createMetric(K key);

    /**
     * Builds the key K from the provided variable arguments.
     *
     * @param parts the parts to build the key from
     * @return the key instance K
     * @throws IllegalArgumentException if the parts do not match the expected format
     */
    protected abstract K buildKey(Object... parts) throws IllegalArgumentException;

    /**
     * Retrieves the metric for the given parts, creating it if it does not exist.
     * @param parts the parts to build the key from
     * @return the metric instance of type V
     */
    public V getMetricsSync(Object... parts)
    {
        return getMetricsSync(buildKey(parts));
    }

    public V getMetricsSync(K key)
    {
        if (asyncRegistration)
            throw new IllegalStateException("getMetricsSync is not supported for " + this.getClass().getSimpleName() +
                                             " when asyncRegistration is enabled. Use maybeRegisterMetricsAsync instead. " +
                                            "Metric JMX registration can be heavy in some cases (e.g. on node startup we " +
                                            "register 1 metric per client service).");
        return metrics.computeIfAbsent(key, this::createMetric);
    }

    @VisibleForTesting
    protected V getMetricsSyncWithoutRegistration(K key)
    {
        return metrics.get(key);
    }

    @VisibleForTesting
    // TODO: require this method for subclass. V must be "ReleasableMetric" to force the release method to be called together
    protected void release(K key)
    {
        metrics.remove(key);
    }

    /**
     * Asynchronously registers the metric and apply the update if it does not already exist.
     * @param onRegistered a consumer that will be called with the metric once it is registered
     * @param key the key for which the metric is registered
     * @return a Future that completes when the metric is registered, result is true if the metric was newly registered,
     *         false if it already existed
     */
    public Future<Boolean> maybeRegisterMetricsAsync(Consumer<V> onRegistered, K key)
    {
        V metric = metrics.get(key);
        if (metric != null)
        {
            if (onRegistered != null)
                onRegistered.accept(metric);
            return ImmediateFuture.success(false);
        }
        return submitTask(() -> {
            V registered = metrics.computeIfAbsent(key, this::createMetric); // create entry synchronously
            if (onRegistered != null)
                onRegistered.accept(registered);
            return true;
        });
    }

    private <T> Future<T> submitTask(Callable<T> task) throws IllegalStateException
    {
        if (!asyncRegistration || executor == null)
            throw new IllegalStateException("Async is not supported for " + this.getClass().getSimpleName());
        return executor.submit(task);
    }
}
