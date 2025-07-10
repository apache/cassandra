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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract class for managing metrics.
 * This class provides a thread-safe way to create and retrieve metrics.
 * It supports both synchronous and asynchronous operations.
 *
 * @param <K> the type of key used to identify metrics
 * @param <V> the type of metric instance being managed
 */
public abstract class AbstractMetricsManager<K, V>
{
    protected final Map<K, V> metrics = new ConcurrentHashMap<>();
    protected AbstractMetricsManager() {}

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
        K key = buildKey(parts);
        return metrics.computeIfAbsent(key, this::createMetric);
    }
}
