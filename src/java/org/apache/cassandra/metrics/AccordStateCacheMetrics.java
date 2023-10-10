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

import com.codahale.metrics.Histogram;

import static org.apache.cassandra.metrics.CacheMetrics.CACHE;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class AccordStateCacheMetrics extends CacheAccessMetrics
{
    public static final String OBJECT_SIZE = "ObjectSize";

    public final Histogram objectSize;

    private final Map<Class<?>, CacheAccessMetrics> instanceMetrics = new ConcurrentHashMap<>(2);

    private final String type;

    public AccordStateCacheMetrics(String type)
    {
        super(new DefaultNameFactory(CACHE, type));
        objectSize = Metrics.histogram(factory.createMetricName(OBJECT_SIZE), false);
        this.type = type;
    }

    public CacheAccessMetrics forInstance(Class<?> klass)
    {
        return instanceMetrics.computeIfAbsent(klass, k -> new CacheAccessMetrics(new DefaultNameFactory(CACHE, String.format("%s-%s", type, k.getSimpleName()))));
    }
}
