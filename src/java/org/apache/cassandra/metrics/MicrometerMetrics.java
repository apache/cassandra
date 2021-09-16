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

import java.util.function.ToDoubleFunction;

import com.google.common.base.Preconditions;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.cassandra.utils.Pair;

public abstract class MicrometerMetrics
{
    private volatile Pair<MeterRegistry, Tags> registryWithTags;

    protected MicrometerMetrics()
    {
        this.registryWithTags = Pair.create(new SimpleMeterRegistry(), Tags.empty());
    }

    public Counter counter(String name)
    {
        Pair<MeterRegistry, Tags> current = registryWithTags;
        return current.left.counter(name, current.right);
    }

    public Counter counter(String name, Tags tags)
    {
        Pair<MeterRegistry, Tags> current = registryWithTags;
        return current.left.counter(name, current.right.and(tags));
    }

    public Timer timer(String name)
    {
        Pair<MeterRegistry, Tags> current = registryWithTags;
        return current.left.timer(name, current.right);
    }

    public <T> T gauge(String name, T obj, ToDoubleFunction<T> fcn)
    {
        Pair<MeterRegistry, Tags> current = registryWithTags;
        return current.left.gauge(name, current.right, obj, fcn);
    }

    public synchronized void register(MeterRegistry newRegistry, Tags newTags)
    {
        Preconditions.checkArgument(!this.registryWithTags.left.equals(newRegistry), "Cannot set the same registry twice!");
        this.registryWithTags = Pair.create(newRegistry, newTags);
    }
}
