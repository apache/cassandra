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
package org.apache.cassandra.service.thresholds;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Immutable counter tracking which replicas reported warnings/aborts and the maximum value.
 * Shared by both read and write threshold tracking.
 *
 * This class is extracted from the duplicated Counter inner classes in:
 * - WarningsSnapshot.Counter (read thresholds)
 * - WriteWarningsSnapshot.Counter (write thresholds)
 */
public final class ThresholdCounter
{
    private static final ThresholdCounter EMPTY = new ThresholdCounter(ImmutableSet.of(), 0);

    public final ImmutableSet<InetAddressAndPort> instances;
    public final long maxValue;

    @VisibleForTesting
    public ThresholdCounter(ImmutableSet<InetAddressAndPort> instances, long maxValue)
    {
        this.instances = instances;
        this.maxValue = maxValue;
    }

    @VisibleForTesting
    public static ThresholdCounter empty()
    {
        return EMPTY;
    }

    public boolean isEmpty()
    {
        return instances.isEmpty();
    }

    public static ThresholdCounter create(Set<InetAddressAndPort> instances, AtomicLong maxValue)
    {
        ImmutableSet<InetAddressAndPort> copy = ImmutableSet.copyOf(instances);
        // if instances is empty ignore value
        // writes and reads are concurrent (write = networking callback, read = coordinator thread), so there is
        // an edge case where instances is empty and maxValue > 0; this is caused by the fact we update value first before count
        // we write: value then instance
        // we read: instance then value
        if (copy.isEmpty())
            return EMPTY;
        return new ThresholdCounter(copy, maxValue.get());
    }

    public ThresholdCounter merge(ThresholdCounter other)
    {
        if (other.isEmpty())
            return this;
        ImmutableSet<InetAddressAndPort> copy = ImmutableSet.<InetAddressAndPort>builder()
                                                .addAll(instances)
                                                .addAll(other.instances)
                                                .build();
        return new ThresholdCounter(copy, Math.max(maxValue, other.maxValue));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ThresholdCounter counter = (ThresholdCounter) o;
        return maxValue == counter.maxValue && Objects.equals(instances, counter.instances);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(instances, maxValue);
    }

    @Override
    public String toString()
    {
        return "(instances=" + instances.size() + ", max=" + maxValue + ')';
    }
}