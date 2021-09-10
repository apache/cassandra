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
package org.apache.cassandra.service.reads.trackwarnings;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.locator.InetAddressAndPort;

public class WarningsSnapshot
{
    private static final WarningsSnapshot EMPTY = new WarningsSnapshot(Warnings.EMPTY, Warnings.EMPTY, Warnings.EMPTY);

    public final Warnings tombstones, localReadSize, rowIndexTooLarge;

    private WarningsSnapshot(Warnings tombstones, Warnings localReadSize, Warnings rowIndexTooLarge)
    {
        this.tombstones = tombstones;
        this.localReadSize = localReadSize;
        this.rowIndexTooLarge = rowIndexTooLarge;
    }

    public static WarningsSnapshot create(Warnings tombstones, Warnings localReadSize, Warnings rowIndexTooLarge)
    {
        if (tombstones == localReadSize && tombstones == rowIndexTooLarge && tombstones == Warnings.EMPTY)
            return EMPTY;
        return new WarningsSnapshot(tombstones, localReadSize, rowIndexTooLarge);
    }

    public static WarningsSnapshot merge(WarningsSnapshot... values)
    {
        if (values == null || values.length == 0)
            return null;

        WarningsSnapshot accum = EMPTY;
        for (WarningsSnapshot a : values)
            accum = accum.merge(a);
        return accum == EMPTY ? null : accum;
    }

    public boolean isEmpty()
    {
        return this == EMPTY;
    }

    private WarningsSnapshot merge(WarningsSnapshot other)
    {
        if (other == null || other == EMPTY)
            return this;
        return WarningsSnapshot.create(tombstones.merge(other.tombstones), localReadSize.merge(other.localReadSize), rowIndexTooLarge.merge(other.rowIndexTooLarge));
    }

    public static final class Warnings
    {
        private static final Warnings EMPTY = new Warnings(Counter.EMPTY, Counter.EMPTY);

        public final Counter warnings;
        public final Counter aborts;

        private Warnings(Counter warnings, Counter aborts)
        {
            this.warnings = warnings;
            this.aborts = aborts;
        }

        public static Warnings create(Counter warnings, Counter aborts)
        {
            if (warnings == Counter.EMPTY && aborts == Counter.EMPTY)
                return EMPTY;
            return new Warnings(warnings, aborts);
        }

        public Warnings merge(Warnings other)
        {
            if (other == EMPTY)
                return this;
            return Warnings.create(warnings.merge(other.warnings), aborts.merge(other.aborts));
        }
    }

    public static final class Counter
    {
        private static final Counter EMPTY = new Counter(ImmutableSet.of(), 0);

        public final ImmutableSet<InetAddressAndPort> instances;
        public final long maxValue;

        private Counter(ImmutableSet<InetAddressAndPort> instances, long maxValue)
        {
            this.instances = instances;
            this.maxValue = maxValue;
        }

        public static Counter create(Set<InetAddressAndPort> instances, AtomicLong maxValue)
        {
            ImmutableSet<InetAddressAndPort> copy = ImmutableSet.copyOf(instances);
            // if instances is empty ignore value
            // writes and reads are concurrent (write = networking callback, read = coordinator thread), so there is
            // an edge case where instances is empty and maxValue > 0; this is caused by the fact we update value first before count
            // we write: value then instance
            // we read: instance then value
            if (copy.isEmpty())
                return EMPTY;
            return new Counter(copy, maxValue.get());
        }

        public Counter merge(Counter other)
        {
            if (other == EMPTY)
                return this;
            ImmutableSet<InetAddressAndPort> copy = ImmutableSet.<InetAddressAndPort>builder()
                                                    .addAll(instances)
                                                    .addAll(other.instances)
                                                    .build();
            // since other is NOT empty, then output can not be empty; so skip create method
            return new Counter(copy, Math.max(maxValue, other.maxValue));
        }
    }
}
