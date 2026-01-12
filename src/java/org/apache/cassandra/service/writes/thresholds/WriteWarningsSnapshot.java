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

package org.apache.cassandra.service.writes.thresholds;


import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Immutable snapshot of write warnings.
 * Simpler than WarningsSnapshot since writes never abort (warnings only).
 */
public class WriteWarningsSnapshot
{
    private static final WriteWarningsSnapshot EMPTY = new WriteWarningsSnapshot(Counter.EMPTY, Counter.EMPTY);

    public final Counter writeSize;
    public final Counter writeTombstone;

    private WriteWarningsSnapshot(Counter writeSize, Counter writeTombstone)
    {
        this.writeSize = writeSize;
        this.writeTombstone = writeTombstone;
    }

    public static WriteWarningsSnapshot empty()
    {
        return EMPTY;
    }

    public static WriteWarningsSnapshot create(Counter writeSize, Counter writeTombstone)
    {
        if (writeSize == Counter.EMPTY && writeTombstone == Counter.EMPTY)
            return EMPTY;
        return new WriteWarningsSnapshot(writeSize, writeTombstone);
    }

    public boolean isEmpty()
    {
        return this == EMPTY;
    }

    public WriteWarningsSnapshot merge(WriteWarningsSnapshot other)
    {
        if (other == null || other == EMPTY)
            return this;
        return WriteWarningsSnapshot.create(
        writeSize.merge(other.writeSize),
        writeTombstone.merge(other.writeTombstone)
        );
    }

    @VisibleForTesting
    public static String writeSizeWarnMessage(int nodes, long bytes)
    {
        return String.format("%d nodes detected write to large partition; estimated size is %d bytes (see write_size_warn_threshold)",
                             nodes, bytes);
    }

    @VisibleForTesting
    public static String writeTombstoneWarnMessage(int nodes, long tombstones)
    {
        return String.format("%d nodes detected write to partition with many tombstones; estimated count is %d (see write_tombstone_warn_threshold)",
                             nodes, tombstones);
    }

    public static final class Counter
    {
        private static final Counter EMPTY = new Counter(ImmutableSet.of(), 0);

        public final ImmutableSet<InetAddressAndPort> instances;
        public final long maxValue;

        Counter(ImmutableSet<InetAddressAndPort> instances, long maxValue)
        {
            this.instances = instances;
            this.maxValue = maxValue;
        }

        static Counter empty()
        {
            return EMPTY;
        }

        public static Counter create(Set<InetAddressAndPort> instances, AtomicLong maxValue)
        {
            ImmutableSet<InetAddressAndPort> copy = ImmutableSet.copyOf(instances);
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
            return new Counter(copy, Math.max(maxValue, other.maxValue));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Counter counter = (Counter) o;
            return maxValue == counter.maxValue && Objects.equals(instances, counter.instances);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(instances, maxValue);
        }
    }
}