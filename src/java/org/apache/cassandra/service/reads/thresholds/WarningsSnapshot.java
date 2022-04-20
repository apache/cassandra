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
package org.apache.cassandra.service.reads.thresholds;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.exceptions.ReadSizeAbortException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.TombstoneAbortException;
import org.apache.cassandra.locator.InetAddressAndPort;

public class WarningsSnapshot
{
    private static final WarningsSnapshot EMPTY = new WarningsSnapshot(Warnings.EMPTY, Warnings.EMPTY, Warnings.EMPTY);

    public final Warnings tombstones, localReadSize, rowIndexReadSize;

    private WarningsSnapshot(Warnings tombstones, Warnings localReadSize, Warnings rowIndexReadSize)
    {
        this.tombstones = tombstones;
        this.localReadSize = localReadSize;
        this.rowIndexReadSize = rowIndexReadSize;
    }

    public static WarningsSnapshot empty()
    {
        return EMPTY;
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

    public boolean isDefined()
    {
        return this != EMPTY;
    }

    @VisibleForTesting
    WarningsSnapshot merge(WarningsSnapshot other)
    {
        if (other == null || other == EMPTY)
            return this;
        return WarningsSnapshot.create(tombstones.merge(other.tombstones), localReadSize.merge(other.localReadSize), rowIndexReadSize.merge(other.rowIndexReadSize));
    }

    public void maybeAbort(ReadCommand command, ConsistencyLevel cl, int received, int blockFor, boolean isDataPresent, Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint)
    {
        if (!tombstones.aborts.instances.isEmpty())
            throw new TombstoneAbortException(tombstones.aborts.instances.size(), tombstones.aborts.maxValue, command.toCQLString(), isDataPresent,
                                              cl, received, blockFor, failureReasonByEndpoint);

        if (!localReadSize.aborts.instances.isEmpty())
            throw new ReadSizeAbortException(localReadSizeAbortMessage(localReadSize.aborts.instances.size(), localReadSize.aborts.maxValue, command.toCQLString()),
                                             cl, received, blockFor, isDataPresent, failureReasonByEndpoint);

        if (!rowIndexReadSize.aborts.instances.isEmpty())
            throw new ReadSizeAbortException(rowIndexReadSizeAbortMessage(rowIndexReadSize.aborts.instances.size(), rowIndexReadSize.aborts.maxValue, command.toCQLString()),
                                             cl, received, blockFor, isDataPresent, failureReasonByEndpoint);
    }

    @VisibleForTesting
    public static String tombstoneAbortMessage(int nodes, long tombstones, String cql)
    {
        return String.format("%s nodes scanned over %s tombstones and aborted the query %s (see tombstone_failure_threshold)", nodes, tombstones, cql);
    }

    @VisibleForTesting
    public static String tombstoneWarnMessage(int nodes, long tombstones, String cql)
    {
        return String.format("%s nodes scanned up to %s tombstones and issued tombstone warnings for query %s  (see tombstone_warn_threshold)", nodes, tombstones, cql);
    }

    @VisibleForTesting
    public static String localReadSizeAbortMessage(long nodes, long bytes, String cql)
    {
        return String.format("%s nodes loaded over %s bytes and aborted the query %s (see local_read_size_fail_threshold)", nodes, bytes, cql);
    }

    @VisibleForTesting
    public static String localReadSizeWarnMessage(int nodes, long bytes, String cql)
    {
        return String.format("%s nodes loaded over %s bytes and issued local read size warnings for query %s  (see local_read_size_warn_threshold)", nodes, bytes, cql);
    }

    @VisibleForTesting
    public static String rowIndexReadSizeAbortMessage(long nodes, long bytes, String cql)
    {
        return String.format("%s nodes loaded over %s bytes in RowIndexEntry and aborted the query %s (see row_index_size_fail_threshold)", nodes, bytes, cql);
    }

    @VisibleForTesting
    public static String rowIndexSizeWarnMessage(int nodes, long bytes, String cql)
    {
        return String.format("%s nodes loaded over %s bytes in RowIndexEntry and issued warnings for query %s  (see row_index_size_warn_threshold)", nodes, bytes, cql);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WarningsSnapshot that = (WarningsSnapshot) o;
        return Objects.equals(tombstones, that.tombstones) && Objects.equals(localReadSize, that.localReadSize) && Objects.equals(rowIndexReadSize, that.rowIndexReadSize);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tombstones, localReadSize, rowIndexReadSize);
    }

    @Override
    public String toString()
    {
        return "(tombstones=" + tombstones + ", localReadSize=" + localReadSize + ", rowIndexTooLarge=" + rowIndexReadSize + ')';
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

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Warnings warnings1 = (Warnings) o;
            return Objects.equals(warnings, warnings1.warnings) && Objects.equals(aborts, warnings1.aborts);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(warnings, aborts);
        }

        @Override
        public String toString()
        {
            return "(warnings=" + warnings + ", aborts=" + aborts + ')';
        }
    }

    public static final class Counter
    {
        private static final Counter EMPTY = new Counter(ImmutableSet.of(), 0);

        public final ImmutableSet<InetAddressAndPort> instances;
        public final long maxValue;

        @VisibleForTesting
        Counter(ImmutableSet<InetAddressAndPort> instances, long maxValue)
        {
            this.instances = instances;
            this.maxValue = maxValue;
        }

        @VisibleForTesting
        static Counter empty()
        {
            return EMPTY;
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

        @Override
        public String toString()
        {
            return "(" + instances + ", " + maxValue + ')';
        }
    }

    @VisibleForTesting
    static Builder builder()
    {
        return new Builder();
    }

    @VisibleForTesting
    public static final class Builder
    {
        private WarningsSnapshot snapshot = empty();

        public Builder tombstonesWarning(ImmutableSet<InetAddressAndPort> instances, long maxValue)
        {
            return tombstonesWarning(new Counter(Objects.requireNonNull(instances), maxValue));
        }

        public Builder tombstonesWarning(Counter counter)
        {
            Objects.requireNonNull(counter);
            snapshot = snapshot.merge(new WarningsSnapshot(new Warnings(counter, Counter.EMPTY), Warnings.EMPTY, Warnings.EMPTY));
            return this;
        }

        public Builder tombstonesAbort(ImmutableSet<InetAddressAndPort> instances, long maxValue)
        {
            return tombstonesAbort(new Counter(Objects.requireNonNull(instances), maxValue));
        }

        public Builder tombstonesAbort(Counter counter)
        {
            Objects.requireNonNull(counter);
            snapshot = snapshot.merge(new WarningsSnapshot(new Warnings(Counter.EMPTY, counter), Warnings.EMPTY, Warnings.EMPTY));
            return this;
        }

        public Builder localReadSizeWarning(ImmutableSet<InetAddressAndPort> instances, long maxValue)
        {
            return localReadSizeWarning(new Counter(Objects.requireNonNull(instances), maxValue));
        }

        public Builder localReadSizeWarning(Counter counter)
        {
            Objects.requireNonNull(counter);
            snapshot = snapshot.merge(new WarningsSnapshot(Warnings.EMPTY, new Warnings(counter, Counter.EMPTY), Warnings.EMPTY));
            return this;
        }

        public Builder localReadSizeAbort(ImmutableSet<InetAddressAndPort> instances, long maxValue)
        {
            return localReadSizeAbort(new Counter(Objects.requireNonNull(instances), maxValue));
        }

        public Builder localReadSizeAbort(Counter counter)
        {
            Objects.requireNonNull(counter);
            snapshot = snapshot.merge(new WarningsSnapshot(Warnings.EMPTY, new Warnings(Counter.EMPTY, counter), Warnings.EMPTY));
            return this;
        }

        public Builder rowIndexSizeWarning(Counter counter)
        {
            Objects.requireNonNull(counter);
            snapshot = snapshot.merge(new WarningsSnapshot(Warnings.EMPTY, Warnings.EMPTY, new Warnings(counter, Counter.EMPTY)));
            return this;
        }

        public Builder rowIndexSizeAbort(Counter counter)
        {
            Objects.requireNonNull(counter);
            snapshot = snapshot.merge(new WarningsSnapshot(Warnings.EMPTY, Warnings.EMPTY, new Warnings(Counter.EMPTY, counter)));
            return this;
        }

        public WarningsSnapshot build()
        {
            return snapshot;
        }
    }
}
