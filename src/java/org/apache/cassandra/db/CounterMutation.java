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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.Striped;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.LatencyMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.btree.BTreeSet;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.net.MessagingService.VERSION_SG_10;
import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.MessagingService.VERSION_40;


public class CounterMutation implements IMutation
{
    private static final Logger logger = LoggerFactory.getLogger(CounterMutation.class);
    private static final NoSpamLogger nospamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.SECONDS);

    public static final CounterMutationSerializer serializer = new CounterMutationSerializer();

    /**
     * This metric tracks the number of timeouts that occurred because the locks could not be
     * acquired within DatabaseDescriptor.getCounterWriteRpcTimeout().
     */
    public static final Counter lockTimeout = Metrics.counter(DefaultNameFactory.createMetricName("Counter", "lock_timeout", null));

    /**
     * This metric tracks how long it took to acquire all the locks
     * that must be acquired before applying the counter mutation.
     */
    public static final LatencyMetrics lockAcquireTime = new LatencyMetrics("Counter", "lock_acquire_time");

    /**
     * This metric tracks the number of locks that must be acquired before applying the counter
     * mutation. A mutation normally has one partition only, unless it comes from a batch,
     * where the same partition key is used across different tables.
     * For each partition, we need to acquire one lock for each column on each row.
     * The locks are striped, see {@link CounterMutation#LOCKS} for details.
     */
    public static final Histogram locksPerUpdate = Metrics.histogram(DefaultNameFactory
                                                                     .createMetricName("Counter",
                                                                                       "locks_per_update",
                                                                                       null),
                                                                     false);

    private static final String LOCK_TIMEOUT_MESSAGE = "Failed to acquire locks for counter mutation on keyspace {} for longer than {} millis, giving up";
    private static final String LOCK_TIMEOUT_TRACE = "Failed to acquire locks for counter mutation for longer than {} millis, giving up";

    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentCounterWriters() * 1024);

    private final Mutation mutation;
    private final ConsistencyLevel consistency;

    public CounterMutation(Mutation mutation, ConsistencyLevel consistency)
    {
        this.mutation = mutation;
        this.consistency = consistency;
    }

    public String getKeyspaceName()
    {
        return mutation.getKeyspaceName();
    }

    public Keyspace getKeyspace()
    {
        return mutation.getKeyspace();
    }

    public Collection<TableId> getTableIds()
    {
        return mutation.getTableIds();
    }

    public Collection<PartitionUpdate> getPartitionUpdates()
    {
        return mutation.getPartitionUpdates();
    }

    public void validateSize(int version, int overhead)
    {
        long totalSize = serializedSize(version) + overhead;
        if(totalSize > MAX_MUTATION_SIZE)
        {
            throw new MutationExceededMaxSizeException(this, version, totalSize);
        }
    }

    public Mutation getMutation()
    {
        return mutation;
    }

    public DecoratedKey key()
    {
        return mutation.key();
    }

    public ConsistencyLevel consistency()
    {
        return consistency;
    }

    /**
     * Applies the counter mutation, returns the result Mutation (for replication to other nodes).
     *
     * 1. Grabs the striped cell-level locks in the proper order
     * 2. Gets the current values of the counters-to-be-modified from the counter cache
     * 3. Reads the rest of the current values (cache misses) from the CF
     * 4. Writes the updated counter values
     * 5. Updates the counter cache
     * 6. Releases the lock(s)
     *
     * See CASSANDRA-4775 and CASSANDRA-6504 for further details.
     *
     * @return the applied resulting Mutation
     */
    public Mutation applyCounterMutation() throws WriteTimeoutException
    {
        Mutation.PartitionUpdateCollector resultBuilder = new Mutation.PartitionUpdateCollector(getKeyspaceName(), key());
        Keyspace keyspace = Keyspace.open(getKeyspaceName());

        List<Lock> locks = new ArrayList<>();
        Tracing.trace("Acquiring counter locks");

        long clock = FBUtilities.timestampMicros();
        CounterId counterId = CounterId.getLocalId();

        try
        {
            grabCounterLocks(keyspace, locks);
            for (PartitionUpdate upd : getPartitionUpdates())
                resultBuilder.add(processModifications(upd, clock, counterId));

            Mutation result = resultBuilder.build();
            result.apply();
            return result;
        }
        finally
        {
            for (Lock lock : locks)
                lock.unlock();
        }
    }

    /**
     * Applies the counter mutation with the provided time and {@link CounterId}. As opposed to
     * {@link #applyCounterMutation()} this method doesn't acquire cell-level locks.
     * <p/>
     * This method is used in CDC counter write path (CNDB).
     * <p/>
     * The time and counter values are evaluated and propagated to all replicas by CDC Service. The replicas
     * use this method to apply the mutation locally without locks. The locks are not needed in the CDC
     * path as all the writes to the same partition are serialized by CDC Service.
     */
    public CompletableFuture<Mutation> applyCounterMutationWithoutLocks(long systemClockMicros, CounterId counterId)
    {
        Mutation.PartitionUpdateCollector resultBuilder = new Mutation.PartitionUpdateCollector(getKeyspaceName(), key());
        for (PartitionUpdate upd : getPartitionUpdates())
            resultBuilder.add(processModifications(upd, systemClockMicros, counterId));

        Mutation mutatation = resultBuilder.build();
        return mutatation.applyFuture(WriteOptions.DEFAULT).thenApply(ignore -> mutatation);
    }

    public void apply()
    {
        applyCounterMutation();
    }

    private int countDistinctLocks(Iterable<Lock> sortedLocks)
    {
        Lock prev = null;
        int counter = 0;
        for(Lock l: sortedLocks)
        {
            if (prev != l)
                counter++;
            prev = l;
        }
        return counter;
    }

    @VisibleForTesting
    public void grabCounterLocks(Keyspace keyspace, List<Lock> locks) throws WriteTimeoutException
    {
        long startTime = System.nanoTime();

        AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
        Iterable<Lock> sortedLocks = LOCKS.bulkGet(getCounterLockKeys());
        locksPerUpdate.update(countDistinctLocks(sortedLocks));

        try
        {
            for (Lock lock : sortedLocks)
            {
                long timeout = getTimeout(NANOSECONDS) - (System.nanoTime() - startTime);
                try
                {
                    if (!lock.tryLock(timeout, NANOSECONDS))
                        handleLockTimeoutAndThrow(replicationStrategy);
                    locks.add(lock);
                }
                catch (InterruptedException e)
                {
                    handleLockTimeoutAndThrow(replicationStrategy);
                }
            }
        }
        finally
        {
            lockAcquireTime.addNano(System.nanoTime() - startTime);
        }
    }

    private void handleLockTimeoutAndThrow(AbstractReplicationStrategy replicationStrategy)
    {
        lockTimeout.inc();

        nospamLogger.error(LOCK_TIMEOUT_MESSAGE,
                           getKeyspaceName(),
                           DatabaseDescriptor.getCounterWriteRpcTimeout(MILLISECONDS));
        Tracing.trace(LOCK_TIMEOUT_TRACE, DatabaseDescriptor.getCounterWriteRpcTimeout(MILLISECONDS));

        throw new WriteTimeoutException(WriteType.COUNTER, consistency(), 0, consistency().blockFor(replicationStrategy));
    }

    /**
     * Returns a wrapper for the Striped#bulkGet() call (via Keyspace#counterLocksFor())
     * Striped#bulkGet() depends on Object#hashCode(), so here we make sure that the cf id and the partition key
     * all get to be part of the hashCode() calculation.
     */
    private Iterable<Object> getCounterLockKeys()
    {
        return Iterables.concat(Iterables.transform(getPartitionUpdates(), new Function<PartitionUpdate, Iterable<Object>>()
        {
            public Iterable<Object> apply(final PartitionUpdate update)
            {
                return Iterables.concat(Iterables.transform(update, new Function<Row, Iterable<Object>>()
                {
                    public Iterable<Object> apply(final Row row)
                    {
                        return Iterables.concat(Iterables.transform(row, new Function<ColumnData, Object>()
                        {
                            public Object apply(final ColumnData data)
                            {
                                return Objects.hashCode(update.metadata().id, key(), row.clustering(), data.column());
                            }
                        }));
                    }
                }));
            }
        }));
    }

    private PartitionUpdate processModifications(PartitionUpdate changes,
                                                 long systemClockMicros,
                                                 CounterId counterId)
    {
        ColumnFamilyStore cfs = Keyspace.open(getKeyspaceName()).getColumnFamilyStore(changes.metadata().id);

        List<PartitionUpdate.CounterMark> marks = changes.collectCounterMarks();

        if (CacheService.instance.counterCache.getCapacity() != 0)
        {
            Tracing.trace("Fetching {} counter values from cache", marks.size());
            updateWithCurrentValuesFromCache(marks, cfs, systemClockMicros, counterId);
            if (marks.isEmpty())
                return changes;
        }

        Tracing.trace("Reading {} counter values from the CF", marks.size());
        updateWithCurrentValuesFromCFS(marks, cfs, systemClockMicros, counterId);

        // What's remain is new counters
        for (PartitionUpdate.CounterMark mark : marks)
            updateWithCurrentValue(mark, ClockAndCount.BLANK, cfs, systemClockMicros, counterId);

        return changes;
    }

    private void updateWithCurrentValue(PartitionUpdate.CounterMark mark,
                                        ClockAndCount currentValue,
                                        ColumnFamilyStore cfs,
                                        long systemClockMicros,
                                        CounterId counterId)
    {
        long clock = Math.max(systemClockMicros, currentValue.clock + 1L);
        long count = currentValue.count + CounterContext.instance().total(mark.value(), ByteBufferAccessor.instance);

        mark.setValue(CounterContext.instance().createGlobal(counterId, clock, count));

        // Cache the newly updated value
        cfs.putCachedCounter(key().getKey(), mark.clustering(), mark.column(), mark.path(), ClockAndCount.create(clock, count));
    }

    // Returns the count of cache misses.
    private void updateWithCurrentValuesFromCache(List<PartitionUpdate.CounterMark> marks,
                                                  ColumnFamilyStore cfs,
                                                  long systemClockMicros,
                                                  CounterId counterId)
    {
        Iterator<PartitionUpdate.CounterMark> iter = marks.iterator();
        while (iter.hasNext())
        {
            PartitionUpdate.CounterMark mark = iter.next();
            ClockAndCount cached = cfs.getCachedCounter(key().getKey(), mark.clustering(), mark.column(), mark.path());
            if (cached != null)
            {
                updateWithCurrentValue(mark, cached, cfs, systemClockMicros, counterId);
                iter.remove();
            }
        }
    }

    // Reads the missing current values from the CFS.
    private void updateWithCurrentValuesFromCFS(List<PartitionUpdate.CounterMark> marks,
                                                ColumnFamilyStore cfs,
                                                long systemClockMicros,
                                                CounterId counterId)
    {
        ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
        BTreeSet.Builder<Clustering<?>> names = BTreeSet.builder(cfs.metadata().comparator);
        for (PartitionUpdate.CounterMark mark : marks)
        {
            if (mark.clustering() != Clustering.STATIC_CLUSTERING)
                names.add(mark.clustering());
            if (mark.path() == null)
                builder.add(mark.column());
            else
                builder.select(mark.column(), mark.path());
        }

        int nowInSec = FBUtilities.nowInSeconds();
        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(names.build(), false);
        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(cfs.metadata(), nowInSec, key(), builder.build(), filter);
        PeekingIterator<PartitionUpdate.CounterMark> markIter = Iterators.peekingIterator(marks.iterator());
        try (ReadExecutionController controller = cmd.executionController();
             RowIterator partition = UnfilteredRowIterators.filter(cmd.queryMemtableAndDisk(cfs, controller), nowInSec))
        {
            updateForRow(markIter, partition.staticRow(), cfs, systemClockMicros, counterId);

            while (partition.hasNext())
            {
                if (!markIter.hasNext())
                    return;

                updateForRow(markIter, partition.next(), cfs, systemClockMicros, counterId);
            }
        }
    }

    private int compare(Clustering<?> c1, Clustering<?> c2, ColumnFamilyStore cfs)
    {
        if (c1 == Clustering.STATIC_CLUSTERING)
            return c2 == Clustering.STATIC_CLUSTERING ? 0 : -1;
        if (c2 == Clustering.STATIC_CLUSTERING)
            return 1;

        return cfs.getComparator().compare(c1, c2);
    }

    private void updateForRow(PeekingIterator<PartitionUpdate.CounterMark> markIter,
                              Row row,
                              ColumnFamilyStore cfs,
                              long systemClockMicros,
                              CounterId counterId)
    {
        int cmp = 0;
        // If the mark is before the row, we have no value for this mark, just consume it
        while (markIter.hasNext() && (cmp = compare(markIter.peek().clustering(), row.clustering(), cfs)) < 0)
            markIter.next();

        if (!markIter.hasNext())
            return;

        while (cmp == 0)
        {
            PartitionUpdate.CounterMark mark = markIter.next();
            Cell<?> cell = mark.path() == null ? row.getCell(mark.column()) : row.getCell(mark.column(), mark.path());
            if (cell != null)
            {
                ClockAndCount localClockAndCount = CounterContext.instance().getLocalClockAndCount(cell.buffer());
                updateWithCurrentValue(mark, localClockAndCount, cfs, systemClockMicros, counterId);
                markIter.remove();
            }
            if (!markIter.hasNext())
                return;

            cmp = compare(markIter.peek().clustering(), row.clustering(), cfs);
        }
    }

    public long getTimeout(TimeUnit unit)
    {
        return DatabaseDescriptor.getCounterWriteRpcTimeout(unit);
    }

    private int serializedSize30;
    private int serializedSize3014;
    private int serializedSize40;
    private int serializedSizeSG10;

    public int serializedSize(int version)
    {
        switch (version)
        {
            case VERSION_30:
                if (serializedSize30 == 0)
                    serializedSize30 = (int) serializer.serializedSize(this, VERSION_30);
                return serializedSize30;
            case VERSION_3014:
                if (serializedSize3014 == 0)
                    serializedSize3014 = (int) serializer.serializedSize(this, VERSION_3014);
                return serializedSize3014;
            case VERSION_40:
                if (serializedSize40 == 0)
                    serializedSize40 = (int) serializer.serializedSize(this, VERSION_40);
                return serializedSize40;
            case VERSION_SG_10:
                if (serializedSizeSG10 == 0)
                    serializedSizeSG10 = (int) serializer.serializedSize(this, VERSION_SG_10);
                return serializedSizeSG10;
            default:
                throw new IllegalStateException("Unknown serialization version: " + version);
        }
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean shallow)
    {
        return String.format("CounterMutation(%s, %s)", mutation.toString(shallow), consistency);
    }

    public static class CounterMutationSerializer implements IVersionedSerializer<CounterMutation>
    {
        public void serialize(CounterMutation cm, DataOutputPlus out, int version) throws IOException
        {
            Mutation.serializer.serialize(cm.mutation, out, version);
            out.writeUTF(cm.consistency.name());
        }

        public CounterMutation deserialize(DataInputPlus in, int version) throws IOException
        {
            Mutation m = Mutation.serializer.deserialize(in, version);
            ConsistencyLevel consistency = Enum.valueOf(ConsistencyLevel.class, in.readUTF());
            return new CounterMutation(m, consistency);
        }

        public long serializedSize(CounterMutation cm, int version)
        {
            return cm.mutation.serializedSize(version)
                 + TypeSizes.sizeof(cm.consistency.name());
        }
    }
}
