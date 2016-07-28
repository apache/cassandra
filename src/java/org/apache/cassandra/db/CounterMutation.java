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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.Striped;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.btree.BTreeSet;

public class CounterMutation implements IMutation
{
    public static final CounterMutationSerializer serializer = new CounterMutationSerializer();

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

    public Collection<UUID> getColumnFamilyIds()
    {
        return mutation.getColumnFamilyIds();
    }

    public Collection<PartitionUpdate> getPartitionUpdates()
    {
        return mutation.getPartitionUpdates();
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

    public MessageOut<CounterMutation> makeMutationMessage()
    {
        return new MessageOut<>(MessagingService.Verb.COUNTER_MUTATION, this, serializer);
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
        Mutation result = new Mutation(getKeyspaceName(), key());
        Keyspace keyspace = Keyspace.open(getKeyspaceName());

        List<Lock> locks = new ArrayList<>();
        Tracing.trace("Acquiring counter locks");
        try
        {
            grabCounterLocks(keyspace, locks);
            for (PartitionUpdate upd : getPartitionUpdates())
                result.add(processModifications(upd));
            result.apply();
            return result;
        }
        finally
        {
            for (Lock lock : locks)
                lock.unlock();
        }
    }

    public void apply()
    {
        applyCounterMutation();
    }

    private void grabCounterLocks(Keyspace keyspace, List<Lock> locks) throws WriteTimeoutException
    {
        long startTime = System.nanoTime();

        for (Lock lock : LOCKS.bulkGet(getCounterLockKeys()))
        {
            long timeout = TimeUnit.MILLISECONDS.toNanos(getTimeout()) - (System.nanoTime() - startTime);
            try
            {
                if (!lock.tryLock(timeout, TimeUnit.NANOSECONDS))
                    throw new WriteTimeoutException(WriteType.COUNTER, consistency(), 0, consistency().blockFor(keyspace));
                locks.add(lock);
            }
            catch (InterruptedException e)
            {
                throw new WriteTimeoutException(WriteType.COUNTER, consistency(), 0, consistency().blockFor(keyspace));
            }
        }
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
                                return Objects.hashCode(update.metadata().cfId, key(), row.clustering(), data.column());
                            }
                        }));
                    }
                }));
            }
        }));
    }

    private PartitionUpdate processModifications(PartitionUpdate changes)
    {
        ColumnFamilyStore cfs = Keyspace.open(getKeyspaceName()).getColumnFamilyStore(changes.metadata().cfId);

        List<PartitionUpdate.CounterMark> marks = changes.collectCounterMarks();

        if (CacheService.instance.counterCache.getCapacity() != 0)
        {
            Tracing.trace("Fetching {} counter values from cache", marks.size());
            updateWithCurrentValuesFromCache(marks, cfs);
            if (marks.isEmpty())
                return changes;
        }

        Tracing.trace("Reading {} counter values from the CF", marks.size());
        updateWithCurrentValuesFromCFS(marks, cfs);

        // What's remain is new counters
        for (PartitionUpdate.CounterMark mark : marks)
            updateWithCurrentValue(mark, ClockAndCount.BLANK, cfs);

        return changes;
    }

    private void updateWithCurrentValue(PartitionUpdate.CounterMark mark, ClockAndCount currentValue, ColumnFamilyStore cfs)
    {
        long clock = Math.max(FBUtilities.timestampMicros(), currentValue.clock + 1L);
        long count = currentValue.count + CounterContext.instance().total(mark.value());

        mark.setValue(CounterContext.instance().createGlobal(CounterId.getLocalId(), clock, count));

        // Cache the newly updated value
        cfs.putCachedCounter(key().getKey(), mark.clustering(), mark.column(), mark.path(), ClockAndCount.create(clock, count));
    }

    // Returns the count of cache misses.
    private void updateWithCurrentValuesFromCache(List<PartitionUpdate.CounterMark> marks, ColumnFamilyStore cfs)
    {
        Iterator<PartitionUpdate.CounterMark> iter = marks.iterator();
        while (iter.hasNext())
        {
            PartitionUpdate.CounterMark mark = iter.next();
            ClockAndCount cached = cfs.getCachedCounter(key().getKey(), mark.clustering(), mark.column(), mark.path());
            if (cached != null)
            {
                updateWithCurrentValue(mark, cached, cfs);
                iter.remove();
            }
        }
    }

    // Reads the missing current values from the CFS.
    private void updateWithCurrentValuesFromCFS(List<PartitionUpdate.CounterMark> marks, ColumnFamilyStore cfs)
    {
        ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
        BTreeSet.Builder<Clustering> names = BTreeSet.builder(cfs.metadata.comparator);
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
        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(cfs.metadata, nowInSec, key(), builder.build(), filter);
        PeekingIterator<PartitionUpdate.CounterMark> markIter = Iterators.peekingIterator(marks.iterator());
        try (ReadExecutionController controller = cmd.executionController();
             RowIterator partition = UnfilteredRowIterators.filter(cmd.queryMemtableAndDisk(cfs, controller), nowInSec))
        {
            updateForRow(markIter, partition.staticRow(), cfs);

            while (partition.hasNext())
            {
                if (!markIter.hasNext())
                    return;

                updateForRow(markIter, partition.next(), cfs);
            }
        }
    }

    private int compare(Clustering c1, Clustering c2, ColumnFamilyStore cfs)
    {
        if (c1 == Clustering.STATIC_CLUSTERING)
            return c2 == Clustering.STATIC_CLUSTERING ? 0 : -1;
        if (c2 == Clustering.STATIC_CLUSTERING)
            return 1;

        return cfs.getComparator().compare(c1, c2);
    }

    private void updateForRow(PeekingIterator<PartitionUpdate.CounterMark> markIter, Row row, ColumnFamilyStore cfs)
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
            Cell cell = mark.path() == null ? row.getCell(mark.column()) : row.getCell(mark.column(), mark.path());
            if (cell != null)
            {
                updateWithCurrentValue(mark, CounterContext.instance().getLocalClockAndCount(cell.value()), cfs);
                markIter.remove();
            }
            if (!markIter.hasNext())
                return;

            cmp = compare(markIter.peek().clustering(), row.clustering(), cfs);
        }
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getCounterWriteRpcTimeout();
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
            return Mutation.serializer.serializedSize(cm.mutation, version)
                 + TypeSizes.sizeof(cm.consistency.name());
        }
    }
}
