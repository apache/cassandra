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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Striped;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;

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

    public Collection<ColumnFamily> getColumnFamilies()
    {
        return mutation.getColumnFamilies();
    }

    public Mutation getMutation()
    {
        return mutation;
    }

    public ByteBuffer key()
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
    public Mutation apply() throws WriteTimeoutException
    {
        Mutation result = new Mutation(getKeyspaceName(), key());
        Keyspace keyspace = Keyspace.open(getKeyspaceName());

        int count = 0;
        for (ColumnFamily cf : getColumnFamilies())
            count += cf.getColumnCount();

        List<Lock> locks = new ArrayList<>(count);
        Tracing.trace("Acquiring {} counter locks", count);
        try
        {
            grabCounterLocks(keyspace, locks);
            for (ColumnFamily cf : getColumnFamilies())
                result.add(processModifications(cf));
            result.apply();
            updateCounterCache(result, keyspace);
            return result;
        }
        finally
        {
            for (Lock lock : locks)
                lock.unlock();
        }
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
     * all get to be part of the hashCode() calculation, not just the cell name.
     */
    private Iterable<Object> getCounterLockKeys()
    {
        return Iterables.concat(Iterables.transform(getColumnFamilies(), new Function<ColumnFamily, Iterable<Object>>()
        {
            public Iterable<Object> apply(final ColumnFamily cf)
            {
                return Iterables.transform(cf, new Function<Cell, Object>()
                {
                    public Object apply(Cell cell)
                    {
                        return Objects.hashCode(cf.id(), key(), cell.name());
                    }
                });
            }
        }));
    }

    // Replaces all the CounterUpdateCell-s with updated regular CounterCell-s
    private ColumnFamily processModifications(ColumnFamily changesCF)
    {
        ColumnFamilyStore cfs = Keyspace.open(getKeyspaceName()).getColumnFamilyStore(changesCF.id());

        ColumnFamily resultCF = changesCF.cloneMeShallow();

        List<CounterUpdateCell> counterUpdateCells = new ArrayList<>(changesCF.getColumnCount());
        for (Cell cell : changesCF)
        {
            if (cell instanceof CounterUpdateCell)
                counterUpdateCells.add((CounterUpdateCell)cell);
            else
                resultCF.addColumn(cell);
        }

        if (counterUpdateCells.isEmpty())
            return resultCF; // only DELETEs

        ClockAndCount[] currentValues = getCurrentValues(counterUpdateCells, cfs);
        for (int i = 0; i < counterUpdateCells.size(); i++)
        {
            ClockAndCount currentValue = currentValues[i];
            CounterUpdateCell update = counterUpdateCells.get(i);

            long clock = currentValue.clock + 1L;
            long count = currentValue.count + update.delta();

            resultCF.addColumn(new BufferCounterCell(update.name(),
                                                     CounterContext.instance().createGlobal(CounterId.getLocalId(), clock, count),
                                                     update.timestamp()));
        }

        return resultCF;
    }

    // Attempt to load the current values(s) from cache. If that fails, read the rest from the cfs.
    private ClockAndCount[] getCurrentValues(List<CounterUpdateCell> counterUpdateCells, ColumnFamilyStore cfs)
    {
        ClockAndCount[] currentValues = new ClockAndCount[counterUpdateCells.size()];
        int remaining = counterUpdateCells.size();

        if (CacheService.instance.counterCache.getCapacity() != 0)
        {
            Tracing.trace("Fetching {} counter values from cache", counterUpdateCells.size());
            remaining = getCurrentValuesFromCache(counterUpdateCells, cfs, currentValues);
            if (remaining == 0)
                return currentValues;
        }

        Tracing.trace("Reading {} counter values from the CF", remaining);
        getCurrentValuesFromCFS(counterUpdateCells, cfs, currentValues);

        return currentValues;
    }

    // Returns the count of cache misses.
    private int getCurrentValuesFromCache(List<CounterUpdateCell> counterUpdateCells,
                                          ColumnFamilyStore cfs,
                                          ClockAndCount[] currentValues)
    {
        int cacheMisses = 0;
        for (int i = 0; i < counterUpdateCells.size(); i++)
        {
            ClockAndCount cached = cfs.getCachedCounter(key(), counterUpdateCells.get(i).name());
            if (cached != null)
                currentValues[i] = cached;
            else
                cacheMisses++;
        }
        return cacheMisses;
    }

    // Reads the missing current values from the CFS.
    private void getCurrentValuesFromCFS(List<CounterUpdateCell> counterUpdateCells,
                                         ColumnFamilyStore cfs,
                                         ClockAndCount[] currentValues)
    {
        SortedSet<CellName> names = new TreeSet<>(cfs.metadata.comparator);
        for (int i = 0; i < currentValues.length; i++)
            if (currentValues[i] == null)
                names.add(counterUpdateCells.get(i).name());

        ReadCommand cmd = new SliceByNamesReadCommand(getKeyspaceName(), key(), cfs.metadata.cfName, Long.MIN_VALUE, new NamesQueryFilter(names));
        Row row = cmd.getRow(cfs.keyspace);
        ColumnFamily cf = row == null ? null : row.cf;

        for (int i = 0; i < currentValues.length; i++)
        {
            if (currentValues[i] != null)
                continue;

            Cell cell = cf == null ? null : cf.getColumn(counterUpdateCells.get(i).name());
            if (cell == null || !cell.isLive()) // absent or a tombstone.
                currentValues[i] = ClockAndCount.BLANK;
            else
                currentValues[i] = CounterContext.instance().getLocalClockAndCount(cell.value());
        }
    }

    private void updateCounterCache(Mutation applied, Keyspace keyspace)
    {
        if (CacheService.instance.counterCache.getCapacity() == 0)
            return;

        for (ColumnFamily cf : applied.getColumnFamilies())
        {
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cf.id());
            for (Cell cell : cf)
                if (cell instanceof CounterCell)
                    cfs.putCachedCounter(key(), cell.name(), CounterContext.instance().getLocalClockAndCount(cell.value()));
        }
    }

    public void addAll(IMutation m)
    {
        if (!(m instanceof CounterMutation))
            throw new IllegalArgumentException();
        CounterMutation cm = (CounterMutation)m;
        mutation.addAll(cm.mutation);
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

        public CounterMutation deserialize(DataInput in, int version) throws IOException
        {
            Mutation m = Mutation.serializer.deserialize(in, version);
            ConsistencyLevel consistency = Enum.valueOf(ConsistencyLevel.class, in.readUTF());
            return new CounterMutation(m, consistency);
        }

        public long serializedSize(CounterMutation cm, int version)
        {
            return Mutation.serializer.serializedSize(cm.mutation, version)
                 + TypeSizes.NATIVE.sizeof(cm.consistency.name());
        }
    }
}
