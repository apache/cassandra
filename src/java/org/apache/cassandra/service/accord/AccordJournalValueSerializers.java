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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import com.google.common.collect.ImmutableSortedMap;

import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.Deps;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.KeySerializers;

import static accord.local.CommandStores.RangesForEpoch;
import static org.apache.cassandra.service.accord.AccordKeyspace.LocalVersionedSerializers.bootstrapBeganAt;
import static org.apache.cassandra.service.accord.AccordKeyspace.LocalVersionedSerializers.durableBefore;
import static org.apache.cassandra.service.accord.AccordKeyspace.LocalVersionedSerializers.redundantBefore;
import static org.apache.cassandra.service.accord.AccordKeyspace.LocalVersionedSerializers.safeToRead;
import static org.apache.cassandra.service.accord.serializers.DepsSerializer.deps;

// TODO (required): test with large collection values, and perhaps split out some fields if they have a tendency to grow larger
// TODO (required): alert on metadata size
// TODO (required): versioning
public class AccordJournalValueSerializers
{
    public interface FlyweightSerializer<ENTRY, IMAGE>
    {
        IMAGE mergerFor(JournalKey key);

        void serialize(JournalKey key, ENTRY from, DataOutputPlus out, int userVersion) throws IOException;

        void reserialize(JournalKey key, IMAGE from, DataOutputPlus out, int userVersion) throws IOException;

        void deserialize(JournalKey key, IMAGE into, DataInputPlus in, int userVersion) throws IOException;
    }

    public static class CommandDiffSerializer
    implements FlyweightSerializer<SavedCommand.DiffWriter, SavedCommand.Builder>
    {
        @Override
        public SavedCommand.Builder mergerFor(JournalKey journalKey)
        {
            return new SavedCommand.Builder();
        }

        @Override
        public void serialize(JournalKey key, SavedCommand.DiffWriter writer, DataOutputPlus out, int userVersion)
        {
            try
            {
                writer.write(out, userVersion);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reserialize(JournalKey key, SavedCommand.Builder from, DataOutputPlus out, int userVersion) throws IOException
        {
            SavedCommand.DiffWriter writer = new SavedCommand.DiffWriter(null, from.construct());
            writer.write(out, userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, SavedCommand.Builder into, DataInputPlus in, int userVersion) throws IOException
        {
            into.deserializeNext(in, userVersion);
        }
    }

    public abstract static class Accumulator<A, V>
    {
        protected A accumulated;

        public Accumulator(A initial)
        {
            this.accumulated = initial;
        }

        protected void update(V newValue)
        {
            accumulated = accumulate(accumulated, newValue);
        }

        protected abstract A accumulate(A oldValue, V newValue);

        public A get()
        {
            return accumulated;
        }
    }

    public static class IdentityAccumulator<T> extends Accumulator<T, T>
    {
        public IdentityAccumulator(T initial)
        {
            super(initial);
        }

        protected T accumulate(T oldValue, T newValue)
        {
            return newValue;
        }
    }

    public static class RedundantBeforeAccumulator extends IdentityAccumulator<RedundantBefore>
    {
        public RedundantBeforeAccumulator()
        {
            super(RedundantBefore.EMPTY);
        }

        // TODO: figure out what out of order `merge` does not produce the same result
        //  See AccordJournalCompactionTest#redundantBeforeTest
        //  when modifying, only modify together with AccordSafeCommandStore
//        @Override
//        protected RedundantBefore accumulate(RedundantBefore oldValue, RedundantBefore newValue)
//        {
//            return RedundantBefore.merge(oldValue, newValue);
//        }
    }

    public static class RedundantBeforeSerializer
    implements FlyweightSerializer<RedundantBefore, RedundantBeforeAccumulator>
    {
        @Override
        public RedundantBeforeAccumulator mergerFor(JournalKey journalKey)
        {
            return new RedundantBeforeAccumulator();
        }

        @Override
        public void serialize(JournalKey key, RedundantBefore entry, DataOutputPlus out, int userVersion)
        {
            try
            {
                if (entry == RedundantBefore.EMPTY)
                {
                    out.writeInt(0);
                    return;
                }
                out.writeInt(1);
                redundantBefore.serialize(entry, out);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reserialize(JournalKey key, RedundantBeforeAccumulator from, DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, RedundantBeforeAccumulator into, DataInputPlus in, int userVersion) throws IOException
        {
            if (in.readInt() == 0)
                return;

            into.update(redundantBefore.deserialize(in));
        }
    }

    public static class DurableBeforeAccumulator extends Accumulator<DurableBefore, DurableBefore>
    {
        public DurableBeforeAccumulator()
        {
            super(DurableBefore.EMPTY);
        }

        @Override
        protected DurableBefore accumulate(DurableBefore oldValue, DurableBefore newValue)
        {
            return DurableBefore.merge(oldValue, newValue);
        }
    }

    public static class DurableBeforeSerializer implements FlyweightSerializer<DurableBefore, DurableBeforeAccumulator>
    {
        public DurableBeforeAccumulator mergerFor(JournalKey journalKey)
        {
            return new DurableBeforeAccumulator();
        }

        @Override
        public void serialize(JournalKey key, DurableBefore entry, DataOutputPlus out, int userVersion)
        {
            try
            {
                durableBefore.serialize(entry, out);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reserialize(JournalKey key, DurableBeforeAccumulator from, DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, DurableBeforeAccumulator into, DataInputPlus in, int userVersion) throws IOException
        {
            // TODO: maybe using local serializer is not the best call here, but how do we distinguish
            // between messaging and disk versioning?
            into.update(durableBefore.deserialize(in));
        }
    }

    public static class BootstrapBeganAtAccumulator extends IdentityAccumulator<NavigableMap<TxnId, Ranges>>
    {
        public BootstrapBeganAtAccumulator()
        {
            super(ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY));
        }
    }

    public static class BootstrapBeganAtSerializer
    implements FlyweightSerializer<NavigableMap<TxnId, Ranges>, BootstrapBeganAtAccumulator>
    {
        @Override
        public BootstrapBeganAtAccumulator mergerFor(JournalKey key)
        {
            return new BootstrapBeganAtAccumulator();
        }

        @Override
        public void serialize(JournalKey key, NavigableMap<TxnId, Ranges> from, DataOutputPlus out, int userVersion) throws IOException
        {
            bootstrapBeganAt.serialize(from, out);
        }

        @Override
        public void reserialize(JournalKey key, BootstrapBeganAtAccumulator from, DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(key, from.accumulated, out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, BootstrapBeganAtAccumulator into, DataInputPlus in, int userVersion) throws IOException
        {
            into.update(bootstrapBeganAt.deserialize(in));
        }
    }

    public static class SafeToReadSerializer implements FlyweightSerializer<NavigableMap<Timestamp, Ranges>, IdentityAccumulator<NavigableMap<Timestamp, Ranges>>>
    {
        @Override
        public IdentityAccumulator<NavigableMap<Timestamp, Ranges>> mergerFor(JournalKey key)
        {
            return new IdentityAccumulator<>(ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY));
        }

        public void serialize(JournalKey key, NavigableMap<Timestamp, Ranges> from, DataOutputPlus out, int userVersion) throws IOException
        {
            safeToRead.serialize(from, out);
        }

        public void reserialize(JournalKey key, IdentityAccumulator<NavigableMap<Timestamp, Ranges>> from, DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        public void deserialize(JournalKey key, IdentityAccumulator<NavigableMap<Timestamp, Ranges>> into, DataInputPlus in, int userVersion) throws IOException
        {
            into.update(safeToRead.deserialize(in));
        }
    }

    public static class RangesForEpochSerializer
    implements FlyweightSerializer<RangesForEpoch.Snapshot, IdentityAccumulator<RangesForEpoch.Snapshot>>
    {

        public IdentityAccumulator<RangesForEpoch.Snapshot> mergerFor(JournalKey key)
        {
            return new IdentityAccumulator<>(null);
        }

        public void serialize(JournalKey key, RangesForEpoch.Snapshot from, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeUnsignedVInt32(from.ranges.length);
            for (Ranges ranges : from.ranges)
                KeySerializers.ranges.serialize(ranges, out, userVersion);

            out.writeUnsignedVInt32(from.epochs.length);
            for (long epoch : from.epochs)
                out.writeLong(epoch);
        }

        public void reserialize(JournalKey key, IdentityAccumulator<RangesForEpoch.Snapshot> from, DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        public void deserialize(JournalKey key, IdentityAccumulator<RangesForEpoch.Snapshot> into, DataInputPlus in, int userVersion) throws IOException
        {
            Ranges[] ranges = new Ranges[in.readUnsignedVInt32()];
            for (int i = 0; i < ranges.length; i++)
                ranges[i] = KeySerializers.ranges.deserialize(in, userVersion);

            long[] epochs = new long[in.readUnsignedVInt32()];
            for (int i = 0; i < epochs.length; i++)
                epochs[i] = in.readLong(); // TODO: assert lengths equal?

            into.update(new RangesForEpoch.Snapshot(epochs, ranges));
        }
    }
    
    public static class HistoricalTransactionsAccumulator extends Accumulator<List<Deps>, Deps>
    {
        public HistoricalTransactionsAccumulator()
        {
            super(new ArrayList<>());
        }

        @Override
        protected List<Deps> accumulate(List<Deps> oldValue, Deps deps)
        {
            accumulated.add(deps); // we can keep it mutable
            return accumulated;
        }
    }

    public static class HistoricalTransactionsSerializer implements FlyweightSerializer<Deps, HistoricalTransactionsAccumulator>
    {
        @Override
        public HistoricalTransactionsAccumulator mergerFor(JournalKey key)
        {
            return new HistoricalTransactionsAccumulator();
        }

        public void serialize(JournalKey key, Deps from, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeUnsignedVInt32(1);
            deps.serialize(from, out, userVersion);
        }

        public void reserialize(JournalKey key, HistoricalTransactionsAccumulator from, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeUnsignedVInt32(from.get().size());
            for (Deps d : from.get())
                deps.serialize(d, out, userVersion);
        }

        public void deserialize(JournalKey key, HistoricalTransactionsAccumulator into, DataInputPlus in, int userVersion) throws IOException
        {
            int count = in.readUnsignedVInt32();
            for (int i = 0; i < count; i++)
                into.update(deps.deserialize(in, userVersion));
        }
    }
}