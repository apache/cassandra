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
import java.util.NavigableMap;

import com.google.common.collect.ImmutableSortedMap;

import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.serializers.CommandStoreSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;

import static accord.local.CommandStores.RangesForEpoch;
import static org.apache.cassandra.service.accord.SavedCommand.Load.ALL;

// TODO (required): test with large collection values, and perhaps split out some fields if they have a tendency to grow larger
// TODO (required): alert on metadata size
// TODO (required): versioning
public class AccordJournalValueSerializers
{
    private static final int messagingVersion = MessagingService.VERSION_40;
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
            return new SavedCommand.Builder(journalKey.id, ALL);
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
            from.serialize(out, userVersion);
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
        boolean hasRead;
        public IdentityAccumulator(T initial)
        {
            super(initial);
        }

        @Override
        protected T accumulate(T oldValue, T newValue)
        {
            if (hasRead)
                return oldValue;
            hasRead = true;
            return newValue;
        }
    }

    public static class RedundantBeforeAccumulator extends Accumulator<RedundantBefore, RedundantBefore>
    {
        public RedundantBeforeAccumulator()
        {
            super(RedundantBefore.EMPTY);
        }

        @Override
        protected RedundantBefore accumulate(RedundantBefore oldValue, RedundantBefore newValue)
        {
            return RedundantBefore.merge(oldValue, newValue);
        }
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
                CommandStoreSerializers.redundantBefore.serialize(entry, out, messagingVersion);
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
            {
                into.update(RedundantBefore.EMPTY);
                return;
            }
            into.update(CommandStoreSerializers.redundantBefore.deserialize(in, messagingVersion));
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
                CommandStoreSerializers.durableBefore.serialize(entry, out, messagingVersion);
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
            into.update(CommandStoreSerializers.durableBefore.deserialize(in, messagingVersion));
        }
    }

    public static class BootstrapBeganAtSerializer
    implements FlyweightSerializer<NavigableMap<TxnId, Ranges>, IdentityAccumulator<NavigableMap<TxnId, Ranges>>>
    {
        @Override
        public IdentityAccumulator<NavigableMap<TxnId, Ranges>> mergerFor(JournalKey key)
        {
            return new IdentityAccumulator<>(ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY));
        }

        @Override
        public void serialize(JournalKey key, NavigableMap<TxnId, Ranges> entry, DataOutputPlus out, int userVersion) throws IOException
        {
            CommandStoreSerializers.bootstrapBeganAt.serialize(entry, out, messagingVersion);
        }

        @Override
        public void reserialize(JournalKey key, IdentityAccumulator<NavigableMap<TxnId, Ranges>> image, DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(key, image.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, IdentityAccumulator<NavigableMap<TxnId, Ranges>> into, DataInputPlus in, int userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.bootstrapBeganAt.deserialize(in, messagingVersion));
        }
    }

    public static class SafeToReadSerializer implements FlyweightSerializer<NavigableMap<Timestamp, Ranges>, IdentityAccumulator<NavigableMap<Timestamp, Ranges>>>
    {
        @Override
        public IdentityAccumulator<NavigableMap<Timestamp, Ranges>> mergerFor(JournalKey key)
        {
            return new IdentityAccumulator<>(ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY));
        }

        @Override
        public void serialize(JournalKey key, NavigableMap<Timestamp, Ranges> from, DataOutputPlus out, int userVersion) throws IOException
        {
            CommandStoreSerializers.safeToRead.serialize(from, out, messagingVersion);
        }

        @Override
        public void reserialize(JournalKey key, IdentityAccumulator<NavigableMap<Timestamp, Ranges>> from, DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, IdentityAccumulator<NavigableMap<Timestamp, Ranges>> into, DataInputPlus in, int userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.safeToRead.deserialize(in, messagingVersion));
        }
    }

    public static class RangesForEpochSerializer
    implements FlyweightSerializer<RangesForEpoch.Snapshot, IdentityAccumulator<RangesForEpoch.Snapshot>>
    {

        public IdentityAccumulator<RangesForEpoch.Snapshot> mergerFor(JournalKey key)
        {
            return new IdentityAccumulator<>(null);
        }

        @Override
        public void serialize(JournalKey key, RangesForEpoch.Snapshot from, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeUnsignedVInt32(from.ranges.length);
            for (Ranges ranges : from.ranges)
                KeySerializers.ranges.serialize(ranges, out, messagingVersion);

            out.writeUnsignedVInt32(from.epochs.length);
            for (long epoch : from.epochs)
                out.writeLong(epoch);
        }

        @Override
        public void reserialize(JournalKey key, IdentityAccumulator<RangesForEpoch.Snapshot> from, DataOutputPlus out, int userVersion) throws IOException
        {
            serialize(key, from.get(), out, messagingVersion);
        }

        @Override
        public void deserialize(JournalKey key, IdentityAccumulator<RangesForEpoch.Snapshot> into, DataInputPlus in, int userVersion) throws IOException
        {
            Ranges[] ranges = new Ranges[in.readUnsignedVInt32()];
            for (int i = 0; i < ranges.length; i++)
                ranges[i] = KeySerializers.ranges.deserialize(in, messagingVersion);

            long[] epochs = new long[in.readUnsignedVInt32()];
            for (int i = 0; i < epochs.length; i++)
                epochs[i] = in.readLong(); // TODO: assert lengths equal?

            into.update(new RangesForEpoch.Snapshot(epochs, ranges));
        }
    }
}