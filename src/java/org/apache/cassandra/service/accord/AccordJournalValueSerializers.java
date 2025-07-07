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
import org.apache.cassandra.service.accord.journal.AccordTopologyUpdate;
import org.apache.cassandra.service.accord.serializers.CommandStoreSerializers;
import org.apache.cassandra.service.accord.serializers.Version;

import static accord.local.CommandStores.RangesForEpoch;

// TODO (required): test with large collection values, and perhaps split out some fields if they have a tendency to grow larger
// TODO (required): alert on metadata size
// TODO (required): versioning
public class AccordJournalValueSerializers
{
    public interface FlyweightImage
    {
        void reset(JournalKey key);
    }

    public interface FlyweightSerializer<ENTRY, IMAGE extends FlyweightImage>
    {
        IMAGE mergerFor();

        void serialize(JournalKey key, ENTRY from, DataOutputPlus out, Version userVersion) throws IOException;

        void reserialize(JournalKey key, IMAGE from, DataOutputPlus out, Version userVersion) throws IOException;

        void deserialize(JournalKey key, IMAGE into, DataInputPlus in, Version userVersion) throws IOException;

        default IMAGE deserialize(JournalKey key, DataInputPlus in, Version userVersion) throws IOException
        {
            IMAGE image = mergerFor();
            deserialize(key, image, in, userVersion);
            return image;
        }
    }

    public static class CommandDiffSerializer
    implements FlyweightSerializer<AccordJournal.Writer, AccordJournal.Builder>
    {
        @Override
        public AccordJournal.Builder mergerFor()
        {
            return new AccordJournal.Builder();
        }

        @Override
        public void serialize(JournalKey key, AccordJournal.Writer writer, DataOutputPlus out, Version userVersion)
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
        public void reserialize(JournalKey key, AccordJournal.Builder from, DataOutputPlus out, Version userVersion) throws IOException
        {
            from.serialize(out,
                           // In CompactionIterator, we are dealing with relatively recent records, so we do not pass redundant before here.
                           // However, we do on load and during Journal SSTable compaction.
                           userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, AccordJournal.Builder into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.deserializeNext(in, userVersion);
        }
    }

    public abstract static class Accumulator<A, V> implements FlyweightImage
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
        final T initial;
        boolean hasRead;
        public IdentityAccumulator(T initial)
        {
            super(initial);
            this.initial = initial;
        }

        @Override
        public void reset(JournalKey key)
        {
            hasRead = false;
            accumulated = initial;
        }

        @Override
        protected T accumulate(T oldValue, T newValue)
        {
            if (hasRead)
                return oldValue;
            hasRead = true;
            return newValue;
        }

        @Override
        public String toString()
        {
            return "IdentityAccumulator{" +
                   initial +
                   '}';
        }
    }

    public static class RedundantBeforeSerializer
    implements FlyweightSerializer<RedundantBefore, IdentityAccumulator<RedundantBefore>>
    {
        @Override
        public IdentityAccumulator<RedundantBefore> mergerFor()
        {
            return new IdentityAccumulator<>(RedundantBefore.EMPTY);
        }

        @Override
        public void serialize(JournalKey key, RedundantBefore entry, DataOutputPlus out, Version userVersion)
        {
            try
            {
                if (entry == RedundantBefore.EMPTY)
                {
                    out.writeInt(0);
                    return;
                }
                out.writeInt(1);
                CommandStoreSerializers.redundantBefore.serialize(entry, out);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reserialize(JournalKey key, IdentityAccumulator<RedundantBefore> from, DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, IdentityAccumulator<RedundantBefore> into, DataInputPlus in, Version userVersion) throws IOException
        {
            if (in.readInt() == 0)
            {
                into.update(RedundantBefore.EMPTY);
                return;
            }
            into.update(CommandStoreSerializers.redundantBefore.deserialize(in));
        }
    }

    public static class DurableBeforeAccumulator extends Accumulator<DurableBefore, DurableBefore>
    {
        public DurableBeforeAccumulator()
        {
            super(DurableBefore.EMPTY);
        }

        @Override
        public void reset(JournalKey key)
        {
            accumulated = DurableBefore.EMPTY;
        }

        @Override
        protected DurableBefore accumulate(DurableBefore oldValue, DurableBefore newValue)
        {
            return DurableBefore.merge(oldValue, newValue);
        }
    }

    public static class DurableBeforeSerializer
    implements FlyweightSerializer<DurableBefore, DurableBeforeAccumulator>
    {
        public DurableBeforeAccumulator mergerFor()
        {
            return new DurableBeforeAccumulator();
        }

        @Override
        public void serialize(JournalKey key, DurableBefore entry, DataOutputPlus out, Version userVersion)
        {
            try
            {
                CommandStoreSerializers.durableBefore.serialize(entry, out);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void reserialize(JournalKey key, DurableBeforeAccumulator from, DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, DurableBeforeAccumulator into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.durableBefore.deserialize(in));
        }
    }

    public static class BootstrapBeganAtSerializer
    implements FlyweightSerializer<NavigableMap<TxnId, Ranges>, IdentityAccumulator<NavigableMap<TxnId, Ranges>>>
    {
        @Override
        public IdentityAccumulator<NavigableMap<TxnId, Ranges>> mergerFor()
        {
            return new IdentityAccumulator<>(ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY));
        }

        @Override
        public void serialize(JournalKey key, NavigableMap<TxnId, Ranges> entry, DataOutputPlus out, Version userVersion) throws IOException
        {
            CommandStoreSerializers.bootstrapBeganAt.serialize(entry, out);
        }

        @Override
        public void reserialize(JournalKey key, IdentityAccumulator<NavigableMap<TxnId, Ranges>> image, DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(key, image.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, IdentityAccumulator<NavigableMap<TxnId, Ranges>> into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.bootstrapBeganAt.deserialize(in));
        }
    }

    public static class SafeToReadSerializer
    implements FlyweightSerializer<NavigableMap<Timestamp, Ranges>, IdentityAccumulator<NavigableMap<Timestamp, Ranges>>>
    {
        @Override
        public IdentityAccumulator<NavigableMap<Timestamp, Ranges>> mergerFor()
        {
            return new IdentityAccumulator<>(ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY));
        }

        @Override
        public void serialize(JournalKey key, NavigableMap<Timestamp, Ranges> from, DataOutputPlus out, Version userVersion) throws IOException
        {
            CommandStoreSerializers.safeToRead.serialize(from, out);
        }

        @Override
        public void reserialize(JournalKey key, IdentityAccumulator<NavigableMap<Timestamp, Ranges>> from, DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, IdentityAccumulator<NavigableMap<Timestamp, Ranges>> into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.safeToRead.deserialize(in));
        }
    }

    public static class RangesForEpochSerializer
    implements FlyweightSerializer<RangesForEpoch, Accumulator<RangesForEpoch, RangesForEpoch>>
    {
        public static final RangesForEpochSerializer instance = new RangesForEpochSerializer();
        public IdentityAccumulator<RangesForEpoch> mergerFor()
        {
            return new IdentityAccumulator<>(null);
        }

        @Override
        public void serialize(JournalKey key, RangesForEpoch from, DataOutputPlus out, Version userVersion) throws IOException
        {
            AccordTopologyUpdate.RangesForEpochSerializer.instance.serialize(from, out);
        }

        @Override
        public void reserialize(JournalKey key, Accumulator<RangesForEpoch, RangesForEpoch> from, DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, Accumulator<RangesForEpoch, RangesForEpoch> into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.update(AccordTopologyUpdate.RangesForEpochSerializer.instance.deserialize(in));
        }
    }
}
