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
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.journal.AccordTopologyUpdate;
import org.apache.cassandra.service.accord.serializers.CommandStoreSerializers;
import org.apache.cassandra.service.accord.serializers.Version;

import static accord.local.CommandStores.RangesForEpoch;

public class AccordJournalSerializers
{
    public interface Builder
    {
        void reset(JournalKey key);
    }

    public interface MergeSerializer<V, DeserializeInto extends Builder, B extends DeserializeInto>
    {
        B builderFor();

        void deserialize(JournalKey key, DeserializeInto into, DataInputPlus in, Version userVersion) throws IOException;

        default B deserialize(JournalKey key, DataInputPlus in, Version userVersion) throws IOException
        {
            B builder = builderFor();
            deserialize(key, builder, in, userVersion);
            return builder;
        }

        void serialize(JournalKey key, V from, DataOutputPlus out, Version userVersion) throws IOException;

        void reserialize(JournalKey key, B from, DataOutputPlus out, Version userVersion) throws IOException;
    }

    public static class CommandChangeSerializer implements MergeSerializer<AccordJournal.CommandChangeWriter, AccordJournal.CommandChanges, AccordJournal.CommandChanges>
    {
        @Override
        public AccordJournal.CommandChanges builderFor()
        {
            return new AccordJournal.CommandChanges();
        }

        @Override
        public void serialize(JournalKey key, AccordJournal.CommandChangeWriter writer, DataOutputPlus out, Version userVersion)
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
        public void reserialize(JournalKey key, AccordJournal.CommandChanges from, DataOutputPlus out, Version userVersion) throws IOException
        {
            from.serialize(out,
                           // In CompactionIterator, we are dealing with relatively recent records, so we do not pass redundant before here.
                           // However, we do on load and during Journal SSTable compaction.
                           userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, AccordJournal.CommandChanges into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.deserializeNext(in, userVersion);
        }
    }

    public abstract static class Accumulator<A, V> implements Builder
    {
        protected A accumulated;

        public Accumulator(A initial)
        {
            this.accumulated = initial;
        }

        public void update(V newValue)
        {
            accumulated = accumulate(accumulated, newValue);
        }

        protected abstract A accumulate(A oldValue, V newValue);

        public A get()
        {
            return accumulated;
        }
    }

    public static class KeepFirst<V> extends Accumulator<V, V>
    {
        final V ifNone;
        boolean hasRead;
        public KeepFirst(V ifNone)
        {
            super(ifNone);
            this.ifNone = ifNone;
        }

        @Override
        public void reset(JournalKey key)
        {
            hasRead = false;
            accumulated = ifNone;
        }

        @Override
        protected V accumulate(V oldValue, V newValue)
        {
            if (hasRead)
                return oldValue;
            hasRead = true;
            return newValue;
        }

        @Override
        public String toString()
        {
            return "KeepFirst{" +
                   accumulated +
                   '}';
        }
    }

    public static class KeepList<V> extends Accumulator<List<V>, V>
    {
        public KeepList(List<V> initial)
        {
            super(initial);
        }

        public KeepList()
        {
            super(new ArrayList<>());
        }

        @Override
        protected List<V> accumulate(List<V> oldValue, V newValue)
        {
            oldValue.add(newValue);
            return oldValue;
        }

        @Override
        public void reset(JournalKey key)
        {
            accumulated.clear();
        }
    }

    public static class RedundantBeforeSerializer
    implements MergeSerializer<RedundantBefore, Accumulator<?, ? super RedundantBefore>, Accumulator<RedundantBefore, RedundantBefore>>
    {
        @Override
        public KeepFirst<RedundantBefore> builderFor()
        {
            return new KeepFirst<>(RedundantBefore.EMPTY);
        }

        @Override
        public void serialize(JournalKey key, RedundantBefore entry, DataOutputPlus out, Version userVersion)
        {
            try
            {
                if (entry == RedundantBefore.EMPTY)
                {
                    // I am fairly sure this branch was to paper over a bug in the RedundantBefore serializer; it should now be defunct
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
        public void reserialize(JournalKey key, Accumulator<RedundantBefore, RedundantBefore> from, DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, Accumulator<?, ? super RedundantBefore> into, DataInputPlus in, Version userVersion) throws IOException
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
    implements MergeSerializer<DurableBefore, Accumulator<?, ? super DurableBefore>, DurableBeforeAccumulator>
    {
        public DurableBeforeAccumulator builderFor()
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
        public void deserialize(JournalKey journalKey, Accumulator<?, ? super DurableBefore> into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.durableBefore.deserialize(in));
        }
    }

    public static class BootstrapBeganAtSerializer
    implements MergeSerializer<NavigableMap<TxnId, Ranges>,
                              Accumulator<?, ? super NavigableMap<TxnId, Ranges>>,
                              Accumulator<NavigableMap<TxnId, Ranges>, NavigableMap<TxnId, Ranges>>>
    {
        @Override
        public KeepFirst<NavigableMap<TxnId, Ranges>> builderFor()
        {
            return new KeepFirst<>(ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY));
        }

        @Override
        public void serialize(JournalKey key, NavigableMap<TxnId, Ranges> entry, DataOutputPlus out, Version userVersion) throws IOException
        {
            CommandStoreSerializers.bootstrapBeganAt.serialize(entry, out);
        }

        @Override
        public void reserialize(JournalKey key, Accumulator<NavigableMap<TxnId, Ranges>, NavigableMap<TxnId, Ranges>> image, DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(key, image.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, Accumulator<?, ? super NavigableMap<TxnId, Ranges>> into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.bootstrapBeganAt.deserialize(in));
        }
    }

    public static class SafeToReadSerializer
    implements MergeSerializer<NavigableMap<Timestamp, Ranges>,
                              Accumulator<?, ? super NavigableMap<Timestamp, Ranges>>,
                              Accumulator<NavigableMap<Timestamp, Ranges>, NavigableMap<Timestamp, Ranges>>>
    {
        @Override
        public KeepFirst<NavigableMap<Timestamp, Ranges>> builderFor()
        {
            return new KeepFirst<>(ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY));
        }

        @Override
        public void serialize(JournalKey key, NavigableMap<Timestamp, Ranges> from, DataOutputPlus out, Version userVersion) throws IOException
        {
            CommandStoreSerializers.safeToRead.serialize(from, out);
        }

        @Override
        public void reserialize(JournalKey key, Accumulator<NavigableMap<Timestamp, Ranges>, NavigableMap<Timestamp, Ranges>> from, DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, Accumulator<?, ? super NavigableMap<Timestamp, Ranges>> into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.safeToRead.deserialize(in));
        }
    }

    public static class RangesForEpochSerializer
    implements MergeSerializer<RangesForEpoch,
                              Accumulator<?, ? super RangesForEpoch>,
                              Accumulator<RangesForEpoch, RangesForEpoch>>
    {
        public static final RangesForEpochSerializer instance = new RangesForEpochSerializer();
        public KeepFirst<RangesForEpoch> builderFor()
        {
            return new KeepFirst<>(null);
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
        public void deserialize(JournalKey key, Accumulator<?, ? super RangesForEpoch> into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.update(AccordTopologyUpdate.RangesForEpochSerializer.instance.deserialize(in));
        }
    }
}
