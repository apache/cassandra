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

package org.apache.cassandra.service.accord.journal;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Objects;

import com.google.common.collect.ImmutableSortedMap;

import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.accord.journal.Merger.KeepFirst;
import org.apache.cassandra.service.accord.journal.Merger.SimpleMerger;
import org.apache.cassandra.service.accord.serializers.CommandStoreSerializers;
import org.apache.cassandra.service.accord.serializers.Version;

import static accord.local.CommandStores.RangesForEpoch;

public class MergeSerializers
{
    public static class CommandChangeSerializer implements MergeSerializer<CommandChangeWriter, CommandChanges, CommandChanges>
    {
        @Override
        public CommandChanges mergerFor()
        {
            return new CommandChanges();
        }

        @Override
        public void serialize(JournalKey key, CommandChangeWriter writer, DataOutputPlus out, Version userVersion)
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
        public void reserialize(JournalKey key, CommandChanges from, DataOutputPlus out, Version userVersion) throws IOException
        {
            from.serialize(out,
                           // In CompactionIterator, we are dealing with relatively recent records, so we do not pass redundant before here.
                           // However, we do on load and during Journal SSTable compaction.
                           userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, CommandChanges into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.deserializeNext(in, userVersion);
        }
    }

    public static class RedundantBeforeSerializer
    implements MergeSerializer<RedundantBefore,
                              SimpleMerger<?, ? super RedundantBefore>,
                              SimpleMerger<RedundantBefore, RedundantBefore>>
    {
        @Override
        public KeepFirst<RedundantBefore> mergerFor()
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
        public void reserialize(JournalKey key, SimpleMerger<RedundantBefore, RedundantBefore> from, DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, SimpleMerger<?, ? super RedundantBefore> into, DataInputPlus in, Version userVersion) throws IOException
        {
            if (in.readInt() == 0)
            {
                into.update(RedundantBefore.EMPTY);
                return;
            }
            into.update(CommandStoreSerializers.redundantBefore.deserialize(in));
        }
    }

    public static class DurableBeforeMerger extends SimpleMerger<DurableBefore, DurableBefore>
    {
        public DurableBeforeMerger()
        {
            super(DurableBefore.EMPTY);
        }

        @Override
        public void reset(JournalKey key)
        {
            accumulated = DurableBefore.EMPTY;
        }

        @Override
        protected DurableBefore merge(DurableBefore oldValue, DurableBefore newValue)
        {
            return DurableBefore.merge(oldValue, newValue);
        }
    }

    public static class DurableBeforeSerializer
    implements MergeSerializer<DurableBefore,
                              SimpleMerger<?, ? super DurableBefore>,
                              DurableBeforeMerger>
    {
        public DurableBeforeMerger mergerFor()
        {
            return new DurableBeforeMerger();
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
        public void reserialize(JournalKey key, DurableBeforeMerger from, DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey journalKey, SimpleMerger<?, ? super DurableBefore> into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.durableBefore.deserialize(in));
        }
    }

    public static class BootstrapBeganAtSerializer
    implements MergeSerializer<NavigableMap<TxnId, Ranges>,
                              SimpleMerger<?, ? super NavigableMap<TxnId, Ranges>>,
                              SimpleMerger<NavigableMap<TxnId, Ranges>, NavigableMap<TxnId, Ranges>>>
    {
        @Override
        public KeepFirst<NavigableMap<TxnId, Ranges>> mergerFor()
        {
            return new KeepFirst<>(ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY));
        }

        @Override
        public void serialize(JournalKey key, NavigableMap<TxnId, Ranges> entry, DataOutputPlus out, Version userVersion) throws IOException
        {
            CommandStoreSerializers.bootstrapBeganAt.serialize(entry, out);
        }

        @Override
        public void reserialize(JournalKey key, SimpleMerger<NavigableMap<TxnId, Ranges>, NavigableMap<TxnId, Ranges>> image, DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(key, image.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, SimpleMerger<?, ? super NavigableMap<TxnId, Ranges>> into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.bootstrapBeganAt.deserialize(in));
        }
    }

    public static class SafeToReadSerializer
    implements MergeSerializer<NavigableMap<Timestamp, Ranges>,
                              SimpleMerger<?, ? super NavigableMap<Timestamp, Ranges>>,
                              SimpleMerger<NavigableMap<Timestamp, Ranges>, NavigableMap<Timestamp, Ranges>>>
    {
        @Override
        public KeepFirst<NavigableMap<Timestamp, Ranges>> mergerFor()
        {
            return new KeepFirst<>(ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY));
        }

        @Override
        public void serialize(JournalKey key, NavigableMap<Timestamp, Ranges> from, DataOutputPlus out, Version userVersion) throws IOException
        {
            CommandStoreSerializers.safeToRead.serialize(from, out);
        }

        @Override
        public void reserialize(JournalKey key, SimpleMerger<NavigableMap<Timestamp, Ranges>, NavigableMap<Timestamp, Ranges>> from, DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, SimpleMerger<?, ? super NavigableMap<Timestamp, Ranges>> into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.safeToRead.deserialize(in));
        }
    }

    public static class RangesForEpochSerializer
    implements MergeSerializer<RangesForEpoch,
                              SimpleMerger<?, ? super RangesForEpoch>,
                              SimpleMerger<RangesForEpoch, RangesForEpoch>>
    {
        public static final RangesForEpochSerializer instance = new RangesForEpochSerializer();
        public KeepFirst<RangesForEpoch> mergerFor()
        {
            return new KeepFirst<>(null);
        }

        @Override
        public void serialize(JournalKey key, RangesForEpoch from, DataOutputPlus out, Version userVersion) throws IOException
        {
            CommandStoreSerializers.rangesForEpoch.serialize(from, out);
        }

        @Override
        public void reserialize(JournalKey key, SimpleMerger<RangesForEpoch, RangesForEpoch> from, DataOutputPlus out, Version userVersion) throws IOException
        {
            serialize(key, from.get(), out, userVersion);
        }

        @Override
        public void deserialize(JournalKey key, SimpleMerger<?, ? super RangesForEpoch> into, DataInputPlus in, Version userVersion) throws IOException
        {
            into.update(CommandStoreSerializers.rangesForEpoch.deserialize(in));
        }
    }

    public static class TopologySerializer implements MergeSerializer<TopologyRecord, TopologyMerger, TopologyMerger>
    {
        public static final TopologySerializer INSTANCE = new TopologySerializer();

        public TopologySerializer() {}

        @Override
        public TopologyMerger mergerFor()
        {
            return new TopologyMerger();
        }

        @Override
        public void serialize(JournalKey key, TopologyRecord from, DataOutputPlus out, Version version) throws IOException
        {
            TopologyRecord.Serializer.instance.serialize(from, out);
        }

        @Override
        public void reserialize(JournalKey key, TopologyMerger from, DataOutputPlus out, Version version) throws IOException
        {
            serialize(key, from.write, out, version);
        }

        @Override
        public void deserialize(JournalKey key, TopologyMerger into, DataInputPlus in, Version version) throws IOException
        {
            into.read(TopologyRecord.Serializer.instance.deserialize(in));
        }
    }

    public static class TopologyMerger implements Merger
    {
        TopologyRecord.TopologyImage read, write;

        public TopologyMerger()
        {
        }

        @Override
        public void reset(JournalKey key)
        {
            read = write = null;
        }

        public TopologyRecord.TopologyImage read()
        {
            return read;
        }

        public void read(TopologyRecord update)
        {
            if (Objects.requireNonNull(update.kind()) == TopologyRecord.Kind.New)
                read = new TopologyRecord.TopologyImage(update.epoch(), TopologyRecord.Kind.Image, update.getUpdate());
            else
                read = (TopologyRecord.TopologyImage) update;
            write = read;
        }

        public void write(TopologyRecord.TopologyImage image)
        {
            Invariants.require(write == read);
            this.write = image;
        }
    }
}
