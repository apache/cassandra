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
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;

import accord.api.Journal;
import accord.local.CommandStores;
import accord.primitives.EpochSupplier;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordConfigurationService;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.service.accord.serializers.Version;

public interface AccordTopologyUpdate
{
    Kind kind();
    void applyTo(TopologyImage accumulator);
    long epoch();

    Journal.TopologyUpdate getUpdate();
    static AccordTopologyUpdate newTopology(Journal.TopologyUpdate update)
    {
        return new NewTopology(update);
    }
    class RangesForEpochSerializer implements UnversionedSerializer<CommandStores.RangesForEpoch>
    {
        public static final RangesForEpochSerializer instance = new RangesForEpochSerializer();

        @Override
        public void serialize(CommandStores.RangesForEpoch from, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(from.size());
            for (int i = 0; i < from.size(); i++)
            {
                out.writeLong(from.epochAtIndex(i));
                KeySerializers.ranges.serialize(from.rangesAtIndex(i), out);
            }
        }

        @Override
        public CommandStores.RangesForEpoch deserialize(DataInputPlus in) throws IOException
        {
            int size = in.readUnsignedVInt32();
            Ranges[] ranges = new Ranges[size];
            long[] epochs = new long[size];
            for (int i = 0; i < ranges.length; i++)
            {
                epochs[i] = in.readLong();
                ranges[i] = KeySerializers.ranges.deserialize(in);
            }
            Invariants.require(ranges.length == epochs.length);
            return new CommandStores.RangesForEpoch(epochs, ranges);
        }

        @Override
        public long serializedSize(CommandStores.RangesForEpoch from)
        {
            long size = TypeSizes.sizeofUnsignedVInt(from.size());
            for (int i = 0; i < from.size(); i++)
            {
                size += TypeSizes.sizeof(from.epochAtIndex(i));
                size += KeySerializers.ranges.serializedSize(from.rangesAtIndex(i));
            }
            return size;
        }
    }

    class TopologyUpdateSerializer implements UnversionedSerializer<Journal.TopologyUpdate>
    {
        public static final TopologyUpdateSerializer instance = new TopologyUpdateSerializer();

        @Override
        public void serialize(Journal.TopologyUpdate from, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(from.commandStores.size());
            for (Map.Entry<Integer, CommandStores.RangesForEpoch> e : from.commandStores.entrySet())
            {
                out.writeUnsignedVInt32(e.getKey());
                RangesForEpochSerializer.instance.serialize(e.getValue(), out);
            }
            TopologySerializers.compactTopology.serialize(from.global, out);
        }

        @Override
        public Journal.TopologyUpdate deserialize(DataInputPlus in) throws IOException
        {
            int commandStoresSize = in.readUnsignedVInt32();
            Int2ObjectHashMap<CommandStores.RangesForEpoch> commandStores = new Int2ObjectHashMap<>();
            for (int j = 0; j < commandStoresSize; j++)
            {
                int commandStoreId = in.readUnsignedVInt32();
                CommandStores.RangesForEpoch rangesForEpoch = RangesForEpochSerializer.instance.deserialize(in);
                commandStores.put(commandStoreId, rangesForEpoch);
            }
            Topology global = TopologySerializers.compactTopology.deserialize(in);
            return new Journal.TopologyUpdate(commandStores, global);
        }

        @Override
        public long serializedSize(Journal.TopologyUpdate from)
        {
            long size = TypeSizes.sizeofUnsignedVInt(from.commandStores.size());
            for (Map.Entry<Integer, CommandStores.RangesForEpoch> e : from.commandStores.entrySet())
            {
                size += TypeSizes.sizeofUnsignedVInt(e.getKey());
                size += RangesForEpochSerializer.instance.serializedSize(e.getValue());
            }

            size += TopologySerializers.compactTopology.serializedSize(from.global);
            return size;
        }
    }

    class Serializer implements UnversionedSerializer<AccordTopologyUpdate>
    {
        public static Serializer instance = new Serializer();

        @Override
        public void serialize(AccordTopologyUpdate t, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt(t.epoch());
            out.writeUnsignedVInt32(t.kind().ordinal());
            switch (t.kind())
            {
                case NewTopology:
                {
                    TopologyUpdateSerializer.instance.serialize(((NewTopology) t).update, out);
                    break;
                }
                case NoOp:
                {
                    TopologyImage image = (TopologyImage) t;
                    Invariants.require(image.update == null);
                    if (image.syncStatus == null)
                        out.writeByte(Byte.MAX_VALUE);
                    else
                        out.writeByte(image.syncStatus.ordinal());

                    KeySerializers.ranges.serialize(image.closed, out);
                    KeySerializers.ranges.serialize(image.retired, out);
                    break;
                }
                case TopologyImage:
                {
                    TopologyImage image = (TopologyImage) t;

                    out.writeBoolean(image.update != null);
                    if (image.update != null)
                        TopologyUpdateSerializer.instance.serialize(image.update, out);
                    if (image.syncStatus == null)
                        out.writeByte(Byte.MAX_VALUE);
                    else
                        out.writeByte(image.syncStatus.ordinal());

                    KeySerializers.ranges.serialize(image.closed, out);
                    KeySerializers.ranges.serialize(image.retired, out);
                    break;
                }
                default:
                    throw new UnhandledEnum(t.kind());
            }
        }

        @Override
        public AccordTopologyUpdate deserialize(DataInputPlus in) throws IOException
        {
            long epoch = in.readUnsignedVInt();
            Kind kind = Kind.values()[in.readUnsignedVInt32()];
            switch (kind)
            {
                case NewTopology:
                    return new NewTopology(TopologyUpdateSerializer.instance.deserialize(in));
                case NoOp:
                {
                    TopologyImage image = new TopologyImage(epoch, Kind.NoOp);
                    byte syncStateByte = in.readByte();
                    if (syncStateByte != Byte.MAX_VALUE)
                        image.syncStatus = AccordConfigurationService.SyncStatus.values()[syncStateByte];

                    image.closed = KeySerializers.ranges.deserialize(in);
                    image.retired = KeySerializers.ranges.deserialize(in);
                    return image;
                }
                case TopologyImage:
                {
                    TopologyImage image = new TopologyImage(epoch, Kind.TopologyImage);
                    if (in.readBoolean())
                        image.update = TopologyUpdateSerializer.instance.deserialize(in);

                    byte syncStateByte = in.readByte();
                    if (syncStateByte != Byte.MAX_VALUE)
                        image.syncStatus = AccordConfigurationService.SyncStatus.values()[syncStateByte];

                    image.closed = KeySerializers.ranges.deserialize(in);
                    image.retired = KeySerializers.ranges.deserialize(in);
                    return image;
                }
                default:
                    throw new UnhandledEnum(kind);
            }
        }

        @Override
        public long serializedSize(AccordTopologyUpdate t)
        {
            long size = TypeSizes.sizeofUnsignedVInt(t.epoch());
            size += TypeSizes.sizeofUnsignedVInt(t.kind().ordinal());

            switch (t.kind())
            {
                case NewTopology:
                    size += TopologyUpdateSerializer.instance.serializedSize(((NewTopology) t).update);
                    break;
                case NoOp:
                {
                    TopologyImage image = (TopologyImage) t;
                    Invariants.require(image.update == null);

                    size += Byte.BYTES;

                    size += KeySerializers.ranges.serializedSize(image.closed);
                    size += KeySerializers.ranges.serializedSize(image.retired);
                    break;
                }
                case TopologyImage:
                {
                    TopologyImage image = (TopologyImage) t;

                    size += TypeSizes.sizeof(image.update != null);
                    if (image.update != null)
                        size += TopologyUpdateSerializer.instance.serializedSize(image.update);

                    size += Byte.BYTES;

                    size += KeySerializers.ranges.serializedSize(image.closed);
                    size += KeySerializers.ranges.serializedSize(image.retired);
                    break;
                }
                default:
                    throw new UnhandledEnum(t.kind());
            }
            return size;
        }
    }

    enum Kind
    {
        // New Topology, written to journal when the node first learned about it
        NewTopology,
        // Used when accumulating state during compaction or replay
        TopologyImage,
        // Effectively unchanged topology
        NoOp
    }

    class ImmutableTopoloyImage extends Journal.TopologyUpdate
    {
        public ImmutableTopoloyImage(TopologyImage image)
        {
            super(image.update.commandStores, image.update.global);
        }
    }

    class TopologyImage implements AccordTopologyUpdate
    {
        private Journal.TopologyUpdate update;
        private AccordConfigurationService.SyncStatus syncStatus = null;

        private Ranges closed = Ranges.EMPTY;
        private Ranges retired = Ranges.EMPTY;

        private final long epoch;
        private final Kind kind;

        public TopologyImage(long epoch, Kind kind)
        {
            Invariants.require(kind != Kind.NewTopology);
            this.epoch = epoch;
            this.kind = kind;
        }

        public TopologyImage asImage(Journal.TopologyUpdate update)
        {
            TopologyImage image = new TopologyImage(epoch, Kind.TopologyImage);
            image.update = update.cloneWithEquivalentEpoch(epoch);
            image.closed = closed;
            image.retired = retired;
            return image;
        }

        public TopologyImage asNoOp()
        {
            TopologyImage image = new TopologyImage(epoch, Kind.NoOp);
            image.closed = closed;
            image.retired = retired;
            return image;
        }

        @Override
        public long epoch()
        {
            return this.epoch;
        }

        @Override
        public Journal.TopologyUpdate getUpdate()
        {
            return update;
        }

        @Override
        public Kind kind()
        {
            return kind;
        }

        @Override
        public void applyTo(TopologyImage accumulator)
        {
            Invariants.require(accumulator.epoch == epoch);
            Invariants.require(accumulator.update == null || accumulator.update.equals(update));
            accumulator.update = update;
            // We're iterating in _reverse_ order
            if (accumulator.syncStatus == null)
                accumulator.syncStatus = syncStatus;
            accumulator.closed = accumulator.closed.with(closed);
            accumulator.retired = accumulator.retired.with(retired);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopologyImage that = (TopologyImage) o;
            return epoch == that.epoch && Objects.equals(update, that.update) && syncStatus == that.syncStatus && closed.equals(that.closed) && retired.equals(that.retired);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(update, syncStatus, closed, retired, epoch);
        }
    }

    class NewTopology implements AccordTopologyUpdate
    {
        public final Journal.TopologyUpdate update;
        private final long epoch;

        public NewTopology(Journal.TopologyUpdate update)
        {
            this.epoch = update.global.epoch();
            this.update = update;
        }

        @Override
        public long epoch()
        {
            return this.epoch;
        }

        @Override
        public Journal.TopologyUpdate getUpdate()
        {
            return update;
        }

        @Override
        public Kind kind()
        {
            return Kind.NewTopology;
        }

        @Override
        public void applyTo(TopologyImage accumulator)
        {
            Invariants.require(accumulator.epoch == epoch);
            Invariants.require(accumulator.update == null);
            accumulator.update = update;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NewTopology that = (NewTopology) o;
            return epoch == that.epoch && update.equals(that.update);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(update, epoch);
        }
    }

    class Accumulator
    extends AccordJournalValueSerializers.Accumulator<NavigableMap<Long, TopologyImage>, AccordTopologyUpdate>
    {
        public Accumulator()
        {
            super(new TreeMap<>());
        }

        @Override
        public void reset(JournalKey key)
        {
            accumulated = new TreeMap<>();
        }

        @Override
        public void update(AccordTopologyUpdate newValue)
        {
            super.update(newValue);
        }

        public Iterator<ImmutableTopoloyImage> images()
        {
            return map(get().values().iterator(), ImmutableTopoloyImage::new);
        }

        @Override
        protected NavigableMap<Long, TopologyImage> accumulate(NavigableMap<Long, TopologyImage> allEpochs, AccordTopologyUpdate update)
        {
            update.applyTo(allEpochs.computeIfAbsent(update.epoch(), v -> new TopologyImage(update.epoch(), Kind.TopologyImage)));
            return allEpochs;
        }
    }

    static <FROM, TO> Iterator<TO> map(Iterator<FROM> iter, Function<FROM, TO> fn)
    {
        return new Iterator<TO>()
        {
            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public TO next()
            {
                return fn.apply(iter.next());
            }
        };
    }

    class AccumulatingSerializer
    implements AccordJournalValueSerializers.FlyweightSerializer<AccordTopologyUpdate, Accumulator>
    {
        public static final AccumulatingSerializer defaultInstance = new AccumulatingSerializer(() -> 0);

        private final EpochSupplier minEpoch;
        public AccumulatingSerializer(EpochSupplier minEpoch)
        {
            this.minEpoch = minEpoch;
        }

        @Override
        public Accumulator mergerFor()
        {
            return new Accumulator();
        }

        @Override
        public void serialize(JournalKey key, AccordTopologyUpdate from, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUnsignedVInt32(1);
            Serializer.instance.serialize(from, out);
        }

        @Override
        public void reserialize(JournalKey key, Accumulator from, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUnsignedVInt32(from.get().size());
            Journal.TopologyUpdate prev = null;
            for (TopologyImage value : from.get().values())
            {
                Journal.TopologyUpdate tmp = value.update;
                if (prev != null && value.update.isEquivalent(prev))
                    value = value.asNoOp();

                prev = tmp;
                Serializer.instance.serialize(value, out);
            }
        }

        @Override
        public void deserialize(JournalKey key, Accumulator into, DataInputPlus in, Version version) throws IOException
        {
            long minEpoch = this.minEpoch.epoch();
            int count = in.readUnsignedVInt32();
            AccordTopologyUpdate prev = null;
            while (--count >= 0)
            {
                AccordTopologyUpdate update = Serializer.instance.deserialize(in);
                if (update.kind() == Kind.NoOp)
                {
                    Invariants.require(prev != null);
                    update = ((TopologyImage) update).asImage(prev.getUpdate());
                }
                if (update.epoch() >= minEpoch)
                    into.update(update);
                prev = update;
            }
        }
    }
}