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
import java.util.Map;
import java.util.Objects;
import accord.api.Journal;
import accord.local.CommandStores;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
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
    AccordTopologyUpdate asRepeat();

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
            return new CommandStores.RangesForEpoch(epochs, ranges);
        }

        @Override
        public long serializedSize(CommandStores.RangesForEpoch from)
        {
            long size = TypeSizes.sizeofUnsignedVInt(from.size());
            for (int i = 0; i < from.size(); i++)
            {
                size += TypeSizes.LONG_SIZE;
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
                case New:
                {
                    TopologyUpdateSerializer.instance.serialize(((NewTopology) t).update, out);
                    break;
                }
                case Repeat:
                case Image:
                    TopologyImage image = (TopologyImage) t;
                    out.writeBoolean(image.update != null);
                    if (image.update != null)
                        TopologyUpdateSerializer.instance.serialize(image.update, out);
                    out.writeByte(0); // defunct enum byte

                    KeySerializers.ranges.serialize(image.closed, out);
                    KeySerializers.ranges.serialize(image.retired, out);
                    break;
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
                case New:
                    return new NewTopology(TopologyUpdateSerializer.instance.deserialize(in));
                case Repeat:
                case Image:
                    Journal.TopologyUpdate update = null;
                    if (in.readBoolean())
                        update = TopologyUpdateSerializer.instance.deserialize(in);

                    TopologyImage image = new TopologyImage(epoch, kind, update);
                    in.readByte(); // defunct enum byte

                    image.closed = KeySerializers.ranges.deserialize(in);
                    image.retired = KeySerializers.ranges.deserialize(in);
                    return image;
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
                case New:
                    size += TopologyUpdateSerializer.instance.serializedSize(((NewTopology) t).update);
                    break;
                case Image:
                case Repeat:
                    TopologyImage image = (TopologyImage) t;
                    size += TypeSizes.BOOL_SIZE;
                    if (image.update != null)
                        size += TopologyUpdateSerializer.instance.serializedSize(image.update);

                    size += TypeSizes.BYTE_SIZE;
                    size += KeySerializers.ranges.serializedSize(image.closed);
                    size += KeySerializers.ranges.serializedSize(image.retired);
                    break;
                default:
                    throw new UnhandledEnum(t.kind());
            }
            return size;
        }
    }

    enum Kind
    {
        // New Topology, written to journal when the node first learned about it
        New,
        // Used when accumulating state during compaction or replay
        Image,
        // Effectively unchanged topology
        // During compaction, we can write a no-op if we know that from Accord's perspective topology has not changed
        // (see CompactionIterator$TopologyCompactor). During replay/deserialization, we collect last known changed
        // epoch, and reconstruct its topology.
        Repeat
    }


    class TopologyImage implements AccordTopologyUpdate
    {
        private final long epoch;
        private final Kind kind;
        private Journal.TopologyUpdate update;

        private Ranges closed = Ranges.EMPTY;
        private Ranges retired = Ranges.EMPTY;


        public TopologyImage(long epoch, Kind kind)
        {
            this.epoch = epoch;
            this.kind = Invariants.requireArgument(kind, kind == Kind.Repeat);
        }

        public TopologyImage(long epoch, Kind kind, Journal.TopologyUpdate update)
        {
            this.epoch = epoch;
            this.kind = kind;
            this.update = Invariants.requireArgument(update, update != null || kind == Kind.Repeat);
        }

        public TopologyImage asImage(Journal.TopologyUpdate update)
        {
            TopologyImage image = new TopologyImage(epoch, Kind.Image, update.cloneWithEquivalentEpoch(epoch));
            image.closed = closed;
            image.retired = retired;
            return image;
        }

        public TopologyImage asRepeat()
        {
            TopologyImage image = new TopologyImage(epoch, Kind.Repeat, update);
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
            Invariants.require(accumulator.epoch == epoch, "Expected %d but got %d", epoch, accumulator.epoch);
            if (kind() == Kind.Repeat)
            {
                accumulator.update = null;
                return;
            }

            Invariants.require(accumulator.update == null || accumulator.update.equals(update));
            accumulator.update = update;
            accumulator.closed = accumulator.closed.with(closed);
            accumulator.retired = accumulator.retired.with(retired);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopologyImage that = (TopologyImage) o;
            return epoch == that.epoch && Objects.equals(update, that.update) && closed.equals(that.closed) && retired.equals(that.retired);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(update, closed, retired, epoch);
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
            return Kind.New;
        }

        @Override
        public void applyTo(TopologyImage accumulator)
        {
            Invariants.require(accumulator.epoch == epoch);
            Invariants.require(accumulator.update == null);
            accumulator.update = update;
        }

        @Override
        public AccordTopologyUpdate asRepeat()
        {
            return new TopologyImage(epoch, Kind.Repeat, update);
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

    class Accumulator implements AccordJournalValueSerializers.FlyweightImage
    {
        TopologyImage read, write;

        public Accumulator()
        {
        }

        @Override
        public void reset(JournalKey key)
        {
            read = write = null;
        }

        public TopologyImage read()
        {
            return read;
        }

        public void read(AccordTopologyUpdate update)
        {
            if (Objects.requireNonNull(update.kind()) == Kind.New)
                read = new TopologyImage(update.epoch(), Kind.Image, update.getUpdate());
            else
                read = (TopologyImage) update;
            write = read;
        }

        public void write(TopologyImage image)
        {
            Invariants.require(write == read);
            this.write = image;
        }
    }

    class FlyweightSerializer implements AccordJournalValueSerializers.FlyweightSerializer<AccordTopologyUpdate, Accumulator>
    {
        public FlyweightSerializer() {}

        @Override
        public Accumulator mergerFor()
        {
            return new Accumulator();
        }

        @Override
        public void serialize(JournalKey key, AccordTopologyUpdate from, DataOutputPlus out, Version version) throws IOException
        {
            Serializer.instance.serialize(from, out);
        }

        @Override
        public void reserialize(JournalKey key, Accumulator from, DataOutputPlus out, Version version) throws IOException
        {
            serialize(key, from.write, out, version);
        }

        @Override
        public void deserialize(JournalKey key, Accumulator into, DataInputPlus in, Version version) throws IOException
        {
            into.read(Serializer.instance.deserialize(in));
        }
    }
}