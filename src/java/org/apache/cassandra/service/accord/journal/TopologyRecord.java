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

import org.agrona.collections.Int2ObjectHashMap;

import accord.api.Journal;
import accord.local.CommandStores;
import accord.local.CommandStores.PreviouslyOwned;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;

import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializers.rangesForEpoch;

public interface TopologyRecord
{
    Kind kind();
    void applyTo(TopologyImage accumulator);
    long epoch();
    TopologyRecord asRepeat();

    Journal.TopologyUpdate update();
    static TopologyRecord newTopology(Journal.TopologyUpdate update)
    {
        return new NewTopology(update);
    }

    static JournalKey journalKey(long epoch)
    {
        return new JournalKey(TxnId.fromValues(epoch, 0L, Node.Id.NONE),
                              JournalKey.Type.TOPOLOGY_UPDATE, Integer.MAX_VALUE);
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

    class TopologyImage implements TopologyRecord
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
        public Journal.TopologyUpdate update()
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

    class NewTopology implements TopologyRecord
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
        public Journal.TopologyUpdate update()
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
        public TopologyRecord asRepeat()
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

    class TopologyUpdateSerializer implements UnversionedSerializer<Journal.TopologyUpdate>
    {
        private static final int TOP_BIT = 0x40000000;
        public static final TopologyUpdateSerializer instance = new TopologyUpdateSerializer();

        @Override
        public void serialize(Journal.TopologyUpdate from, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(from.commandStores.size() | TOP_BIT);
            out.writeUnsignedVInt32(0);
            out.writeUnsignedVInt32(from.previouslyOwned.size());
            for (int i = 0 ; i < from.previouslyOwned.size() ; ++i)
            {
                out.writeUnsignedVInt(from.previouslyOwned.epochs(i));
                KeySerializers.ranges.serialize(from.previouslyOwned.ranges(i), out);
            }
            for (Map.Entry<Integer, CommandStores.RangesForEpoch> e : from.commandStores.entrySet())
            {
                out.writeUnsignedVInt32(e.getKey());
                rangesForEpoch.serialize(e.getValue(), out);
            }
            TopologySerializers.compactTopology.serialize(from.global, out);
        }

        @Override
        public Journal.TopologyUpdate deserialize(DataInputPlus in) throws IOException
        {
            int commandStoresSize = in.readUnsignedVInt32();
            int flags = 0;
            PreviouslyOwned previouslyOwned = PreviouslyOwned.EMPTY;
            if ((commandStoresSize & TOP_BIT) != 0)
            {
                commandStoresSize ^= TOP_BIT;
                // future proofing
                flags = in.readUnsignedVInt32();
                int previouslyOwnedSize = in.readUnsignedVInt32();
                long[] epochs = new long[previouslyOwnedSize];
                Ranges[] ranges = new Ranges[previouslyOwnedSize];
                for (int i = 0 ; i < previouslyOwnedSize ; ++i)
                {
                    epochs[i] = in.readUnsignedVInt();
                    ranges[i] = KeySerializers.ranges.deserialize(in);
                }
                previouslyOwned = new PreviouslyOwned(epochs.length == 0 ? 0 : epochs[0], epochs, ranges);
            }
            Int2ObjectHashMap<CommandStores.RangesForEpoch> commandStores = new Int2ObjectHashMap<>();
            for (int j = 0; j < commandStoresSize; j++)
            {
                int commandStoreId = in.readUnsignedVInt32();
                CommandStores.RangesForEpoch rfe = rangesForEpoch.deserialize(in);
                commandStores.put(commandStoreId, rfe);
            }
            Topology global = TopologySerializers.compactTopology.deserialize(in);
            return new Journal.TopologyUpdate(commandStores, global, previouslyOwned);
        }

        @Override
        public long serializedSize(Journal.TopologyUpdate from)
        {
            long size = TypeSizes.sizeofUnsignedVInt(from.commandStores.size() | TOP_BIT);
            size += TypeSizes.sizeofUnsignedVInt(0);
            size += TypeSizes.sizeofUnsignedVInt(from.previouslyOwned.size());
            for (int i = 0 ; i < from.previouslyOwned.size() ; ++i)
            {
                size += TypeSizes.sizeofUnsignedVInt(from.previouslyOwned.epochs(i));
                size += KeySerializers.ranges.serializedSize(from.previouslyOwned.ranges(i));
            }
            for (Map.Entry<Integer, CommandStores.RangesForEpoch> e : from.commandStores.entrySet())
            {
                size += TypeSizes.sizeofUnsignedVInt(e.getKey());
                size += rangesForEpoch.serializedSize(e.getValue());
            }

            size += TopologySerializers.compactTopology.serializedSize(from.global);
            return size;
        }
    }

    class Serializer implements UnversionedSerializer<TopologyRecord>
    {
        public static Serializer instance = new Serializer();

        @Override
        public void serialize(TopologyRecord t, DataOutputPlus out) throws IOException
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
        public TopologyRecord deserialize(DataInputPlus in) throws IOException
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
        public long serializedSize(TopologyRecord t)
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
}