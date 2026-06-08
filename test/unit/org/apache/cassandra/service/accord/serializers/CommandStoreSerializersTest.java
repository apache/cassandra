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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;

import org.junit.Test;

import accord.local.AbstractDurableBeforeTest.DurableBeforeLinear;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.CommandStoreSerializers.ReducingRangeMapSerializer;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.NullableSerializer;

import static accord.utils.Property.qt;

public class CommandStoreSerializersTest
{
    private static final long[] EPOCHS = new long[0];
    private static final Ranges[] RANGES = new Ranges[0];

    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void redundantBeforeEntry()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().forAll(Gens.random(), AccordGenerators.partitioner()).check((rs, partitioner) -> {
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            RedundantBefore.Bounds entry = AccordGenerators.redundantBeforeEntry(partitioner).next(rs);
            Serializers.testSerde(buffer, CommandStoreSerializers.redundantBeforeShortBounds, entry);
        });
    }

    @Test
    public void redundantBefore()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().forAll(Gens.random(), AccordGenerators.partitioner()).check((rs, partitioner) -> {
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            // serializer doesn't support the empty set, so filter out
            RedundantBefore redundantBefore = AccordGenerators.redundantBefore(partitioner).filter(r -> r.size() != 0).next(rs);
            Serializers.testSerde(buffer, CommandStoreSerializers.redundantBefore, redundantBefore);
        });
    }

    @Test
    public void durableBefore()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().forAll(Gens.random(), AccordGenerators.partitioner()).check((rs, partitioner) -> {
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            // serializer doesn't support the empty set, so filter out
            DurableBefore durableBefore = AccordGenerators.durableBeforeGen(partitioner).next(rs);
            Serializers.testSerde(buffer, CommandStoreSerializers.durableBefore, durableBefore);
            Serializers.testSerde(buffer, durableBeforeLinear, DurableBeforeLinear.from(durableBefore), CommandStoreSerializers.durableBefore, DurableBeforeLinear::isEqualTo);
        });
    }

    @Test
    public void rangesForEpoch()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(rangesForEpochGen()).check(expected -> {
            maybeUpdatePartitioner(expected);
            Serializers.testSerde(output, CommandStoreSerializers.rangesForEpoch, expected);
        });
    }

    public static Gen<CommandStores.RangesForEpoch> rangesForEpochGen()
    {
        return AccordGenerators.partitioner().flatMap(p -> rangesForEpochGen(AccordGenerators.rangesSplitOrArbitrary(p)));
    }

    public static Gen<CommandStores.RangesForEpoch> rangesForEpochGen(Gen<Ranges> rangesGen)
    {
        Gen.IntGen sizeGen = Gens.ints().between(0, 10);
        Gen.LongGen epochGen = AccordGens.epochs();
        return rs -> {
            int size = sizeGen.nextInt(rs);
            if (size == 0)
                return new CommandStores.RangesForEpoch(EPOCHS, RANGES);
            long epoch = epochGen.nextLong(rs);
            long[] epochs = new long[size];
            Ranges[] ranges = new Ranges[size];
            for (int i = 0; i < size; i++)
            {
                epochs[i] = epoch++;
                ranges[i] = rangesGen.next(rs);
            }
            return new CommandStores.RangesForEpoch(epochs, ranges);
        };
    }

    public static Gen<CommandStores.PreviouslyOwned> previouslyOwnedGen(Gen<Ranges> rangesGen)
    {
        Gen.IntGen sizeGen = Gens.ints().between(0, 10);
        Gen.LongGen epochGen = AccordGens.epochs();
        return rs -> {
            int size = sizeGen.nextInt(rs);
            if (size == 0)
                return CommandStores.PreviouslyOwned.EMPTY;
            long maxEpoch = 0;
            long[] epochs = new long[size];
            Ranges[] ranges = new Ranges[size];
            for (int i = 0; i < size; i++)
            {
                epochs[i] = epochGen.nextLong(rs);
                ranges[i] = rangesGen.next(rs);
                maxEpoch = Math.max(maxEpoch, epochs[i]);
            }
            return new CommandStores.PreviouslyOwned(maxEpoch, epochs, ranges);
        };
    }

    private void maybeUpdatePartitioner(CommandStores.RangesForEpoch expected)
    {
        if (expected.size() > 0)
        {
            for (int i = 0; i < expected.size(); i++)
            {
                Ranges ranges = expected.rangesAtIndex(i);
                if (AccordGenerators.maybeUpdatePartitioner(ranges))
                    return;
            }
        }
    }

    static final UnversionedSerializer<DurableBefore.Entry> durableBeforeEntry = new NonTreeDurableBeforeEntrySerializer();
    public static final UnversionedSerializer<DurableBeforeLinear> durableBeforeLinear = new ReducingRangeMapSerializer<>(NullableSerializer.wrap(durableBeforeEntry), DurableBefore.Entry[]::new, (i1, i2) -> { throw new UnsupportedOperationException(); }, DurableBeforeLinear.EMPTY);
    private static final class NonTreeDurableBeforeEntrySerializer implements UnversionedSerializer<DurableBefore.Entry>
    {
        private NonTreeDurableBeforeEntrySerializer() {}

        @Override
        public void serialize(DurableBefore.Entry t, DataOutputPlus out) throws IOException
        {
            CommandSerializers.txnId.serialize(t.quorum, out);
            CommandSerializers.txnId.serialize(t.universal, out);
        }

        @Override
        public DurableBefore.Entry deserialize(DataInputPlus in) throws IOException
        {
            TxnId quorumBefore = CommandSerializers.txnId.deserialize(in);
            TxnId universalBefore = CommandSerializers.txnId.deserialize(in);
            return DurableBefore.Entry.constructWithoutRange(quorumBefore, universalBefore);
        }

        @Override
        public long serializedSize(DurableBefore.Entry t)
        {
            return CommandSerializers.txnId.serializedSize(t.quorum)
                    + CommandSerializers.txnId.serializedSize(t.universal);
        }
    }
}