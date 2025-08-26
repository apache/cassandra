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

import org.junit.Before;
import org.junit.Test;

import accord.api.Journal;
import accord.local.CommandStores;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.AccordGenerators;

import static accord.utils.Property.qt;

public class AccordTopologyUpdateTest
{
    private static final long[] EPOCHS = new long[0];
    private static final Ranges[] RANGES = new Ranges[0];
    private static final TableId TBL1 = TableId.fromRaw(0, 0);

    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Before
    public void before()
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void rangesForEpoch()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(rangesForEpochGen()).check(expected -> {
            maybeUpdatePartitioner(expected);
            Serializers.testSerde(output, AccordTopologyUpdate.RangesForEpochSerializer.instance, expected);
        });
    }

    @Test
    public void topologyUpdate()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(topologyUpdateGen()).check(expected -> {
            maybeUpdatePartitioner(expected);
            Serializers.testSerde(output, AccordTopologyUpdate.TopologyUpdateSerializer.instance, expected);
        });
    }

    @Test
    public void accordTopologyUpdate()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(accordTopologyUpdateGen()).check(expected -> {
            maybeUpdatePartitioner(expected);
            Serializers.testSerde(output, AccordTopologyUpdate.Serializer.instance, expected);
        });
    }

    private static Gen<CommandStores.RangesForEpoch> rangesForEpochGen()
    {
        return AccordGenerators.partitioner().flatMap(p -> rangesForEpochGen(AccordGenerators.rangesSplitOrArbitrary(p)));
    }

    private static Gen<CommandStores.RangesForEpoch> rangesForEpochGen(Gen<Ranges> rangesGen)
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

    private static Gen<Journal.TopologyUpdate> topologyUpdateGen()
    {
        Gen<IPartitioner> partitionerGen = AccordGenerators.partitioner();
        return rs -> {
            IPartitioner partitioner = partitionerGen.next(rs);
            Gen<Ranges> rangesGen = AccordGenerators.ranges(TBL1, partitioner);
            Gen<CommandStores.RangesForEpoch> rangesForEpochGen = rangesForEpochGen(rangesGen);
            Topology topology = AccordGenerators.topologyGen(rangesGen).next(rs);

            Int2ObjectHashMap<CommandStores.RangesForEpoch> commandStores = new Int2ObjectHashMap<>();
            for (Node.Id node : topology.nodes())
                commandStores.put(node.id, rangesForEpochGen.next(rs));

            Node.Id self = rs.pick(topology.nodes());

            return new Journal.TopologyUpdate(commandStores, topology);
        };
    }

    private static Gen<AccordTopologyUpdate> accordTopologyUpdateGen()
    {
        Gen.LongGen epochGen = AccordGens.epochs();
        Gen<Journal.TopologyUpdate> topologyUpdateGen = topologyUpdateGen();
        Gen<AccordTopologyUpdate.Kind> kindGen = Gens.enums().all(AccordTopologyUpdate.Kind.class);
        return rs -> {
            AccordTopologyUpdate.Kind kind = kindGen.next(rs);
            switch (kind)
            {
                case NewTopology: return new AccordTopologyUpdate.NewTopology(topologyUpdateGen.next(rs));
                case TopologyImage: return new AccordTopologyUpdate.TopologyImage(epochGen.nextLong(rs), AccordTopologyUpdate.Kind.TopologyImage);
                case NoOp: return new AccordTopologyUpdate.TopologyImage(epochGen.nextLong(rs), AccordTopologyUpdate.Kind.NoOp);
                default: throw new AssertionError("Unknown kind: " + kind);
            }
        };
    }

    private static void maybeUpdatePartitioner(Journal.TopologyUpdate expected)
    {
        AccordGenerators.maybeUpdatePartitioner(expected.global.ranges());
    }

    private static void maybeUpdatePartitioner(AccordTopologyUpdate expected)
    {
        if (expected instanceof AccordTopologyUpdate.NewTopology)
        {
            maybeUpdatePartitioner(((AccordTopologyUpdate.NewTopology) expected).update);
        }
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
}