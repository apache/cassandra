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

import org.agrona.collections.Int2ObjectHashMap;
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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.AccordGenerators;

import static accord.utils.Property.qt;
import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializersTest.rangesForEpochGen;

public class TopologyRecordTest
{
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
    public void topologyUpdate()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(topologyUpdateGen()).check(expected -> {
            maybeUpdatePartitioner(expected);
            Serializers.testSerde(output, TopologyRecord.TopologyUpdateSerializer.instance, expected);
        });
    }

    @Test
    public void accordTopologyUpdate()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(accordTopologyUpdateGen()).check(expected -> {
            maybeUpdatePartitioner(expected);
            Serializers.testSerde(output, TopologyRecord.Serializer.instance, expected);
        });
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

    private static Gen<TopologyRecord> accordTopologyUpdateGen()
    {
        Gen.LongGen epochGen = AccordGens.epochs();
        Gen<Journal.TopologyUpdate> topologyUpdateGen = topologyUpdateGen();
        Gen<TopologyRecord.Kind> kindGen = Gens.enums().all(TopologyRecord.Kind.class);
        return rs -> {
            TopologyRecord.Kind kind = kindGen.next(rs);
            switch (kind)
            {
                case New: return new TopologyRecord.NewTopology(topologyUpdateGen.next(rs));
                case Image: return new TopologyRecord.TopologyImage(epochGen.nextLong(rs), TopologyRecord.Kind.Image, topologyUpdateGen.next(rs));
                case Repeat: return new TopologyRecord.TopologyImage(epochGen.nextLong(rs), TopologyRecord.Kind.Repeat);
                default: throw new AssertionError("Unknown kind: " + kind);
            }
        };
    }

    private static void maybeUpdatePartitioner(Journal.TopologyUpdate expected)
    {
        AccordGenerators.maybeUpdatePartitioner(expected.global.ranges());
    }

    private static void maybeUpdatePartitioner(TopologyRecord expected)
    {
        Journal.TopologyUpdate update = expected.getUpdate();
        if (update != null)
            maybeUpdatePartitioner(expected.getUpdate());
    }
}