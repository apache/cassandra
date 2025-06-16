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

import accord.local.Node;
import accord.topology.Topology;
import accord.utils.AccordGens;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.AccordGenerators;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;


public class TopologySerializersTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void nodeId() throws IOException
    {
        qt().forAll(AccordGens.nodes()).check(n -> Serializers.testSerde(TopologySerializers.nodeId, n));
    }

    @Test
    public void topology()
    {
        //NOTE FOR MONDAY
        // This test does random ranges, so the dictionary method takes more bytes than non-dictionary
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().withSeed(3625886965894734595L).forAll(AccordGenerators.partitioner().flatMap(p -> AccordGenerators.topologyGen(p))).check(expected -> {
            AccordGenerators.maybeUpdatePartitioner(expected.ranges());
            Serializers.testSerde(output, TopologySerializers.topology, expected);
            TopologySerializers.CompactTopology global = new TopologySerializers.CompactTopology(expected);
            Serializers.testSerde(output, TopologySerializers.dictionaryTopology, global);

            Assertions.assertThat(TopologySerializers.dictionaryTopology.serializedSize(global)).isLessThan(TopologySerializers.topology.serializedSize(expected));

            for (Node.Id node : expected.nodes())
            {
                Serializers.testSerde(output, TopologySerializers.topology, expected.forNode(node));
                Serializers.testSerde(output, TopologySerializers.dictionaryTopology, new TopologySerializers.CompactTopology(expected.forNode(node)));
            }
        });
    }

    @Test
    public void compactTopology()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(AccordGenerators.partitioner().flatMap(p -> AccordGenerators.topologyGen(p))).check(expected -> {
            AccordGenerators.maybeUpdatePartitioner(expected.ranges());

            TopologySerializers.CompactTopology global = new TopologySerializers.CompactTopology(expected);
            Serializers.testSerde(output, TopologySerializers.dictionaryTopology, global);

            Assertions.assertThat(global.topology()).isEqualTo(expected);

            for (Node.Id node : expected.nodes())
            {
                Topology expectedSubset = expected.forNode(node);
                TopologySerializers.CompactTopology subset = new TopologySerializers.CompactTopology(expectedSubset);
                Serializers.testSerde(output, TopologySerializers.dictionaryTopology, subset);

                Assertions.assertThat(subset.topology()).isEqualTo(expectedSubset);
            }
        });
    }
}
