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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.Generators;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;


public class TopologySerializersTest
{
    private static final Logger logger = LoggerFactory.getLogger(TopologySerializersTest.class);

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
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(AccordGenerators.partitioner().flatMap(p -> AccordGenerators.topologyGen(p))).check(expected -> {
            AccordGenerators.maybeUpdatePartitioner(expected.ranges());
            Serializers.testSerde(output, TopologySerializers.topology, expected);

            for (Node.Id node : expected.nodes())
                Serializers.testSerde(output, TopologySerializers.topology, expected.forNode(node));
        });
    }

    /**
     * This test focuses on correctness, given any random topology does the serde operation work as expected?
     */
    @Test
    public void compactTopology()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(AccordGenerators.partitioner().flatMap(p -> AccordGenerators.topologyGen(p))).check(expected -> {
            AccordGenerators.maybeUpdatePartitioner(expected.ranges());

            Serializers.testSerde(output, TopologySerializers.compactTopology, expected);

            for (Node.Id node : expected.nodes())
                Serializers.testSerde(output, TopologySerializers.compactTopology, expected.forNode(node));
        });
    }

    /**
     * This test tries to create Topologies that would look closer to what one would expect in a production environment; namily each table having the same ranges.
     */
    @Test
    public void compactTopologyAreCompact()
    {
        Gen<Ranges> rangeGen = AccordGenerators.ranges(Gens.lists(Generators.toGen(CassandraGenerators.TABLE_ID_GEN)).ofSizeBetween(2, 10),
                                                       AccordGenerators.partitioner(),
                                                       Gens.ints().between(2, 1000));
        Gen<Topology> gen = AccordGenerators.topologyGen(AccordGens.epochs(), rangeGen);
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        CompactCollector collector = new CompactCollector();
        qt().forAll(gen).check(expected -> {
            AccordGenerators.maybeUpdatePartitioner(expected.ranges());

            Serializers.testSerde(output, TopologySerializers.compactTopology, expected);

            long size = TopologySerializers.compactTopology.serializedSize(expected);
            long upperLimit = TopologySerializers.topology.serializedSize(expected);
            collector.register(expected, ((upperLimit - size) / (double) upperLimit) * 100.0D);
            Assertions.assertThat(size).isLessThan(upperLimit);
        });
        logger.info(collector.toString());
    }

    private static class CompactCollector
    {
        private static class E implements Comparable<E>
        {
            final int numTables;
            final int numRanges;
            final double savings;
            final IPartitioner partitioner;

            private E(int numTables, int numRanges, double savings, IPartitioner partitioner)
            {
                this.numTables = numTables;
                this.numRanges = numRanges;
                this.savings = savings;
                this.partitioner = partitioner;
            }

            @Override
            public int compareTo(E o)
            {
                int rc = Double.compare(savings, o.savings);
                if (rc == 0)
                    rc = Integer.compare(numRanges, o.numRanges);
                if (rc == 0)
                    rc = Integer.compare(numTables, o.numTables);
                return rc;
            }
        }

        E min, max;
        void register(Topology topology, double savings)
        {
            int numTables = Math.toIntExact(topology.shards().stream().map(s -> (TokenRange) s.range).map(TokenRange::table).distinct().count());
            int numRanges = Math.toIntExact(topology.shards().stream().map(s -> (TokenRange) s.range).map(r -> r.withTable(TableId.UNDEFINED)).distinct().count());

            E e = new E(numTables, numRanges, savings, DatabaseDescriptor.getPartitioner());
            if (min == null || e.compareTo(min) < 0)
                min = e;
            if (max == null || e.compareTo(max) > 0)
                max = e;
        }

        @Override
        public String toString()
        {
            return "min: " + String.format("tables=%d, ranges=%d By %.2f%%", min.numTables, min.numRanges, min.savings)
                   + ", partitioner: " + min.partitioner.getClass().getSimpleName()
                   + "\nmax: " + String.format("tables=%d, ranges=%d By %.2f%%", max.numTables, max.numRanges, max.savings)
                   + ", partitioner: " + max.partitioner.getClass().getSimpleName();
        }
    }
}
