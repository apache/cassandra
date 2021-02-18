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

package org.apache.cassandra.service.reads.range;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.Util.testPartitioner;
import static org.apache.cassandra.db.ConsistencyLevel.ALL;
import static org.apache.cassandra.db.ConsistencyLevel.ANY;
import static org.apache.cassandra.db.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_SERIAL;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.db.ConsistencyLevel.THREE;
import static org.apache.cassandra.db.ConsistencyLevel.TWO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for {@link ReplicaPlanMerger}.
 */
public class ReplicaPlanMergerTest
{
    private static final String KEYSPACE = "ReplicaPlanMergerTest";
    private static Keyspace keyspace;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(2));
        keyspace = Keyspace.open(KEYSPACE);
    }

    /**
     * Tests range merging with a single node cluster and a read consistency level that allows to merge ranges.
     */
    @Test
    public void testSingleNode()
    {
        new TokenUpdater().withTokens(10, 20, 30, 40).update();

        // with CLs requiring a single node all ranges are merged (unless they are wrapping)
        for (ConsistencyLevel cl : Arrays.asList(ONE, LOCAL_ONE, ANY))
        {
            testRanges(cl, range(min(), min()));
            testRanges(cl, range(min(), max(25)));
            testRanges(cl, range(min(), max(40)));
            testRanges(cl, range(min(), max(50)));
            testRanges(cl, range(max(20), max(30)));
            testRanges(cl, range(max(25), min()));
            testRanges(cl, range(max(25), max(35)));
            testRanges(cl, range(max(50), min()));
            testRanges(cl, range(max(40), max(10)), range(max(40), min()), range(min(), max(10))); // wrapped is split
            testRanges(cl, range(max(25), max(15)), range(max(25), min()), range(min(), max(15))); // wrapped is split
        }

        // with CLs requiring more than a single node ranges are not merged due to the RF=2
        for (ConsistencyLevel cl : Arrays.asList(ALL, QUORUM, LOCAL_QUORUM, EACH_QUORUM, TWO, THREE, SERIAL, LOCAL_SERIAL))
        {
            testRanges(cl,
                       range(min(), min()),
                       range(min(), max(10)),
                       range(max(10), max(20)),
                       range(max(20), max(30)),
                       range(max(30), max(40)),
                       range(max(40), min()));
            testRanges(cl,
                       range(min(), max(25)),
                       range(min(), max(10)), range(max(10), max(20)), range(max(20), max(25)));
            testRanges(cl,
                       range(min(), max(40)),
                       range(min(), max(10)), range(max(10), max(20)), range(max(20), max(30)), range(max(30), max(40)));
            testRanges(cl,
                       range(min(), max(50)),
                       range(min(), max(10)),
                       range(max(10), max(20)),
                       range(max(20), max(30)),
                       range(max(30), max(40)),
                       range(max(40), max(50)));
            testRanges(cl,
                       range(max(20), max(30)));
            testRanges(cl,
                       range(max(25), min()),
                       range(max(25), max(30)), range(max(30), max(40)), range(max(40), min()));
            testRanges(cl,
                       range(max(50), min()));
            testRanges(cl,
                       range(max(30), max(10)), // wrapped
                       range(max(30), max(40)), range(max(40), min()), range(min(), max(10)));
            testRanges(cl,
                       range(max(25), max(15)), // wrapped
                       range(max(25), max(30)),
                       range(max(30), max(40)),
                       range(max(40), min()),
                       range(min(), max(10)),
                       range(max(10), max(15)));
        }
    }

    /**
     * Tests range merging with a multinode cluster when the token ranges don't overlap between replicas.
     */
    @Test
    public void testMultiNodeWithContinuousRanges()
    {
        new TokenUpdater().withTokens("127.0.0.1", 10, 20, 30)
                          .withTokens("127.0.0.2", 40, 50, 60)
                          .withTokens("127.0.0.3", 70, 80, 90)
                          .update();

        // with CL=ANY the ranges are fully merged (unless they are wrapping)
        testMultiNodeFullMerge(ANY);

        // with CL=THREE the ranges are not merged at all
        testMultiNodeNoMerge(THREE);

        // with CLs requiring a single node the ranges are merged in a per-node basis
        for (ConsistencyLevel cl : Arrays.asList(ONE, LOCAL_ONE))
        {
            testRanges(cl,
                       range(min(), min()),
                       range(min(), max(60)), range(max(60), min()));
            testRanges(cl,
                       range(min(), max(25)));
            testRanges(cl,
                       range(min(), max(40)));
            testRanges(cl,
                       range(min(), max(50)));
            testRanges(cl,
                       range(max(20), max(30)));
            testRanges(cl,
                       range(max(25), min()),
                       range(max(25), max(60)), range(max(60), min()));
            testRanges(cl,
                       range(max(25), max(35)),
                       range(max(25), max(35)));
            testRanges(cl,
                       range(max(50), min()),
                       range(max(50), max(90)), range(max(90), min()));
            testRanges(cl,
                       range(max(50), max(10)), // wrapping range
                       range(max(50), max(90)), range(max(90), min()), range(min(), max(10)));
            testRanges(cl,
                       range(max(25), max(15)), // wrapping range
                       range(max(25), max(60)), range(max(60), min()), range(min(), max(15)));
        }

        // with other CLs the ranges are merged in a similar per-node basis
        for (ConsistencyLevel cl : Arrays.asList(ALL, QUORUM, LOCAL_QUORUM, EACH_QUORUM, TWO, SERIAL, LOCAL_SERIAL))
        {
            testRanges(cl,
                       range(min(), min()),
                       range(min(), max(30)), range(max(30), max(60)), range(max(60), max(90)), range(max(90), min()));
            testRanges(cl,
                       range(min(), max(25)));
            testRanges(cl,
                       range(min(), max(40)),
                       range(min(), max(30)), range(max(30), max(40)));
            testRanges(cl,
                       range(min(), max(50)),
                       range(min(), max(30)), range(max(30), max(50)));
            testRanges(cl,
                       range(max(20), max(30)));
            testRanges(cl,
                       range(max(25), min()),
                       range(max(25), max(30)), range(max(30), max(60)), range(max(60), max(90)), range(max(90), min()));
            testRanges(cl,
                       range(max(25), max(35)),
                       range(max(25), max(30)), range(max(30), max(35)));
            testRanges(cl,
                       range(max(50), min()),
                       range(max(50), max(60)), range(max(60), max(90)), range(max(90), min()));
            testRanges(cl,
                       range(max(50), max(10)), // wrapping range
                       range(max(50), max(60)), range(max(60), max(90)), range(max(90), min()), range(min(), max(10)));
            testRanges(cl,
                       range(max(25), max(15)), // wrapping range
                       range(max(25), max(30)),
                       range(max(30), max(60)),
                       range(max(60), max(90)),
                       range(max(90), min()),
                       range(min(), max(15)));
        }
    }

    /**
     * Tests range merging with a multinode cluster when the token ranges overlap between replicas.
     */
    @Test
    public void testMultiNodeWithDiscontinuousRanges()
    {
        new TokenUpdater().withTokens("127.0.0.1", 10, 40, 70)
                          .withTokens("127.0.0.2", 20, 50, 80)
                          .withTokens("127.0.0.3", 30, 60, 90)
                          .update();

        // with CL=ANY the ranges are fully merged (unless they are wrapping)
        testMultiNodeFullMerge(ANY);

        // with CLs requiring a single node the ranges are merged in a per-node basis
        for (ConsistencyLevel cl : Arrays.asList(ONE, LOCAL_ONE))
        {
            testRanges(cl,
                       range(min(), min()), // full range
                       range(min(), max(20)),
                       range(max(20), max(40)),
                       range(max(40), max(60)),
                       range(max(60), max(80)),
                       range(max(80), min()));
            testRanges(cl,
                       range(min(), max(25)),
                       range(min(), max(20)), range(max(20), max(25)));
            testRanges(cl,
                       range(min(), max(40)),
                       range(min(), max(20)), range(max(20), max(40)));
            testRanges(cl,
                       range(min(), max(50)),
                       range(min(), max(20)), range(max(20), max(40)), range(max(40), max(50)));
            testRanges(cl,
                       range(max(20), max(30)));
            testRanges(cl,
                       range(max(25), min()),
                       range(max(25), max(40)), range(max(40), max(60)), range(max(60), max(80)), range(max(80), min()));
            testRanges(cl,
                       range(max(25), max(35)));
            testRanges(cl,
                       range(max(50), min()),
                       range(max(50), max(70)), range(max(70), max(90)), range(max(90), min()));
            testRanges(cl,
                       range(max(50), max(10)), // wrapping range
                       range(max(50), max(70)), range(max(70), max(90)), range(max(90), min()), range(min(), max(10)));
            testRanges(cl,
                       range(max(25), max(15)), // wrapping range
                       range(max(25), max(40)),
                       range(max(40), max(60)),
                       range(max(60), max(80)),
                       range(max(80), min()),
                       range(min(), max(15)));
        }

        // with other CLs the ranges are not merged at all
        for (ConsistencyLevel cl : Arrays.asList(ALL, QUORUM, LOCAL_QUORUM, EACH_QUORUM, TWO, THREE, SERIAL, LOCAL_SERIAL))
        {
            testMultiNodeNoMerge(cl);
        }
    }

    private void testMultiNodeFullMerge(ConsistencyLevel cl)
    {
        testRanges(cl, range(min(), min()));
        testRanges(cl, range(min(), max(25)));
        testRanges(cl, range(min(), max(40)));
        testRanges(cl, range(min(), max(50)));
        testRanges(cl, range(max(20), max(30)));
        testRanges(cl, range(max(25), min()));
        testRanges(cl, range(max(25), max(35)));
        testRanges(cl, range(max(50), min()));
        testRanges(cl, range(max(50), max(10)), range(max(50), min()), range(min(), max(10))); // wrapping range
        testRanges(cl, range(max(25), max(15)), range(max(25), min()), range(min(), max(15))); // wrapping range
    }

    private void testMultiNodeNoMerge(ConsistencyLevel cl)
    {
        testRanges(cl,
                   range(min(), min()),
                   range(min(), max(10)),
                   range(max(10), max(20)),
                   range(max(20), max(30)),
                   range(max(30), max(40)),
                   range(max(40), max(50)),
                   range(max(50), max(60)),
                   range(max(60), max(70)),
                   range(max(70), max(80)),
                   range(max(80), max(90)),
                   range(max(90), min()));
        testRanges(cl,
                   range(min(), max(25)),
                   range(min(), max(10)), range(max(10), max(20)), range(max(20), max(25)));
        testRanges(cl,
                   range(min(), max(40)),
                   range(min(), max(10)), range(max(10), max(20)), range(max(20), max(30)), range(max(30), max(40)));
        testRanges(cl,
                   range(min(), max(50)),
                   range(min(), max(10)),
                   range(max(10), max(20)),
                   range(max(20), max(30)),
                   range(max(30), max(40)),
                   range(max(40), max(50)));
        testRanges(cl,
                   range(max(20), max(30)));
        testRanges(cl,
                   range(max(25), min()),
                   range(max(25), max(30)),
                   range(max(30), max(40)),
                   range(max(40), max(50)),
                   range(max(50), max(60)),
                   range(max(60), max(70)),
                   range(max(70), max(80)),
                   range(max(80), max(90)),
                   range(max(90), min()));
        testRanges(cl,
                   range(max(25), max(35)),
                   range(max(25), max(30)), range(max(30), max(35)));
        testRanges(cl,
                   range(max(50), min()),
                   range(max(50), max(60)),
                   range(max(60), max(70)),
                   range(max(70), max(80)),
                   range(max(80), max(90)),
                   range(max(90), min()));
        testRanges(cl,
                   range(max(50), max(10)), // wrapping range
                   range(max(50), max(60)),
                   range(max(60), max(70)),
                   range(max(70), max(80)),
                   range(max(80), max(90)),
                   range(max(90), min()),
                   range(min(), max(10)));
        testRanges(cl,
                   range(max(25), max(15)), // wrapping range
                   range(max(25), max(30)),
                   range(max(30), max(40)),
                   range(max(40), max(50)),
                   range(max(50), max(60)),
                   range(max(60), max(70)),
                   range(max(70), max(80)),
                   range(max(80), max(90)),
                   range(max(90), min()),
                   range(min(), max(10)),
                   range(max(10), max(15)));
    }

    private static PartitionPosition min()
    {
        return testPartitioner().getMinimumToken().minKeyBound();
    }

    private static PartitionPosition max(int key)
    {
        return new Murmur3Partitioner.LongToken(key).maxKeyBound();
    }

    private static Range<PartitionPosition> range(PartitionPosition left, PartitionPosition right)
    {
        return new Range<>(left, right);
    }

    private void testRanges(ConsistencyLevel consistencyLevel, AbstractBounds<PartitionPosition> queryRange)
    {
        testRanges(consistencyLevel, queryRange, queryRange);
    }

    @SafeVarargs
    private final void testRanges(ConsistencyLevel consistencyLevel,
                                  AbstractBounds<PartitionPosition> queryRange,
                                  AbstractBounds<PartitionPosition>... expected)
    {
        try (ReplicaPlanIterator originals = new ReplicaPlanIterator(queryRange, null, keyspace, ANY); // ANY avoids endpoint erros
             ReplicaPlanMerger merger = new ReplicaPlanMerger(originals, keyspace, consistencyLevel))
        {
            // collect the merged ranges
            List<AbstractBounds<PartitionPosition>> mergedRanges = new ArrayList<>(expected.length);
            while (merger.hasNext())
                mergedRanges.add(merger.next().range());

            assertFalse("The number of merged ranges should never be greater than the number of original ranges",
                        mergedRanges.size() > originals.size());

            // verify the merged ranges
            assertEquals(expected.length, mergedRanges.size());
            for (int i = 0; i < expected.length; i++)
                assertEquals("Mismatch for index " + i + ": " + mergedRanges, expected[i], mergedRanges.get(i));
        }
    }
}
