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
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;

import static org.apache.cassandra.Util.rp;
import static org.apache.cassandra.Util.token;
import static org.junit.Assert.assertEquals;

public class ReplicaPlanIteratorTest
{
    private static final String KEYSPACE = "ReplicaPlanIteratorTest";
    private static Keyspace keyspace;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
        keyspace = Keyspace.open(KEYSPACE);

        new TokenUpdater().withKeys(InetAddressAndPort.getByName("127.0.0.1"), "1")
                          .withKeys(InetAddressAndPort.getByName("127.0.0.6"), "6")
                          .update();
    }

    @Test
    public void testRanges()
    {
        // no splits
        testRanges(range(rp("2"), rp("5")), range(rp("2"), rp("5")));
        testRanges(bounds(rp("2"), rp("5")), bounds(rp("2"), rp("5")));
        testRanges(exBounds(rp("2"), rp("5")), exBounds(rp("2"), rp("5")));
        // single split testGRR(range("2", "7"), range(rp("2"), endOf("6")), range(endOf("6"), rp("7")));
        testRanges(bounds(rp("2"), rp("7")), bounds(rp("2"), endOf("6")), range(endOf("6"), rp("7")));
        testRanges(exBounds(rp("2"), rp("7")), range(rp("2"), endOf("6")), exBounds(endOf("6"), rp("7")));
        testRanges(incExBounds(rp("2"), rp("7")), bounds(rp("2"), endOf("6")), exBounds(endOf("6"), rp("7")));
        // single split starting from min
        testRanges(range(rp(""), rp("2")), range(rp(""), endOf("1")), range(endOf("1"), rp("2")));
        testRanges(bounds(rp(""), rp("2")), bounds(rp(""), endOf("1")), range(endOf("1"), rp("2")));
        testRanges(exBounds(rp(""), rp("2")), range(rp(""), endOf("1")), exBounds(endOf("1"), rp("2")));
        testRanges(incExBounds(rp(""), rp("2")), bounds(rp(""), endOf("1")), exBounds(endOf("1"), rp("2")));
        // single split ending with max
        testRanges(range(rp("5"), rp("")), range(rp("5"), endOf("6")), range(endOf("6"), rp("")));
        testRanges(bounds(rp("5"), rp("")), bounds(rp("5"), endOf("6")), range(endOf("6"), rp("")));
        testRanges(exBounds(rp("5"), rp("")), range(rp("5"), endOf("6")), exBounds(endOf("6"), rp("")));
        testRanges(incExBounds(rp("5"), rp("")), bounds(rp("5"), endOf("6")), exBounds(endOf("6"), rp("")));
        // two splits
        testRanges(range(rp("0"), rp("7")), range(rp("0"), endOf("1")), range(endOf("1"), endOf("6")), range(endOf("6"), rp("7")));
        testRanges(bounds(rp("0"), rp("7")), bounds(rp("0"), endOf("1")), range(endOf("1"), endOf("6")), range(endOf("6"), rp("7")));
        testRanges(exBounds(rp("0"), rp("7")), range(rp("0"), endOf("1")), range(endOf("1"), endOf("6")), exBounds(endOf("6"), rp("7")));
        testRanges(incExBounds(rp("0"), rp("7")), bounds(rp("0"), endOf("1")), range(endOf("1"), endOf("6")), exBounds(endOf("6"), rp("7")));
    }

    @Test
    public void testExact()
    {
        // min
        testRanges(range(endOf("1"), endOf("5")), range(endOf("1"), endOf("5")));
        testRanges(range(rp("1"), endOf("5")), range(rp("1"), endOf("1")), range(endOf("1"), endOf("5")));
        testRanges(bounds(startOf("1"), endOf("5")), bounds(startOf("1"), endOf("1")), range(endOf("1"), endOf("5")));
        testRanges(exBounds(endOf("1"), rp("5")), exBounds(endOf("1"), rp("5")));
        testRanges(exBounds(rp("1"), rp("5")), range(rp("1"), endOf("1")), exBounds(endOf("1"), rp("5")));
        testRanges(exBounds(startOf("1"), endOf("5")), range(startOf("1"), endOf("1")), exBounds(endOf("1"), endOf("5")));
        testRanges(incExBounds(rp("1"), rp("5")), bounds(rp("1"), endOf("1")), exBounds(endOf("1"), rp("5")));
        // max
        testRanges(range(endOf("2"), endOf("6")), range(endOf("2"), endOf("6")));
        testRanges(bounds(startOf("2"), endOf("6")), bounds(startOf("2"), endOf("6")));
        testRanges(exBounds(rp("2"), rp("6")), exBounds(rp("2"), rp("6")));
        testRanges(incExBounds(rp("2"), rp("6")), incExBounds(rp("2"), rp("6")));
        // bothKeys
        testRanges(range(rp("1"), rp("6")), range(rp("1"), endOf("1")), range(endOf("1"), rp("6")));
        testRanges(bounds(rp("1"), rp("6")), bounds(rp("1"), endOf("1")), range(endOf("1"), rp("6")));
        testRanges(exBounds(rp("1"), rp("6")), range(rp("1"), endOf("1")), exBounds(endOf("1"), rp("6")));
        testRanges(incExBounds(rp("1"), rp("6")), bounds(rp("1"), endOf("1")), exBounds(endOf("1"), rp("6")));
    }

    @Test
    public void testWrapped()
    {
        // one token in wrapped range
        testRanges(range(rp("7"), rp("0")), range(rp("7"), rp("")), range(rp(""), rp("0")));
        // two tokens in wrapped range
        testRanges(range(rp("5"), rp("0")), range(rp("5"), endOf("6")), range(endOf("6"), rp("")), range(rp(""), rp("0")));
        testRanges(range(rp("7"), rp("2")), range(rp("7"), rp("")), range(rp(""), endOf("1")), range(endOf("1"), rp("2")));
        // full wraps
        testRanges(range(rp("0"), rp("0")), range(rp("0"), endOf("1")), range(endOf("1"), endOf("6")), range(endOf("6"), rp("")), range(rp(""), rp("0")));
        testRanges(range(rp(""), rp("")), range(rp(""), endOf("1")), range(endOf("1"), endOf("6")), range(endOf("6"), rp("")));
        // wrap on member tokens
        testRanges(range(rp("6"), rp("6")), range(rp("6"), endOf("6")), range(endOf("6"), rp("")), range(rp(""), endOf("1")), range(endOf("1"), rp("6")));
        testRanges(range(rp("6"), rp("1")), range(rp("6"), endOf("6")), range(endOf("6"), rp("")), range(rp(""), rp("1")));
        // end wrapped
        testRanges(range(rp("5"), rp("")), range(rp("5"), endOf("6")), range(endOf("6"), rp("")));
    }

    @Test
    public void testExactBounds()
    {
        // equal tokens are special cased as non-wrapping for bounds
        testRanges(bounds(rp("0"), rp("0")), bounds(rp("0"), rp("0")));
        // completely empty bounds match everything
        testRanges(bounds(rp(""), rp("")), bounds(rp(""), endOf("1")), range(endOf("1"), endOf("6")), range(endOf("6"), rp("")));
        testRanges(exBounds(rp(""), rp("")), range(rp(""), endOf("1")), range(endOf("1"), endOf("6")), exBounds(endOf("6"), rp("")));
        testRanges(incExBounds(rp(""), rp("")), bounds(rp(""), endOf("1")), range(endOf("1"), endOf("6")), exBounds(endOf("6"), rp("")));
    }

    @Test
    public void testLocalReplicationStrategy()
    {
        Keyspace systemKeyspace = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME);

        // ranges that would produce multiple splits with not-local strategy, but not with local strategy
        testRanges(systemKeyspace, range(rp("0"), rp("7")), range(rp("0"), rp("7")));
        testRanges(systemKeyspace, bounds(rp("0"), rp("7")), bounds(rp("0"), rp("7")));
        testRanges(systemKeyspace, exBounds(rp("0"), rp("7")), exBounds(rp("0"), rp("7")));
        testRanges(systemKeyspace, incExBounds(rp("0"), rp("7")), incExBounds(rp("0"), rp("7")));

        // wrapping ranges that should be unwrapped but not further splitted
        testRanges(systemKeyspace, range(rp("7"), rp("0")), range(rp("7"), rp("")), range(rp(""), rp("0")));
        testRanges(systemKeyspace, range(rp("7"), rp("2")), range(rp("7"), rp("")), range(rp(""), rp("2")));
    }

    @SafeVarargs
    private final void testRanges(AbstractBounds<PartitionPosition> queryRange, AbstractBounds<PartitionPosition>... expected)
    {
        testRanges(keyspace, queryRange, expected);
    }

    @SafeVarargs
    private final void testRanges(Keyspace keyspace, AbstractBounds<PartitionPosition> queryRange, AbstractBounds<PartitionPosition>... expected)
    {
        try (ReplicaPlanIterator iterator = new ReplicaPlanIterator(queryRange, null, keyspace, ConsistencyLevel.ANY))
        {
            List<AbstractBounds<PartitionPosition>> restrictedRanges = new ArrayList<>(expected.length);
            while (iterator.hasNext())
                restrictedRanges.add(iterator.next().range());

            // verify range counts
            assertEquals(expected.length, restrictedRanges.size());
            assertEquals(expected.length, iterator.size());

            // verify the ranges
            for (int i = 0; i < expected.length; i++)
                assertEquals("Mismatch for index " + i + ": " + restrictedRanges, expected[i], restrictedRanges.get(i));
        }
    }

    private static Range<PartitionPosition> range(PartitionPosition left, PartitionPosition right)
    {
        return new Range<>(left, right);
    }

    private static Bounds<PartitionPosition> bounds(PartitionPosition left, PartitionPosition right)
    {
        return new Bounds<>(left, right);
    }

    private static ExcludingBounds<PartitionPosition> exBounds(PartitionPosition left, PartitionPosition right)
    {
        return new ExcludingBounds<>(left, right);
    }

    private static IncludingExcludingBounds<PartitionPosition> incExBounds(PartitionPosition left, PartitionPosition right)
    {
        return new IncludingExcludingBounds<>(left, right);
    }

    private static PartitionPosition startOf(String key)
    {
        return token(key).minKeyBound();
    }

    private static PartitionPosition endOf(String key)
    {
        return token(key).maxKeyBound();
    }
}
