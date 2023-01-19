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

import com.google.common.collect.Iterators;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.CloseableIterator;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;

public class RangeCommandIteratorTest
{
    private static final String KEYSPACE1 = "RangeCommandIteratorTest";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testRangeCountWithRangeMerge()
    {
        List<Token> tokens = setTokens(100, 200, 300, 400);
        int vnodeCount = 0;

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        List<ReplicaPlan.ForRangeRead> ranges = new ArrayList<>();
        for (int i = 0; i + 1 < tokens.size(); i++)
        {
            Range<PartitionPosition> range = Range.makeRowRange(tokens.get(i), tokens.get(i + 1));
            ranges.add(ReplicaPlans.forRangeRead(keyspace, null, ConsistencyLevel.ONE, range, 1));
            vnodeCount++;
        }

        ReplicaPlanMerger merge = new ReplicaPlanMerger(ranges.iterator(), keyspace, ConsistencyLevel.ONE);
        ReplicaPlan.ForRangeRead mergedRange = Iterators.getOnlyElement(merge);
        // all ranges are merged as test has only one node.
        assertEquals(vnodeCount, mergedRange.vnodeCount());
    }

    @Test
    public void testRangeQueried()
    {
        List<Token> tokens = setTokens(100, 200, 300, 400);
        int vnodeCount = tokens.size() + 1; // n tokens divide token ring into n+1 ranges

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.clearUnsafe();

        int rows = 100;
        for (int i = 0; i < rows; ++i)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 10, String.valueOf(i));
            builder.clustering("c");
            builder.add("val", String.valueOf(i));
            builder.build().applyUnsafe();
        }
        Util.flush(cfs);

        PartitionRangeReadCommand command = (PartitionRangeReadCommand) Util.cmd(cfs).build();
        AbstractBounds<PartitionPosition> keyRange = command.dataRange().keyRange();

        // without range merger, there will be 2 batches requested: 1st batch with 1 range and 2nd batch with remaining ranges
        CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans = replicaPlanIterator(keyRange, keyspace, false);
        RangeCommandIterator data = new RangeCommandIterator(replicaPlans, command, 1, 1000, vnodeCount, nanoTime());
        verifyRangeCommandIterator(data, rows, 2, vnodeCount);

        // without range merger and initial cf=5, there will be 1 batches requested: 5 vnode ranges for 1st batch
        replicaPlans = replicaPlanIterator(keyRange, keyspace, false);
        data = new RangeCommandIterator(replicaPlans, command, vnodeCount, 1000, vnodeCount, nanoTime());
        verifyRangeCommandIterator(data, rows, 1, vnodeCount);

        // without range merger and max cf=1, there will be 5 batches requested: 1 vnode range per batch
        replicaPlans = replicaPlanIterator(keyRange, keyspace, false);
        data = new RangeCommandIterator(replicaPlans, command, 1, 1, vnodeCount, nanoTime());
        verifyRangeCommandIterator(data, rows, vnodeCount, vnodeCount);

        // with range merger, there will be only 1 batch requested, as all ranges share the same replica - localhost
        replicaPlans = replicaPlanIterator(keyRange, keyspace, true);
        data = new RangeCommandIterator(replicaPlans, command, 1, 1000, vnodeCount, nanoTime());
        verifyRangeCommandIterator(data, rows, 1, vnodeCount);

        // with range merger and max cf=1, there will be only 1 batch requested, as all ranges share the same replica - localhost
        replicaPlans = replicaPlanIterator(keyRange, keyspace, true);
        data = new RangeCommandIterator(replicaPlans, command, 1, 1, vnodeCount, nanoTime());
        verifyRangeCommandIterator(data, rows, 1, vnodeCount);
    }

    @Test
    public void testComputeConcurrencyFactor()
    {
        int maxConcurrentRangeRequest = 32;

        // no live row returned, fetch all remaining ranges but hit the max instead
        int cf = RangeCommandIterator.computeConcurrencyFactor(100, 30, maxConcurrentRangeRequest, 500, 0);
        assertEquals(maxConcurrentRangeRequest, cf); // because 100 - 30 = 70 > maxConccurrentRangeRequest

        // no live row returned, fetch all remaining ranges
        cf = RangeCommandIterator.computeConcurrencyFactor(100, 80, maxConcurrentRangeRequest, 500, 0);
        assertEquals(20, cf); // because 100-80 = 20 < maxConccurrentRangeRequest

        // returned half rows, fetch rangesQueried again but hit the max instead
        cf = RangeCommandIterator.computeConcurrencyFactor(100, 60, maxConcurrentRangeRequest, 480, 240);
        assertEquals(maxConcurrentRangeRequest, cf); // because 60 > maxConccurrentRangeRequest

        // returned half rows, fetch rangesQueried again
        cf = RangeCommandIterator.computeConcurrencyFactor(100, 30, maxConcurrentRangeRequest, 480, 240);
        assertEquals(30, cf); // because 30 < maxConccurrentRangeRequest

        // returned most of rows, 1 more range to fetch
        cf = RangeCommandIterator.computeConcurrencyFactor(100, 1, maxConcurrentRangeRequest, 480, 479);
        assertEquals(1, cf); // because 1 < maxConccurrentRangeRequest
    }

    private static List<Token> setTokens(int... values)
    {
        return new TokenUpdater().withKeys(values).update().getTokens();
    }

    private static CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlanIterator(AbstractBounds<PartitionPosition> keyRange,
                                                                                   Keyspace keyspace,
                                                                                   boolean withRangeMerger)
    {
        CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans = new ReplicaPlanIterator(keyRange, null, keyspace, ConsistencyLevel.ONE);
        if (withRangeMerger)
            replicaPlans = new ReplicaPlanMerger(replicaPlans, keyspace, ConsistencyLevel.ONE);

        return  replicaPlans;
    }

    private static void verifyRangeCommandIterator(RangeCommandIterator data, int rows, int batches, int vnodeCount)
    {
        int num = Util.size(data);
        assertEquals(rows, num);
        assertEquals(batches, data.batchesRequested());
        assertEquals(vnodeCount, data.rangesQueried());
    }
}
