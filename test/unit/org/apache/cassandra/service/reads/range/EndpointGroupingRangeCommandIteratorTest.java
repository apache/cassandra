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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class EndpointGroupingRangeCommandIteratorTest extends CQLTester
{
    public static final String KEYSPACE1 = "EndpointGroupingRangeReadTest";
    public static final String CF_STANDARD1 = "Standard1";

    private static final int MAX_CONCURRENCY_FACTOR = 1;

    private static Keyspace keyspace;
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        System.setProperty("cassandra.max_concurrent_range_requests", String.valueOf(MAX_CONCURRENCY_FACTOR));

        requireNetwork();

        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));

        keyspace = Keyspace.open(KEYSPACE1);
        cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.clearUnsafe();
    }

    @AfterClass
    public static void cleanup()
    {
        System.clearProperty("cassandra.max_concurrent_range_requests");
    }

    @Test
    public void testEndpointGrouping() throws Throwable
    {
        // n tokens divide token ring into n+1 ranges
        int vnodeCount = setTokens(100, 200, 300, 400).size() + 1;

        int rowCount = 1000;
        for (int i = 0; i < rowCount; ++i)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), 10, String.valueOf(i));
            builder.clustering("c");
            builder.add("val", String.valueOf(i));
            builder.build().applyUnsafe();
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        PartitionRangeReadCommand command = (PartitionRangeReadCommand) Util.cmd(cfs).build();

        for (int initialConcurrencyFactor : Arrays.asList(1, 2, 5, 10, 50, 100, 250, 300, 500))
        {
            verifyEndpointGrouping(command, vnodeCount, initialConcurrencyFactor);
        }
    }

    private static List<Token> setTokens(int... values)
    {
        return new TokenUpdater().withKeys(values).update().getTokens();
    }

    private void verifyEndpointGrouping(PartitionRangeReadCommand command, int vnodeCount, int concurrencyFactor) throws Exception
    {
        EndpointGroupingCoordinator coordinator = endpointGroupingCoordinator(command, concurrencyFactor);

        // verify queried vnode ranges respects concurrency factor.
        assertEquals(vnodeCount, coordinator.vnodeRanges());

        // verify number of replica ranges is the same as grouped ranges.
        int rangesForQuery = 1;
        EndpointGroupingCoordinator.EndpointQueryContext endpointContext = Iterables.getOnlyElement(coordinator.endpointRanges());
        assertEquals(rangesForQuery, endpointContext.rangesCount());

        // verify that endpoint grouping coordinator fetches given ranges according to concurrency factor
        RangeCommandIterator tokenOrderedIterator = tokenOrderIterator(command, vnodeCount, concurrencyFactor);
        int expected = Util.size(tokenOrderedIterator.sendNextRequests());
        int actual = Util.size(coordinator.execute());
        assertEquals(expected, actual);

        // verify that endpoint grouping executor fetches all data
        RangeCommandIterator endpointGroupingIterator = endpointGroupingIterator(command, vnodeCount, concurrencyFactor);
        tokenOrderedIterator = tokenOrderIterator(command, vnodeCount, concurrencyFactor);
        expected = Util.size(tokenOrderedIterator.sendNextRequests());
        actual = Util.size(endpointGroupingIterator.sendNextRequests());
        assertEquals(1, endpointGroupingIterator.batchesRequested());
        assertEquals(expected, actual);
    }

    private static EndpointGroupingCoordinator endpointGroupingCoordinator(PartitionRangeReadCommand command, int concurrencyFactor)
    {
        CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans = replicaPlanIterator(command);
        DataLimits.Counter counter = DataLimits.NONE.newCounter(command.nowInSec(), true, command.selectsFullPartition(), true);
        return new EndpointGroupingCoordinator(command, counter, replicaPlans, concurrencyFactor, System.nanoTime());
    }

    private static EndpointGroupingRangeCommandIterator endpointGroupingIterator(PartitionRangeReadCommand command, int vnodeCount, int concurrencyFactor)
    {
        CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans = replicaPlanIterator(command);
        return new EndpointGroupingRangeCommandIterator(replicaPlans, command, concurrencyFactor, concurrencyFactor, vnodeCount, System.nanoTime());
    }

    private static NonGroupingRangeCommandIterator tokenOrderIterator(PartitionRangeReadCommand command, int vnodeCount, int concurrencyFactor)
    {
        CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans = replicaPlanIterator(command);
        return new NonGroupingRangeCommandIterator(replicaPlans, command, concurrencyFactor, 10000, vnodeCount, System.nanoTime());
    }

    private static CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlanIterator(PartitionRangeReadCommand command)
    {
        AbstractBounds<PartitionPosition> keyRange = command.dataRange().keyRange();
        CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans = new ReplicaPlanIterator(keyRange, null, keyspace, ConsistencyLevel.ONE);
        return new ReplicaPlanMerger(replicaPlans, keyspace, ConsistencyLevel.ONE);
    }
}
