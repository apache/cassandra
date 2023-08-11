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

import java.nio.ByteBuffer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.schema.IndexMetadata;

import static org.apache.cassandra.config.CassandraRelevantProperties.MAX_CONCURRENT_RANGE_REQUESTS;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link RangeCommands}.
 */
public class RangeCommandsTest extends CQLTester
{

    static WithProperties properties;
    private static final int MAX_CONCURRENCY_FACTOR = 4;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        properties = new WithProperties().set(MAX_CONCURRENT_RANGE_REQUESTS, MAX_CONCURRENCY_FACTOR);
    }

    @AfterClass
    public static void cleanup()
    {
        properties.close();
    }

    @Test
    public void tesConcurrencyFactor()
    {
        new TokenUpdater().withTokens("127.0.0.1", 1, 2)
                          .withTokens("127.0.0.2", 3, 4)
                          .update();

        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(table);

        // verify that a low concurrency factor is not capped by the max concurrency factor
        PartitionRangeReadCommand command = command(cfs, 50, 50);
        try (RangeCommandIterator partitions = RangeCommands.rangeCommandIterator(command, ONE, nanoTime());
             ReplicaPlanIterator ranges = new ReplicaPlanIterator(command.dataRange().keyRange(), command.indexQueryPlan(), keyspace, ONE))
        {
            assertEquals(2, partitions.concurrencyFactor());
            assertEquals(MAX_CONCURRENCY_FACTOR, partitions.maxConcurrencyFactor());
            assertEquals(5, ranges.size());
        }

        // verify that a high concurrency factor is capped by the max concurrency factor
        command = command(cfs, 1000, 50);
        try (RangeCommandIterator partitions = RangeCommands.rangeCommandIterator(command, ONE, nanoTime());
             ReplicaPlanIterator ranges = new ReplicaPlanIterator(command.dataRange().keyRange(), command.indexQueryPlan(), keyspace, ONE))
        {
            assertEquals(MAX_CONCURRENCY_FACTOR, partitions.concurrencyFactor());
            assertEquals(MAX_CONCURRENCY_FACTOR, partitions.maxConcurrencyFactor());
            assertEquals(5, ranges.size());
        }

        // with 0 estimated results per range the concurrency factor should be 1
        command = command(cfs, 1000, 0);
        try (RangeCommandIterator partitions = RangeCommands.rangeCommandIterator(command, ONE, nanoTime());
             ReplicaPlanIterator ranges = new ReplicaPlanIterator(command.dataRange().keyRange(), command.indexQueryPlan(), keyspace, ONE))
        {
            assertEquals(1, partitions.concurrencyFactor());
            assertEquals(MAX_CONCURRENCY_FACTOR, partitions.maxConcurrencyFactor());
            assertEquals(5, ranges.size());
        }
    }

    @Test
    public void testEstimateResultsPerRange()
    {
        testEstimateResultsPerRange(1);
        testEstimateResultsPerRange(2);
    }

    private void testEstimateResultsPerRange(int rf)
    {
        String ks = createKeyspace(String.format("CREATE KEYSPACE %%s WITH replication={'class':'SimpleStrategy', 'replication_factor':%s}", rf));
        String table = createTable(ks, "CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex(String.format("CREATE CUSTOM INDEX ON %s.%s(v) USING '%s'", ks, table, MockedIndex.class.getName()));
        Keyspace keyspace = Keyspace.open(ks);
        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(table);

        setNumTokens(1);
        testEstimateResultsPerRange(keyspace, cfs, rf, 0, null, 0);
        testEstimateResultsPerRange(keyspace, cfs, rf, 1, null, 1);
        testEstimateResultsPerRange(keyspace, cfs, rf, 10, null, 10);
        testEstimateResultsPerRange(keyspace, cfs, rf, 100, null, 100);
        testEstimateResultsPerRange(keyspace, cfs, rf, 0, 0, 0);
        testEstimateResultsPerRange(keyspace, cfs, rf, 1, 0, 0);
        testEstimateResultsPerRange(keyspace, cfs, rf, 10, 0, 0);
        testEstimateResultsPerRange(keyspace, cfs, rf, 100, 0, 0);
        testEstimateResultsPerRange(keyspace, cfs, rf, 0, 1000, 1000);
        testEstimateResultsPerRange(keyspace, cfs, rf, 1, 1000, 1000);
        testEstimateResultsPerRange(keyspace, cfs, rf, 10, 1000, 1000);
        testEstimateResultsPerRange(keyspace, cfs, rf, 100, 1000, 1000);

        setNumTokens(5);
        testEstimateResultsPerRange(keyspace, cfs, rf, 0, null, 0);
        testEstimateResultsPerRange(keyspace, cfs, rf, 1, null, 0.2f);
        testEstimateResultsPerRange(keyspace, cfs, rf, 10, null, 2);
        testEstimateResultsPerRange(keyspace, cfs, rf, 100, null, 20);
        testEstimateResultsPerRange(keyspace, cfs, rf, 0, 0, 0);
        testEstimateResultsPerRange(keyspace, cfs, rf, 1, 0, 0);
        testEstimateResultsPerRange(keyspace, cfs, rf, 10, 0, 0);
        testEstimateResultsPerRange(keyspace, cfs, rf, 100, 0, 0);
        testEstimateResultsPerRange(keyspace, cfs, rf, 0, 1000, 200);
        testEstimateResultsPerRange(keyspace, cfs, rf, 1, 1000, 200);
        testEstimateResultsPerRange(keyspace, cfs, rf, 10, 1000, 200);
        testEstimateResultsPerRange(keyspace, cfs, rf, 100, 1000, 200);
    }

    private static void testEstimateResultsPerRange(Keyspace keyspace,
                                                    ColumnFamilyStore cfs,
                                                    int rf,
                                                    int commandEstimate,
                                                    Integer indexEstimate,
                                                    float expectedEstimate)
    {
        PartitionRangeReadCommand command = command(cfs, Integer.MAX_VALUE, commandEstimate, indexEstimate);
        assertEquals(expectedEstimate / rf, RangeCommands.estimateResultsPerRange(command, keyspace), 0);
    }

    private static PartitionRangeReadCommand command(ColumnFamilyStore cfs, int limit, int commandEstimate)
    {
        return command(cfs, limit, commandEstimate, null);
    }

    private static PartitionRangeReadCommand command(ColumnFamilyStore cfs, int limit, int commandEstimate, Integer indexEstimate)
    {
        AbstractReadCommandBuilder.PartitionRangeBuilder commandBuilder = Util.cmd(cfs);
        if (indexEstimate != null)
        {
            commandBuilder.filterOn("v", Operator.EQ, 0);
            MockedIndex.estimatedResultRows = indexEstimate;
        }
        PartitionRangeReadCommand command = (PartitionRangeReadCommand) commandBuilder.build();
        return command.withUpdatedLimit(new MockedDataLimits(DataLimits.cqlLimits(limit), commandEstimate));
    }

    private static void setNumTokens(int numTokens)
    {
        DatabaseDescriptor.getRawConfig().num_tokens = numTokens;
    }

    private static class MockedDataLimits extends DataLimits
    {
        private final DataLimits wrapped;
        private final int estimateTotalResults;

        public MockedDataLimits(DataLimits wrapped, int estimateTotalResults)
        {
            this.wrapped = wrapped;
            this.estimateTotalResults = estimateTotalResults;
        }

        @Override
        public float estimateTotalResults(ColumnFamilyStore cfs)
        {
            return estimateTotalResults;
        }

        @Override
        public Kind kind()
        {
            return wrapped.kind();
        }

        @Override
        public boolean isUnlimited()
        {
            return wrapped.isUnlimited();
        }

        @Override
        public boolean isDistinct()
        {
            return wrapped.isDistinct();
        }

        @Override
        public DataLimits forPaging(int pageSize)
        {
            return wrapped.forPaging(pageSize);
        }

        @Override
        public DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            return wrapped.forPaging(pageSize, lastReturnedKey, lastReturnedKeyRemaining);
        }

        @Override
        public DataLimits forShortReadRetry(int toFetch)
        {
            return wrapped.forShortReadRetry(toFetch);
        }

        @Override
        public boolean hasEnoughLiveData(CachedPartition cached, long nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            return wrapped.hasEnoughLiveData(cached, nowInSec, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        }

        @Override
        public Counter newCounter(long nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            return wrapped.newCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        }

        @Override
        public int count()
        {
            return wrapped.count();
        }

        @Override
        public int perPartitionCount()
        {
            return wrapped.perPartitionCount();
        }

        @Override
        public DataLimits withoutState()
        {
            return wrapped.withoutState();
        }
    }

    public static final class MockedIndex extends StubIndex
    {
        private static long estimatedResultRows = 0;

        public MockedIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public long getEstimatedResultRows()
        {
            return estimatedResultRows;
        }
    }
}
