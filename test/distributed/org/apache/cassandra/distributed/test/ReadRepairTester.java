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

package org.apache.cassandra.distributed.test;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertEquals;
import static org.apache.cassandra.distributed.test.TestBaseImpl.KEYSPACE;

/**
 * Extensible helper class for read repair tests.
 */
public abstract class ReadRepairTester<T extends ReadRepairTester<T>>
{
    private static final AtomicInteger seqNumber = new AtomicInteger();

    private final String keyspaceName = "ks_" + seqNumber.getAndIncrement();
    private static final String tableName = "tbl";
    final String qualifiedTableName = keyspaceName + '.' + tableName;

    protected final Cluster cluster;
    protected final ReadRepairStrategy strategy;
    protected final boolean flush;
    protected final boolean paging;
    protected final boolean reverse;
    protected final int coordinator;
    protected final ReplicationType replicationType;

    // logged replication test support
    protected int lastMutatedNode = -1;

    ReadRepairTester(Cluster cluster, ReadRepairStrategy strategy, int coordinator, boolean flush, boolean paging, boolean reverse, ReplicationType replicationType)
    {
        this.cluster = cluster;
        this.strategy = strategy;
        this.flush = flush;
        this.paging = paging;
        this.reverse = reverse;
        this.coordinator = coordinator;
        this.replicationType = replicationType;
    }

    abstract T self();

    T schemaChange(String... queries)
    {
        for (String query : queries)
            cluster.schemaChange(query);

        return self();
    }

    T createTable(String createTable)
    {
        cluster.schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': " + cluster.size() + "} AND replication_type='%s'",
                                           keyspaceName, replicationType.name()));
        cluster.schemaChange(String.format("CREATE TYPE IF NOT EXISTS %s.udt (x int, y int)", keyspaceName));
        String query;
        switch (StringUtils.countMatches(createTable, "%s"))
        {
            case 1:
                query = String.format(createTable + " WITH read_repair='%s'", qualifiedTableName, strategy);
                break;
            case 2:
                query = String.format(createTable, qualifiedTableName, strategy);
                break;
            case 3:
                query = String.format(createTable, qualifiedTableName, reverse ? "DESC" : "ASC", strategy);
                break;
            default:
                throw new AssertionError("Expected 1 to 3 placeholders");
        }

        return schemaChange(query);
    }

    /**
     * Runs the specified mutations in only one replica.
     */
    T mutate(int node, String... queries)
    {
        lastMutatedNode = node;
        // run the write queries only on one node
        for (String query : queries)
            cluster.get(node).executeInternal(String.format(query, qualifiedTableName));

        // flush the update node to ensure reads come from sstables
        if (flush)
            cluster.get(node).flush(KEYSPACE);

        return self();
    }

    private Object[][] queryDistributed(String query, Object... boundValues)
    {
        String formattedQuery = String.format(query, qualifiedTableName);
        ICoordinator coordinator = cluster.coordinator(this.coordinator);
        return paging
               ? Iterators.toArray(coordinator.executeWithPaging(formattedQuery, ALL, 1, boundValues), Object[].class)
               : coordinator.execute(formattedQuery, ALL, boundValues);
    }

    T assertRowsDistributed(String query, long expectedRepaired, Object[]... expectedRows)
    {
        // run the query in the coordinator recording the increase in repaired rows metric
        long actualRepaired = readRepairRequestsCount(coordinator);
        Object[][] actualRows = queryDistributed(query);
        actualRepaired = readRepairRequestsCount(coordinator) - actualRepaired;

        // verify the returned rows
        if (reverse)
            expectedRows = reverse(expectedRows);
        AssertUtils.assertRows(actualRows, expectedRows);

        // verify the number of repaired rows
        if (strategy == ReadRepairStrategy.NONE)
            expectedRepaired = 0;
        assertEquals(String.format("Expected %d repaired rows, but found %d", expectedRepaired, actualRepaired),
                     expectedRepaired, actualRepaired);

        return self();
    }

    protected Object[][] reverse(Object[][] rows)
    {
        Object[][] reversed = ArrayUtils.clone(rows);
        ArrayUtils.reverse(reversed);
        return reversed;
    }

    long readRepairRequestsCount(int node)
    {
        return readRepairRequestsCount(cluster.get(node), keyspaceName, tableName);
    }

    static long readRepairRequestsCount(IInvokableInstance node, String keyspace, String table)
    {
        return node.callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            return cfs.metric.readRepairRequests.getCount();
        });
    }
}
