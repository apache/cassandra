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

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.reads.range.RangeCommandIterator;

import static junit.framework.TestCase.assertEquals;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class ConcurrencyFactorTest extends TestBaseImpl
{
    private static final String SAI_TABLE = "sai_simple_primary_key";

    private static final int nodes = 3;

    private org.apache.cassandra.distributed.Cluster cluster;

    @Before
    public void init() throws IOException
    {
        cluster = init(Cluster.build(nodes).withTokenSupplier(node -> {
            switch (node)
            {
                case 1: return -9223372036854775808L;
                case 2: return -3074457345618258602L;
                case 3: return 3074457345618258603L;
                default: throw new IllegalArgumentException();
            }
        }).withConfig(config -> config.with(NETWORK).with(GOSSIP)).start());
    }

    @After
    public void cleanup()
    {
        cluster.close();
    }

    private void insertRows(long startVal, long endVal, long increment)
    {
        String template = "INSERT INTO %s.%s (pk, state, gdp) VALUES (%s, %s)";
        Random rnd = new Random();
        String fakeState, rowData;
        int i = 0;
        for (long val = startVal; val <= endVal; val += increment)
        {
            fakeState = String.format("%c%c", (char)(rnd.nextInt(26) + 'A'), (char)(rnd.nextInt(26) + 'A'));
            rowData = String.format("'%s', %s", fakeState, val);
            cluster.coordinator(1).execute(String.format(template, KEYSPACE, SAI_TABLE, i++, rowData), ConsistencyLevel.LOCAL_ONE);
        }
    }

    @Test
    public void testInitialConcurrencySelection()
    {
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int, state ascii, gdp bigint, PRIMARY KEY (pk)) WITH compaction = " +
                                           " {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }", KEYSPACE, SAI_TABLE));
        cluster.schemaChange(String.format("CREATE CUSTOM INDEX ON %s.%s (gdp) USING 'StorageAttachedIndex'", KEYSPACE, SAI_TABLE));

        insertRows(1_000_000_000L, 16_000_000_000L, 1_000_000_000L);

        // flush all nodes, expected row distribution by partition key value
        // node0: 9, 14, 12, 3
        // node1: 5, 10, 13, 11, 1, 8, 0, 2
        // node2: 4, 15, 7, 6
        cluster.forEach((node) -> node.flush(KEYSPACE));

        // we expect to use StorageProxy#RangeCommandIterator and the hit count to increase
        String query = String.format("SELECT state FROM %s.%s WHERE gdp > ? AND gdp < ? LIMIT 20", KEYSPACE, SAI_TABLE);
        int prevHistCount = getRangeReadCount();
        runAndValidate(prevHistCount, 1, query, 3_000_000_000L, 7_000_000_000L);

        // partition-restricted query
        // we don't expect to use StorageProxy#RangeCommandIterator so previous hit count remains the same
        query = String.format("SELECT state FROM %s.%s WHERE pk = ?", KEYSPACE, SAI_TABLE);
        runAndValidate(prevHistCount, 1, query, 0);

        // token-restricted query
        // we expect StorageProxy#RangeCommandIterator to be used so reset previous hit count
        query = String.format("SELECT * FROM %s.%s WHERE token(pk) > 0", KEYSPACE, SAI_TABLE);
        prevHistCount = getRangeReadCount();
        runAndValidate(prevHistCount, 1, query);

        // token-restricted query and index
        // we expect StorageProxy#RangeCommandIterator to be used so reset previous hit count
        query = String.format("SELECT * FROM %s.%s WHERE token(pk) > 0 AND gdp > ?", KEYSPACE, SAI_TABLE);
        prevHistCount = getRangeReadCount();
        runAndValidate(prevHistCount, 1, query, 3_000_000_000L);
    }

    /*
        Run the given query, check the hit count, check the max round trips.
     */
    private void runAndValidate(int prevHistCount, int maxRoundTrips, String query, Object... bondValues)
    {
        cluster.coordinator(1).execute(query, ConsistencyLevel.ALL, bondValues);
        assertEquals(prevHistCount + 1,  getRangeReadCount());
        assertEquals(maxRoundTrips, getMaxRoundTrips());
    }

    private int getRangeReadCount()
    {
        return cluster.get(1).callOnInstance(() -> Math.toIntExact(RangeCommandIterator.rangeMetrics.roundTrips.getCount()));
    }

    private int getMaxRoundTrips()
    {
        return cluster.get(1).callOnInstance(() -> Math.toIntExact(RangeCommandIterator.rangeMetrics.roundTrips.getSnapshot().getMax()));
    }
}
