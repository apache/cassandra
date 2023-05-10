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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.TracingUtil;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.utils.TimeUUID;

import static org.awaitility.Awaitility.await;

public class ConcurrencyFactorTest extends TestBaseImpl
{
    private static final String SAI_TABLE = "sai_simple_primary_key";
    private static final int NODES = 3;

    private Cluster cluster;

    @Before
    public void init() throws IOException
    {
        cluster = init(Cluster.build(NODES).withTokenSupplier(generateTokenSupplier()).withTokenCount(1).start());
    }

    @After
    public void cleanup()
    {
        cluster.close();
    }

    @Test
    public void testInitialConcurrencySelection()
    {
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int, state ascii, gdp bigint, PRIMARY KEY (pk)) WITH compaction = " +
                                           " {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }", KEYSPACE, SAI_TABLE));
        cluster.schemaChange(String.format("CREATE CUSTOM INDEX ON %s.%s (gdp) USING 'StorageAttachedIndex'", KEYSPACE, SAI_TABLE));

        String template = "INSERT INTO %s.%s (pk, state, gdp) VALUES (%s, %s)";
        Random rnd = new Random();
        String fakeState, rowData;
        int i = 0;
        for (long val = 1_000_000_000L; val <= 16_000_000_000L; val += 1_000_000_000L)
        {
            fakeState = String.format("%c%c", (char)(rnd.nextInt(26) + 'A'), (char)(rnd.nextInt(26) + 'A'));
            rowData = String.format("'%s', %s", fakeState, val);
            cluster.coordinator(1).execute(String.format(template, KEYSPACE, SAI_TABLE, i++, rowData), ConsistencyLevel.LOCAL_ONE);
        }

        // flush all nodes, expected row distribution by partition key value
        // node0: 9, 14, 12, 3
        // node1: 5, 10, 13, 11, 1, 8, 0, 2
        // node2: 4, 15, 7, 6
        cluster.forEach((node) -> node.flush(KEYSPACE));

        // We are expecting any of 3 specific trace messages indicating how the query has been handled:
        //
        // Submitting range requests on <n> ranges with a concurrency of <n>
        //   Initial concurrency wasn't estimated and max concurrency was used instead (and SAI index was involved)
        // Submitting range requests on <n> ranges with a concurrency of <n> (<m> rows per range expected)
        //   Initial concurrency was estimated based on estimated rows per range (non-SAI range query)
        // Executing single-partition query on <table>
        //   Non-range single-partition query

        // SAI range query so should bypass initial concurrency estimation
        String query = String.format("SELECT state FROM %s.%s WHERE gdp > ? AND gdp < ? LIMIT 20", KEYSPACE, SAI_TABLE);
        runAndValidate("Submitting range requests on 3 ranges with a concurrency of 3", query, 3_000_000_000L, 7_000_000_000L);

        // Partition-restricted query so not a range query
        query = String.format("SELECT state FROM %s.%s WHERE pk = ?", KEYSPACE, SAI_TABLE);
        runAndValidate("Executing single-partition query on sai_simple_primary_key", query, 0);

        // Token-restricted range query not using SAI so should use initial concurrency estimation
        query = String.format("SELECT * FROM %s.%s WHERE token(pk) > 0", KEYSPACE, SAI_TABLE);
        runAndValidate("Submitting range requests on 2 ranges with a concurrency of 2 (230.4 rows per range expected)", query);

        // Token-restricted range query with SAI so should bypass initial concurrency estimation
        query = String.format("SELECT * FROM %s.%s WHERE token(pk) > 0 AND gdp > ?", KEYSPACE, SAI_TABLE);
        runAndValidate("Submitting range requests on 2 ranges with a concurrency of 2", query, 3_000_000_000L);
    }

    /**
     * Run the given query and check that the given trace message exists in the trace entries.
     */
    private void runAndValidate(String trace, String query, Object... bondValues)
    {
        UUID sessionId = TimeUUID.Generator.nextTimeAsUUID();

        cluster.coordinator(1).executeWithTracingWithResult(sessionId, query, ConsistencyLevel.ALL, bondValues);

        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            List<TracingUtil.TraceEntry> traceEntries = TracingUtil.getTrace(cluster, sessionId, ConsistencyLevel.ONE);
            return traceEntries.stream().anyMatch(entry -> entry.activity.equals(trace));
        });
    }

    private static TokenSupplier generateTokenSupplier()
    {
        List<List<String>> tokens = Arrays.asList(Collections.singletonList("-9223372036854775808"),
                                                  Collections.singletonList("-3074457345618258602"),
                                                  Collections.singletonList("3074457345618258603"));
        return nodeIdx -> tokens.get(nodeIdx - 1);
    }
}
