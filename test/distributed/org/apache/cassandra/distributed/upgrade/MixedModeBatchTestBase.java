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

package org.apache.cassandra.distributed.upgrade;

import java.util.ArrayList;
import java.util.List;

import com.vdurmont.semver4j.Semver;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.BATCH_STORE_REQ;
import static org.apache.cassandra.net.Verb.REQUEST_RSP;

/**
 * A base class for testing the replication of logged/unlogged batches on mixed-version clusters.
 * 
 * The tests based on this class partially replace the Python dtests in batch_test.py created for CASSANDRA-9673.
 */
public class MixedModeBatchTestBase extends UpgradeTestBase
{
    private static final int KEYS_PER_BATCH = 10;

    protected void testSimpleStrategy(Semver from, boolean isLogged) throws Throwable
    {
        testSimpleStrategy(from, UpgradeTestBase.CURRENT, isLogged);
    }

    protected void testSimpleStrategy(Semver from, Semver to, boolean isLogged) throws Throwable
    {
        String insert = "INSERT INTO test_simple.names (key, name) VALUES (%d, '%s')";
        String select = "SELECT * FROM test_simple.names WHERE key = ?";

        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2)
        .upgrades(from, to)
        .setup(cluster -> {
            cluster.schemaChange("CREATE KEYSPACE test_simple WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};");
            cluster.schemaChange("CREATE TABLE test_simple.names (key int PRIMARY KEY, name text)");
        })
        .runAfterNodeUpgrade((cluster, upgraded) -> {
            if (isLogged)
            {
                // If we're testing logged batches, exercise the case were batchlog writes fail.
                IMessageFilters.Filter dropBatchlogWrite = cluster.filters().inbound().verbs(BATCH_STORE_REQ.id, REQUEST_RSP.id).drop();
                dropBatchlogWrite.on();
                testBatches(true, true, insert, select, cluster, upgraded);
                cluster.filters().reset();
            }

            cluster.coordinator(1).execute("TRUNCATE test_simple.names", ConsistencyLevel.ALL);
            testBatches(isLogged, false, insert, select, cluster, upgraded);
            cluster.coordinator(1).execute("TRUNCATE test_simple.names", ConsistencyLevel.ALL);
        })
        .run();
    }

    private void testBatches(boolean isLogged, boolean failBatchlog, String insert, String select, UpgradeableCluster cluster, int upgraded)
    {
        List<Long> initialTokens = new ArrayList<>(cluster.size());

        for (int i = 1; i <= cluster.size(); i++)
            initialTokens.add(Long.valueOf(cluster.get(i).config().get("initial_token").toString()));

        // Exercise all the coordinators...
        for (int i = 1; i <= cluster.size(); i++)
        {
            StringBuilder batchBuilder = new StringBuilder("BEGIN " + (isLogged ? "" : "UNLOGGED ") + "BATCH\n");
            String name = "Julia";
            Runnable[] tests = new Runnable[KEYS_PER_BATCH];

            // ...and sample enough keys that we cover the ring.
            for (int j = 0; j < KEYS_PER_BATCH; j++)
            {
                int key = j + (i * KEYS_PER_BATCH);
                batchBuilder.append(String.format(insert, key, name)).append('\n');

                // Track the test that will later verify that this mutation was replicated properly.
                tests[j] = () -> {
                    Object[] row = row(key, name);
                    Long token = tokenFrom(key);
                    int node = primaryReplica(initialTokens, token);
                    
                    Object[][] primaryResult = cluster.get(node).executeInternal(select, key);
                    
                    // We shouldn't expect to see any results if the batchlog write failed.
                    if (failBatchlog)
                        assertRows(primaryResult);
                    else
                        assertRows(primaryResult, row);

                    node = nextNode(node, cluster.size());

                    Object[][] nextResult = cluster.get(node).executeInternal(select, key);

                    if (failBatchlog)
                        assertRows(nextResult);
                    else
                        assertRows(nextResult, row);

                    // At RF=2, this node should not have received the write.
                    node = nextNode(node, cluster.size());
                    assertRows(cluster.get(node).executeInternal(select, key));
                };
            }

            String batch = batchBuilder.append("APPLY BATCH").toString();
            
            try
            {
                cluster.coordinator(i).execute(batch, ConsistencyLevel.ALL);
            }
            catch (Throwable t)
            {
                if (!failBatchlog || !exceptionMatches(t, WriteTimeoutException.class))
                {
                    // The standard write failure exception won't give us any context for what actually failed.
                    if (exceptionMatches(t, WriteFailureException.class))
                    {
                        String message = "Failed to write following batch to coordinator %d after upgrading node %d:\n%s";
                        throw new AssertionError(String.format(message, i, upgraded, batch), t);
                    }

                    throw t;
                }
                
                // Failing the batchlog write will involve a timeout, so that's expected. Just continue...
            }
            
            for (Runnable test : tests)
                test.run();
        }
    }

    private boolean exceptionMatches(Throwable t, Class<?> clazz)
    {
        return t.getClass().getSimpleName().equals(clazz.getSimpleName()) 
               || t.getCause() != null && t.getCause().getClass().getSimpleName().equals(clazz.getSimpleName());
    }
}
