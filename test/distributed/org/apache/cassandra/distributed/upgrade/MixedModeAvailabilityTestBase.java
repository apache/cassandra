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

import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;


public abstract class MixedModeAvailabilityTestBase extends UpgradeTestBase
{
    private static final int NUM_NODES = 3;
    private static final int COORDINATOR = 1;
    private static final String INSERT = withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (?, ?, ?)");
    private static final String SELECT = withKeyspace("SELECT * FROM %s.t WHERE k = ?");
    private static final Map<ConsistencyLevel, ConsistencyLevel> CONSISTENCY_LEVELS = new EnumMap<>(ConsistencyLevel.class)
    {{
        put(ALL, ONE);
        put(ONE, ALL);
        put(QUORUM, QUORUM);
    }};

    protected abstract boolean upgradedCoordinator();

    @Test
    public void testAvailability() throws Throwable
    {
        new TestCase()
        .nodes(NUM_NODES)
        .nodesToUpgrade(upgradedCoordinator() ? 1 : 2)
        .upgradesToCurrentFrom(v30)
        .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                    .set("request_timeout_in_ms", MINUTES.toMillis(10))
                                    .set("read_request_timeout_in_ms", MINUTES.toMillis(10))
                                    .set("write_request_timeout_in_ms", MINUTES.toMillis(10)))
        .setup(cluster -> {
            // always use rapid read protection to speed up queries with down nodes
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k uuid, c int, v int, PRIMARY KEY (k, c)) " +
                                              "WITH speculative_retry = 'ALWAYS'"));
            cluster.setUncaughtExceptionsFilter(throwable -> throwable instanceof RejectedExecutionException);
        })
        .runAfterNodeUpgrade((cluster, n) -> {

            ICoordinator coordinator = cluster.coordinator(COORDINATOR);

            // using 0 to 2 down nodes...
            for (int i = 0; i < NUM_NODES; i++)
            {
                // stop the replica node that we want to be down during queries
                final int numNodesDown = i;
                if (numNodesDown > 0)
                    cluster.get(replica(COORDINATOR, numNodesDown)).shutdown().get();

                // for each write-read consistency level combination...
                CONSISTENCY_LEVELS.forEach((writeConsistencyLevel, readConsistencyLevel) -> {

                    UUID key = UUID.randomUUID();
                    Object[] row1 = row(key, 1, 10);
                    Object[] row2 = row(key, 2, 20);

                    boolean reading = false;
                    try
                    {
                        // test writes if the write consistency level is compatible with the number of down nodes
                        if (numNodesDown <= maxNodesDown(writeConsistencyLevel))
                        {
                            coordinator.execute(INSERT, writeConsistencyLevel, row1);
                            coordinator.execute(INSERT, writeConsistencyLevel, row2);
                        }

                        reading = true;

                        // test reads if the read consistency level is compatible with the number of down nodes
                        if (numNodesDown <= maxNodesDown(readConsistencyLevel))
                        {
                            Object[][] rows = coordinator.execute(SELECT, readConsistencyLevel, key);
                            if (numNodesDown <= maxNodesDown(writeConsistencyLevel))
                                assertRows(rows, row1, row2);
                        }
                    }
                    catch (Throwable t)
                    {
                        throw new AssertionError(format("Unexpected error while %s in case write-read consistency " +
                                                        "%s-%s with %s coordinator and %d nodes down: %s",
                                                        reading ? "reading" : "writing",
                                                        writeConsistencyLevel,
                                                        readConsistencyLevel,
                                                        upgradedCoordinator() ? "upgraded" : "not upgraded",
                                                        numNodesDown,
                                                        t), t);
                    }
                });
            }

        }).run();
    }

    private static int replica(int node, int depth)
    {
        assert depth >= 0;
        return depth == 0 ? node : replica(node == NUM_NODES ? 1 : node + 1, depth - 1);
    }

    private static int maxNodesDown(ConsistencyLevel cl)
    {
        if (cl == ONE)
            return 2;

        if (cl == QUORUM)
            return 1;

        if (cl == ALL)
            return 0;

        throw new IllegalArgumentException("Unsupported consistency level: " + cl);
    }
}
