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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestFailureReason;

public class ReadFailureTest extends TestBaseImpl
{
    static final int TOMBSTONE_FAIL_THRESHOLD = 20;
    static final int TOMBSTONE_FAIL_KEY = 100001;
    static final String TABLE = "t";

    /**
     * This test attempts to create a race condition with speculative executions that would previously cause an AssertionError.
     * N=2, RF=2, read ONE
     * The read will fail on the local node due to tombstone read threshold. At the same time, a spec exec is triggered
     * reading from the other node.
     * <p>
     * See CASSANDRA-16097 for further details.
     */
    @Test
    public void testSpecExecRace() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build().withNodes(2).withConfig(config -> config.set("tombstone_failure_threshold", TOMBSTONE_FAIL_THRESHOLD)).start()))
        {
            // Create a table with the spec exec policy set to a low percentile so it's more likely to produce a spec exec racing with the local request.
            // Not using 'Always' because that actually uses a different class/mechanism and doesn't exercise the bug
            // we're trying to produce.
            cluster.schemaChange(String.format("CREATE TABLE %s.%s (k int, c int, v int, PRIMARY KEY (k,c)) WITH speculative_retry = '5p';", KEYSPACE, TABLE));

            // Create a partition with enough tombstones to create a read failure according to the configured threshold
            for (int i = 0; i <= TOMBSTONE_FAIL_THRESHOLD; ++i)
                cluster.coordinator(1).execute(String.format("DELETE FROM %s.t WHERE k=%d AND c=%d", KEYSPACE, TOMBSTONE_FAIL_KEY, i),
                                               ConsistencyLevel.TWO);

            // Create a bunch of latency samples for this failed operation.
            loopFailStatement(cluster, 5000);
            // Update the spec exec threshold based on the above samples.
            // This would normally be done by the periodic task CassandraDaemon.SPECULATION_THRESHOLD_UPDATER.
            cluster.get(1).runOnInstance(() ->
                                         {
                                             ColumnFamilyStore cfs = Keyspace.open(KEYSPACE)
                                                                             .getColumnFamilyStore(TABLE);
                                             cfs.updateSpeculationThreshold();
                                         });

            // Run the request a bunch of times under racy conditions.
            loopFailStatement(cluster, 5000);
        }
    }

    private void loopFailStatement(ICluster cluster, int iterations)
    {
        final String query = String.format("SELECT k FROM %s.t WHERE k=%d", KEYSPACE, TOMBSTONE_FAIL_KEY);
        for (int i = 0; i < iterations; ++i)
        {
            try
            {
                cluster.coordinator(1).execute(query, ConsistencyLevel.ONE);
                fail("Request did not throw a ReadFailureException as expected.");
            }
            catch (Throwable t) // Throwable because the raised ReadFailure is loaded from a different classloader and doesn't match "ours"
            {
                String onFail = String.format("Did not receive expected ReadFailureException. Instead caught %s\n%s",
                                              t, ExceptionUtils.getStackTrace(t));
                assertNotNull(onFail, t.getMessage());
                assertTrue(onFail, t.getMessage().contains(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.name()));
            }
        }
    }
}

