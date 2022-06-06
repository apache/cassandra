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

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.Verb;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.READ_REQ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public abstract class MixedModeAvailabilityTestBase extends UpgradeTestBase
{
    private static final int NUM_NODES = 3;
    private static final int COORDINATOR = 1;
    private static final List<Tester> TESTERS = Arrays.asList(new Tester(ONE, ALL),
                                                              new Tester(QUORUM, QUORUM),
                                                              new Tester(ALL, ONE));

    private final Semver initial;

    protected MixedModeAvailabilityTestBase(Semver initial)
    {
        this.initial = initial;
    }

    @Test
    public void testAvailabilityCoordinatorNotUpgraded() throws Throwable
    {
        testAvailability(false, initial);
    }

    @Test
    public void testAvailabilityCoordinatorUpgraded() throws Throwable
    {
        testAvailability(true, initial);
    }

    private static void testAvailability(boolean upgradedCoordinator,
                                         Semver initial) throws Throwable
    {
        new TestCase()
        .nodes(NUM_NODES)
        .nodesToUpgrade(upgradedCoordinator ? 1 : 2)
        .upgradesToCurrentFrom(initial)
        .withConfig(config -> config.set("read_request_timeout_in_ms", SECONDS.toMillis(2))
                                    .set("write_request_timeout_in_ms", SECONDS.toMillis(2)))
        // use retry of 10ms so that each check is consistent
        // At the start of the world cfs.sampleLatencyNanos == 0, which means speculation acts as if ALWAYS is done,
        // but after the first refresh this gets set high enough that we don't trigger speculation for the rest of the test!
        // To be consistent set retry to 10ms so cfs.sampleLatencyNanos stays consistent for the duration of the test.
        .setup(c -> c.schemaChange(withKeyspace("CREATE TABLE %s.t (k uuid, c int, v int, PRIMARY KEY (k, c)) WITH speculative_retry = '10ms'")))
        .runBeforeClusterUpgrade(cluster -> cluster.filters().reset())
        .runAfterNodeUpgrade((cluster, n) -> {

            // using 0 to 2 down nodes...
            for (int numNodesDown = 0; numNodesDown < NUM_NODES; numNodesDown++)
            {
                // disable communications to the down nodes
                if (numNodesDown > 0)
                {
                    cluster.filters().outbound().verbs(READ_REQ.id).to(replica(COORDINATOR, numNodesDown)).drop();
                    cluster.filters().outbound().verbs(Verb.MUTATION_REQ.id).to(replica(COORDINATOR, numNodesDown)).drop();
                }

                // run the test cases that are compatible with the number of down nodes
                ICoordinator coordinator = cluster.coordinator(COORDINATOR);
                for (Tester tester : TESTERS)
                    tester.test(coordinator, numNodesDown, upgradedCoordinator);
            }
        }).run();
    }

    private static int replica(int node, int depth)
    {
        assert depth >= 0;
        return depth == 0 ? node : replica(node == NUM_NODES ? 1 : node + 1, depth - 1);
    }

    private static class Tester
    {
        private static final String INSERT = withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (?, ?, ?)");
        private static final String SELECT = withKeyspace("SELECT * FROM %s.t WHERE k = ?");

        private final ConsistencyLevel writeConsistencyLevel;
        private final ConsistencyLevel readConsistencyLevel;

        private Tester(ConsistencyLevel writeConsistencyLevel, ConsistencyLevel readConsistencyLevel)
        {
            this.writeConsistencyLevel = writeConsistencyLevel;
            this.readConsistencyLevel = readConsistencyLevel;
        }

        public void test(ICoordinator coordinator, int numNodesDown, boolean upgradedCoordinator)
        {
            UUID key = UUID.randomUUID();
            Object[] row1 = row(key, 1, 10);
            Object[] row2 = row(key, 2, 20);

            boolean wrote = false;
            try
            {
                // test write
                maybeFail(WriteTimeoutException.class, numNodesDown > maxNodesDown(writeConsistencyLevel), () -> {
                    coordinator.execute(INSERT, writeConsistencyLevel, row1);
                    coordinator.execute(INSERT, writeConsistencyLevel, row2);
                });

                wrote = true;

                // test read
                maybeFail(ReadTimeoutException.class, numNodesDown > maxNodesDown(readConsistencyLevel), () -> {
                    Object[][] rows = coordinator.execute(SELECT, readConsistencyLevel, key);
                    if (numNodesDown <= maxNodesDown(writeConsistencyLevel))
                        assertRows(rows, row1, row2);
                });
            }
            catch (Throwable t)
            {
                throw new AssertionError(format("Unexpected error while %s in case write-read consistency %s-%s with %s coordinator and %d nodes down",
                                                wrote ? "reading" : "writing",
                                                writeConsistencyLevel,
                                                readConsistencyLevel,
                                                upgradedCoordinator ? "upgraded" : "not upgraded",
                                                numNodesDown), t);
            }
        }

        private static <E extends Exception> void maybeFail(Class<E> exceptionClass, boolean shouldFail, Runnable test)
        {
            try
            {
                test.run();
                assertFalse(shouldFail);
            }
            catch (Exception e)
            {
                // we should use exception class names due to the different classpaths
                String className = e.getClass().getCanonicalName();
                if (e instanceof RuntimeException && e.getCause() != null)
                    className = e.getCause().getClass().getCanonicalName();

                if (shouldFail)
                    assertEquals(exceptionClass.getCanonicalName(), className);
                else
                    throw e;
            }
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
}
