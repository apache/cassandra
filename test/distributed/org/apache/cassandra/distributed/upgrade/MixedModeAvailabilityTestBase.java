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

import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.Verb;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.READ_REQ;
import static org.junit.Assert.assertFalse;


public abstract class MixedModeAvailabilityTestBase extends UpgradeTestBase
{
    private static final int NUM_NODES = 3;
    private static final int COORDINATOR = 1;
    private static final String INSERT = withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (?, ?, ?)");
    private static final String SELECT = withKeyspace("SELECT * FROM %s.t WHERE k = ?");

    private final Semver initial;
    private final ConsistencyLevel writeConsistencyLevel;
    private final ConsistencyLevel readConsistencyLevel;

    public MixedModeAvailabilityTestBase(Semver initial, ConsistencyLevel writeConsistencyLevel, ConsistencyLevel readConsistencyLevel)
    {
        this.initial = initial;
        this.writeConsistencyLevel = writeConsistencyLevel;
        this.readConsistencyLevel = readConsistencyLevel;
    }

    @Test
    public void testAvailabilityCoordinatorNotUpgraded() throws Throwable
    {
        testAvailability(false, initial, writeConsistencyLevel, readConsistencyLevel);
    }

    @Test
    public void testAvailabilityCoordinatorUpgraded() throws Throwable
    {
        testAvailability(true, initial, writeConsistencyLevel, readConsistencyLevel);
    }

    private static void testAvailability(boolean upgradedCoordinator,
                                         Semver initial,
                                         ConsistencyLevel writeConsistencyLevel,
                                         ConsistencyLevel readConsistencyLevel) throws Throwable
    {
        new TestCase()
        .nodes(NUM_NODES)
        .nodesToUpgrade(upgradedCoordinator ? 1 : 2)
        .upgrades(initial, UpgradeTestBase.CURRENT)
        .withConfig(config -> config.set("read_request_timeout_in_ms", SECONDS.toMillis(5))
                                    .set("write_request_timeout_in_ms", SECONDS.toMillis(5)))
        // use retry of 10ms so that each check is consistent
        // At the start of the world cfs.sampleLatencyNanos == 0, which means speculation acts as if ALWAYS is done,
        // but after the first refresh this gets set high enough that we don't trigger speculation for the rest of the test!
        // To be consistent set retry to 10ms so cfs.sampleLatencyNanos stays consistent for the duration of the test.
        .setup(cluster -> {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k uuid, c int, v int, PRIMARY KEY (k, c)) WITH speculative_retry = '10ms'"));
            cluster.setUncaughtExceptionsFilter(throwable -> throwable instanceof RejectedExecutionException);
        })
        .runAfterNodeUpgrade((cluster, n) -> {

            ICoordinator coordinator = cluster.coordinator(COORDINATOR);

            // using 0 to 2 down nodes...
            for (int i = 0; i < NUM_NODES; i++)
            {
                final int numNodesDown = i;

                // disable communications to the down nodes
                if (numNodesDown > 0)
                {
                    cluster.filters().outbound().verbs(READ_REQ.id).to(replica(COORDINATOR, numNodesDown)).drop();
                    cluster.filters().outbound().verbs(Verb.MUTATION_REQ.id).to(replica(COORDINATOR, numNodesDown)).drop();
                }

                UUID key = UUID.randomUUID();
                Object[] row1 = row(key, 1, 10);
                Object[] row2 = row(key, 2, 20);

                boolean wrote = false;
                try
                {
                    // test write
                    maybeFail(false, numNodesDown > maxNodesDown(writeConsistencyLevel), () -> {
                        coordinator.execute(INSERT, writeConsistencyLevel, row1);
                        coordinator.execute(INSERT, writeConsistencyLevel, row2);
                    });

                    wrote = true;

                    // test read
                    maybeFail(true, numNodesDown > maxNodesDown(readConsistencyLevel), () -> {
                        Object[][] rows = coordinator.execute(SELECT, readConsistencyLevel, key);
                        if (numNodesDown <= maxNodesDown(writeConsistencyLevel))
                            assertRows(rows, row1, row2);
                    });
                }
                catch (Throwable t)
                {
                    throw new AssertionError(format("Unexpected error while %s in case write-read consistency %s-%s with %s coordinator and %d nodes down: %s",
                                                    wrote ? "reading" : "writing",
                                                    writeConsistencyLevel,
                                                    readConsistencyLevel,
                                                    upgradedCoordinator ? "upgraded" : "not upgraded",
                                                    numNodesDown,
                                                    t), t);
                }
            }
        }).run();
    }

    private static int replica(int node, int depth)
    {
        assert depth >= 0;
        return depth == 0 ? node : replica(node == NUM_NODES ? 1 : node + 1, depth - 1);
    }

    private static void maybeFail(boolean isRead, boolean shouldFail, Runnable test)
    {
        try
        {
            test.run();
            assertFalse("Should have failed", shouldFail);
        }
        catch (Exception e)
        {
            if (!shouldFail)
                throw e;

            // we should use exception class names due to the different classpaths
            String className = (e instanceof RuntimeException && e.getCause() != null)
                               ? e.getCause().getClass().getCanonicalName()
                               : e.getClass().getCanonicalName();

            if (isRead)
            {
                Assertions.assertThat(className)
                          .isIn(ReadTimeoutException.class.getCanonicalName(),
                                ReadFailureException.class.getCanonicalName());
            }
            else
            {
                Assertions.assertThat(className)
                          .isIn(WriteTimeoutException.class.getCanonicalName(),
                                WriteFailureException.class.getCanonicalName());
            }
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
