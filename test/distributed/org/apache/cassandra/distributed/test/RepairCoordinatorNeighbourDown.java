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

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairParallelism;
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairType;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.IMessageFilters.Matcher.of;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertParentRepairFailedWithMessageContains;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertParentRepairNotExist;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.getRepairExceptions;
import static org.apache.cassandra.utils.AssertUtil.assertTimeoutPreemptively;

public abstract class RepairCoordinatorNeighbourDown extends RepairCoordinatorBase
{
    public RepairCoordinatorNeighbourDown(RepairType repairType, RepairParallelism parallelism, boolean withNotifications)
    {
        super(repairType, parallelism, withNotifications);
    }

    @Before
    public void beforeTest()
    {
        CLUSTER.filters().reset();
        CLUSTER.forEach(i -> {
            try
            {
                i.startup();
            }
            catch (IllegalStateException e)
            {
                // ignore, node wasn't down
            }
        });
    }

    @Test
    public void neighbourDown()
    {
        String table = tableName("neighbourdown");
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
            String downNodeAddress = CLUSTER.get(2).callOnInstance(() -> FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort());
            Future<Void> shutdownFuture = CLUSTER.get(2).shutdown();
            try
            {
                // wait for the node to stop
                shutdownFuture.get();
                // wait for the failure detector to detect this
                CLUSTER.get(1).runOnInstance(() -> {
                    InetAddressAndPort neighbor;
                    try
                    {
                        neighbor = InetAddressAndPort.getByName(downNodeAddress);
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                    while (FailureDetector.instance.isAlive(neighbor))
                        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                });

                long repairExceptions = getRepairExceptions(CLUSTER, 1);
                NodeToolResult result = repair(1, KEYSPACE, table);
                result.asserts()
                      .failure()
                      .errorContains("Endpoint not alive");
                if (withNotifications)
                {
                    result.asserts()
                          .notificationContains(NodeToolResult.ProgressEventType.START, "Starting repair command")
                          .notificationContains(NodeToolResult.ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                          .notificationContains(NodeToolResult.ProgressEventType.ERROR, "Endpoint not alive")
                          .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
                }

                Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
            }
            finally
            {
                CLUSTER.get(2).startup();
            }

            // make sure to call outside of the try/finally so the node is up so we can actually query
            if (repairType != RepairType.PREVIEW)
            {
                assertParentRepairFailedWithMessageContains(CLUSTER, KEYSPACE, table, "Endpoint not alive");
            }
            else
            {
                assertParentRepairNotExist(CLUSTER, KEYSPACE, table);
            }
        });
    }

    @Test
    public void validationParticipentCrashesAndComesBack()
    {
        // Test what happens when a participant restarts in the middle of validation
        // Currently this isn't recoverable but could be.
        // TODO since this is a real restart, how would I test "long pause"? Can't send SIGSTOP since same procress
        String table = tableName("validationparticipentcrashesandcomesback");
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
            AtomicReference<Future<Void>> participantShutdown = new AtomicReference<>();
            CLUSTER.verbs(Verb.VALIDATION_REQ).to(2).messagesMatching(of(m -> {
                // the nice thing about this is that this lambda is "capturing" and not "transfer", what this means is that
                // this lambda isn't serialized and any object held isn't copied.
                participantShutdown.set(CLUSTER.get(2).shutdown());
                return true; // drop it so this node doesn't reply before shutdown.
            })).drop();
            // since nodetool is blocking, need to handle participantShutdown in the background
            CompletableFuture<Void> recovered = CompletableFuture.runAsync(() -> {
                try {
                    while (participantShutdown.get() == null) {
                        // event not happened, wait for it
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                    Future<Void> f = participantShutdown.get();
                    f.get(); // wait for shutdown to complete
                    CLUSTER.get(2).startup();
                } catch (Exception e) {
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    }
                    throw new RuntimeException(e);
                }
            });

            long repairExceptions = getRepairExceptions(CLUSTER, 1);
            NodeToolResult result = repair(1, KEYSPACE, table);
            recovered.join(); // if recovery didn't happen then the results are not what are being tested, so block here first
            result.asserts()
                  .failure()
                  .errorContains("/127.0.0.2:7012 died");
            if (withNotifications)
            {
                result.asserts()
                      .notificationContains(NodeToolResult.ProgressEventType.ERROR, "/127.0.0.2:7012 died")
                      .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
            }

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
            if (repairType != RepairType.PREVIEW)
            {
                assertParentRepairFailedWithMessageContains(CLUSTER, KEYSPACE, table, "/127.0.0.2:7012 died");
            }
            else
            {
                assertParentRepairNotExist(CLUSTER, KEYSPACE, table);
            }
        });
    }
}
