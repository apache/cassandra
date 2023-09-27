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

package org.apache.cassandra.repair;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.RetrySpec;
import org.apache.cassandra.db.compaction.ICompactionManager;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.consistent.ConsistentSession;
import org.apache.cassandra.repair.consistent.LocalSession;
import org.apache.cassandra.repair.messages.ValidationRequest;
import org.apache.cassandra.repair.state.Completable;
import org.apache.cassandra.utils.Closeable;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

import static accord.utils.Property.qt;

public class FailedAckTest extends FuzzTestBase
{
    private enum RepairStage
    { PREPARE, VALIDATION } // SYNC doesn't have a good entry point for injecting the failure, if we win the race to register it, we accept right away
    
    @Test
    public void failedAck()
    {
        DatabaseDescriptor.getRepairRetrySpec().maxAttempts = new RetrySpec.MaxAttempt(Integer.MAX_VALUE);
        DatabaseDescriptor.setRepairPendingCompactionRejectThreshold(1);
        Gen<RepairStage> stageGen = Gens.enums().all(RepairStage.class);
        qt().withPure(false).withExamples(10).check(rs -> {
            Cluster cluster = new Cluster(rs);
            enableMessageFaults(cluster);

            Gen<Cluster.Node> coordinatorGen = Gens.pick(cluster.nodes.keySet()).map(cluster.nodes::get);

            List<Closeable> closeables = new ArrayList<>();
            for (int example = 0; example < 100; example++)
            {
                Cluster.Node coordinator = coordinatorGen.next(rs);

                RepairCoordinator repair = coordinator.repair(KEYSPACE, irOption(rs, coordinator, KEYSPACE, ignore -> TABLES), false);
                repair.run();
                // make sure the failing node is not the coordinator, else messaging isn't used
                InetAddressAndPort failingAddress = rs.pick(repair.state.getNeighborsAndRanges().participants);
                Cluster.Node failingNode = cluster.nodes.get(failingAddress);
                RepairStage stage = stageGen.next(rs);
                switch (stage)
                {
                    case PREPARE:
                    {
                        ICompactionManager cm = failingNode.compactionManager();
                        Mockito.when(cm.getPendingTasks()).thenReturn(42);
                        closeables.add(() -> Mockito.when(cm.getPendingTasks()).thenReturn(0));
                    }
                    break;
                    case VALIDATION:
                    {
                        cluster.addListener(new MessageListener() {
                            @Override
                            public void preHandle(Cluster.Node node, Message<?> msg)
                            {
                                if (node != failingNode) return;
                                if (msg.verb() != Verb.VALIDATION_REQ) return;
                                ValidationRequest req = (ValidationRequest) msg.payload;
                                if (rs.nextBoolean())
                                {
                                    // fail ctx.repair().consistent.local.maybeSetRepairing(desc.parentSessionId);
                                    LocalSession session = node.activeRepairService.consistent.local.getSession(req.desc.parentSessionId);
                                    session.setState(ConsistentSession.State.FAILED);
                                }
                                else
                                {
                                    // fail previewKind(desc.parentSessionId);
                                    node.activeRepairService.removeParentRepairSession(req.desc.parentSessionId);
                                }
                                cluster.removeListener(this);
                            }
                        });
                    }
                    break;
                    default:
                        throw new IllegalArgumentException("Unknown stage: " + stage);
                }

                cluster.processAll();
                Assertions.assertThat(repair.state.getResult().kind).describedAs("Unexpected state: %s -> %s; example %d", repair.state, repair.state.getResult(), example).isEqualTo(Completable.Result.Kind.FAILURE);
                switch (stage)
                {
                    case PREPARE:
                    {
                        Assertions.assertThat(repair.state.getResult().message)
                                  .describedAs("Unexpected state: %s -> %s; example %d", repair.state, repair.state.getResult(), example)
                                  .contains("Got negative replies from endpoints [" + failingAddress + "]");
                    }
                    break;
                    case VALIDATION:
                    {
                        Assertions.assertThat(repair.state.getResult().message)
                                  .describedAs("Unexpected state: %s -> %s; example %d", repair.state, repair.state.getResult(), example)
                                  .contains("Got VALIDATION_REQ failure from " + failingAddress + ": UNKNOWN");
                    }
                    break;
                    default:
                        throw new IllegalArgumentException("Unknown stage: " + stage);
                }
                closeables.forEach(Closeable::close);
                closeables.clear();
            }
        });
    }
}
