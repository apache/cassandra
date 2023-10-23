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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.RetrySpec;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.state.Completable;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.Closeable;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class FailingRepairFuzzTest extends FuzzTestBase
{
    private enum RepairJobStage { VALIDATION, SYNC }

    @Test
    public void failingRepair()
    {
        // to avoid unlucky timing issues, retry until success; given enough retries we should eventually become success
        DatabaseDescriptor.getRepairRetrySpec().maxAttempts = new RetrySpec.MaxAttempt(Integer.MAX_VALUE);
        Gen<RepairJobStage> stageGen = Gens.enums().all(RepairJobStage.class);
        qt().withPure(false).withExamples(10).check(rs -> {
            Cluster cluster = new Cluster(rs);
            enableMessageFaults(cluster);

            Gen<Cluster.Node> coordinatorGen = Gens.pick(cluster.nodes.keySet()).map(cluster.nodes::get);

            List<Closeable> closeables = new ArrayList<>();
            for (int example = 0; example < 100; example++)
            {
                Cluster.Node coordinator = coordinatorGen.next(rs);

                RepairCoordinator repair = coordinator.repair(KEYSPACE, repairOption(rs, coordinator, KEYSPACE, TABLES), false);
                repair.run();
                InetAddressAndPort failingAddress = pickParticipant(rs, coordinator, repair);
                Cluster.Node failingNode = cluster.nodes.get(failingAddress);
                RepairJobStage stage = stageGen.next(rs);
                // because of local syncs reaching out to the failing address, a different address may actually be what failed
                Set<InetAddressAndPort> syncFailedAddresses = new HashSet<>();
                switch (stage)
                {
                    case VALIDATION:
                    {
                        closeables.add(failingNode.doValidation((cfs, validator) -> {
                            long delayNanos = rs.nextLong(TimeUnit.MILLISECONDS.toNanos(5), TimeUnit.MINUTES.toNanos(1));
                            cluster.unorderedScheduled.schedule(() -> validator.fail(new SimulatedFault("Validation failed")), delayNanos, TimeUnit.NANOSECONDS);
                        }));
                    }
                    break;
                    case SYNC:
                    {
                        closeables.add(failingNode.doValidation((cfs, validator) -> addMismatch(rs, cfs, validator)));
                        List<InetAddressAndPort> addresses = ImmutableList.<InetAddressAndPort>builder().add(coordinator.addressAndPort).addAll(repair.state.getNeighborsAndRanges().participants).build();
                        for (InetAddressAndPort address : addresses)
                        {
                            closeables.add(cluster.nodes.get(address).doSync(plan -> {
                                long delayNanos = rs.nextLong(TimeUnit.SECONDS.toNanos(5), TimeUnit.MINUTES.toNanos(10));
                                cluster.unorderedScheduled.schedule(() -> {
                                    if (address == failingAddress || plan.getCoordinator().getPeers().contains(failingAddress))
                                    {
                                        syncFailedAddresses.add(address);
                                        SimulatedFault fault = new SimulatedFault("Sync failed");
                                        for (StreamEventHandler handler : plan.handlers())
                                            handler.onFailure(fault);
                                    }
                                    else
                                    {
                                        StreamState success = new StreamState(plan.planId(), plan.streamOperation(), Collections.emptySet());
                                        for (StreamEventHandler handler : plan.handlers())
                                            handler.onSuccess(success);
                                    }
                                }, delayNanos, TimeUnit.NANOSECONDS);
                                return null;
                            }));
                        }
                    }
                    break;
                    default:
                        throw new IllegalArgumentException("Unknown stage: " + stage);
                }

                cluster.processAll();
                Assertions.assertThat(repair.state.isComplete()).describedAs("Repair job did not complete, and no work is pending...").isTrue();
                Assertions.assertThat(repair.state.getResult().kind).describedAs("Unexpected state: %s -> %s; example %d", repair.state, repair.state.getResult(), example).isEqualTo(Completable.Result.Kind.FAILURE);
                switch (stage)
                {
                    case VALIDATION:
                    {
                        // Got VALIDATION_REQ failure from Ero2.MJ.N8kkw2.w3iFYdDw.HJiVYC32.mWb.b.xwi3tZ.s5k1l.mb.asTy_7QmQ.Q3.u.kjgh.GKjx.g1aKfkjB.YlyKg9.DyQszn7F.Ox2DMYIph.xlgH.EV.A9yEz2J.l6UHdC.C6FYLXE.J0CNHBH./[4905:e9f:2f00:e418:baac:b8d9:9ff9:6604]:33363: UNKNOWN"
                        Assertions.assertThat(repair.state.getResult().message)
                                  .describedAs("Unexpected state: %s -> %s; example %d", repair.state, repair.state.getResult(), example)
                                  // ValidationResponse with null tree seen
                                  .containsAnyOf("Validation failed in " + failingAddress,
                                                 // ack was dropped and on retry the participate detected dup so rejected as the task failed
                                                 "Got VALIDATION_REQ failure from " + failingAddress + ": UNKNOWN");
                    }
                    break;
                    case SYNC:
                        AbstractStringAssert<?> a = Assertions.assertThat(repair.state.getResult().message).describedAs("Unexpected state: %s -> %s; example %d", repair.state, repair.state.getResult(), example);
                        // SymmetricRemoteSyncTask + AsymmetricRemoteSyncTask
                        // ... Sync failed between /[81fc:714:2c56:a2d3:faf3:eb7c:e4dd:cb9e]:54401 and /220.3.10.72:21402
                        // LocalSyncTask
                        // ... failed with error Sync failed
                        // Dedup nack, but may be remote or local sync!
                        // ... Got SYNC_REQ failure from ...: UNKNOWN
                        String failingMsg = repair.state.getResult().message;
                        if (failingMsg.contains("Sync failed between"))
                        {
                            a.contains("Sync failed between").contains(failingAddress.toString());
                        }
                        else if (failingMsg.contains("Got SYNC_REQ failure from"))
                        {
                            Assertions.assertThat(syncFailedAddresses).isNotEmpty();
                            a.containsAnyOf(syncFailedAddresses.stream().map(s -> "Got SYNC_REQ failure from " + s + ": UNKNOWN").collect(Collectors.toList()).toArray(String[]::new));
                        }
                        else
                        {
                            a.contains("failed with error Sync failed");
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
