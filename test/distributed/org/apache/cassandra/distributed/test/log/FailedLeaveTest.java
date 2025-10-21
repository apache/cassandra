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

package org.apache.cassandra.distributed.test.log;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.ClusterUtils.SerializableBiPredicate;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.streaming.IncomingStream;
import org.apache.cassandra.streaming.StreamReceiveTask;
import org.apache.cassandra.tcm.Commit;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.transformations.CancelInProgressSequence;
import org.apache.cassandra.tcm.transformations.PrepareLeave;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.shared.ClusterUtils.cancelInProgressSequences;
import static org.apache.cassandra.distributed.shared.ClusterUtils.decommission;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getClusterMetadataVersion;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getSequenceAfterCommit;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class FailedLeaveTest extends FuzzTestBase
{
    private static int WRITES = 500;

    @Test
    public void resumeDecommissionWithStreamingFailureTest() throws Throwable
    {
        // After the leave operation fails (and we've re-enabled streaming), retry it
        // and wait for a FINISH_LEAVE event to be successfully committed.
        failedLeaveTest((ex, inst) -> ex.submit(() -> decommission(inst)),
                        (e, r) -> e instanceof PrepareLeave.FinishLeave && r.isSuccess());
    }

    @Test
    public void cancelDecommissionWithStreamingFailureTest() throws Throwable
    {
        // After the leave operation fails, cancel it and wait for a CANCEL_SEQUENCE event
        // to be successfully committed.
        failedLeaveTest((ex, inst) -> ex.submit(() -> cancelInProgressSequences(inst)),
                        (e, r) -> e instanceof CancelInProgressSequence && r.isSuccess());
    }

    private void failedLeaveTest(BiFunction<ExecutorService, IInvokableInstance, Future<Boolean>> runAfterFailure,
                                 SerializableBiPredicate<Transformation, Commit.Result> actionCommitted)
    throws Exception
    {
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(KEYSPACE, "failed_leave_test", 1000);
        try (Cluster cluster = builder().withNodes(3)
                                        .withInstanceInitializer(BB::install)
                                        .appendConfig(c -> c.with(Feature.NETWORK))
                                        .start())
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            IInvokableInstance leavingInstance = cluster.get(2);

            withRandom(rng -> {
                SchemaSpec schema = schemaGen.generate(rng);
                Generators.TrackingGenerator<Integer> pkGen = Generators.tracking(Generators.int32(0, Math.min(schema.valueGenerators.pkPopulation(), 1000)));
                Generator<Integer> ckGen = Generators.int32(0, Math.min(schema.valueGenerators.ckPopulation(), 1000));

                HistoryBuilder history = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                                     (hb) -> InJvmDTestVisitExecutor.builder()
                                                                                                    .nodeSelector(i -> 1)
                                                                                                    .build(schema, hb, cluster));
                history.custom(() -> {
                    cluster.schemaChange("CREATE KEYSPACE " + schema.keyspace +
                                         " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};");
                    cluster.schemaChange(schema.compile());
                }, "Setup");

                Runnable writeAndValidate = () -> {
                    for (int i = 0; i < WRITES; i++)
                        history.insert(pkGen.generate(rng), ckGen.generate(rng));

                    for (int pk : pkGen.generated())
                        history.selectPartition(pk);
                };
                writeAndValidate.run();

                history.customThrowing(() -> {
                    ExecutorService es = Executors.newSingleThreadExecutor();
                    Epoch startEpoch = getClusterMetadataVersion(cmsInstance);
                    // Configure node 3 to fail when receiving streams, then start decommissioning node 2
                    cluster.get(3).runOnInstance(() -> BB.failReceivingStream.set(true));
                    Future<Boolean> success = es.submit(() -> decommission(leavingInstance));
                    Assert.assertFalse(success.get());

                    // metadata event log should have advanced by 2 entries, PREPARE_LEAVE & START_LEAVE
                    ClusterUtils.waitForCMSToQuiesce(cluster, cmsInstance);
                    Epoch currentEpoch = getClusterMetadataVersion(cmsInstance);
                    Assert.assertEquals(startEpoch.getEpoch() + 2, currentEpoch.getEpoch());

                    // Node 2's leaving failed due to the streaming errors. If decommission is called again on the node, it should
                    // resume where it left off. Allow streaming to succeed this time and verify that the node is able to
                    // finish leaving.
                    cluster.get(3).runOnInstance(() -> BB.failReceivingStream.set(false));

                    // Run the desired action to mitigate the failure (i.e. retry or cancel)
                    success = runAfterFailure.apply(es, leavingInstance);

                    // get the Epoch of the event resulting from that action, so we can wait for it
                    Epoch nextEpoch = getSequenceAfterCommit(cmsInstance, actionCommitted).call();

                    Assert.assertTrue(success.get());

                    es.shutdown();
                    es.awaitTermination(1, TimeUnit.MINUTES);
                    // wait for the cluster to all witness the event submitted after failure
                    // (i.e. the FINISH_JOIN or CANCEL_SEQUENCE).
                    ClusterUtils.waitForCMSToQuiesce(cluster, nextEpoch);
                }, "Failed leave");

                writeAndValidate.run();
            });
        }
    }

    public static class BB
    {
        static AtomicBoolean failReceivingStream = new AtomicBoolean(false);

        public static void install(ClassLoader cl, int instance)
        {
            if (instance == 3)
            {
                new ByteBuddy().rebase(StreamReceiveTask.class)
                               .method(named("received"))
                               .intercept(MethodDelegation.to(BB.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static void received(IncomingStream stream, @SuperCall Callable<Void> zuper) throws Exception
        {
            if (failReceivingStream.get())
                throw new RuntimeException("XXX Stream receiving error");
            zuper.call();
        }
    }
}