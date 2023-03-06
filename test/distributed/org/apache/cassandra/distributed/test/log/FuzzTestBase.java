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


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.junit.BeforeClass;

import harry.core.Run;
import harry.data.ResultSetRow;
import harry.model.OpSelectors;
import harry.model.QuiescentChecker;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;
import harry.operations.Query;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.fuzz.HarryHelper;
import org.apache.cassandra.distributed.fuzz.InJvmSut;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.ExecUtil;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.tcm.Commit;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static harry.model.SelectHelper.resultSetToRow;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class FuzzTestBase extends TestBaseImpl
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        TestBaseImpl.beforeClass();
        HarryHelper.init();
    }

    @Override
    public Cluster.Builder builder() {
        return super.builder()
                    .withConfig(cfg -> cfg.with(GOSSIP, NETWORK)
                                          // Since we'll be pausing the commit request, it may happen that it won't get
                                          // unpaused before event expiration.
                               .set("request_timeout", String.format("%dms", TimeUnit.MINUTES.toMillis(10))));
    }

    protected static Callable<Void> pauseBeforeCommit(IInvokableInstance cmsInstance, SerializablePredicate<Transformation> predicate)
    {
        return cmsInstance.callOnInstance(() -> {
            TestProcessor processor = (TestProcessor) ((ClusterMetadataService.SwitchableProcessor) ClusterMetadataService.instance().processor()).delegate();
            AsyncPromise<?> promise = new AsyncPromise<>();
            processor.pauseIf(predicate, () -> promise.setSuccess(null));
            return () -> {
                try
                {
                    promise.get(30, TimeUnit.SECONDS);
                    return null;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            };
        });
    }

    // todo; assumes period = 1
    protected static Callable<Epoch> getSequenceAfterCommit(IInvokableInstance cmsInstance,
                                                            SerializableBiPredicate<Transformation, Commit.Result> predicate)
    {
        Callable<Long> remoteCallable = cmsInstance.callOnInstance(() -> {
            TestProcessor processor = (TestProcessor) ((ClusterMetadataService.SwitchableProcessor) ClusterMetadataService.instance().processor()).delegate();

            AsyncPromise<Epoch> promise = new AsyncPromise<>();
            processor.registerCommitPredicate((event, result) -> {
                if (predicate.test(event, result))
                {
                    promise.setSuccess(result.success().replication.latestEpoch());
                    return true;
                }

                return false;
            });
            return () -> {
                try
                {
                    return ClusterUtils.encode(promise.get(30, TimeUnit.SECONDS));
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            };
        });

        return () -> ClusterUtils.decode(remoteCallable.call());
    }

    public static IIsolatedExecutor.SerializableRunnable toRunnable(ExecUtil.ThrowingSerializableRunnable runnable)
    {
        return () -> {
            try
            {
                runnable.run();
            }
            catch (Throwable t)
            {
                System.out.println(t.getMessage());
                t.printStackTrace();
            }
        };
    }

    public static class QuiescentLocalStateChecker extends QuiescentChecker
    {
        public final InJvmSut inJvmSut;
        public final ReplicationFactor rf;
        private final OpSelectors.PdSelector pdSelector;

        public QuiescentLocalStateChecker(Run run)
        {
            this(run, null);
        }

        public QuiescentLocalStateChecker(Run run, ReplicationFactor rf)
        {
            super(run);
            assert run.sut instanceof InJvmSut;

            this.inJvmSut = (InJvmSut) run.sut;
            this.rf = rf;
            this.pdSelector = run.pdSelector;
        }

        public void validateAll()
        {
            for (int lts = 0; lts < clock.peek(); lts++)
                validate(Query.selectPartition(schema, pdSelector.pd(lts, schema), false));
        }

        @Override
        public void validate(Query query)
        {
            CompiledStatement compiled = query.toSelectStatement();
            int[] replicas = inJvmSut.getReadReplicasFor(schema.inflatePartitionKey(query.pd), schema.keyspace, schema.table);
            if (rf != null && replicas.length != rf.allReplicas)
                throw new IllegalStateException(String.format("Total number of replicas %d does not match expectation %d",
                                                              replicas.length, rf.allReplicas));

            for (int node : replicas)
            {
                try
                {
                    validate(() -> {
                        Object[][] objects = inJvmSut.execute(compiled.cql(),
                                                              SystemUnderTest.ConsistencyLevel.NODE_LOCAL,
                                                              node,
                                                              compiled.bindings());
                        List<ResultSetRow> result = new ArrayList<>();
                        for (Object[] obj : objects)
                            result.add(resultSetToRow(query.schemaSpec, clock, obj));

                        return result;
                    }, query);
                }
                catch (ValidationException e)
                {
                    throw new AssertionError(String.format("Caught error while validating replica %d of replica set %s",
                                                           node, Arrays.toString(replicas)),
                                             e);
                }
            }
        }
    }

    public static interface SerializablePredicate<T> extends Predicate<T>, Serializable {}
    public static interface SerializableBiPredicate<T1, T2> extends BiPredicate<T1, T2>, Serializable {}
}
