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

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.execution.RingAwareInJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.model.TokenPlacementModel;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.PrepareJoin;

import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getClusterMetadataVersion;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getSequenceAfterCommit;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class ResumableStartupTest extends FuzzTestBase
{
    private static final String KS = "resumable_startup_test";
    private static int WRITES = 500;

    @Test
    public void bootstrapWithDeferredJoinTest() throws Throwable
    {
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(KS, "bootstrap_with_deferred_join", 1000);
        try (Cluster cluster = builder().withNodes(1)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                        .appendConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP))
                                        .createWithoutStarting())
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            cmsInstance.config().set("auto_bootstrap", true);
            cmsInstance.startup();

            withRandom(rng -> {
                SchemaSpec schema = schemaGen.generate(rng);
                Generators.TrackingGenerator<Integer> pkGen = Generators.tracking(Generators.int32(0, Math.min(schema.valueGenerators.pkPopulation(), 1000)));
                Generator<Integer> ckGen = Generators.int32(0, Math.min(schema.valueGenerators.ckPopulation(), 1000));

                HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);
                history.customThrowing(() -> {
                    cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};", schema.keyspace));
                    cluster.schemaChange(schema.compile());
                    ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
                }, "Setup");

                Runnable writeAndValidate = () -> {
                    for (int i = 0; i < WRITES; i++)
                        history.insert(pkGen.generate(rng), ckGen.generate(rng));

                    for (int pk : pkGen.generated())
                        history.selectPartition(pk);
                };
                writeAndValidate.run();

                // First write with ONE, as we only have 1 node
                TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(1);
                DataTracker tracker = new DataTracker.SequentialDataTracker();
                QuiescentChecker checker = new QuiescentChecker(schema.valueGenerators, tracker, history);

                RingAwareInJvmDTestVisitExecutor executor;
                // RF is ONE here since we have no pending nodes
                executor = RingAwareInJvmDTestVisitExecutor.builder()
                                                           .replicationFactor(rf)
                                                           .consistencyLevel(ConsistencyLevel.ONE)
                                                           .build(schema, tracker, checker, cluster);
                Iterator<Visit> iterator = history.iterator();
                while (iterator.hasNext())
                    executor.execute(iterator.next());

                AtomicReference<IInvokableInstance> newInstance = new AtomicReference<>();
                history.customThrowing(() -> {
                    IInstanceConfig config = cluster.newInstanceConfig()
                                                    .set("auto_bootstrap", true)
                                                    .set(Constants.KEY_DTEST_FULL_STARTUP, true);
                    newInstance.set(cluster.bootstrap(config));

                    withProperty(CassandraRelevantProperties.TEST_WRITE_SURVEY, true, newInstance.get()::startup);
                }, "Bootstrap");

                writeAndValidate.run();

                rf = new TokenPlacementModel.SimpleReplicationFactor(2);
                // RF is ONE here since we have 1 regular and 1 pending node, but want to check for RF2
                executor = RingAwareInJvmDTestVisitExecutor.builder()
                                                           .replicationFactor(rf)
                                                           .consistencyLevel(ConsistencyLevel.ONE)
                                                           .build(schema, tracker, checker, cluster);
                while (iterator.hasNext())
                    executor.execute(iterator.next());

                history.customThrowing(() -> {
                    Epoch currentEpoch = getClusterMetadataVersion(cmsInstance);
                    // Quick check that schema changes are possible with nodes in write survey mode (i.e. with ranges locked)
                    cluster.schemaChange(String.format("ALTER TABLE %s.%s WITH comment = 'Schema alterations which do not affect placements should not be restricted by in flight operations';", schema.keyspace, schema.table));

                    final String newAddress = ClusterUtils.getBroadcastAddressHostWithPortString(newInstance.get());
                    final String keyspace = schema.keyspace;
                    boolean newReplicaInCorrectState = cluster.get(1).callOnInstance(() -> {
                        ClusterMetadata metadata = ClusterMetadata.current();
                        KeyspaceMetadata ksm = metadata.schema.getKeyspaceMetadata(keyspace);
                        boolean isWriteReplica = false;
                        boolean isReadReplica = false;
                        for (InetAddressAndPort readReplica : metadata.placements.get(ksm.params.replication).reads.byEndpoint().keySet())
                        {
                            if (readReplica.getHostAddressAndPort().equals(newAddress))
                                isReadReplica = true;
                        }
                        for (InetAddressAndPort writeReplica : metadata.placements.get(ksm.params.replication).writes.byEndpoint().keySet())
                        {
                            if (writeReplica.getHostAddressAndPort().equals(newAddress))
                                isWriteReplica = true;
                        }
                        return (isWriteReplica && !isReadReplica);
                    });
                    Assert.assertTrue("Expected new instance to be a write replica only", newReplicaInCorrectState);

                    Callable<Epoch> finishedBootstrap = getSequenceAfterCommit(cmsInstance, (e, r) -> e instanceof PrepareJoin.FinishJoin && r.isSuccess());
                    newInstance.get().runOnInstance(() -> {
                        try
                        {
                            StorageService.instance.joinRing();
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException("Error joining ring", e);
                        }
                    });
                    Epoch next = finishedBootstrap.call();
                    Assert.assertEquals(String.format("Expected epoch after schema change, mid join & finish join to be %s, but was %s",
                                                      next.getEpoch(), currentEpoch.getEpoch() + 3),
                                        next.getEpoch(), currentEpoch.getEpoch() + 3);

                }, "Finish bootstrap");

                writeAndValidate.run();

                while (iterator.hasNext())
                    executor.execute(iterator.next());

            });
        }
    }
}