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

package org.apache.cassandra.distributed.test.accord.journal;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.durability.DurabilityService;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.utils.Property;
import accord.utils.Property.SimpleCommand;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.service.accord.AccordKeyspace.JOURNAL;

/**
 * There are 2 errors blocking this test from being run
 * <p>
 * <pre>
 * INFO  [node1_AccordExecutor[0,13]] 2025-05-27 16:02:51,792 SubstituteLogger.java:169 - ERROR 23:02:51 Uncaught accord exception
 * java.lang.NullPointerException: Cannot invoke "accord.topology.Topology.nodes()" because "topology" is null
 * 	at org.apache.cassandra.service.accord.AccordConfigurationService.reportEpochClosed(AccordConfigurationService.java:490)
 * 	at accord.coordinate.CoordinationAdapter$Adapters$ExclusiveSyncPointAdapter.invokeSuccess(CoordinationAdapter.java:351)
 * 	at accord.coordinate.CoordinationAdapter$Adapters$SyncPointAdapter.persist(CoordinationAdapter.java:282)
 * 	at accord.coordinate.CoordinationAdapter.persist(CoordinationAdapter.java:80)
 * 	at accord.coordinate.CoordinationAdapter$Adapters$SyncPointAdapter.execute(CoordinationAdapter.java:274)
 * 	at accord.coordinate.CoordinationAdapter$Adapters$AbstractExclusiveSyncPointAdapter.execute(CoordinationAdapter.java:313)
 * 	at accord.coordinate.Propose.onAccepted(Propose.java:199)
 * 	at accord.coordinate.Propose.onSuccess(Propose.java:150)
 * </pre>
 * <p>
 * and
 * <p>
 * <pre>
 * INFO  [node1_isolatedExecutor:1] 2025-05-27 15:38:06,880 SubstituteLogger.java:169 - ERROR 22:38:06 Exiting due to error while processing journal AccordJournal during initialization.
 * java.lang.IllegalStateException: duplicate key detected [6,1748385477237760,151(RX),1] == [6,1748385477237760,151(RX),1]
 * 	at accord.utils.Invariants.createIllegalState(Invariants.java:77)
 * 	at accord.utils.Invariants.illegalState(Invariants.java:82)
 * 	at accord.utils.Invariants.require(Invariants.java:272)
 * 	at org.apache.cassandra.service.accord.AccordJournal.replay(AccordJournal.java:452)
 * 	at org.apache.cassandra.service.accord.AccordService.replayJournal(AccordService.java:246)
 * 	at org.apache.cassandra.service.accord.AccordService.startup(AccordService.java:235)
 * 	at org.apache.cassandra.distributed.impl.Instance.partialStartup(Instance.java:878)
 * </pre>
 */
@Ignore("Unstable, need to fix")
public class StatefulJournalRestartTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(StatefulJournalRestartTest.class);

    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = Cluster.build(1).withInstanceInitializer(JournalAccessRouteIndexOnStartupRaceTest.BBHelper::install).start())
        {
            stateful().withSeed(42).withExamples(2).withSteps(10).withStepTimeout(Duration.ofMinutes(1))
                      .check(commands(() -> ignore -> setup(cluster))
                             .add(new SimpleCommand<>("Insert Txn", StatefulJournalRestartTest::insert))
                             .add(new SimpleCommand<>("Restart", ClusterUtils::restartUnchecked))
                             .add(new SimpleCommand<>("Restart with race", StatefulJournalRestartTest::restartWithRace))
                             .onSuccess((state, sut, history) -> logger.info("Successful for the following:\nState {}\nHistory:\n{}", state, Property.formatList("\t\t", history)))
                             .destroyState(ClusterUtils::cleanup)
                             .build());
        }
    }

    private static IInvokableInstance setup(Cluster cluster)
    {
        IInvokableInstance node = cluster.get(1);
        node.nodetoolResult("disableautocompaction", ACCORD_KEYSPACE_NAME, JOURNAL).asserts().success();
        init(cluster);
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl(pk int primary key) WITH " + TransactionalMode.full.asCqlParam()));
        ClusterUtils.awaitAccordEpochReady(cluster, ClusterUtils.getCurrentEpoch(node).getEpoch());
        return node;
    }

    private static void insert(IInvokableInstance node)
    {
        String ks = KEYSPACE;
        String table = "tbl";
        node.runOnInstance(() -> {
            AccordService accord = (AccordService) AccordService.instance();
            TableMetadata metadata = Keyspace.open(ks).getColumnFamilyStore(table).metadata();
            Ranges ranges = Ranges.single(TokenRange.fullRange(metadata.id, metadata.partitioner));
            for (int i = 0; i < 10; i++)
            {
                AsyncChains.getBlockingAndRethrow(accord.sync(null, Timestamp.NONE, ranges, null, DurabilityService.SyncLocal.Self, DurabilityService.SyncRemote.Quorum, 10L, TimeUnit.MINUTES));

                accord.journal().closeCurrentSegmentForTestingIfNonEmpty();
                accord.journal().runCompactorForTesting();
            }
        });
    }

    private static void restartWithRace(IInvokableInstance node)
    {
        logger.info("Restarting instance with blocked 2i, triggering race condition");
        ClusterUtils.stopUnchecked(node);
        JournalAccessRouteIndexOnStartupRaceTest.State.block();
        node.startup();
    }
}
