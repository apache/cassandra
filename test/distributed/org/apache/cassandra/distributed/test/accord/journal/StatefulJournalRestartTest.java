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
                AsyncChains.getBlockingAndRethrow(accord.sync(null, Timestamp.NONE, ranges, null, DurabilityService.SyncLocal.Self, DurabilityService.SyncRemote.Quorum));

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
