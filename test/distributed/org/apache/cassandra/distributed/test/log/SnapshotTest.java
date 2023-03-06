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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class SnapshotTest extends TestBaseImpl
{
    @Test
    public void testSimpleSnapshot() throws Throwable
    {
        try (Cluster cluster = init(builder().withDC("DC1", 3)
                                             .withDC("DC2", 3)
                                             .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                         .set("user_defined_functions_enabled", "true")
                                                                         .set("materialized_views_enabled", "true"))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key, x int)"));
            cluster.schemaChange(withKeyspace("CREATE OR REPLACE FUNCTION %s.fLog (input double) CALLED ON NULL INPUT RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.log(input.doubleValue()));';"));
            cluster.schemaChange(withKeyspace("CREATE OR REPLACE FUNCTION %s.avgState ( state tuple<int,bigint>, val int ) CALLED ON NULL INPUT RETURNS tuple<int,bigint> LANGUAGE java AS \n" +
                                              "  'if (val !=null) { state.setInt(0, state.getInt(0)+1); state.setLong(1, state.getLong(1)+val.intValue()); } return state;'; "));
            cluster.schemaChange(withKeyspace("CREATE OR REPLACE FUNCTION %s.avgFinal ( state tuple<int,bigint> ) CALLED ON NULL INPUT RETURNS double LANGUAGE java AS \n" +
                                              "  'double r = 0; if (state.getInt(0) == 0) return null; r = state.getLong(1); r/= state.getInt(0); return Double.valueOf(r);';"));
            cluster.schemaChange(withKeyspace("CREATE AGGREGATE IF NOT EXISTS %s.average ( int ) \n" +
                                 "SFUNC avgState STYPE tuple<int,bigint> FINALFUNC avgFinal INITCOND (0,0);"));
            cluster.schemaChange(withKeyspace("CREATE MATERIALIZED VIEW %s.test_mv \n" +
                                              "AS SELECT x, id FROM distributed_test_keyspace.tbl \n" +
                                              "WHERE id IS NOT NULL AND x IS NOT NULL\n" +
                                              "PRIMARY KEY (x, id)"));

            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata before = ClusterMetadata.current();
                ClusterMetadata after = ClusterMetadataService.instance().sealPeriod();
                ClusterMetadata serialized = ClusterMetadataService.instance().snapshotManager().getSnapshot(after.epoch);
                assertEquals(before.placements, serialized.placements);
                assertEquals(before.tokenMap, serialized.tokenMap);
                assertEquals(before.directory, serialized.directory);
                assertEquals(before.schema, serialized.schema);
            });

            cluster.schemaChange(withKeyspace("create table %s.tbl2 (id int primary key, x int)"));
            cluster.get(1).runOnInstance(() -> {
                assertEquals(2, ClusterMetadata.current().period);
            });
        }
    }

    @Test
    public void testSnapshotReplayBootstrap() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2)
                                             .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                             .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                             .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                         .set("user_defined_functions_enabled", "true")
                                                                         .set("materialized_views_enabled", "true"))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl1 (id int primary key, x int)"));

            cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().sealPeriod());

            cluster.schemaChange(withKeyspace("create table %s.tbl2 (id int primary key, x int)"));
            cluster.schemaChange(withKeyspace("create table %s.tbl3 (id int primary key, x int)"));

            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            for (int i = 1; i <= 2; i++)
                cluster.get(i).runOnInstance(() -> { assertEquals(2, ClusterMetadata.current().period); });


            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("auto_bootstrap", true)
                                            .set(Constants.KEY_DTEST_FULL_STARTUP, true);
            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup();

            cluster.schemaChange(withKeyspace("create table %s.tbl4 (id int primary key, x int)"));
            cluster.schemaChange(withKeyspace("create table %s.tbl5 (id int primary key, x int)"));

            cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().sealPeriod());

            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).runOnInstance(() -> {
                    // no events executed after NewPeriod, period is still 2
                    assertEquals(2, ClusterMetadata.current().period);
                    // but the next one is 3
                    assertEquals(true, ClusterMetadata.current().lastInPeriod);
                });
            }

            config = cluster.newInstanceConfig()
                            .set("auto_bootstrap", true)
                            .set(Constants.KEY_DTEST_FULL_STARTUP, true);
            newInstance = cluster.bootstrap(config);
            newInstance.startup();

            cluster.schemaChange(withKeyspace("create table %s.tbl6 (id int primary key, x int)"));
            cluster.schemaChange(withKeyspace("create table %s.tbl7 (id int primary key, x int)"));

            // make sure all tables exist
            for (int tbl = 1; tbl <= 7; tbl++)
                for (int i = 1; i <= cluster.size(); i++)
                    cluster.get(i).executeInternal(withKeyspace("insert into %s.tbl"+tbl+" (id, x) values (1,1)"));
        }
    }

    @Test
    public void testSnapshotReplayNonBootstrap() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(config -> config.with(NETWORK, GOSSIP))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl1 (id int primary key, x int)"));

            cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().sealPeriod());

            cluster.schemaChange(withKeyspace("create table %s.tbl2 (id int primary key, x int)"));
            cluster.schemaChange(withKeyspace("create table %s.tbl3 (id int primary key, x int)"));

            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            for (int i = 1; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> { assertEquals(2, ClusterMetadata.current().period); });

            cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().sealPeriod());
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            long epochBefore = ClusterUtils.getCurrentEpoch(cluster.get(1)).getEpoch();

            // isolate node3, perform some metadata changes and have it catch up
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();

            cluster.get(1).runOnInstance(() -> {
                ClusterMetadataService.instance().commit(CustomTransformation.make(1));
                ClusterMetadataService.instance().commit(CustomTransformation.make(2));
            });

            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1), 3);
            long epochAfter = epochBefore + 2;
            for (int i = 1; i <= 2; i++)
                cluster.get(i).runOnInstance(() -> { assertEquals(epochAfter, ClusterMetadata.current().epoch.getEpoch()); });

            cluster.filters().reset();
            cluster.get(3).runOnInstance(() -> {
                long beforeCatchup = ClusterMetadata.current().epoch.getEpoch();
                assertEquals(epochBefore, beforeCatchup);
                long afterCatchup = ClusterMetadataService.instance().replayAndWait().epoch.getEpoch();
                assertEquals(epochAfter, afterCatchup);
            });

            cluster.schemaChange(withKeyspace("create table %s.tbl4 (id int primary key, x int)"));
            cluster.schemaChange(withKeyspace("create table %s.tbl5 (id int primary key, x int)"));
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));

            // make sure all tables exist
            for (int tbl = 1; tbl <= 5; tbl++)
                for (int i = 1; i <= cluster.size(); i++)
                    cluster.get(i).executeInternal(withKeyspace("insert into %s.tbl"+tbl+" (id, x) values (1,1)"));
        }
    }

    @Test
    public void testSnapshotReplayNonBootstrapImmediatelyAfterSnapshot() throws Throwable
    {
        // the intention here is to observe (not actually assert anything yet) a replay where the
        // requester hasn't missed any epochs *and* the last log entry sealed the period, creating
        // a snapshot. We should ensure that the snapshot isn't unnecessarily sent by the CMS.
        // TODO don't use snapshots during regular (non-startup) catchup at all, ensure peers
        //      see all individual log entries.
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(config -> config.with(NETWORK, GOSSIP))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl1 (id int primary key, x int)"));

            cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().sealPeriod());

            cluster.schemaChange(withKeyspace("create table %s.tbl2 (id int primary key, x int)"));
            cluster.schemaChange(withKeyspace("create table %s.tbl3 (id int primary key, x int)"));

            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            for (int i = 1; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> { assertEquals(2, ClusterMetadata.current().period); });

            cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().sealPeriod());
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            long epochBefore = ClusterUtils.getCurrentEpoch(cluster.get(1)).getEpoch();

            cluster.get(3).runOnInstance(() -> {
                long beforeCatchup = ClusterMetadata.current().epoch.getEpoch();
                assertEquals(epochBefore, beforeCatchup);
                // TODO assert that this is a no-op and that the CMS doesn't actually send a snapshot
                //      just for the requester to drop it as the entry is already in the log
                long afterCatchup = ClusterMetadataService.instance().replayAndWait().epoch.getEpoch();
                assertEquals(epochBefore, afterCatchup);
            });

            cluster.schemaChange(withKeyspace("create table %s.tbl4 (id int primary key, x int)"));
            cluster.schemaChange(withKeyspace("create table %s.tbl5 (id int primary key, x int)"));
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));

            // make sure all tables exist
            for (int tbl = 1; tbl <= 5; tbl++)
                for (int i = 1; i <= cluster.size(); i++)
                    cluster.get(i).executeInternal(withKeyspace("insert into %s.tbl"+tbl+" (id, x) values (1,1)"));
        }
    }

}
