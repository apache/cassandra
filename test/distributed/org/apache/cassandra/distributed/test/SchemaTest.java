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

import java.util.concurrent.Callable;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.AlterSchema;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;

import static java.time.Duration.ofSeconds;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseAfterEnacting;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeEnacting;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseCommits;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseEnactment;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;
import static org.junit.Assert.assertTrue;

public class SchemaTest extends TestBaseImpl
{
    public static final String TABLE_ONE = "tbl_one";
    public static final String TABLE_TWO = "tbl_two";

    @Test
    public void readRepair() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int,  primary key (pk, ck))");
            String name = "aaa";
            // have the CMS node pause directly before committing the ALTER TABLE so we can infer the next epoch
            Callable<Epoch> beforeCommit = pauseBeforeCommit(cluster.get(1), (e) -> e instanceof AlterSchema);
            new Thread(() -> {
                cluster.get(1).schemaChangeInternal("ALTER TABLE " + KEYSPACE + ".tbl ADD " + name + " list<int>");
                cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) values (?,1,1,1)", 1);
            }).start();

            Epoch targetEpoch = beforeCommit.call().nextEpoch();
            // pause the replica immediately before and after enacting the ALTER TABLE stmt
            Callable<?> beforeEnactedOnReplica = pauseBeforeEnacting(cluster.get(2), targetEpoch);
            Callable<?> afterEnactedOnReplica = pauseAfterEnacting(cluster.get(2), targetEpoch);
            // unpause the CMS node and allow it to commit and replicate the ALTER TABLE
            unpauseCommits(cluster.get(1));

            // Wait for the replica to signal that it has paused before enacting the schema change
            // then execute the query and assert that a schema disagreement error was triggered
            beforeEnactedOnReplica.call();
            selectExpectingError(cluster, name);

            // unpause the replica and wait until it notifies that it has enacted the schema change
            unpauseEnactment(cluster.get(2));
            afterEnactedOnReplica.call();
            unpauseEnactment(cluster.get(2));

            cluster.get(2).flush(KEYSPACE);
            // now that the replica has enacted the alter table, an attempt to repeat it should be rejected
            alterTableExpectingError(cluster.get(2), name);
            // bouncing the replica should be safe as SSTables aren't loaded until the log replay is complete
            // and the schema is in it's most up to date state
            cluster.get(2).shutdown().get();
            cluster.get(2).startup();
            cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"), ConsistencyLevel.ALL, 1);
            cluster.get(2).forceCompact(KEYSPACE, "tbl");
            cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"), ConsistencyLevel.ALL, 1);
        }
    }

    @Test
    public void readRepairWithCompaction() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int,  primary key (pk, ck))");
            String name = "v10";

            // have the CMS node pause directly before committing the ALTER TABLE so we can infer the next epoch
            Callable<Epoch> beforeCommit = pauseBeforeCommit(cluster.get(1), (e) -> e instanceof AlterSchema);
            new Thread(() -> {
                cluster.get(1).schemaChangeInternal("ALTER TABLE " + KEYSPACE + ".tbl ADD " + name + " list<int>");
                cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) values (?,1,1,1)", 1);
            }).start();
            Epoch targetEpoch = beforeCommit.call().nextEpoch();

            // pause the replica immediately before and after enacting the ALTER TABLE stmt
            Callable<?> beforeEnactedOnReplica = pauseBeforeEnacting(cluster.get(2), targetEpoch);
            Callable<?> afterEnactedOnReplica = pauseAfterEnacting(cluster.get(2), targetEpoch);
            // unpause the CMS node and allow it to commit and replicate the ALTER TABLE
            unpauseCommits(cluster.get(1));

            // Wait for the replica to signal that it has paused before enacting the schema change
            // then execute the query and assert that a schema disagreement error was triggered
            beforeEnactedOnReplica.call();
            selectExpectingError(cluster, name);

            // unpause the replica and wait until it notifies that it has enacted the schema change
            unpauseEnactment(cluster.get(2));
            afterEnactedOnReplica.call();
            unpauseEnactment(cluster.get(2));

            cluster.get(2).flush(KEYSPACE);
            // now that the replica has enacted the alter table, an attempt to repeat it should be rejected
            alterTableExpectingError(cluster.get(2), name);
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2, " + name + ") values (?,1,1,1,[1])", 1);
            cluster.get(2).flush(KEYSPACE);
            cluster.get(2).forceCompact(KEYSPACE, "tbl");

            // bouncing the replica should be safe as SSTables aren't loaded until the log replay is complete
            // and the schema is in it's most up to date state
            cluster.get(2).shutdown().get();
            cluster.get(2).startup();

            cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"), ConsistencyLevel.ALL, 1);
            cluster.get(2).forceCompact(KEYSPACE, "tbl");
            cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"), ConsistencyLevel.ALL, 1);
        }
    }

    private void selectExpectingError(Cluster cluster, String name)
    {
        try
        {
            cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"), ConsistencyLevel.ALL, 1);
        }
        catch (Exception e)
        {
            boolean causeIsUnknownColumn = false;
            Throwable cause = e;
            while (cause != null)
            {
                if (cause.getMessage() != null && cause.getMessage().contains("Unknown column " + name + " during deserialization"))
                    causeIsUnknownColumn = true;
                cause = cause.getCause();
            }
            assertTrue(causeIsUnknownColumn);
        }
    }

    private void alterTableExpectingError(IInvokableInstance instance, String name)
    {
        try
        {
            instance.schemaChangeInternal("ALTER TABLE " + KEYSPACE + ".tbl ADD " + name + " list<int>");
        }
        catch (Exception e)
        {
            boolean causeIsColumnExists = false;
            Throwable cause = e;
            while (cause != null)
            {
                if (cause.getMessage() != null && cause.getMessage().contains("Column with name '" + name + "' already exists"))
                    causeIsColumnExists = true;
                cause = cause.getCause();
            }
            assertTrue(causeIsColumnExists);
        }
    }

    /**
     * The original purpose of this test was to verify manual schema reset functionality, but with schema updates being
     * serialized in the cluster metadata log local schema reset no longer makes sense so the assertions have been
     * modified to verify that schema changes are correctly propagated to down nodes once they come back up.
     * <p>
     * There is a 2-node cluster and a TABLE_ONE created. The schema version is agreed on both nodes. Then the 2nd node
     * is shutdown. We introduce a disagreement by dropping TABLE_ONE and creating TABLE_TWO on the 1st node. Therefore,
     * the 1st node has a newer schema version with TABLE_TWO, while the shutdown 2nd node has older schema version with
     * TABLE_ONE.
     * <p>
     * At this point, if we just start the 2nd node, it would sync its schema by getting the transformations that it
     * missed while down, which would result in both nodes having only the definition of TABLE_TWO.
     * <p>
     */
    @Test
    public void schemaPropagationToDownNode() throws Throwable
    {
        CassandraRelevantProperties.MIGRATION_DELAY.setLong(10000);
        CassandraRelevantProperties.SCHEMA_PULL_INTERVAL_MS.setLong(10000);
        try (Cluster cluster = init(Cluster.build(2).withConfig(cfg -> cfg.with(Feature.GOSSIP, Feature.NETWORK)).start()))
        {
            // create TABLE_ONE and make sure it is propagated
            cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk INT PRIMARY KEY, v TEXT)", KEYSPACE, TABLE_ONE));
            assertTrue(checkTablesPropagated(cluster.get(1), true, false));
            assertTrue(checkTablesPropagated(cluster.get(2), true, false));

            // shutdown the 2nd node and make sure that the 1st does not see it any longer as alive
            cluster.get(2).shutdown().get();
            await(30).until(() -> cluster.get(1).callOnInstance(() -> {
                return Gossiper.instance.getLiveMembers()
                                        .stream()
                                        .allMatch(e -> e.equals(getBroadcastAddressAndPort()));
            }));

            // now, let's make a disagreement, the shutdown node 2 has a definition of TABLE_ONE, while the running
            // node 1 will have a definition of TABLE_TWO
            cluster.coordinator(1).execute(String.format("DROP TABLE %s.%s", KEYSPACE, TABLE_ONE), ConsistencyLevel.ONE);
            cluster.coordinator(1).execute(String.format("CREATE TABLE %s.%s (pk INT PRIMARY KEY, v TEXT)", KEYSPACE, TABLE_TWO), ConsistencyLevel.ONE);
            await(30).until(() -> checkTablesPropagated(cluster.get(1), false, true));

            // when the 2nd node is started, schema should be back in sync
            cluster.get(2).startup();
            assertTrue(checkTablesPropagated(cluster.get(1), false, true));
            assertTrue(checkTablesPropagated(cluster.get(2), false, true));
        }
    }

    private static ConditionFactory await(int seconds)
    {
        return Awaitility.await().atMost(ofSeconds(seconds)).pollDelay(ofSeconds(1));
    }

    private static boolean checkTablesPropagated(IInvokableInstance instance, boolean one, boolean two)
    {
        return instance.callOnInstance(() -> {
            return (Schema.instance.getTableMetadata(KEYSPACE, TABLE_ONE) != null ^ !one)
                   && (Schema.instance.getTableMetadata(KEYSPACE, TABLE_TWO) != null ^ !two);
        });
    }
}
