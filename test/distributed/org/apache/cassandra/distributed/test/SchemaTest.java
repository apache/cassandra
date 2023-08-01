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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;

import static java.time.Duration.ofSeconds;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;
import static org.junit.Assert.assertFalse;
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
            cluster.get(1).schemaChangeInternal("ALTER TABLE " + KEYSPACE + ".tbl ADD " + name + " list<int>");
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) values (?,1,1,1)", 1);
            selectSilent(cluster, name);

            cluster.get(2).flush(KEYSPACE);
            cluster.get(2).schemaChangeInternal("ALTER TABLE " + KEYSPACE + ".tbl ADD " + name + " list<int>");
            cluster.get(2).shutdown().get();
            cluster.get(2).startup();
            cluster.get(2).forceCompact(KEYSPACE, "tbl");
        }
    }

    @Test
    public void readRepairWithCompaction() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int,  primary key (pk, ck))");
            String name = "v10";
            cluster.get(1).schemaChangeInternal("ALTER TABLE " + KEYSPACE + ".tbl ADD " + name + " list<int>");
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) values (?,1,1,1)", 1);
            selectSilent(cluster, name);
            cluster.get(2).flush(KEYSPACE);
            cluster.get(2).schemaChangeInternal("ALTER TABLE " + KEYSPACE + ".tbl ADD " + name + " list<int>");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2, " + name + ") values (?,1,1,1,[1])", 1);
            cluster.get(2).flush(KEYSPACE);
            cluster.get(2).forceCompact(KEYSPACE, "tbl");
            cluster.get(2).shutdown().get();
            cluster.get(2).startup();
            cluster.get(2).forceCompact(KEYSPACE, "tbl");
        }
    }

    private void selectSilent(Cluster cluster, String name)
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

    /**
     * The purpose of this test is to verify manual schema reset functinality.
     * <p>
     * There is a 2-node cluster and a TABLE_ONE created. The schema version is agreed on both nodes. Then the 2nd node
     * is shutdown. We introduce a disagreement by dropping TABLE_ONE and creating TABLE_TWO on the 1st node. Therefore,
     * the 1st node has a newer schema version with TABLE_TWO, while the shutdown 2nd node has older schema version with
     * TABLE_ONE.
     * <p>
     * At this point, if we just started the 2nd node, it would sync its schema by getting fresh mutations from the 1st
     * node which would result in both nodes having only the definition of TABLE_TWO.
     * <p>
     * However, before starting the 2nd node the schema is reset on the 1st node, so the 1st node will discard its local
     * schema whenever it manages to fetch a schema definition from some other node (the 2nd node in this case).
     * It is expected to end up with both nodes having only the definition of TABLE_ONE.
     * <p>
     * In the second phase of the test we simply break the schema on the 1st node and call reset to fetch the schema
     * definition it from the 2nd node.
     */
    @Test
    public void schemaReset() throws Throwable
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

            // when there is no node to fetch the schema from, reset local schema should immediately fail
            Assertions.assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
                cluster.get(1).runOnInstance(() -> Schema.instance.resetLocalSchema());
            }).withMessageContaining("Cannot reset local schema when there are no other live nodes");

            // now, let's make a disagreement, the shutdown node 2 has a definition of TABLE_ONE, while the running
            // node 1 will have a definition of TABLE_TWO
            cluster.coordinator(1).execute(String.format("DROP TABLE %s.%s", KEYSPACE, TABLE_ONE), ConsistencyLevel.ONE);
            cluster.coordinator(1).execute(String.format("CREATE TABLE %s.%s (pk INT PRIMARY KEY, v TEXT)", KEYSPACE, TABLE_TWO), ConsistencyLevel.ONE);
            await(30).until(() -> checkTablesPropagated(cluster.get(1), false, true));

            // Schema.resetLocalSchema is guarded by some conditions which would not let us reset schema if there is no
            // live node in the cluster, therefore we simply call SchemaUpdateHandler.clear (this is the only real thing
            // being done by Schema.resetLocalSchema under the hood)
            SerializableCallable<Boolean> clear = () -> Schema.instance.updateHandler.clear().awaitUninterruptibly(1, TimeUnit.MINUTES);
            Future<Boolean> clear1 = cluster.get(1).asyncCallsOnInstance(clear).call();
            assertFalse(clear1.isDone());

            // when the 2nd node is started, schema should be back in sync
            cluster.get(2).startup();
            await(30).until(() -> clear1.isDone() && clear1.get());

            // this proves that reset schema works on the 1st node - the most recent change should be discarded because
            // it receives the schema from the 2nd node and applies it on empty schema
            await(60).until(() -> checkTablesPropagated(cluster.get(1), true, false));

            // now let's break schema locally and let it be reset
            cluster.get(1).runOnInstance(() -> Schema.instance.getLocalKeyspaces()
                                                              .get(SchemaConstants.SCHEMA_KEYSPACE_NAME)
                                                              .get().tables.forEach(t -> ColumnFamilyStore.getIfExists(t.keyspace, t.name).truncateBlockingWithoutSnapshot()));

            // when schema is removed and there is a node to fetch it from, the 1st node should immediately restore it
            cluster.get(1).runOnInstance(() -> Schema.instance.resetLocalSchema());
            // note that we should not wait for this to be true because resetLocalSchema is blocking
            // and after successfully completing it, the schema should be already back in sync
            assertTrue(checkTablesPropagated(cluster.get(1), true, false));
            assertTrue(checkTablesPropagated(cluster.get(2), true, false));
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
