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

import java.util.HashSet;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosCommit;
import org.apache.cassandra.service.paxos.PaxosPrepare;
import org.apache.cassandra.service.paxos.PaxosPropose;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.shared.NetworkTopology.singleDcNetworkTopology;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StrictMVConsistencyTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(StrictMVConsistencyTest.class);
    static String baseTableName = "base_tbl";
    static String MVName = "mv";

    @Test
    public void happyPathTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(evenlyDistributedTokens(3, 1))
                                        .withNodeIdTopology(singleDcNetworkTopology(3, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK).set("materialized_views_enabled", "true")
                                                                    .set("materialized_view_strict_consistency_enabled", "true")
                                                                    .set("paxos_variant", "v2"))
                                        .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc0': 3}"));
            cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH strict_mv_consistency = true;", KEYSPACE, baseTableName));
            cluster.schemaChange(String.format("CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE pk IS NOT NULL AND ck IS NOT NULL " +
                                               "AND v IS NOT NULL PRIMARY KEY (v, ck,pk)", KEYSPACE, MVName, KEYSPACE, baseTableName));

            populateRandomData(cluster, 10, 0, 0, false);
            verifyBaseTableAndMVInSync(cluster, 10);

            // test LWT is still working
            populateRandomData(cluster, 10, 20, 20, false, true);
            verifyBaseTableAndMVInSync(cluster, 20);
        }
    }

    @Test
    public void preparePhaseFailedTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(evenlyDistributedTokens(3, 1))
                                        .withNodeIdTopology(singleDcNetworkTopology(3, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK).set("materialized_views_enabled", "true")
                                                                    .set("materialized_view_strict_consistency_enabled", "true")
                                                                    .set("paxos_variant", "v2"))
                                        .withInstanceInitializer(BBPaxosPrepareRequestHandler::install)
                                        .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc0': 3}"));
            cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH strict_mv_consistency = true;", KEYSPACE, baseTableName));
            cluster.schemaChange(String.format("CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE pk IS NOT NULL AND ck IS NOT NULL " +
                                               "AND v IS NOT NULL PRIMARY KEY (v, ck,pk)", KEYSPACE, MVName, KEYSPACE, baseTableName));
            // blocking prepare, we will get timeout for base table update, making sure base table and MV is in sync
            // block prepare phase on 3 nodes
            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).runOnInstance(
                () -> {
                    BBPaxosPrepareRequestHandler.disableHandlingPrepare.set(true);
                }
                );
            }

            populateRandomData(cluster, 10, 0, 0, true);
            verifyBaseTableAndMVInSync(cluster, 0);
            // unblock prepare
            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).runOnInstance(
                () -> {
                    BBPaxosPrepareRequestHandler.disableHandlingPrepare.set(false);
                }
                );
            }

            // read with serial consistency will not add new rows because no proposal is accepted
            readBaseTableWithSerialConsistency(cluster, 10, 0, 0);
            verifyBaseTableAndMVInSync(cluster, 0);

            populateRandomData(cluster, 10, 0, 0, false);
            verifyBaseTableAndMVInSync(cluster, 10);


        }
    }

    @Test
    public void proposePhaseFailedTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(evenlyDistributedTokens(3, 1))
                                        .withNodeIdTopology(singleDcNetworkTopology(3, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK).set("materialized_views_enabled", "true")
                                                                    .set("materialized_view_strict_consistency_enabled", "true")
                                                                    .set("paxos_variant", "v2"))
                                        .withInstanceInitializer(BBPaxosProposeRequestHandler::install)
                                        .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc0': 3}"));
            cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH strict_mv_consistency = true;", KEYSPACE, baseTableName));
            cluster.schemaChange(String.format("CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE pk IS NOT NULL AND ck IS NOT NULL " +
                                               "AND v IS NOT NULL PRIMARY KEY (v, ck,pk)", KEYSPACE, MVName, KEYSPACE, baseTableName));
            // blocking propose, we will get timeout for base table update, making sure base table and MV is in sync
            // block propose phase on 3 nodes
            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).runOnInstance(
                () -> {
                    BBPaxosProposeRequestHandler.disableHandlingPropose.set(true);
                }
                );
            }

            populateRandomData(cluster, 10, 0, 0, true);
            verifyBaseTableAndMVInSync(cluster, 0);
            // unblock propose, making change to exact key/row, we hanve only 10 rows
            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).runOnInstance(
                () -> {
                    BBPaxosProposeRequestHandler.disableHandlingPropose.set(false);
                }
                );
            }
            populateRandomData(cluster, 10, 0, 0, false);
            verifyBaseTableAndMVInSync(cluster, 10);

            // block propose phase on 3 nodes
            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).runOnInstance(
                () -> {
                    BBPaxosProposeRequestHandler.disableHandlingPropose.set(true);
                }
                );
            }

            populateRandomData(cluster, 10, 100, 0, true);
            verifyBaseTableAndMVInSync(cluster, 10);

            // unblock propose
            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).runOnInstance(
                () -> {
                    BBPaxosProposeRequestHandler.disableHandlingPropose.set(false);
                }
                );
            }
            readBaseTableWithSerialConsistency(cluster, 10, 100, 0);
            // reading with serial consistency will replay the failed proposal, we are expecting 20 rows now.
            verifyBaseTableAndMVInSync(cluster, 20);
        }
    }

    @Test
    public void paxosRepairAlwaysBringMVInConsistentWithBaseTableTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(evenlyDistributedTokens(3, 1))
                                        .withNodeIdTopology(singleDcNetworkTopology(3, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK).set("materialized_views_enabled", "true")
                                                                    .set("materialized_view_strict_consistency_enabled", "true")
                                                                    .set("paxos_variant", "v2"))
                                        .withInstanceInitializer(BBPaxosCommitExecuteHandler::install)
                                        .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc0': 3}"));
            cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH strict_mv_consistency = true;", KEYSPACE, baseTableName));
            cluster.schemaChange(String.format("CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE pk IS NOT NULL AND ck IS NOT NULL " +
                                               "AND v IS NOT NULL PRIMARY KEY (v, ck,pk)", KEYSPACE, MVName, KEYSPACE, baseTableName));


            // verify block prepare so that prepare will fail and both MV and base table get 0 row
            IMessageFilters.Filter filter = cluster.filters().verbs(Verb.PAXOS2_PREPARE_REQ.id).from(1).to(2, 3).drop();
            populateRandomData(cluster, 5, 0, 0, true);
            verifyBaseTableAndMVInSync(cluster, 0);
            // resume message, run paxos repair, verify we still get 0 row
            filter.off();
            runPaxosRepairInCluster(cluster);
            verifyBaseTableAndMVInSync(cluster, 0);
            // verify there is no uncommitted data ie nothing to be repaired
            assertClusterNoUncommitted(cluster);
            // verify block propose so that propose will fail and both MV and base table get 0 row
            filter = cluster.filters().verbs(Verb.PAXOS2_PROPOSE_REQ.id).from(1).to(2, 3).drop();
            populateRandomData(cluster, 5, 0, 0, true);
            verifyBaseTableAndMVInSync(cluster, 0);
            // resume message, run paxos repair, verify we still get same rows
            filter.off();
            runPaxosRepairInCluster(cluster);
            int rowCount = verifyBaseTableAndMVInSync(cluster, -1);
            // verify there is no uncommitted data ie nothing to be repaired
            assertClusterNoUncommitted(cluster);
            // drop mutation request to node 2 and node 3 so the MV apply will not get quorum
            // This will cause base table and MV mismatch because MV may have the change but base table has not
            // committed the update yet.
            filter = cluster.filters().verbs(Verb.MUTATION_REQ.id).from(1).to(2, 3).drop();
            populateRandomData(cluster, 5, 10, 0, true);
            Set<RowData> baseTableResultSet = getTableData("SELECT * FROM " + KEYSPACE + "." + baseTableName, cluster);
            Set<RowData> MVResultSet = getTableData("SELECT * FROM " + KEYSPACE + "." + MVName, cluster);
            assertEquals(rowCount, baseTableResultSet.size());
            assertTrue(MVResultSet.size() > rowCount);
            filter.off();
            // verify run paxos repair will bring MV and base table in sync
            runPaxosRepairInCluster(cluster);
            rowCount = verifyBaseTableAndMVInSync(cluster, -1);
            // verify there is no uncommitted data ie nothing to be repaired
            assertClusterNoUncommitted(cluster);
            // disable commit message execution on all nodes
            // This will cause base table and MV mismatch because MV may have the change but base table has not
            // committed the update yet.
            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).runOnInstance(
                () -> {
                    BBPaxosCommitExecuteHandler.disableHandlingCommit.set(true);
                }
                );
            }
            populateRandomData(cluster, 5, 20, 0, true);
            baseTableResultSet = getTableData("SELECT * FROM " + KEYSPACE + "." + baseTableName, cluster);
            MVResultSet = getTableData("SELECT * FROM " + KEYSPACE + "." + MVName, cluster);
            assertEquals(rowCount, baseTableResultSet.size());
            assertTrue(MVResultSet.size() > rowCount);
            // resume commit message, run repair MV and base table get back in sync
            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).runOnInstance(
                () -> {
                    BBPaxosCommitExecuteHandler.disableHandlingCommit.set(false);
                }
                );
            }
            // verify run paxos repair will bring MV and base table in sync
            runPaxosRepairInCluster(cluster);
            verifyBaseTableAndMVInSync(cluster, -1);
            // verify there is no uncommitted data ie nothing to be repaired
            assertClusterNoUncommitted(cluster);
        }
    }

    private void assertClusterNoUncommitted(Cluster cluster)
    {
        for (int i = 1; i <= 3; i++)
            PaxosRepair2Test.assertUncommitted(cluster.get(i), KEYSPACE, baseTableName, 0);
    }

    private void runPaxosRepairInCluster(Cluster cluster)
    {
        // run paxos repari on all three nodes
        for (int i = 1; i <= 3; i++)
        {
            cluster.get(i).runOnInstance(
            () -> {
                try
                {
                    TableId tableid = Schema.instance.getTableMetadata(KEYSPACE, baseTableName).id;
                    StorageService.instance.autoRepairPaxos(tableid).get();
                    return;
                }
                catch (Exception e)
                {
                    e.printStackTrace();

                }
                fail("Paxos repair failed");
            }
            );
        }
    }

    private void populateRandomData(Cluster cluster, int rowCount, int pkFrom, int ckFrom, boolean expectExceptions)
    {
        populateRandomData(cluster, rowCount, pkFrom, ckFrom, expectExceptions, false);
    }

    private void populateRandomData(Cluster cluster, int rowCount, int pkFrom, int ckFrom, boolean expectExceptions, boolean withCondition)
    {
        Random random = new Random();
        for (int i = 0; i < rowCount; i++)
        {
            int pk = pkFrom + i;
            int ck = ckFrom + i;
            int v = random.nextInt();
            try
            {
                if (withCondition)
                {
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + baseTableName + " (pk, ck, v) VALUES (?, ?, ?) IF NOT EXISTS;",
                                                   ConsistencyLevel.LOCAL_QUORUM,
                                                   pk, ck, v);
                }
                else
                {
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + baseTableName + " (pk, ck, v) VALUES (?, ?, ?)",
                                                   ConsistencyLevel.LOCAL_QUORUM,
                                                   pk, ck, v);
                }
            } catch (Exception e)
            {
                if (!expectExceptions)
                {
                    throw e;
                }
                else
                {
                    continue;
                }
            }
            if (expectExceptions)
            {
                fail(String.format("Expecting exceptions but got nothing, i: %s", i));
            }
        }

    }

    private void readBaseTableWithSerialConsistency(Cluster cluster, int rowCount, int pkFrom, int ckFrom)
    {
        for (int i = 0; i < rowCount; i++)
        {
            int pk = pkFrom + i;
            int ck = ckFrom + i;
            String query = String.format("Select * from " + KEYSPACE + "." + baseTableName + " where pk=" + pk + " and ck=" + ck);
            cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.LOCAL_SERIAL);
        }

    }

    private int verifyBaseTableAndMVInSync(Cluster cluster, int expectedRows)
    {
        Set<RowData> baseTableResultSet = getTableData("SELECT * FROM " + KEYSPACE + "." + baseTableName, cluster);
        Set<RowData> MVResultSet = getTableData("SELECT * FROM " + KEYSPACE + "." + MVName, cluster);
        // disable size check if expected rows < 0
        if (expectedRows >= 0)
            Assert.assertEquals(expectedRows, baseTableResultSet.size());
        Assert.assertEquals(baseTableResultSet, MVResultSet);
        return baseTableResultSet.size();
    }

    private Set<RowData> getTableData(String query, Cluster cluster)
    {
        SimpleQueryResult tableResult = cluster.coordinator(1).executeWithResult(
        query, ConsistencyLevel.LOCAL_QUORUM);
        Set<RowData> rst = new HashSet<>();
        while (tableResult.hasNext())
        {
            Row row = tableResult.next();
            rst.add(new RowData(row.getInteger("pk"), row.getInteger("ck"), row.getInteger("v")));
        }
        return rst;
    }

    public static class BBPaxosPrepareRequestHandler
    {
        public static final AtomicBoolean disableHandlingPrepare = new AtomicBoolean();
        public static void install(ClassLoader cl, Integer i)
        {
            new ByteBuddy().rebase(PaxosPrepare.RequestHandler.class)
                           .method(named("doVerb"))
                           .intercept(MethodDelegation.to(StrictMVConsistencyTest.BBPaxosPrepareRequestHandler.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void doVerb(Message<PaxosPrepare.Request> message, @SuperCall Callable<Void> zuper) throws Exception
        {
            if (disableHandlingPrepare.get())
            {
                logger.info("Paxos Prepare request handling is disabled");
                return;
            }
            zuper.call();
        }
    }

    public static class BBPaxosProposeRequestHandler
    {
        public static final AtomicBoolean disableHandlingPropose = new AtomicBoolean();
        public static void install(ClassLoader cl, Integer i)
        {
            new ByteBuddy().rebase(PaxosPropose.RequestHandler.class)
                           .method(named("doVerb"))
                           .intercept(MethodDelegation.to(StrictMVConsistencyTest.BBPaxosProposeRequestHandler.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void doVerb(Message<PaxosPrepare.Request> message, @SuperCall Callable<Void> zuper) throws Exception
        {
            if (disableHandlingPropose.get())
            {
                logger.info("Paxos Propose request handling is disabled");
                return;
            }
            zuper.call();
        }
    }

    public static class BBPaxosCommitExecuteHandler
    {
        public static final AtomicBoolean disableHandlingCommit = new AtomicBoolean();
        public static void install(ClassLoader cl, Integer i)
        {
            new ByteBuddy().rebase(PaxosCommit.RequestHandler.class)
                           .method(named("execute"))
                           .intercept(MethodDelegation.to(StrictMVConsistencyTest.BBPaxosCommitExecuteHandler.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static NoPayload execute(Commit.Agreed agreed, InetAddressAndPort from, @SuperCall Callable<NoPayload> zuper) throws Exception
        {
            if (disableHandlingCommit.get())
            {
                logger.info("Paxos commit handling is disabled");
                return null;
            }
            return zuper.call();
        }
    }

    private class RowData
    {
        int pk, ck, v;
        RowData(int pk, int ck, int v)
        {
            this.pk = pk;
            this.ck = ck;
            this.v = v;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RowData other = (RowData) o;
            return this.pk == other.pk && this.ck == other.ck && this.v == other.v;
        }

        public int hashCode()
        {
            return Objects.hash(pk, ck, v);
        }
    }
}
