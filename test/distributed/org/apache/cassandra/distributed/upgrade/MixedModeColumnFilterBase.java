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

package org.apache.cassandra.distributed.upgrade;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.Shared;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.ExpiringMemoizingSupplier;
import org.apache.cassandra.utils.FBUtilities;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class MixedModeColumnFilterBase
{
    private static Logger logger = LoggerFactory.getLogger(MixedModeColumnFilterBase.class);

    static final String KEYSPACE = "cftest";
    static final boolean SIMULATE_UPGRADE_VERSION_OVERRIDE_SKEW = true;

    int failures = 0;

    void setupSchema(UpgradeableCluster cluster)
    {
        cluster.schemaChange("CREATE KEYSPACE cftest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + cluster.size() + "};");
        cluster.schemaChange("CREATE TABLE cftest.tbl_s1v1 (pk varchar, ck int, s1 int static, v1 int, PRIMARY KEY (pk,ck));");
        cluster.schemaChange("CREATE TABLE cftest.tbl_s2v1 (pk varchar, ck int, s1 int static, s2 set<int> static, v1 int, PRIMARY KEY (pk,ck));");
        cluster.schemaChange("CREATE TABLE cftest.tbl_s2v2 (pk varchar, ck int, s1 int static, s2 set<int> static, v1 int, v2 set<int>, PRIMARY KEY (pk,ck));");
        cluster.schemaChange("CREATE TABLE cftest.tbl_subsel (pk varchar, ck int, smap map<int,int> static, sset set<int> static, slist list<int> static, vmap map<int,int> static, vset set<int>, vlist list<int>, PRIMARY KEY (pk,ck));");
    }


    void populate(UpgradeableCluster cluster)
    {
        cluster.disableAutoCompaction(KEYSPACE); // for pre-4.x clusters

        // CASSANDRA-16686
        cluster.coordinator(1).execute("INSERT INTO cftest.tbl_s2v1(pk,s1,s2) " +
                                       "VALUES('CASSANDRA-16686-1',1,{1}) USING TIMESTAMP 1000",
                                       ConsistencyLevel.ALL);
        cluster.coordinator(1).execute("INSERT INTO cftest.tbl_s2v1(pk,ck,v1,s2) " +
                                       "VALUES('CASSANDRA-16686-2',1,2,{3}) USING TIMESTAMP 1000",
                                       ConsistencyLevel.ALL);
        cluster.forEach(instance -> instance.flush(KEYSPACE));

        cluster.coordinator(1).execute("INSERT INTO cftest.tbl_s2v1(pk,s1)" +
                                       " VALUES('CASSANDRA-16686-1',2) USING TIMESTAMP 2000",
                                       ConsistencyLevel.ALL);
        cluster.coordinator(1).execute("INSERT INTO cftest.tbl_s2v1(pk,s1)" +
                                       " VALUES('CASSANDRA-16686-2',2) USING TIMESTAMP 2000",
                                       ConsistencyLevel.ALL);
        cluster.forEach(instance -> instance.flush(KEYSPACE));

        cluster.coordinator(1).execute("DELETE s1 FROM cftest.tbl_s2v1 " +
                                       "USING TIMESTAMP 3000 WHERE pk = 'CASSANDRA-16686-1'",
                                       ConsistencyLevel.ALL);
        cluster.coordinator(1).execute("DELETE s1 FROM cftest.tbl_s2v1 " +
                                       "USING TIMESTAMP 3000 WHERE pk = 'CASSANDRA-16686-2'",
                                       ConsistencyLevel.ALL);
        cluster.forEach(instance -> instance.flush(KEYSPACE));

        // CASSANDRA-17601 - variant 1
        cluster.coordinator(1).execute("INSERT INTO cftest.tbl_s2v1 (pk, ck, s2, v1) " +
                                       "VALUES ('CASSANDRA-17601-1', 2, {30}, 400)",
                                       ConsistencyLevel.ALL);
        cluster.forEach(instance -> instance.flush(KEYSPACE));

        // CASSANDRA-17601 - variant 2
        cluster.coordinator(1).execute("INSERT INTO cftest.tbl_s2v1 (pk, ck, s1, s2, v1) " +
                                       "VALUES ('CASSANDRA-17601-2', 2, 30, {40}, 500)",
                                       ConsistencyLevel.ALL);
        cluster.forEach(instance -> instance.flush(KEYSPACE));

        // CASSANDRA-17601 - extra check safe if only one static column
        cluster.coordinator(1).execute("INSERT INTO cftest.tbl_s1v1 (pk, ck, s1, v1) " +
                                       "VALUES ('CASSANDRA-17601-singlestatic', 1, 2, 30)",
                                       ConsistencyLevel.ALL);
        cluster.forEach(instance -> instance.flush(KEYSPACE));

        // CASSANDRA-16415
        cluster.coordinator(1).execute("INSERT INTO cftest.tbl_s2v2 (pk, ck, s1, s2, v1, v2) " +
                                       "VALUES ('CASSANDRA-16415', 1, 2, {1, 2, 3, 4, 5}, 3, {6, 7, 8, 9, 10})",
                                       ConsistencyLevel.ALL);
        cluster.coordinator(1).execute("INSERT INTO cftest.tbl_s2v2 (pk, ck, s1, s2, v1, v2) " +
                                       "VALUES ('CASSANDRA-16415', 2, 3, {2, 3, 4, 5, 6}, 4, {7, 8, 9, 10, 11})",
                                       ConsistencyLevel.ALL);

        // Subselections
        cluster.coordinator(1).execute("INSERT INTO cftest.tbl_subsel(pk,ck,smap,sset,slist) " +
                                       "VALUES('collections',1,{3:30},{400},[5000]) USING TIMESTAMP 1000",
                                       ConsistencyLevel.ALL);
        cluster.forEach(instance -> instance.flush(KEYSPACE));

        cluster.coordinator(1).execute("UPDATE cftest.tbl_subsel USING TIMESTAMP 2000 " +
                                       "SET smap[4]=40, sset=sset+{500}, slist=slist+[6000]  " +
                                       "WHERE pk='collections'", ConsistencyLevel.ALL);
        // Do not flush beyond sstables before this write -- want to make sure the memtable is involved
        // on nodes that have not been upgraded yet as subselections may exercise a different path when choosing
        // whether to include cells.
    }

    void reportException(Runnable thing)
    {
        try
        {
            thing.run();
        }
        catch (Throwable tr)
        {
            logger.error("Threw exception", tr);
            failures++;
        }
    }

    static final String PK16686_1 = "CASSANDRA-16686-1";
    static final Map<String, Object[]> C16686_1_CHECKS;

    static
    {
        Integer ck16686_1 = null;
        Integer s1_16686_1 = null;
        Set<Integer> s2_16686_1 = ImmutableSet.of(1);

        Object[] s1_16686_1_cols = { s1_16686_1 };
        C16686_1_CHECKS = new ImmutableMap.Builder()
        .put("*", row(PK16686_1, null, null, s2_16686_1, null))
        .put("pk, s1, s2", row(PK16686_1, s1_16686_1, s2_16686_1))
        .put("s1, s2", row(s1_16686_1, s2_16686_1))
        .put("pk, s1", row(PK16686_1, s1_16686_1))
        .put("s1", row(s1_16686_1_cols))
        .put("pk, s2", row(PK16686_1, s2_16686_1))
        .put("s2", row(s2_16686_1))
        .put("pk, ck", row(PK16686_1, null))
        .put("ck", row(ck16686_1))
        .put("DISTINCT pk, s1, s2", row(PK16686_1, (Integer) null, s2_16686_1))
        .put("DISTINCT s1, s2", row((Integer) null, s2_16686_1))
        .put("DISTINCT pk, s1", row(PK16686_1, (Integer) null))
        .put("DISTINCT pk, s2", row(PK16686_1, s2_16686_1))
        .put("DISTINCT s1", row(s1_16686_1_cols))
        .put("DISTINCT s2", row(s2_16686_1))
        .build();
    }

    // CASSANDRA-16686 based on testStaticColumnDeletionWithMultipleStaticColumns
    void validate16686_1(IInstance instance)
    {
        C16686_1_CHECKS.forEach((columnsClause, expectedRow) -> {
            reportException(() -> {
                String query = String.format("SELECT %s FROM cftest.tbl_s2v1 WHERE pk = ?",
                                             columnsClause);
                try
                {
                    Object[][] c16686result = instance.coordinator().execute(query, ConsistencyLevel.ALL, PK16686_1);
                    assertRows(c16686result, expectedRow);
                }
                catch (Throwable tr)
                {
                    logger.error("Failure with query {}", columnsClause);
                    throw tr;
                }
            });
        });
    }

    static final String PK16686_2 = "CASSANDRA-16686-2";
    static final Map<String, Object[]> C16686_2_CHECKS;

    static
    {
        int ck16686_2 = 1;
        int v1_16686_2 = 2;
        Integer s1_16686_2 = null;
        Set<Integer> s2_16686_2 = ImmutableSet.of(3);
        Object[] s1_16686_1_cols = { s1_16686_2 };

        C16686_2_CHECKS = new ImmutableMap.Builder()
        .put("*", row(PK16686_2, ck16686_2, null, s2_16686_2, v1_16686_2))
        .put("pk, s1, s2", row(PK16686_2, s1_16686_2, s2_16686_2))
        .put("s1, s2", row(s1_16686_2, s2_16686_2))
        .put("pk, s1", row(PK16686_2, s1_16686_2))
        .put("s1", row(s1_16686_1_cols))
        .put("pk, s2", row(PK16686_2, s2_16686_2))
        .put("s2", row(s2_16686_2))
        .put("pk, ck", row(PK16686_2, ck16686_2))
        .put("ck", row(ck16686_2))
        .put("DISTINCT pk, s1, s2", row(PK16686_2, (Integer) null, s2_16686_2))
        .put("DISTINCT s1, s2", row((Integer) null, s2_16686_2))
        .put("DISTINCT pk, s1", row(PK16686_2, (Integer) null))
        .put("DISTINCT pk, s2", row(PK16686_2, s2_16686_2))
        .put("DISTINCT s1", row(s1_16686_1_cols))
        .put("DISTINCT s2", row(s2_16686_2))
        .build();
    }

    // CASSANDRA-16686 based on testStaticColumnDeletionWithMultipleStaticColumnsAndRegularColumns
    void validate16686_2(IInstance instance)
    {
        C16686_2_CHECKS.forEach((columnsClause, expectedRow) -> {
            reportException(() -> {
                String query = String.format("SELECT %s FROM cftest.tbl_s2v1 WHERE pk = ?",
                                             columnsClause);
                try
                {
                    Object[][] c16686result = instance.coordinator().execute(query, ConsistencyLevel.ALL, PK16686_2);
                    assertRows(c16686result, expectedRow);
                }
                catch (Throwable tr)
                {
                    logger.error("Failure with query {}", columnsClause);
                    throw tr;
                }
            });
        });
    }

    static final String[] C16415_COLUMNS1 = {
    "pk, ck",
    "*",
    "v1",
    "v2",
    "v1, s1",
    "v1, s2"
    };
    String[] C16415_COLUMNS2 = {
    "s1",
    "s2"
    };

    private void validate16415(IUpgradeableInstance instance)
    {
        reportException(() -> {
            String queryPattern = "SELECT %s FROM cftest.tbl_s2v2 WHERE %s";
            for (String columns : C16415_COLUMNS1)
            {
                instance.coordinator().execute(String.format(queryPattern, columns, "pk = 'CASSANDRA-16415'"), ConsistencyLevel.ALL);
                instance.coordinator().execute(String.format(queryPattern, columns, "pk = 'CASSANDRA-16415' AND ck = 2"), ConsistencyLevel.ALL);
            }
            for (String columns : C16415_COLUMNS2)
            {
                instance.coordinator().execute(String.format(queryPattern, columns, "pk = 'CASSANDRA-16415'"), ConsistencyLevel.ALL);
            }
        });
    }

    private void validate17601(IUpgradeableInstance instance)
    {
        reportException(() -> {
            Object[][] c17601v1 = instance.coordinator().execute(
                                                              "SELECT v1, s1 FROM cftest.tbl_s2v1 " +
                                                              "WHERE pk = 'CASSANDRA-17601-1' AND ck = 2", ConsistencyLevel.ALL);
            assertRows(c17601v1, row(400, null));
        });

        // CASSANDRA-17601 - variant 2
        reportException(() -> {
            Object[][] c17601v2 = instance.coordinator().execute(
                                                              "SELECT v1, s1 FROM cftest.tbl_s2v1 " +
                                                              "WHERE pk = 'CASSANDRA-17601-2' AND ck = 2", ConsistencyLevel.ALL);
            assertRows(c17601v2, row(500, 30));
        });

        // CASSANDRA-17601 - extra check for just single static - should be unaffected by the original issue
        reportException(() -> {
            Object[][] c17601singlestatic1 = instance.coordinator().execute(
                                                              "SELECT v1, s1 FROM cftest.tbl_s1v1 " +
                                                              "WHERE pk = 'CASSANDRA-17601-singlestatic' AND ck = 1", ConsistencyLevel.ALL);
            assertRows(c17601singlestatic1, row(30, 2));

            Object[][] c17601singlestatic2 = instance.coordinator().execute(
            "SELECT s1 FROM cftest.tbl_s1v1 " +
            "WHERE pk = 'CASSANDRA-17601-singlestatic'", ConsistencyLevel.ALL);
            assertRows(c17601singlestatic2, row(2));

            Object[][] c17601singlestatic4 = instance.coordinator().execute(
            "SELECT * FROM cftest.tbl_s1v1 " +
            "WHERE pk = 'CASSANDRA-17601-singlestatic' AND ck = 1", ConsistencyLevel.ALL);
            assertRows(c17601singlestatic4, row("CASSANDRA-17601-singlestatic", 1, 2, 30));

            Object[][] c17601singlestatic5 = instance.coordinator().execute(
            "SELECT * FROM cftest.tbl_s1v1 " +
            "WHERE pk = 'CASSANDRA-17601-singlestatic'", ConsistencyLevel.ALL);
            assertRows(c17601singlestatic5, row("CASSANDRA-17601-singlestatic", 1, 2, 30));
        });
    }

    static final String PK_SUBSEL = "collections";
    static final Map<String, Object[]> SUBSEL_CHECKS;
    // Extra where clauses that should match the row so the expected result does not need to change
    static final List<String> SUBSEL_WHERE = ImmutableList.of("", "AND smap CONTAINS KEY 3", "AND sset CONTAINS 400");
    static
    {
        int ckSubSel = 1;
        Map<Integer,Integer> smap = ImmutableMap.of(3, 30, 4, 40);
        Set<Integer> sset = ImmutableSet.of(400, 500);
        List<Integer> slist = ImmutableList.of(5000, 6000);

        SUBSEL_CHECKS = new ImmutableMap.Builder()
        .put("*", row(PK_SUBSEL, ckSubSel, slist, smap, sset, null, null, null))
        .put("pk", row(PK_SUBSEL))
        .put("pk, smap, sset, slist", row(PK_SUBSEL, smap, sset, slist))
        .put("pk, smap[3], sset[399..401]", row(PK_SUBSEL, 30, ImmutableSet.of(400)))
        .put("pk, sset[399..401]", row(PK_SUBSEL, ImmutableSet.of(400)))
        .put("pk, ck", row(PK_SUBSEL, ckSubSel))
        .put("ck", row(ckSubSel))
        .put("DISTINCT pk, smap, sset, slist", row(PK_SUBSEL, smap, sset, slist))
        .put("DISTINCT pk, smap[3], sset[399..401]", row(PK_SUBSEL, 30, ImmutableSet.of(400)))
        .put("DISTINCT pk, sset[399..401]", row(PK_SUBSEL, ImmutableSet.of(400)))
        .build();
    }
    private void validateSubSel(IUpgradeableInstance instance)
    {
        reportException(() -> {
            SUBSEL_CHECKS.forEach((columnsClause, expectedRow) -> {
                SUBSEL_WHERE.forEach(extraWhereClause -> {
                    reportException(() -> {
                        String query = String.format("SELECT %s FROM cftest.tbl_subsel WHERE pk = ? %s ALLOW FILTERING",
                                                     columnsClause, extraWhereClause);
                        try
                        {
                            Object[][] result = instance.coordinator().execute(query, ConsistencyLevel.ALL, PK_SUBSEL);
                            if (result != null) // query is supported
                                assertRows(result, expectedRow);
                        }
                        catch (Throwable tr)
                        {
                            logger.error("Failure with query '{}'", query, tr);
                            throw tr;
                        }
                    });
                });
            });
        });
    }
    
    void validate(UpgradeableCluster cluster)
    {
        if (cluster.stream().allMatch(i -> i.getReleaseVersionString().compareTo("4") >= 0))
            cluster.forEach(this::validateSubSel); // Skip pre-4.0
        cluster.forEach(this::validate16686_1);
        cluster.forEach(this::validate16686_2);
        cluster.forEach(this::validate17601);
        cluster.forEach(this::validate16415);
    }

    @BeforeClass
    static public void beforeClass() throws Throwable
    {
        ICluster.setup();
    }

    public void run(final List<String> stringVersions) throws Throwable
    {
        failures = 0;

        final List<CassandraVersion> versions = stringVersions
        .stream()
        .map(CassandraVersion::new)
        .collect(Collectors.toList());

        // 4 node cluster - when in mixed mode, two of each version so they can be tested as local and remote.
        final int nodeCount = 4;

        // Check versions required for test are present
        Versions dtestVersions = Versions.find();
        List<Versions.Version> upgradeOrder = stringVersions.stream()
                                                      .map(v -> dtestVersions.get(v))
                                                      .collect(Collectors.toList());
        Versions.Version earliestVersion = upgradeOrder.get(0);


        // Prepare Cluster
        java.util.function.Consumer<IInstanceConfig> configUpdater = config -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                                               .set("autocompaction_on_startup_enabled", false)
                                                                               .set("hinted_handoff_enabled", false); // generates extra exceptions around shutdown
        Consumer<UpgradeableCluster.Builder > builderUpdater = builder -> builder.withInstanceInitializer(BBInstaller::installUpgradeVersionBB);
        try (UpgradeableCluster cluster = UpgradeableCluster.create(nodeCount, earliestVersion, configUpdater, builderUpdater))
        {
            setupSchema(cluster);
            cluster.filters().outbound().messagesMatching((from, to, m) -> {
                if (m.verb() == Verb.READ_REPAIR_REQ.id)
                    BBState.readRepairRequests.incrementAndGet();
                return false;
            }).drop();
            populate(cluster);

            // Install a filter to count the number of read repair messages that are actually sent as a result of digest
            // mismatches and then check it works.
            // Easier than trying to use the read repair diagnostic events as they are not present in 3.x
            cluster.get(1).executeInternal("INSERT INTO cftest.tbl_s2v1(pk,ck,v1) " +
                                           "VALUES('check-read-repair',1,1) USING TIMESTAMP 1000");
            cluster.coordinator(1).execute("SELECT * FROM cftest.tbl_s2v1 WHERE pk='check-read-repair' AND ck=1",
                                           ConsistencyLevel.ALL);
            Assert.assertTrue(BBState.readRepairRequests.get() != 0L);
            BBState.readRepairRequests.set(0L);

            if (SIMULATE_UPGRADE_VERSION_OVERRIDE_SKEW)
            {
                BBState.shouldIntercept = true;

                // simulate the upgraded nodes coming up and preparing against their current version in a mixed cluster
                BBState.upgradeVersionOverride.put(cluster.get(1).config().broadcastAddress().getAddress(), "current");
                BBState.upgradeVersionOverride.put(cluster.get(2).config().broadcastAddress().getAddress(), "current");
            }

            // Upgrade two cluster nodes, leave two on version[-1]
            Versions.Version upgradeVersion = upgradeOrder.get(1);

            upgradeInstances(cluster, upgradeVersion, 1, 2);
            validate(cluster);

            // Upgrade final two cluster nodes - force their version to be the lower version to simulate upgrade version
            // gradually expiring across the cluster and arriving at the final value.
            BBState.upgradeVersionOverride.put(cluster.get(3).config().broadcastAddress().getAddress(), versions.get(0).toString());
            BBState.upgradeVersionOverride.put(cluster.get(4).config().broadcastAddress().getAddress(), versions.get(0).toString());

            upgradeInstances(cluster, upgradeVersion, 3, 4);
            validate(cluster);

            // Update and select from fully upgraded cluster with upgradeFromVersion cleared on all hosts
            if (SIMULATE_UPGRADE_VERSION_OVERRIDE_SKEW)
            {
                BBState.upgradeVersionOverride.clear();
                validate(cluster);
            }

            Assert.assertEquals(0L, BBState.readRepairRequests.get());
            Assert.assertEquals(0, failures);

            // Give the tracing stage time to complate to avoid hang on shutdown. At the time this test
            // was written a late tracing entry mutation could be submitted after the commitlog was shutdown,
            // causing a timeout on instance shutdown.
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        }
    }

    private void upgradeInstances(UpgradeableCluster cluster, Versions.Version upgradeTo, int... instanceIdsArray)
    {
        Arrays.stream(instanceIdsArray).forEach(instanceId -> {
            logger.info("Upgrading instance{} to {}", instanceId, upgradeTo.version);
            IUpgradeableInstance instance = cluster.get(instanceId);
            try
            {
                long preShutdownLogs = instance.logs().mark();
                instance.shutdown(true).get(1, TimeUnit.MINUTES);
                instance.setVersion(upgradeTo);
                instance.startup();
                instance.logs().watchFor(preShutdownLogs, "[0-9] NORMAL$");
                logger.info("Upgraded instance{} to {} - status NORMAL", instanceId, upgradeTo.version);
            }
            catch (Throwable tr)
            {
                throw new RuntimeException("Unable to upgrade instance " + instanceId + " to version " + upgradeTo.version, tr);
            }
        });
    }

    // Shared state with the test runner to be to control upgradeFromVersion
    @Shared
    public static class BBState
    {
        static volatile boolean shouldIntercept = false;
        public static ConcurrentMap<InetAddress, String> upgradeVersionOverride = new ConcurrentHashMap<>();
        static AtomicLong readRepairRequests = new AtomicLong();
    }

    public static class BBInstaller
    {
        public static void installUpgradeVersionBB(ClassLoader classLoader, Integer num)
        {
            if (!BBState.shouldIntercept)
                return;
            try
            {
                new ByteBuddy().rebase(Gossiper.class)
                               .method(named("upgradeFromVersion"))
                               .intercept(MethodDelegation.to(BBInterceptor.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
            catch (NoClassDefFoundError noClassDefFoundError)
            {
                logger.info("... but no class def", noClassDefFoundError);
            }
            catch (Throwable tr)
            {
                logger.info("Unable to intercept upgradeFromVersion method", tr);
                throw tr;
            }
        }
    }

    // AbstrctCluster instance initializer to intercept upgradeFromVersion for testing
    public static class BBInterceptor
    {
        @SuppressWarnings("unused")
        public static ExpiringMemoizingSupplier.ReturnValue<CassandraVersion> upgradeFromVersion(@SuperCall Callable<ExpiringMemoizingSupplier.ReturnValue<CassandraVersion>> zuper)
        {
            try
            {
                String override = BBState.upgradeVersionOverride.get(FBUtilities.getJustBroadcastAddress());

                if (override != null && override.equals("current"))
                    return new ExpiringMemoizingSupplier.NotMemoized(SystemKeyspace.CURRENT_VERSION);
                else if (override != null)
                    return new ExpiringMemoizingSupplier.NotMemoized(new CassandraVersion(override));
                else
                    return new ExpiringMemoizingSupplier.NotMemoized(zuper.call().value());
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
