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

package org.apache.cassandra.distributed.test.repair;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.*;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairUtilsV2;
import org.apache.cassandra.repair.state.AutoRepairStateFactory;
import org.apache.cassandra.streaming.StreamSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class AutoRepairBootstrapRepairTypeTest extends TestBaseImpl
{
    private long savedMigrationDelay;

    static String originalResetBootstrapProgress = null;

    @Before
    public void beforeTest()
    {
        // MigrationCoordinator schedules schema pull requests immediatelly when the node is just starting up, otherwise
        // the first pull request is sent in 60 seconds. Whether we are starting up or not is detected by examining
        // the node up-time and if it is lower than MIGRATION_DELAY, we consider the server is starting up.
        // When we are running multiple test cases in the class, where each starts a node but in the same JVM, the
        // up-time will be more or less relevant only for the first test. In order to enforce the startup-like behaviour
        // for each test case, the MIGRATION_DELAY time is adjusted accordingly
        savedMigrationDelay = CassandraRelevantProperties.MIGRATION_DELAY.getLong();
        CassandraRelevantProperties.MIGRATION_DELAY.setLong(ManagementFactory.getRuntimeMXBean().getUptime() + savedMigrationDelay);

        originalResetBootstrapProgress = RESET_BOOTSTRAP_PROGRESS.getString();
    }

    @After
    public void afterTest()
    {
        CassandraRelevantProperties.MIGRATION_DELAY.setLong(savedMigrationDelay);
        if (originalResetBootstrapProgress == null)
            RESET_BOOTSTRAP_PROGRESS.clearValue();
        else
            RESET_BOOTSTRAP_PROGRESS.setString(originalResetBootstrapProgress);
    }

    public static class BBStreamFailure
    {
        public static final AtomicBoolean failStream = new AtomicBoolean();
        public static void install(ClassLoader cl, Integer i)
        {
            new ByteBuddy().rebase(StreamSession.class)
                           .method(named("startStreamingFiles"))
                           .intercept(MethodDelegation.to(AutoRepairBootstrapRepairTypeTest.BBStreamFailure.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void startStreamingFiles(StreamSession.PrepareDirection prepareDirection, @SuperCall Callable<Boolean> zuper) throws Exception
        {
            if (failStream.get())
            {
                throw new RuntimeException("Trigger stream failure");
            }
            zuper.call();
        }
    }

    @Test
    public void bootstrapAutoRepairTurn() throws Throwable
    {
        RESET_BOOTSTRAP_PROGRESS.setBoolean(true);

        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount, 1))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withInstanceInitializer(AutoRepairBootstrapRepairTypeTest.BBStreamFailure::install)
                                        .start())
        {
            populate(cluster, 0, 100, 1, 3, ConsistencyLevel.QUORUM);

            // Make node 1 stream fail
            cluster.get(1).runOnInstance(
            ()-> {
                // verify that the normal node (cluster.get(1)) returns "NOT_MY_TURN" when probed for "bootstrap" repair type
                assertEquals(AutoRepairUtilsV2.RepairTurn.NOT_MY_TURN, AutoRepairStateFactory.getAutoRepairState(AutoRepairConfig.RepairType.bootstrap).calcRepairTurn(null));
                BBStreamFailure.failStream.set(true);
            }
            );

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup(cluster);
            newInstance.logs().watchFor("Stream failed");

            // Make node 1 stream normal
            cluster.get(1).runOnInstance(
            ()-> {
                BBStreamFailure.failStream.set(false);
            }
            );
            // verify that the bootstrapping node "newInstance" returns "MY_TURN" when probed for "bootstrap" repair type
            newInstance.runOnInstance(
            () -> {
                assertEquals(AutoRepairUtilsV2.RepairTurn.MY_TURN, AutoRepairStateFactory.getAutoRepairState(AutoRepairConfig.RepairType.bootstrap).calcRepairTurn(null));
            }
            );
        }
    }

    public static void populate(ICluster cluster, int from, int to, int coord, int rf, ConsistencyLevel cl)
    {
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + "};");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        for (int i = from; i < to; i++)
        {
            cluster.coordinator(coord).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                               cl,
                                               i, i, i);
        }
    }
}
