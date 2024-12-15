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

package org.apache.cassandra.simulator.test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ConfigurationService;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.TopologyManager;
import org.apache.cassandra.service.accord.AccordConfigurationService;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.cluster.ClusterActionListener;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.simulator.cluster.ClusterActions.InitialConfiguration.initializeAll;
import static org.apache.cassandra.simulator.cluster.ClusterActions.Options.noActions;

/**
 * In order to run these tests in your IDE, you need to first build a simulator jara
 *
 *    ant simulator-jars
 *
 * And then run your test using the following settings (omit add-* if you are running on jdk8):
 *
 -Dstorage-config=/Users/dcapwell/src/github/apache/cassandra/cep-15-accord/test/conf
 -Djava.awt.headless=true
 -javaagent:/Users/dcapwell/src/github/apache/cassandra/cep-15-accord/lib/jamm-0.4.0.jar
 -ea
 -Dcassandra.debugrefcount=true
 -Xss384k
 -XX:SoftRefLRUPolicyMSPerMB=0
 -XX:ActiveProcessorCount=2
 -XX:HeapDumpPath=build/test
 -Dcassandra.test.driver.connection_timeout_ms=10000
 -Dcassandra.test.driver.read_timeout_ms=24000
 -Dcassandra.memtable_row_overhead_computation_step=100
 -Dcassandra.test.use_prepared=true
 -Dcassandra.test.sstableformatdevelopment=true
 -Djava.security.egd=file:/dev/urandom
 -Dcassandra.testtag=.jdk11
 -Dcassandra.keepBriefBrief=true
 -Dcassandra.allow_simplestrategy=true
 -Dcassandra.strict.runtime.checks=true
 -Dcassandra.reads.thresholds.coordinator.defensive_checks_enabled=true
 -Dcassandra.test.flush_local_schema_changes=false
 -Dcassandra.test.messagingService.nonGracefulShutdown=true
 -Dcassandra.use_nix_recursive_delete=true
 -Dcie-cassandra.disable_schema_drop_log=true
 -Dlogback.configurationFile=file:///Users/dcapwell/src/github/apache/cassandra/cep-15-accord/test/conf/logback-simulator.xml
 -Dcassandra.ring_delay_ms=10000
 -Dcassandra.tolerate_sstable_size=true
 -Dcassandra.skip_sync=true
 -Dcassandra.debugrefcount=false
 -Dcassandra.test.simulator.determinismcheck=strict
 -Dcassandra.test.simulator.print_asm=none
 -javaagent:/Users/dcapwell/src/github/apache/cassandra/cep-15-accord/build/test/lib/jars/simulator-asm.jar
 -Xbootclasspath/a:/Users/dcapwell/src/github/apache/cassandra/cep-15-accord/build/test/lib/jars/simulator-bootstrap.jar
 -XX:ActiveProcessorCount=4
 -XX:-TieredCompilation
 -XX:-BackgroundCompilation
 -XX:CICompilerCount=1
 -XX:Tier4CompileThreshold=1000
 -XX:ReservedCodeCacheSize=256M
 -Xmx16G
 -Xmx4G
 --add-exports java.base/jdk.internal.misc=ALL-UNNAMED
 --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
 --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED
 --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED
 --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED
 --add-exports java.sql/java.sql=ALL-UNNAMED
 --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED
 --add-opens java.base/java.lang.module=ALL-UNNAMED
 --add-opens java.base/java.net=ALL-UNNAMED
 --add-opens java.base/jdk.internal.loader=ALL-UNNAMED
 --add-opens java.base/jdk.internal.ref=ALL-UNNAMED
 --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED
 --add-opens java.base/jdk.internal.math=ALL-UNNAMED
 --add-opens java.base/jdk.internal.module=ALL-UNNAMED
 --add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED
 --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED
 --add-opens jdk.management.jfr/jdk.management.jfr=ALL-UNNAMED
 --add-opens java.desktop/com.sun.beans.introspect=ALL-UNNAMED
 */
public class EpochStressTest extends SimulationTestBase
{
    @Test
    public void manyEpochsAndAccordConverges() throws IOException
    {
        simulate(simulation -> {
                     // setup
                     ClusterActions.Options options = noActions(simulation.cluster.size());
                     ClusterActions clusterActions = new ClusterActions(simulation.simulated, simulation.cluster,
                                                                        options, new ClusterActionListener.NoOpListener(), new Debug(new EnumMap<>(Debug.Info.class), new int[0]));
                     return ActionList.of(clusterActions.initializeCluster(initializeAll(simulation.cluster.size())),
                                          simulation.schemaChange(1, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3}"),
                                          simulation.schemaChange(1, "CREATE TABLE IF NOT EXISTS ks.tbl (pk int PRIMARY KEY, v int) WITH " + TransactionalMode.full.asCqlParam()));
                 },
                 simulation -> {
                     // test
                     RandomSource random = simulation.simulated.random;
                     int numEpochs = 100;
                     List<Action> actions = new ArrayList<>(numEpochs);
                     for (int i = 0; i < numEpochs; i++)
                     {
                         int node = random.uniform(1, simulation.cluster.size() + 1);
                         actions.add(simulation.schemaChange(node, "ALTER TABLE ks.tbl WITH comment = 'step=" + i + "'"));
                     }
                     return ActionList.of(actions);
                 },
                 simulation -> {
                     // teardown
                     List<Action> actions = new ArrayList<>(simulation.cluster.size());
                     for (int i = 0; i < simulation.cluster.size(); i++)
                         actions.add(HarrySimulatorTest.lazy(simulation.simulated, simulation.cluster.get(i + 1), EpochStressTest::validate));
                     return ActionList.of(actions);
                 },
                 config -> config.nodes(3, 3)
                                 .dcs(1, 1)
                                 .threadCount(100));
    }

    private static void validate()
    {
        Logger logger = LoggerFactory.getLogger(EpochStressTest.class);
        NodeId nodeId = ClusterMetadata.current().myNodeId();
        long maxEpoch = ClusterMetadataService.instance().log().waitForHighestConsecutive().epoch.getEpoch();
        long startNano = Clock.Global.nanoTime();
        long deadlineNanos = startNano + TimeUnit.MINUTES.toNanos(10);

        AccordService accord = (AccordService) AccordService.instance();
        Node node = accord.node();
        AccordConfigurationService configService = (AccordConfigurationService) node.configService();
        TopologyManager tm = node.topology();
        long minEpoch = tm.minEpoch();

        logger.info("Starting validation on node {} for epochs {} -> {}", nodeId, minEpoch, maxEpoch);

        Consumer<Supplier<String>> sleep = msg -> {
            long now = Clock.Global.nanoTime();
            if (now > deadlineNanos)
                throw new AssertionError(msg.get());
            logger.debug("Step is not ready yet: {}", msg.get());
            Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
        };

        for (long epoch = minEpoch; epoch <= maxEpoch; epoch++)
        {
            long finalEpoch = epoch;
            ConfigurationService.EpochReady ready = tm.epochReady(epoch);
            while (!isDone(ready))
                sleep.accept(() -> "Epoch " + finalEpoch + "'s EpochReady is not done; " + ready);

            AccordConfigurationService.EpochSnapshot snapshot = configService.getEpochSnapshot(epoch);
            while (!isDone(snapshot))
            {
                AccordConfigurationService.SyncStatus status = snapshot.syncStatus;
                sleep.accept(() -> "Epoch " + finalEpoch + "'s SyncStatus is not done; " + status);
                snapshot = configService.getEpochSnapshot(epoch);
            }

            Ranges expected = tm.globalForEpoch(epoch).ranges().mergeTouching();
            Ranges synced = tm.syncComplete(epoch).mergeTouching();
            while (!isDone(synced, expected))
            {
                Ranges finalSynced = synced;
                sleep.accept(() -> "Epoch " + finalEpoch + "'s syncComplete is not done; missing " + expected.without(finalSynced));
                synced = tm.syncComplete(epoch).mergeTouching();
            }
        }
        logger.info("All epochs completed in {}", Duration.ofNanos(Clock.Global.nanoTime() - startNano));
    }

    private static boolean isDone(Ranges synced, Ranges expected)
    {
        return synced.equals(expected);
    }

    private static boolean isDone(AccordConfigurationService.EpochSnapshot snapshot)
    {
        return snapshot.syncStatus == AccordConfigurationService.SyncStatus.COMPLETED;
    }

    private static boolean isDone(ConfigurationService.EpochReady ready)
    {
        return ready.metadata.isDone()
               && ready.coordinate.isDone()
               && ready.data.isDone()
               && ready.reads.isDone();
    }
}
