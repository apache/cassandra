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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import accord.utils.DefaultRandom;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.SeedProvider;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.simulator.AlwaysDeliverNetworkScheduler;
import org.apache.cassandra.simulator.ClusterSimulation;
import org.apache.cassandra.simulator.FutureActionScheduler;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * In order to run these tests in your IDE, you need to first build a simulator jara
 *
 *    ant simulator-jars
 *
 * And then run your test using the following settings (omit add-* if you are running on jdk8):
 *
 -Dstorage-config=$MODULE_DIR$/test/conf
 -Djava.awt.headless=true
 -javaagent:$MODULE_DIR$/lib/jamm-0.4.0.jar
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
 -Dlogback.configurationFile=file://$MODULE_DIR$/test/conf/logback-simulator.xml
 -Dcassandra.ring_delay_ms=10000
 -Dcassandra.tolerate_sstable_size=true
 -Dcassandra.skip_sync=true
 -Dcassandra.debugrefcount=false
 -Dcassandra.test.simulator.determinismcheck=strict
 -Dcassandra.test.simulator.print_asm=none
 -javaagent:$MODULE_DIR$/build/test/lib/jars/simulator-asm.jar
 -Xbootclasspath/a:$MODULE_DIR$/build/test/lib/jars/simulator-bootstrap.jar
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
public class SingleNodeSingleTableASTTest extends SimulationTestBase
{
    private static final Gen<Gen.IntGen> VERB_DELAY_DISTRIBUTION_MS = Gens.ints().mixedDistribution(10, 1000);

    /**
     * Simulator requires that loggers are not setup before simulator starts, but the generators below touch {@link accord.utils.Invariants} which
     * allocates a logger which leads to the following errors
     *
     * <pre>{@code Caused by: java.lang.NoClassDefFoundError: Could not initialize class org.apache.cassandra.simulator.logging.RunStartDefiner}</pre>
     */
    private static class LazyInit
    {
        private static final Gen.IntGen THREAD_GEN = Gens.pickInt(10, 100, 1_000);
    }

    @Test
    public void normal() throws IOException
    {
//        testOne(SimulationRunner.parseHex("0x2fd91c2a2be59d7d"), SingleTableASTSimulation::new);
        testOne(SeedProvider.instance.nextSeed(), SingleTableASTSimulation::new);
    }

    @Test
    public void accordFull() throws IOException
    {
//        testOne(SimulationRunner.parseHex("0xd72a3d79134c4dbb"), SingleTableASTSimulation.FullAccordSingleTableASTSimulation::new);
        testOne(SeedProvider.instance.nextSeed(), SingleTableASTSimulation.FullAccordSingleTableASTSimulation::new);
    }

    @Test
    public void accordMixedReads() throws IOException
    {
//        testOne(SimulationRunner.parseHex("0x2fd91c2a2be59d7d"), SingleTableASTSimulation.MixedReadsAccordSingleTableASTSimulation::new);
        testOne(SeedProvider.instance.nextSeed(), SingleTableASTSimulation.MixedReadsAccordSingleTableASTSimulation::new);
    }

    private void testOne(long seed, ClusterSimulation.SimulationFactory<SingleTableASTSimulation> factory) throws IOException
    {
        RandomSource rs = new DefaultRandom(seed);
        Gen.IntGen delayMillis = VERB_DELAY_DISTRIBUTION_MS.next(rs);
        simulate(seed, factory, b ->
                                                 b.futureActionScheduler((i1, time, i2) -> new AlwaysDeliverNetworkScheduler(time))
                                                  .perVerbFutureActionSchedulers((i1, time, i2) -> {
                                                      Map<Verb, FutureActionScheduler> map = new HashMap<>();
                                                      for (Verb verb : Verb.values())
                                                          map.put(verb, new AlwaysDeliverNetworkScheduler(time, TimeUnit.MILLISECONDS.toNanos(delayMillis.nextInt(rs))));
                                                      return map;
                                                  })
                                                  .writeTimeoutNanos(SECONDS.toNanos(120))
                                                  .readTimeoutNanos(SECONDS.toNanos(120))
                                                  .requestTimeoutNanos(SECONDS.toNanos(120))
                                                  .threadCount(LazyInit.THREAD_GEN.nextInt(rs))
                                                  .nodes(1, 1)
                                                  .dcs(1, 1));
    }
}