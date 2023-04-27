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

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.simulator.paxos.PaxosSimulationRunner;

/**
 * In order to run these tests in your IDE, you need to first build a simulator jar
 *
 *    ant simulator-jars
 *
 * And then run your test using the following settings:
 *
 *        -Dstorage-config=$MODULE_DIR$/test/conf
 *        -Djava.awt.headless=true
 *        -javaagent:$MODULE_DIR$/lib/jamm-0.3.2.jar
 *        -ea
 *        -Dcassandra.debugrefcount=true
 *        -Xss384k
 *        -XX:SoftRefLRUPolicyMSPerMB=0
 *        -XX:ActiveProcessorCount=2
 *        -XX:HeapDumpPath=build/test
 *        -Dcassandra.test.driver.connection_timeout_ms=10000
 *        -Dcassandra.test.driver.read_timeout_ms=24000
 *        -Dcassandra.memtable_row_overhead_computation_step=100
 *        -Dcassandra.test.use_prepared=true
 *        -Dcassandra.test.sstableformatdevelopment=true
 *        -Djava.security.egd=file:/dev/urandom
 *        -Dcassandra.testtag=.jdk1.8
 *        -Dcassandra.keepBriefBrief=true
 *        -Dcassandra.strict.runtime.checks=true
 *        -Dcassandra.reads.thresholds.coordinator.defensive_checks_enabled=true
 *        -Dcassandra.test.flush_local_schema_changes=false
 *        -Dcassandra.test.messagingService.nonGracefulShutdown=true
 *        -Dcassandra.use_nix_recursive_delete=true
 *        -DQT_SHRINKS=0
 *        -Dlog4j2.configurationFile=$MODULE_DIR$/test/conf/log4j2-simulator.xml
 *        -Dcassandra.ring_delay_ms=10000
 *        -Dcassandra.tolerate_sstable_size=true
 *        -Dcassandra.skip_sync=true
 *        -Dcassandra.debugrefcount=false
 *        -Dcassandra.test.simulator.determinismcheck=strict
 *        -Dcassandra.test.simulator.print_asm=none
 *        -javaagent:$MODULE_DIR$/build/test/lib/jars/simulator-asm.jar
 *        -Xbootclasspath/a:$MODULE_DIR$/build/test/lib/jars/simulator-bootstrap.jar
 *        -XX:ActiveProcessorCount=4
 *        -XX:-TieredCompilation
 *        -XX:-BackgroundCompilation
 *        -XX:CICompilerCount=1
 *        -XX:Tier4CompileThreshold=1000
 *        -XX:ReservedCodeCacheSize=256M
 *        -Xmx16G
 *        -Xmx4G
 */
public class ShortPaxosSimulationTest
{
    @Test
    public void simulationTest() throws IOException
    {
        PaxosSimulationRunner.main(new String[] { "run", "-n", "3..6", "-t", "1000", "-c", "2", "--cluster-action-limit", "2", "-s", "30" });
    }

    @Test
    @Ignore("fails due to OOM DirectMemory - unclear why")
    public void selfReconcileTest() throws IOException
    {
        PaxosSimulationRunner.main(new String[] { "reconcile", "-n", "3..6", "-t", "1000", "-c", "2", "--cluster-action-limit", "2", "-s", "30", "--with-self" });
    }
}

