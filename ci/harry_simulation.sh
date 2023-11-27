#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -x
set -o errexit
set -o nounset
set -o pipefail

rm -f build/test/lib/jars/guava-18.0.jar
current_dir=$(dirname "$(readlink -f "$0")")

common=(-Dstorage-config=$current_dir/../test/conf
        -Djava.awt.headless=true
        -ea
        -XX:SoftRefLRUPolicyMSPerMB=0
        -XX:HeapDumpPath=build/test
        -Dcassandra.test.driver.connection_timeout_ms=10000
        -Dcassandra.test.driver.read_timeout_ms=24000
        -Dcassandra.memtable_row_overhead_computation_step=100
        -Dcassandra.test.use_prepared=true
        -Dcassandra.test.sstableformatdevelopment=true
        -Djava.security.egd=file:/dev/urandom
        -Dcassandra.testtag=.jdk11
        -Dstorage-config=$current_dir/../test/conf
        -Djava.awt.headless=true
        -Dcassandra.keepBriefBrief=true
        -Dcassandra.allow_simplestrategy=true
        -Dcassandra.strict.runtime.checks=true
        -Dcassandra.reads.thresholds.coordinator.defensive_checks_enabled=true
        -Dcassandra.test.flush_local_schema_changes=false
        -Dcassandra.test.messagingService.nonGracefulShutdown=true
        -Dcassandra.use_nix_recursive_delete=true
        -Dcie-cassandra.disable_schema_drop_log=true
        -Dcassandra.ring_delay_ms=10000
        -Dcassandra.tolerate_sstable_size=true
        -Dcassandra.skip_sync=true
        -Dcassandra.debugrefcount=false
        -Dcassandra.test.simulator.determinismcheck=strict
        -Dcassandra.test.simulator.print_asm=none
        -Dlog4j2.disableJmx=true
        -Dlog4j2.disable.jmx=true
        -Dlog4j.shutdownHookEnabled=false
        -Dcassandra.test.logConfigPath=$current_dir/../test/conf/log4j2-dtest-simulator.xml
        -Dcassandra.test.logConfigProperty=log4j.configurationFile
        -Dlog4j2.configurationFile=$current_dir/../test/conf/log4j2-dtest-simulator.xml
        -javaagent:$current_dir/../lib/jamm-0.3.2.jar
        -javaagent:$current_dir/../build/test/lib/jars/simulator-asm.jar
        -Xbootclasspath/a:$current_dir/../build/test/lib/jars/simulator-bootstrap.jar
        -XX:ActiveProcessorCount=4
        -XX:-TieredCompilation
        -XX:-BackgroundCompilation
        -XX:CICompilerCount=1
        -XX:Tier4CompileThreshold=1000
        -XX:ReservedCodeCacheSize=256M
        -Xmx16G
        -Xmx4G
        -Xss384k
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
        -classpath $current_dir/../build/classes/main/:$current_dir/../build/test/classes/:$current_dir/../build/test/lib/jars/*:$current_dir/../build/lib/jars/*

       )


java "${common[@]}" org.apache.cassandra.simulator.test.HarrySimulatorTest
