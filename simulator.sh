#!/bin/bash
#
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
#

#ant jar simulator-jars

DIR=`pwd`
JVM_OPTS="$JVM_OPTS -Dcassandra.config=file://$DIR/test/conf/cassandra.yaml"
JVM_OPTS="$JVM_OPTS -Dlogback.configurationFile=file://$DIR/test/conf/logback-simulator.xml"
JVM_OPTS="$JVM_OPTS -Dcassandra.logdir=$DIR/build/test/logs"
#JVM_OPTS="$JVM_OPTS -Djava.library.path=$DIR/lib/sigar-bin"
JVM_OPTS="$JVM_OPTS -Dlegacy-sstable-root=$DIR/test/data/legacy-sstables"
JVM_OPTS="$JVM_OPTS -Dinvalid-legacy-sstable-root=$DIR/test/data/invalid-legacy-sstables"
JVM_OPTS="$JVM_OPTS -Dcassandra.ring_delay_ms=1000"
JVM_OPTS="$JVM_OPTS -Dcassandra.skip_sync=true"
JVM_OPTS="$JVM_OPTS -ea"
JVM_OPTS="$JVM_OPTS -XX:MaxMetaspaceSize=1G"
JVM_OPTS="$JVM_OPTS -XX:SoftRefLRUPolicyMSPerMB=0"
JVM_OPTS="$JVM_OPTS -Dcassandra.strict.runtime.checks=true"
JVM_OPTS="$JVM_OPTS -javaagent:$DIR/build/test/lib/jars/simulator-asm.jar"
JVM_OPTS="$JVM_OPTS -Xbootclasspath/a:$DIR/build/test/lib/jars/simulator-bootstrap.jar"
JVM_OPTS="$JVM_OPTS -XX:ActiveProcessorCount=4"
JVM_OPTS="$JVM_OPTS -XX:-TieredCompilation"
JVM_OPTS="$JVM_OPTS -XX:Tier4CompileThreshold=1000"
JVM_OPTS="$JVM_OPTS -XX:ReservedCodeCacheSize=256M"
JVM_OPTS="$JVM_OPTS -Xmx8G"
JVM_OPTS="$JVM_OPTS -Dcassandra.test.simulator.determinismcheck=strict"
JVM_OPTS="$JVM_OPTS -Dcassandra.debugrefcount=false"
JVM_OPTS="$JVM_OPTS -Dcassandra.skip_sync=true"
JVM_OPTS="$JVM_OPTS -Dcassandra.tolerate_sstable_size=true"
JVM_OPTS="$JVM_OPTS -Dcassandra.test.simulator.debug=true"
JVM_OPTS="$JVM_OPTS -Dcassandra.test.simulator.determinismcheck=strict"
echo $JVM_OPTS

CLASSPATH="$DIR"/build/test/classes
for dir in "$DIR"/build/classes/*; do
    CLASSPATH="$CLASSPATH:$dir"
done

for jar in "$DIR"/lib/*.jar; do
  CLASSPATH="$CLASSPATH:$jar"
done
for jar in "$DIR"/build/*.jar; do
  if [[ $jar != *"logback-classic"* ]]; then
    CLASSPATH="$CLASSPATH:$jar"
  fi
done
for jar in "$DIR"/build/lib/jars/*.jar; do
  if [[ $jar != *"logback-classic"* ]]; then
    CLASSPATH="$CLASSPATH:$jar"
  fi
done
for jar in "$DIR"/build/test/lib/jars/*.jar; do
  if [[ $jar != *"logback-classic"* ]]; then
    CLASSPATH="$CLASSPATH:$jar"
  fi
done

CLASS="org.apache.cassandra.simulator.paxos.AccordSimulationRunner"
OPTS="run -n 3..6 -t 1000 --cluster-action-limit -1 -c 2 -s 30"

echo "java -cp <...> $CLASS $OPTS $@"

while true
do
  echo ""
  java -cp $CLASSPATH $JVM_OPTS $CLASS $OPTS $@
  status=$?
  if [ $status -ne 0 ] ; then
    exit $status
  fi

done
