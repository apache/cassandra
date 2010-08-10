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


# The amount of memory to allocate to the JVM at startup, you almost
# certainly want to adjust this for your environment.
MAX_HEAP_SIZE="1G" 


# Here we create the arguments that will get passed to the jvm when
# starting cassandra.
JVM_OPTS="$JVM_OPTS -ea"

JVM_OPTS="$JVM_OPTS -Xms256M"
JVM_OPTS="$JVM_OPTS -Xmx$MAX_HEAP_SIZE"
JVM_OPTS="$JVM_OPTS -Xss128k" 

JVM_OPTS="$JVM_OPTS -XX:+UseParNewGC" 
JVM_OPTS="$JVM_OPTS -XX:+UseConcMarkSweepGC" 
JVM_OPTS="$JVM_OPTS -XX:+CMSParallelRemarkEnabled" 
JVM_OPTS="$JVM_OPTS -XX:SurvivorRatio=8" 
JVM_OPTS="$JVM_OPTS -XX:MaxTenuringThreshold=1" 
JVM_OPTS="$JVM_OPTS -XX:+HeapDumpOnOutOfMemoryError" 

JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.port=8080" 
JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl=false" 
JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=false" 
