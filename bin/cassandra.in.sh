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

if [ "x$CASSANDRA_HOME" = "x" ]; then
    CASSANDRA_HOME="`dirname "$0"`/.."
fi

# The directory where Cassandra's configs live (required)
if [ "x$CASSANDRA_CONF" = "x" ]; then
    CASSANDRA_CONF="$CASSANDRA_HOME/conf"
fi

# This can be the path to a jar file, or a directory containing the 
# compiled classes. NOTE: This isn't needed by the startup script,
# it's just used here in constructing the classpath.
cassandra_bin="$CASSANDRA_HOME/build/classes/main"
cassandra_bin="$cassandra_bin:$CASSANDRA_HOME/build/classes/thrift"
#cassandra_bin="$CASSANDRA_HOME/build/cassandra.jar"

# the default location for commitlogs, sstables, and saved caches
# if not set in cassandra.yaml
cassandra_storagedir="$CASSANDRA_HOME/data"

# JAVA_HOME can optionally be set here
#JAVA_HOME=/usr/local/jdk6

# The java classpath (required)
CLASSPATH="$CASSANDRA_CONF:$cassandra_bin"

for jar in "$CASSANDRA_HOME"/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

# JSR223 - collect all JSR223 engines' jars
for jsr223jar in "$CASSANDRA_HOME"/lib/jsr223/*/*.jar; do
    CLASSPATH="$CLASSPATH:$jsr223jar"
done
# JSR223/JRuby - set ruby lib directory
if [ -d "$CASSANDRA_HOME"/lib/jsr223/jruby/ruby ] ; then
    export JVM_OPTS="$JVM_OPTS -Djruby.lib=$CASSANDRA_HOME/lib/jsr223/jruby"
fi
# JSR223/JRuby - set ruby JNI libraries root directory
if [ -d "$CASSANDRA_HOME"/lib/jsr223/jruby/jni ] ; then
    export JVM_OPTS="$JVM_OPTS -Djffi.boot.library.path=$CASSANDRA_HOME/lib/jsr223/jruby/jni"
fi
# JSR223/Jython - set python.home system property
if [ -f "$CASSANDRA_HOME"/lib/jsr223/jython/jython.jar ] ; then
    export JVM_OPTS="$JVM_OPTS -Dpython.home=$CASSANDRA_HOME/lib/jsr223/jython"
fi
# JSR223/Scala - necessary system property
if [ -f "$CASSANDRA_HOME"/lib/jsr223/scala/scala-compiler.jar ] ; then
    export JVM_OPTS="$JVM_OPTS -Dscala.usejavacp=true"
fi

# set JVM javaagent opts to avoid warnings/errors
if [ "$JVM_VENDOR" != "OpenJDK" -o "$JVM_VERSION" \> "1.6.0" ] \
      || [ "$JVM_VERSION" = "1.6.0" -a "$JVM_PATCH_VERSION" -ge 23 ]
then
    JAVA_AGENT="$JAVA_AGENT -javaagent:$CASSANDRA_HOME/lib/jamm-0.3.0.jar"
fi

# Added sigar-bin to the java.library.path CASSANDRA-7838
JAVA_OPTS="$JAVA_OPTS:-Djava.library.path=$CASSANDRA_HOME/lib/sigar-bin"
