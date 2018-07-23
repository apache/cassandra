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

# The java classpath (required)
CLASSPATH="$CASSANDRA_CONF"

# This can be the path to a jar file, or a directory containing the 
# compiled classes. NOTE: This isn't needed by the startup script,
# it's just used here in constructing the classpath.
if [ -d $CASSANDRA_HOME/build ] ; then
    #cassandra_bin="$CASSANDRA_HOME/build/classes/main"
    cassandra_bin=`ls -1 $CASSANDRA_HOME/build/apache-cassandra*.jar`

    CLASSPATH="$CLASSPATH:$cassandra_bin"
fi

# the default location for commitlogs, sstables, and saved caches
# if not set in cassandra.yaml
cassandra_storagedir="$CASSANDRA_HOME/data"

# JAVA_HOME can optionally be set here
#JAVA_HOME=/usr/local/jdk6

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
JAVA_AGENT="$JAVA_AGENT -javaagent:$CASSANDRA_HOME/lib/jamm-0.3.2.jar"

# Added sigar-bin to the java.library.path CASSANDRA-7838
JAVA_OPTS="$JAVA_OPTS:-Djava.library.path=$CASSANDRA_HOME/lib/sigar-bin"


#
# Java executable and per-Java version JVM settings
#

# Use JAVA_HOME if set, otherwise look for java in PATH
if [ -n "$JAVA_HOME" ]; then
    # Why we can't have nice things: Solaris combines x86 and x86_64
    # installations in the same tree, using an unconventional path for the
    # 64bit JVM.  Since we prefer 64bit, search the alternate path first,
    # (see https://issues.apache.org/jira/browse/CASSANDRA-4638).
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

if [ -z $JAVA ] ; then
    echo Unable to find java executable. Check JAVA_HOME and PATH environment variables. >&2
    exit 1;
fi

# Determine the sort of JVM we'll be running on.
java_ver_output=`"${JAVA:-java}" -version 2>&1`
jvmver=`echo "$java_ver_output" | grep '[openjdk|java] version' | awk -F'"' 'NR==1 {print $2}' | cut -d\- -f1`
JVM_VERSION=${jvmver%_*}

JAVA_VERSION=11
if [ "$JVM_VERSION" = "1.8.0" ]  ; then
    JVM_PATCH_VERSION=${jvmver#*_}
    if [ "$JVM_VERSION" \< "1.8" ] || [ "$JVM_VERSION" \> "1.8.2" ] ; then
        echo "Cassandra 4.0 requires either Java 8 (update 151 or newer) or Java 11 (or newer). Java $JVM_VERSION is not supported."
        exit 1;
    fi
    if [ "$JVM_PATCH_VERSION" -lt 151 ] ; then
        echo "Cassandra 4.0 requires either Java 8 (update 151 or newer) or Java 11 (or newer). Java 8 update $JVM_PATCH_VERSION is not supported."
        exit 1;
    fi
    JAVA_VERSION=8
elif [ "$JVM_VERSION" \< "11" ] ; then
    echo "Cassandra 4.0 requires either Java 8 (update 151 or newer) or Java 11 (or newer)."
    exit 1;
fi

jvm=`echo "$java_ver_output" | grep -A 1 '[openjdk|java] version' | awk 'NR==2 {print $1}'`
case "$jvm" in
    OpenJDK)
        JVM_VENDOR=OpenJDK
        # this will be "64-Bit" or "32-Bit"
        JVM_ARCH=`echo "$java_ver_output" | awk 'NR==3 {print $2}'`
        ;;
    "Java(TM)")
        JVM_VENDOR=Oracle
        # this will be "64-Bit" or "32-Bit"
        JVM_ARCH=`echo "$java_ver_output" | awk 'NR==3 {print $3}'`
        ;;
    *)
        # Help fill in other JVM values
        JVM_VENDOR=other
        JVM_ARCH=unknown
        ;;
esac

# Read user-defined JVM options from jvm-server.options file
JVM_OPTS_FILE=$CASSANDRA_CONF/jvm${jvmoptions_variant:--clients}.options
if [ $JAVA_VERSION -ge 11 ] ; then
    JVM_DEP_OPTS_FILE=$CASSANDRA_CONF/jvm11${jvmoptions_variant:--clients}.options
else
    JVM_DEP_OPTS_FILE=$CASSANDRA_CONF/jvm8${jvmoptions_variant:--clients}.options
fi

for opt in `grep "^-" $JVM_OPTS_FILE` `grep "^-" $JVM_DEP_OPTS_FILE`
do
  JVM_OPTS="$JVM_OPTS $opt"
done
