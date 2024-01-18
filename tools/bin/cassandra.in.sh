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
    CASSANDRA_HOME="`dirname $0`/../.."
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
    cassandra_bin="$cassandra_bin:$CASSANDRA_HOME/build/classes/stress:$CASSANDRA_HOME/build/classes/fqltool"
    CLASSPATH="$CLASSPATH:$cassandra_bin"
fi

# the default location for commitlogs, sstables, and saved caches
# if not set in cassandra.yaml
cassandra_storagedir="$CASSANDRA_HOME/data"

# JAVA_HOME can optionally be set here
#JAVA_HOME=/usr/local/jdk11

for jar in "$CASSANDRA_HOME"/tools/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done
for jar in "$CASSANDRA_HOME"/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done


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
    JAVA=`command -v java 2> /dev/null`
fi

if [ -z $JAVA ] ; then
    echo Unable to find java executable. Check JAVA_HOME and PATH environment variables. >&2
    exit 1;
fi

# Matches variable 'java.supported' in build.xml
java_versions_supported=11,17

# Determine the sort of JVM we'll be running on.
java_ver_output=`"${JAVA:-java}" -version 2>&1`
jvmver=`echo "$java_ver_output" | grep '[openjdk|java] version' | awk -F'"' 'NR==1 {print $2}' | cut -d\- -f1`
JVM_VERSION=${jvmver%_*}
short=$(echo "${jvmver}" | cut -c1-2)

# Unsupported JDKs below the upper supported version are not allowed
if [ "$short" != "$(echo "$java_versions_supported" | cut -d, -f1)" ] && [ "$JVM_VERSION" \< "$(echo "$java_versions_supported" | cut -d, -f2)" ] ; then
    echo "Unsupported Java $JVM_VERSION. Supported are $java_versions_supported"
    exit 1;
fi

# Allow execution of supported Java versions, and newer if CASSANDRA_JDK_UNSUPPORTED is set
is_supported_version=$(echo "$java_versions_supported" | tr "," '\n' | grep -F -x "$short")
if [ -z "$is_supported_version" ] ; then
   if [ -z "$CASSANDRA_JDK_UNSUPPORTED" ] ; then
        echo "Unsupported Java $JVM_VERSION. Supported are $java_versions_supported"
        echo "If you would like to test with newer Java versions set CASSANDRA_JDK_UNSUPPORTED to any value (for example, CASSANDRA_JDK_UNSUPPORTED=true). Unset the parameter for default behavior"
        exit 1;
    else
        echo "######################################################################"
        echo "Warning! You are using JDK$short. This Cassandra version only supports $java_versions_supported."
        echo "######################################################################"
    fi
fi
JAVA_VERSION=$short

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
if [ $JAVA_VERSION -ge 17 ] ; then
    JVM_DEP_OPTS_FILE=$CASSANDRA_CONF/jvm17${jvmoptions_variant:--clients}.options
elif [ $JAVA_VERSION -ge 11 ] ; then
    JVM_DEP_OPTS_FILE=$CASSANDRA_CONF/jvm11${jvmoptions_variant:--clients}.options
fi

for opt in `grep "^-" $JVM_OPTS_FILE` `grep "^-" $JVM_DEP_OPTS_FILE`
do
  JVM_OPTS="$JVM_OPTS $opt"
done
