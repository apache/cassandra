
# The directory where Cassandra's configs live (required)
CASSANDRA_CONF=/etc/cassandra/conf

CASSANDRA_HOME=/usr/share/cassandra

# The java classpath (required)
if [ -n "$CLASSPATH" ]; then
    CLASSPATH=$CLASSPATH:$CASSANDRA_CONF
else
    CLASSPATH=$CASSANDRA_CONF
fi

for jar in /usr/share/cassandra/lib/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

for jar in /usr/share/cassandra/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

CLASSPATH="$CLASSPATH:$EXTRA_CLASSPATH"


# set JVM javaagent opts to avoid warnings/errors
JAVA_AGENT="$JAVA_AGENT -javaagent:$CASSANDRA_HOME/lib/jamm-0.3.2.jar"


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
