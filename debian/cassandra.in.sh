
# The directory where Cassandra's configs live (required)
CASSANDRA_CONF=/etc/cassandra

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
if [ "$JVM_VENDOR" != "OpenJDK" -o "$JVM_VERSION" \> "1.6.0" ] \
      || [ "$JVM_VERSION" = "1.6.0" -a "$JVM_PATCH_VERSION" -ge 23 ]
then
    JAVA_AGENT="$JAVA_AGENT -javaagent:$CASSANDRA_HOME/lib/jamm-0.2.6.jar"
fi
