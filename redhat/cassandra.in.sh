
# The directory where Cassandra's configs live (required)
CASSANDRA_CONF=/etc/cassandra/conf

# use JNA if installed in standard location
[ -r /usr/share/java/jna.jar ] && CLASSPATH="$CLASSPATH:/usr/share/java/jna.jar"

# The java classpath (required)
CLASSPATH=$CLASSPATH:$CASSANDRA_CONF

for jar in /usr/share/cassandra/lib/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done
