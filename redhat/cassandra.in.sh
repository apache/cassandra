
# The directory where Cassandra's configs live (required)
CASSANDRA_CONF=/etc/cassandra/conf

# The java classpath (required)
if [ -n "$CLASSPATH" ]; then
	CLASSPATH=$CLASSPATH:$CASSANDRA_CONF
else
	CLASSPATH=$CASSANDRA_CONF
fi

# use JNA if installed in standard location
[ -r /usr/share/java/jna.jar ] && CLASSPATH="$CLASSPATH:/usr/share/java/jna.jar"


for jar in /usr/share/cassandra/lib/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done
