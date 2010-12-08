
# The directory where Cassandra's configs live (required)
CASSANDRA_CONF=/etc/cassandra/conf

# The java classpath (required)
CLASSPATH=$CLASSPATH:$CASSANDRA_CONF

for jar in /usr/share/cassandra/lib/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done
