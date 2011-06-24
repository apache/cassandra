
# The directory where Cassandra's configs live (required)
CASSANDRA_CONF=/etc/cassandra

CASSANDRA_HOME=/usr/share/cassandra

# The java classpath (required)
CLASSPATH=$CASSANDRA_CONF

for jar in /usr/share/cassandra/lib/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

for jar in /usr/share/cassandra/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done
