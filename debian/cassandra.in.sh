
# The directory where Cassandra's configs live (required)
CASSANDRA_CONF=/etc/cassandra

# The java classpath (required)
CLASSPATH=$CASSANDRA_CONF

for jar in /usr/share/cassandra/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done
