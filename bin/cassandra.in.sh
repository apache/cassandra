
cassandra_home=`dirname $0`/..

# The directory where Cassandra's configs live (required)
CASSANDRA_CONF=$cassandra_home/conf

# This can be the path to a jar file, or a directory containing the 
# compiled classes. NOTE: This isn't needed by the startup script,
# it's just used here in constructing the classpath.
cassandra_bin=$cassandra_home/build/classes
#cassandra_bin=$cassandra_home/build/cassandra.jar

# The java classpath (required)
CLASSPATH=$CASSANDRA_CONF:$cassandra_bin

for jar in $cassandra_home/lib/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

# Arguments to pass to the JVM
JVM_OPTS=" \
        -Xdebug \
        -Xrunjdwp:transport=dt_socket,server=y,address=8888,suspend=n \
        -Xms128M \
        -Xmx2G \
        -XX:SurvivorRatio=8 \
        -XX:TargetSurvivorRatio=90 \
        -XX:+AggressiveOpts \
        -XX:+UseParNewGC \
        -XX:+UseConcMarkSweepGC \
        -XX:CMSInitiatingOccupancyFraction=1 \
        -XX:+CMSParallelRemarkEnabled \
        -XX:+HeapDumpOnOutOfMemoryError \
        -Dcom.sun.management.jmxremote.port=8080 \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Dcom.sun.management.jmxremote.authenticate=false"
