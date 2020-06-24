package org.apache.cassandra.hints;

/**
 * Marker interface for file positions as provided by the various ChecksummedDataReader implementations.
 */
public interface InputPosition
{
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11960
    long subtract(InputPosition other);
}
