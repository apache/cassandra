package org.apache.cassandra.hints;

/**
 * Marker interface for file positions as provided by the various ChecksummedDataReader implementations.
 */
public interface InputPosition
{
    long subtract(InputPosition other);
}
