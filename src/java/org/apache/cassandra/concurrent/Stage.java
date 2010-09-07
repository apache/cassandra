package org.apache.cassandra.concurrent;

public enum Stage
{
    READ,
    MUTATION,
    STREAM,
    GOSSIP,
    RESPONSE,
    AE_SERVICE,
    MIGRATION,
    MISC,
}
