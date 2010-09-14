package org.apache.cassandra.gms;

public enum ApplicationState
{
    STATUS,
    LOAD,
    SCHEMA,
    // pad to allow adding new states to existing cluster
    X1,
    X2,
    X3,
    X4,
    X5,
}
