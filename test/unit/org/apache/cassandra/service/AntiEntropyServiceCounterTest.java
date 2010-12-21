package org.apache.cassandra.service;

public class AntiEntropyServiceCounterTest extends AntiEntropyServiceTestAbstract
{
    public void init()
    {
        tablename = "Keyspace5";
        cfname    = "Counter1";
    }
}
