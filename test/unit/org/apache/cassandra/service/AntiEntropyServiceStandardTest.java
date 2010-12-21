package org.apache.cassandra.service;

public class AntiEntropyServiceStandardTest extends AntiEntropyServiceTestAbstract
{
    public void init()
    {
        tablename = "Keyspace5";
        cfname    = "Standard1";
    }
}
