package org.apache.cassandra.service;

public interface NativeAccessMBean 
{
    boolean isAvailable();

    boolean isMemoryLockable();
}

