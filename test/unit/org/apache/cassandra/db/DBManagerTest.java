package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;

public class DBManagerTest extends CleanupHelper
{
    @Test
    public void testMain() throws Throwable {
        // TODO clean up old detritus
        DBManager.instance().start();
    }
}
