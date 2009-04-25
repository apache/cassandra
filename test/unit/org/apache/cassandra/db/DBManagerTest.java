package org.apache.cassandra.db;

import org.testng.annotations.Test;
import org.apache.cassandra.ServerTest;

public class DBManagerTest extends ServerTest {
    @Test
    public void testMain() throws Throwable {
        // TODO clean up old detritus
        DBManager.instance().start();
    }
}
