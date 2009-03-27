package org.apache.cassandra.db;

import org.testng.annotations.Test;

import java.io.IOException;

import org.apache.cassandra.ServerTest;

public class RecoveryManagerTest extends ServerTest {
    @Test
    public void testDoRecovery() throws IOException {
        // TODO nothing to recover
        RecoveryManager rm = RecoveryManager.instance();
        rm.doRecovery();  
    }
}
