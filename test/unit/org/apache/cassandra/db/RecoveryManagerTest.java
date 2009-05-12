package org.apache.cassandra.db;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;

public class RecoveryManagerTest extends CleanupHelper
{
    @Test
    public void testDoRecovery() throws IOException {
        // TODO nothing to recover
        RecoveryManager rm = RecoveryManager.instance();
        rm.doRecovery();  
    }
}
