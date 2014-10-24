package org.apache.cassandra.utils;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JVMStabilityInspectorTest
{
    @Test
    public void testKill() throws Exception
    {
        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);

        Config.DiskFailurePolicy oldPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        Config.CommitFailurePolicy oldCommitPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new IOException());
            assertFalse(killerForTests.wasKilled());

            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new OutOfMemoryError());
            assertTrue(killerForTests.wasKilled());

            DatabaseDescriptor.setDiskFailurePolicy(Config.DiskFailurePolicy.die);
            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new FSReadError(new IOException(), "blah"));
            assertTrue(killerForTests.wasKilled());

            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.die);
            killerForTests.reset();
            JVMStabilityInspector.inspectCommitLogThrowable(new Throwable());
            assertTrue(killerForTests.wasKilled());
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(originalKiller);
            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
            DatabaseDescriptor.setCommitFailurePolicy(oldCommitPolicy);
        }
    }

}
