/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.commitlog;


import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.apache.cassandra.CassandraIsolatedJunit4ClassRunner;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.JVMStabilityInspector;

@RunWith(CassandraIsolatedJunit4ClassRunner.class)
public class CommitLogInitWithExceptionTest
{
    private static Thread initThread;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();

        if (DatabaseDescriptor.getDiskFailurePolicy() == Config.DiskFailurePolicy.die ||
            DatabaseDescriptor.getDiskFailurePolicy() == Config.DiskFailurePolicy.ignore)
        {
            DatabaseDescriptor.setDiskFailurePolicy(Config.DiskFailurePolicy.stop);
        }

        DatabaseDescriptor.setCommitLogSegmentMgrProvider(c -> new MockCommitLogSegmentMgr(c, DatabaseDescriptor.getCommitLogLocation()));

        JVMStabilityInspector.killerHook = (t) -> {
            Assert.assertEquals("MOCK EXCEPTION: createSegment", t.getMessage());

            try
            {
                // Avoid JVM exit. The JVM still needs to run other junit tests.
                return false;
            }
            finally
            {
                Assert.assertNotNull(initThread);
                // We have to manually stop init thread because the JVM does not exit actually.
                initThread.stop();
            }
        };
    }

    @Test(timeout = 30000)
    public void testCommitLogInitWithException() {
        // This line will trigger initialization process because it's the first time to access CommitLog class.
        initThread = new Thread(CommitLog.instance::start);

        initThread.setName("initThread");
        initThread.start();

        try
        {
            initThread.join(); // Should not block here
        }
        catch (InterruptedException expected)
        {
        }

        Assert.assertFalse(initThread.isAlive());

        try
        {
            Thread.sleep(1000); // Wait for COMMIT-LOG-ALLOCATOR exit
        }
        catch (InterruptedException e)
        {
            Assert.fail();
        }

        Assert.assertEquals(Thread.State.TERMINATED, CommitLog.instance.segmentManager.managerThread.getState()); // exit successfully
    }

    private static class MockCommitLogSegmentMgr extends CommitLogSegmentManagerStandard {

        public MockCommitLogSegmentMgr(CommitLog commitLog, String storageDirectory)
        {
            super(commitLog, storageDirectory);
        }

        @Override
        public CommitLogSegment createSegment()
        {
            throw new RuntimeException("MOCK EXCEPTION: createSegment");
        }
    }

}
