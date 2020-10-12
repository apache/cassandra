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
package org.apache.cassandra.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JVMStabilityInspectorTest
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

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

            DatabaseDescriptor.setDiskFailurePolicy(Config.DiskFailurePolicy.die);
            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new FSReadError(new IOException(), "blah"));
            assertTrue(killerForTests.wasKilled());

            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new FSWriteError(new IOException(), "blah"));
            assertTrue(killerForTests.wasKilled());

            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new CorruptSSTableException(new IOException(), "blah"));
            assertTrue(killerForTests.wasKilled());

            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new RuntimeException(new CorruptSSTableException(new IOException(), "blah")));
            assertTrue(killerForTests.wasKilled());

            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.die);
            killerForTests.reset();
            JVMStabilityInspector.inspectCommitLogThrowable(new Throwable());
            assertTrue(killerForTests.wasKilled());

            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new Exception(new IOException()));
            assertFalse(killerForTests.wasKilled());
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(originalKiller);
            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
            DatabaseDescriptor.setCommitFailurePolicy(oldCommitPolicy);
        }
    }

    @Test
    public void testOutOfMemoryHandling()
    {
        for (Throwable oom : asList(new OutOfMemoryError(), new Exception(new OutOfMemoryError())))
        {
            try
            {
                JVMStabilityInspector.inspectThrowable(oom);
                fail("The JVMStabilityInspector should delegate the handling of OutOfMemoryErrors to the JVM");
            }
            catch (OutOfMemoryError e)
            {
                assertTrue(true);
            }
        }
    }

    @Test
    public void fileHandleTest()
    {
        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);

        try
        {
            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new SocketException("Should not fail"));
            assertFalse(killerForTests.wasKilled());

            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new FileNotFoundException("Also should not fail"));
            assertFalse(killerForTests.wasKilled());

            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new SocketException("Too many open files"));
            assertTrue(killerForTests.wasKilled());

            killerForTests.reset();
            JVMStabilityInspector.inspectCommitLogThrowable(new FileNotFoundException("Too many open files"));
            assertTrue(killerForTests.wasKilled());
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }
}
