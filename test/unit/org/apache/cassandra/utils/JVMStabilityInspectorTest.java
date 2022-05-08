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
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.assertj.core.api.Assertions;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.DefaultFSErrorHandler;
import org.apache.cassandra.service.StorageService;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
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
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
        try
        {
            CassandraDaemon daemon = new CassandraDaemon();
            daemon.completeSetup();
            for (boolean daemonSetupCompleted : Arrays.asList(false, true))
            {
                // disk policy acts differently depending on if setup is complete or not; which is defined by
                // the daemon thread not being null
                StorageService.instance.registerDaemon(daemonSetupCompleted ? daemon : null);

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
                catch (Exception | Error e)
                {
                    throw new AssertionError("Failure when daemonSetupCompleted=" + daemonSetupCompleted, e);
                }
            }
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(originalKiller);
            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
            DatabaseDescriptor.setCommitFailurePolicy(oldCommitPolicy);
            StorageService.instance.registerDaemon(null);
            FileUtils.setFSErrorHandler(null);
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
    public void testForceHeapSpaceOom()
    {
        try
        {
            JVMStabilityInspector.inspectThrowable(new OutOfMemoryError("Direct buffer memory"));
            fail("The JVMStabilityInspector should force trigger a heap space OutOfMemoryError and delegate the handling to the JVM");
        }
        catch (Throwable e)
        {
            assertSame(e.getClass(), OutOfMemoryError.class);
            assertEquals("Java heap space", e.getMessage());
        }
    }

    @Test
    public void testForceHeapSpaceOomExclude()
    {
        OutOfMemoryError error = new OutOfMemoryError("Java heap space");
        Assertions.assertThatThrownBy(() -> JVMStabilityInspector.inspectThrowable(error))
                  .isInstanceOf(OutOfMemoryError.class)
                  .isEqualTo(error);
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
            JVMStabilityInspector.inspectThrowable(new SocketException());
            assertFalse(killerForTests.wasKilled());

            killerForTests.reset();
            JVMStabilityInspector.inspectThrowable(new FileNotFoundException());
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
