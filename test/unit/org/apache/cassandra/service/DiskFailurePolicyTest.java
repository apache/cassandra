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

package org.apache.cassandra.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;

@RunWith(Parameterized.class)
public class DiskFailurePolicyTest
{
    DiskFailurePolicy originalDiskFailurePolicy;
    JVMStabilityInspector.Killer originalKiller;
    KillerForTests killerForTests;
    DiskFailurePolicy testPolicy;
    boolean isStartUpInProgress;
    Throwable t;
    boolean expectGossipRunning;
    boolean expectJVMKilled;
    boolean expectJVMKilledQuiet;


    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
    }

    public DiskFailurePolicyTest(DiskFailurePolicy testPolicy, boolean isStartUpInProgress, Throwable t,
                                 boolean expectGossipRunning, boolean jvmKilled, boolean jvmKilledQuiet)
    {
        this.testPolicy = testPolicy;
        this.isStartUpInProgress = isStartUpInProgress;
        this.t = t;
        this.expectGossipRunning = expectGossipRunning;
        this.expectJVMKilled = jvmKilled;
        this.expectJVMKilledQuiet = jvmKilledQuiet;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][]{
                             { Config.DiskFailurePolicy.die, true, new FSReadError(new IOException(), "blah"), false, true, true},
                             { DiskFailurePolicy.ignore, true, new FSReadError(new IOException(), "blah"), true, false, false},
                             { DiskFailurePolicy.stop, true, new FSReadError(new IOException(), "blah"), false, true, true},
                             { DiskFailurePolicy.stop_paranoid, true, new FSReadError(new IOException(), "blah"), false, true, true},
                             { Config.DiskFailurePolicy.die, true, new CorruptSSTableException(new IOException(), "blah"), false, true, true},
                             { DiskFailurePolicy.ignore, true, new CorruptSSTableException(new IOException(), "blah"), true, false, false},
                             { DiskFailurePolicy.stop, true, new CorruptSSTableException(new IOException(), "blah"), false, true, true},
                             { DiskFailurePolicy.stop_paranoid, true, new CorruptSSTableException(new IOException(), "blah"), false, true, true},
                             { Config.DiskFailurePolicy.die, false, new FSReadError(new IOException(), "blah"), false, true, false},
                             { DiskFailurePolicy.ignore, false, new FSReadError(new IOException(), "blah"), true, false, false},
                             { DiskFailurePolicy.stop, false, new FSReadError(new IOException(), "blah"), false, false, false},
                             { DiskFailurePolicy.stop_paranoid, false, new FSReadError(new IOException(), "blah"), false, false, false},
                             { Config.DiskFailurePolicy.die, false, new CorruptSSTableException(new IOException(), "blah"), false, true, false},
                             { DiskFailurePolicy.ignore, false, new CorruptSSTableException(new IOException(), "blah"), true, false, false},
                             { DiskFailurePolicy.stop, false, new CorruptSSTableException(new IOException(), "blah"), true, false, false},
                             { DiskFailurePolicy.stop_paranoid, false, new CorruptSSTableException(new IOException(), "blah"), false, false, false}
                             }
        );
    }

    @Before
    public void setup()
    {
        CassandraDaemon daemon = new CassandraDaemon();
        if (!isStartUpInProgress)
            daemon.completeSetup(); //mark startup completed
        StorageService.instance.registerDaemon(daemon);
        killerForTests = new KillerForTests();
        originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        originalDiskFailurePolicy = DatabaseDescriptor.getDiskFailurePolicy();
        StorageService.instance.startGossiping();
        Assert.assertTrue(Gossiper.instance.isEnabled());
    }

    @After
    public void teardown()
    {
        JVMStabilityInspector.replaceKiller(originalKiller);
        DatabaseDescriptor.setDiskFailurePolicy(originalDiskFailurePolicy);
    }

    @Test
    public void testPolicies()
    {
        DatabaseDescriptor.setDiskFailurePolicy(testPolicy);
        JVMStabilityInspector.inspectThrowable(t);
        Assert.assertEquals(expectJVMKilled, killerForTests.wasKilled());
        Assert.assertEquals(expectJVMKilledQuiet, killerForTests.wasKilledQuietly());
        if (!expectJVMKilled) {
            // only verify gossip if JVM is not killed
            Assert.assertEquals(expectGossipRunning, Gossiper.instance.isEnabled());
        }
    }
}
