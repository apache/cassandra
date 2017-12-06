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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.FSErrorHandler;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class DefaultFSErrorHandlerTest
{
    private FSErrorHandler handler = new DefaultFSErrorHandler();
    Config.DiskFailurePolicy oldDiskPolicy;
    Config.CorruptSSTablePolicy oldSSTablePolicy;
    Object testPolicy;
    private boolean gossipRunning;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        CassandraDaemon daemon = new CassandraDaemon();
        daemon.completeSetup(); //startup must be completed, otherwise FS error will kill JVM regardless of failure policy
        StorageService.instance.registerDaemon(daemon);
        StorageService.instance.initServer();
    }

    @AfterClass
    public static void shutdown()
    {
        StorageService.instance.stopClient();
    }

    @Before
    public void setup()
    {
        StorageService.instance.startGossiping();
        assertTrue(Gossiper.instance.isEnabled());
        oldDiskPolicy = DatabaseDescriptor.getDiskFailurePolicy();
    }

    public DefaultFSErrorHandlerTest(Object policy,
                                     boolean gossipRunning)
    {
        this.testPolicy = policy;
        this.gossipRunning = gossipRunning;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][]{
            // tests for disk failure policies
            {Config.DiskFailurePolicy.die, false},
            {Config.DiskFailurePolicy.ignore, true},
            {Config.DiskFailurePolicy.stop, false},
            {Config.DiskFailurePolicy.stop_paranoid, false},
            {Config.DiskFailurePolicy.best_effort, true},
            // tests for corrupted SSTable policies
            {Config.CorruptSSTablePolicy.ignore, true},
            {Config.CorruptSSTablePolicy.stop, false},
            {Config.CorruptSSTablePolicy.die, false},
        }
        );
    }

    @After
    public void teardown()
    {
        DatabaseDescriptor.setDiskFailurePolicy(oldDiskPolicy);
        DatabaseDescriptor.setCorruptSSTablePolicy(oldSSTablePolicy);
    }

    @Test
    public void testFSErrors()
    {
        if (testPolicy instanceof Config.DiskFailurePolicy) {
            DatabaseDescriptor.setDiskFailurePolicy((Config.DiskFailurePolicy)testPolicy);
            handler.handleFSError(new FSReadError(new IOException(), "blah"));
            assertEquals(gossipRunning, Gossiper.instance.isEnabled());
        }
    }

    @Test
    public void testCorruptSSTableException()
    {
        if (testPolicy instanceof Config.CorruptSSTablePolicy) {
            DatabaseDescriptor.setCorruptSSTablePolicy((Config.CorruptSSTablePolicy) testPolicy);
            handler.handleCorruptSSTable(new CorruptSSTableException(new IOException(), "blah"));
            assertEquals(gossipRunning, Gossiper.instance.isEnabled());
        }
    }
}
