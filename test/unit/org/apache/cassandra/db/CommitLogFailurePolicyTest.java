/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.db;

import java.io.File;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManager;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;

public class CommitLogFailurePolicyTest
{

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        System.setProperty("cassandra.commitlog.stop_on_errors", "true");
    }

    @Test
    public void testCommitFailurePolicy_stop() throws ConfigurationException
    {
        CassandraDaemon daemon = new CassandraDaemon();
        daemon.completeSetup(); //startup must be completed, otherwise commit log failure must kill JVM regardless of failure policy
        StorageService.instance.registerDaemon(daemon);

        // Need storage service active so stop policy can shutdown gossip
        StorageService.instance.initServer();
        Assert.assertTrue(Gossiper.instance.isEnabled());

        Config.CommitFailurePolicy oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.stop);
            CommitLog.handleCommitError("Test stop error", new Throwable());
            Assert.assertFalse(Gossiper.instance.isEnabled());
        }
        finally
        {
            DatabaseDescriptor.setCommitFailurePolicy(oldPolicy);
        }
    }

    @Test
    public void testCommitFailurePolicy_die()
    {
        CassandraDaemon daemon = new CassandraDaemon();
        daemon.completeSetup(); //startup must be completed, otherwise commit log failure must kill JVM regardless of failure policy
        StorageService.instance.registerDaemon(daemon);

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        Config.CommitFailurePolicy oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.die);
            CommitLog.handleCommitError("Testing die policy", new Throwable());
            Assert.assertTrue(killerForTests.wasKilled());
            Assert.assertFalse(killerForTests.wasKilledQuietly()); //only killed quietly on startup failure
        }
        finally
        {
            DatabaseDescriptor.setCommitFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }

    @Test
    public void testCommitFailurePolicy_mustDieIfNotStartedUp()
    {
        //startup was not completed successfuly (since method completeSetup() was not called)
        CassandraDaemon daemon = new CassandraDaemon();
        StorageService.instance.registerDaemon(daemon);

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        Config.CommitFailurePolicy oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            //even though policy is ignore, JVM must die because Daemon has not finished initializing
            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.ignore);
            CommitLog.handleCommitError("Testing die policy", new Throwable());
            Assert.assertTrue(killerForTests.wasKilled());
            Assert.assertTrue(killerForTests.wasKilledQuietly()); //killed quietly due to startup failure
        }
        finally
        {
            DatabaseDescriptor.setCommitFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }

    @Test
    public void testCommitLogFailureBeforeInitialization_mustKillJVM() throws Exception
    {
        //startup was not completed successfuly (since method completeSetup() was not called)
        CassandraDaemon daemon = new CassandraDaemon();
        StorageService.instance.registerDaemon(daemon);

        //let's make the commit log directory non-writable
        File commitLogDir = new File(DatabaseDescriptor.getCommitLogLocation());
        commitLogDir.setWritable(false);

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        Config.CommitFailurePolicy oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.ignore);

            //now let's create a commit log segment manager and wait for it to fail
            new CommitLogSegmentManager(CommitLog.instance);

            //busy wait since commitlogsegmentmanager spawns another thread
            int retries = 0;
            while (!killerForTests.wasKilled() && retries++ < 5)
                Thread.sleep(10);

            //since failure was before CassandraDaemon startup, the JVM must be killed
            Assert.assertTrue(killerForTests.wasKilled());
            Assert.assertTrue(killerForTests.wasKilledQuietly()); //killed quietly due to startup failure
        }
        finally
        {
            DatabaseDescriptor.setCommitFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
            commitLogDir.setWritable(true);
        }
    }

    @Test
    public void testCommitLogFailureAfterInitialization_mustRespectFailurePolicy() throws Exception
    {
        //startup was not completed successfuly (since method completeSetup() was not called)
        CassandraDaemon daemon = new CassandraDaemon();
        daemon.completeSetup(); //startup must be completed, otherwise commit log failure must kill JVM regardless of failure policy
        StorageService.instance.registerDaemon(daemon);

        //let's make the commit log directory non-writable
        File commitLogDir = new File(DatabaseDescriptor.getCommitLogLocation());
        commitLogDir.setWritable(false);

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        Config.CommitFailurePolicy oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.ignore);

            //now let's create a commit log segment manager and wait for it to fail
            new CommitLogSegmentManager(CommitLog.instance);

            //wait commit log segment manager thread to execute
            Thread.sleep(50);

            //error policy is set to IGNORE, so JVM must not be killed if error ocurs after startup
            Assert.assertFalse(killerForTests.wasKilled());
        }
        finally
        {
            DatabaseDescriptor.setCommitFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
            commitLogDir.setWritable(true);
        }
    }
}
