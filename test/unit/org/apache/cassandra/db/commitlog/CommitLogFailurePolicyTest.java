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

package org.apache.cassandra.db.commitlog;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMMITLOG_STOP_ON_ERRORS;

public class CommitLogFailurePolicyTest
{
    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        COMMITLOG_STOP_ON_ERRORS.setBoolean(true);
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
    public void testCommitFailurePolicy_ignore_beforeStartup()
    {
        //startup was not completed successfuly (since method completeSetup() was not called)
        CassandraDaemon daemon = new CassandraDaemon();
        StorageService.instance.registerDaemon(daemon);

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        Config.CommitFailurePolicy oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.ignore);
            CommitLog.handleCommitError("Testing ignore policy", new Throwable());
            //even though policy is ignore, JVM must die because Daemon has not finished initializing
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
    public void testCommitFailurePolicy_ignore_afterStartup() throws Exception
    {
        CassandraDaemon daemon = new CassandraDaemon();
        daemon.completeSetup(); //startup must be completed, otherwise commit log failure must kill JVM regardless of failure policy
        StorageService.instance.registerDaemon(daemon);

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        Config.CommitFailurePolicy oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
        try
        {
            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.ignore);
            CommitLog.handleCommitError("Testing ignore policy", new Throwable());
            //error policy is set to IGNORE, so JVM must not be killed if error ocurs after startup
            Assert.assertFalse(killerForTests.wasKilled());
        }
        finally
        {
            DatabaseDescriptor.setCommitFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }
}
