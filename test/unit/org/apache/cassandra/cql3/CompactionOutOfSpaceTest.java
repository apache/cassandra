/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(BMUnitRunner.class)
public class CompactionOutOfSpaceTest extends CQLTester
{
    @BeforeClass
    public static void setupClass()
    {
        CQLTester.setUpClass();
        CQLTester.requireNetwork();
        CassandraDaemon d = new CassandraDaemon();
        d.activate();
        StorageService.instance.registerDaemon(d);
    }

    @Before
    public void setup()
    {
        // restart the services in case a previous test has stopped them

        if (!StorageService.instance.isNativeTransportRunning())
            StorageService.instance.startNativeTransport();

        if (!StorageService.instance.isGossipActive())
            StorageService.instance.startGossiping();
    }

    @Test
    @BMRule(name = "Simulate disk full during background compaction",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "AT ENTRY",
    action = "throw new java.io.IOError(new java.io.IOException(\"No space left on device\"))")
    public void testUcsBackgroundCompactionNoDiskSpaceIgnore() throws Throwable
    {
        String ucsCqlCompactionParams = "{'class':'UnifiedCompactionStrategy', 'num_shards':'1'}";
        flush4SstablesAndEnableAutoCompaction(Config.DiskFailurePolicy.ignore, ucsCqlCompactionParams);
    }

    @Test
    @BMRule(name = "Simulate disk full during background compaction",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "AT ENTRY",
    action = "throw new java.io.IOError(new java.io.IOException(\"No space left on device\"))")
    public void testUcsBackgroundCompactionNoDiskSpaceStop() throws Throwable
    {
        String ucsCqlCompactionParams = "{'class':'UnifiedCompactionStrategy', 'num_shards':'1'}";
        flush4SstablesAndEnableAutoCompaction(Config.DiskFailurePolicy.stop, ucsCqlCompactionParams);
    }

    @Test
    @BMRule(name = "Simulate disk full during background compaction",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "AT ENTRY",
    action = "throw new java.io.IOError(new java.io.IOException(\"No space left on device\"))")
    public void testUcsBackgroundCompactionNoDiskSpaceDie() throws Throwable
    {
        String ucsCqlCompactionParams = "{'class':'UnifiedCompactionStrategy', 'num_shards':'1'}";
        flush4SstablesAndEnableAutoCompaction(Config.DiskFailurePolicy.die, ucsCqlCompactionParams);
    }

    @Test
    @BMRule(name = "Simulate disk full during background compaction",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "AT ENTRY",
    action = "throw new java.io.IOError(new java.io.IOException(\"No space left on device\"))")
    public void testStcsBackgroundCompactionNoDiskSpaceIgnore() throws Throwable
    {
        String stcsCqlCompactionParams = "{'class':'SizeTieredCompactionStrategy', 'max_threshold':'4'}";
        flush4SstablesAndEnableAutoCompaction(Config.DiskFailurePolicy.ignore, stcsCqlCompactionParams);
    }

    @Test
    @BMRule(name = "Simulate disk full during background compaction",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "AT ENTRY",
    action = "throw new java.io.IOError(new java.io.IOException(\"No space left on device\"))")
    public void testStcsBackgroundCompactionNoDiskSpaceStop() throws Throwable
    {
        String stcsCqlCompactionParams = "{'class':'SizeTieredCompactionStrategy', 'max_threshold':'4'}";
        flush4SstablesAndEnableAutoCompaction(Config.DiskFailurePolicy.stop, stcsCqlCompactionParams);
    }

    @Test
    @BMRule(name = "Simulate disk full during background compaction",
    targetClass = "CompactionTask",
    targetMethod = "runMayThrow",
    targetLocation = "AT ENTRY",
    action = "throw new java.io.IOError(new java.io.IOException(\"No space left on device\"))")
    public void testStcsBackgroundCompactionNoDiskSpaceDie() throws Throwable
    {
        String stcsCqlCompactionParams = "{'class':'SizeTieredCompactionStrategy', 'max_threshold':'4'}";
        flush4SstablesAndEnableAutoCompaction(Config.DiskFailurePolicy.die, stcsCqlCompactionParams);
    }

    private void flush4SstablesAndEnableAutoCompaction(Config.DiskFailurePolicy policy, String cqlCompactionParams) throws Throwable
    {
        createTable("CREATE TABLE %s (k INT, c INT, v INT, PRIMARY KEY (k, c)) WITH compaction = " + cqlCompactionParams);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 1, 1);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 2, 2);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 3, 3);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 4, 4);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(new KillerForTests());
        Config.DiskFailurePolicy originalPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        try
        {
            DatabaseDescriptor.setDiskFailurePolicy(policy);
            cfs.enableAutoCompaction(true);
            verifyDiskFailurePolicy(policy);
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(originalPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }

    private void verifyDiskFailurePolicy(Config.DiskFailurePolicy policy)
    {
        switch (policy)
        {
            case stop:
            case stop_paranoid:
                verifyDiskFailurePolicyStop();
                break;
            case die:
                verifyDiskFailurePolicyDie();
                break;
            case best_effort:
                verifyDiskFailurePolicyBestEffort();
                break;
            case ignore:
                verifyDiskFailurePolicyIgnore();
                break;
            default:
                fail("Unsupported disk failure policy: " + policy);
                break;
        }
    }

    private void verifyDiskFailurePolicyStop()
    {
        verifyGossip(false);
        verifyNativeTransports(false);
        verifyJVMWasKilled(false);
    }

    private void verifyDiskFailurePolicyDie()
    {
        verifyJVMWasKilled(true);
    }

    private void verifyDiskFailurePolicyBestEffort()
    {
        assertFalse(Util.getDirectoriesWriteable(getCurrentColumnFamilyStore(KEYSPACE_PER_TEST)));
        FBUtilities.sleepQuietly(10); // give them a chance to stop before verifying they were not stopped
        verifyGossip(true);
        verifyNativeTransports(true);
        verifyJVMWasKilled(false);
    }

    private void verifyDiskFailurePolicyIgnore()
    {
        FBUtilities.sleepQuietly(10); // give them a chance to stop before verifying they were not stopped
        verifyGossip(true);
        verifyNativeTransports(true);
        verifyJVMWasKilled(false);
    }

    private void verifyJVMWasKilled(boolean killed)
    {
        KillerForTests killer = (KillerForTests) JVMStabilityInspector.killer();
        assertEquals(killed, killer.wasKilled());
        if (killed)
            assertFalse(killer.wasKilledQuietly()); // true only on startup
    }

    private void verifyGossip(boolean isEnabled)
    {
        assertEquals(isEnabled, Gossiper.instance.isEnabled());
    }

    private void verifyNativeTransports(boolean isRunning)
    {
        // Native transports are also stopped asynchronously, but isRunning is set synchronously
        assertEquals(isRunning, StorageService.instance.isNativeTransportRunning());

        // if the transport has been stopped, we wait for it to be fully stopped so that restarting it for
        // the next test will not fail due to the port being already in use
        if (!isRunning)
            StorageService.instance.stopNativeTransport();
    }
}
