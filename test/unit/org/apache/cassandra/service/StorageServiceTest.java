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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.ReplicaMultimap;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.transformations.Register;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StorageServiceTest extends TestBaseImpl
{
    static InetAddressAndPort aAddress;
    static InetAddressAndPort bAddress;
    static InetAddressAndPort cAddress;
    static InetAddressAndPort dAddress;
    static InetAddressAndPort eAddress;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        aAddress = InetAddressAndPort.getByName("127.0.0.1");
        bAddress = InetAddressAndPort.getByName("127.0.0.2");
        cAddress = InetAddressAndPort.getByName("127.0.0.3");
        dAddress = InetAddressAndPort.getByName("127.0.0.4");
        eAddress = InetAddressAndPort.getByName("127.0.0.5");

        ServerTestUtils.prepareServerNoRegister();
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);

        ClusterMetadataService.instance().commit(new Register(NodeAddresses.current(),
                                                              new Location(SimpleSnitch.DATA_CENTER_NAME, SimpleSnitch.RACK_NAME),
                                                              NodeVersion.CURRENT));
        IEndpointSnitch snitch = new AbstractEndpointSnitch()
        {
            public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
            {
                return 0;
            }

            public String getRack(InetAddressAndPort endpoint)
            {
                return "R1";
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return "DC1";
            }
        };

        DatabaseDescriptor.setEndpointSnitch(snitch);
        CommitLog.instance.start();
    }

    @Before
    public void setUp()
    {
        Rebuild.unsafeResetRebuilding();
    }

    public static <K, C extends ReplicaCollection<? extends C>>  void assertMultimapEqualsIgnoreOrder(ReplicaMultimap<K, C> a, ReplicaMultimap<K, C> b)
    {
        if (!a.keySet().equals(b.keySet()))
            fail(formatNeq(a, b));
        for (K key : a.keySet())
        {
            C ac = a.get(key);
            C bc = b.get(key);
            if (ac.size() != bc.size())
                fail(formatNeq(a, b));
            for (Replica r : ac)
            {
                if (!bc.contains(r))
                    fail(formatNeq(a, b));
            }
        }
    }

    public static String formatNeq(Object v1, Object v2)
    {
        return "\nExpected: " + formatClassAndValue(v1) + "\n but was: " + formatClassAndValue(v2);
    }

    public static String formatClassAndValue(Object value)
    {
        String className = value == null ? "null" : value.getClass().getName();
        return String.format("%s<%s>", className, value);
    }

    @Test
    public void testSetGetSSTablePreemptiveOpenIntervalInMB()
    {
        StorageService.instance.setSSTablePreemptiveOpenIntervalInMB(-1);
        assertEquals(-1, StorageService.instance.getSSTablePreemptiveOpenIntervalInMB());
    }

    @Test
    public void testScheduledExecutorsShutdownOnDrain() throws Throwable
    {
        final AtomicInteger numberOfRuns = new AtomicInteger(0);

        ScheduledFuture<?> f = ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(numberOfRuns::incrementAndGet,
                                                                                     0, 1, SECONDS);

        // Prove the task was scheduled more than once before checking cancelled.
        await("first run").atMost(1, MINUTES).until(() -> numberOfRuns.get() > 1);

        assertFalse(f.isCancelled());
        StorageService.instance.drain();
        assertTrue(f.isCancelled());

        assertTrue(ScheduledExecutors.scheduledTasks.isTerminated());
        assertTrue(ScheduledExecutors.nonPeriodicTasks.isTerminated());
        assertTrue(ScheduledExecutors.optionalTasks.isTerminated());

        // fast tasks are shut down as part of the Runtime shutdown hook.
        assertFalse(ScheduledExecutors.scheduledFastTasks.isTerminated());
    }

    @Test
    public void testRepairSessionMaximumTreeDepth()
    {
        StorageService storageService = StorageService.instance;
        int previousDepth = storageService.getRepairSessionMaximumTreeDepth();
        try
        {
            assertEquals(20, storageService.getRepairSessionMaximumTreeDepth());
            storageService.setRepairSessionMaximumTreeDepth(10);
            assertEquals(10, storageService.getRepairSessionMaximumTreeDepth());

            try
            {
                storageService.setRepairSessionMaximumTreeDepth(9);
                fail("Should have received a IllegalArgumentException for depth of 9");
            }
            catch (IllegalArgumentException ignored) { }
            assertEquals(10, storageService.getRepairSessionMaximumTreeDepth());

            try
            {
                storageService.setRepairSessionMaximumTreeDepth(-20);
                fail("Should have received a IllegalArgumentException for depth of -20");
            }
            catch (IllegalArgumentException ignored) { }
            assertEquals(10, storageService.getRepairSessionMaximumTreeDepth());

            storageService.setRepairSessionMaximumTreeDepth(22);
            assertEquals(22, storageService.getRepairSessionMaximumTreeDepth());
        }
        finally
        {
            storageService.setRepairSessionMaximumTreeDepth(previousDepth);
        }
    }

    @Test
    public void testColumnIndexSizeInKiB()
    {
        StorageService storageService = StorageService.instance;
        int previousColumnIndexSize = storageService.getColumnIndexSizeInKiB();
        try
        {
            storageService.setColumnIndexSizeInKiB(1024);
            assertEquals(1024, storageService.getColumnIndexSizeInKiB());

            try
            {
                storageService.setColumnIndexSizeInKiB(2 * 1024 * 1024);
                fail("Should have received an IllegalArgumentException column_index_size = 2GiB");
            }
            catch (IllegalArgumentException ignored) { }
            assertEquals(1024, storageService.getColumnIndexSizeInKiB());
        }
        finally
        {
            storageService.setColumnIndexSizeInKiB(previousColumnIndexSize);
        }
    }

    @Test
    public void testColumnIndexCacheSizeInKiB()
    {
        StorageService storageService = StorageService.instance;
        int previousColumnIndexCacheSize = storageService.getColumnIndexCacheSizeInKiB();
        try
        {
            storageService.setColumnIndexCacheSizeInKiB(1024);
            assertEquals(1024, storageService.getColumnIndexCacheSizeInKiB());

            try
            {
                storageService.setColumnIndexCacheSizeInKiB(2 * 1024 * 1024);
                fail("Should have received an IllegalArgumentException column_index_cache_size= 2GiB");
            }
            catch (IllegalArgumentException ignored) { }
            assertEquals(1024, storageService.getColumnIndexCacheSizeInKiB());
        }
        finally
        {
            storageService.setColumnIndexCacheSizeInKiB(previousColumnIndexCacheSize);
        }
    }

    @Test
    public void testBatchSizeWarnThresholdInKiB()
    {
        StorageService storageService = StorageService.instance;
        int previousBatchSizeWarnThreshold = storageService.getBatchSizeWarnThresholdInKiB();
        try
        {
            storageService.setBatchSizeWarnThresholdInKiB(1024);
            assertEquals(1024, storageService.getBatchSizeWarnThresholdInKiB());

            try
            {
                storageService.setBatchSizeWarnThresholdInKiB(2 * 1024 * 1024);
                fail("Should have received an IllegalArgumentException batch_size_warn_threshold = 2GiB");
            }
            catch (IllegalArgumentException ignored) { }
            assertEquals(1024, storageService.getBatchSizeWarnThresholdInKiB());
        }
        finally
        {
            storageService.setBatchSizeWarnThresholdInKiB(previousBatchSizeWarnThreshold);
        }
    }

    @Test
    public void testLocalDatacenterNodesExcludedDuringRebuild()
    {
        try
        {
            StorageService.instance.rebuild(DatabaseDescriptor.getLocalDataCenter(), "StorageServiceTest", null, null, true);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Cannot set source data center to be local data center, when excludeLocalDataCenter flag is set", e.getMessage());
        }
    }

    @Test
    public void testRebuildFailOnNonExistingDatacenter()
    {
        String nonExistentDC = "NON_EXISTENT_DC";

        try
        {
            StorageService.instance.rebuild(nonExistentDC, "StorageServiceTest", null, null, true, false);
            fail();
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals(String.format("Provided datacenter '%s' is not a valid datacenter, available datacenters are: %s",
                                       nonExistentDC,
                                       SimpleSnitch.DATA_CENTER_NAME),
                                ex.getMessage());
        }
    }

    @Test
    public void testRebuildingWithTokensWithoutKeyspace() throws Exception
    {
        try
        {
            StorageService.instance.rebuild("datacenter1", null, "123", null, false);
            fail();
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals("Cannot specify tokens without keyspace.", ex.getMessage());
        }
    }
}
