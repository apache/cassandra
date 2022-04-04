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

package org.apache.cassandra.db.guardrails;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.FileStore;
import java.util.Arrays;
import java.util.function.Consumer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.disk.usage.DiskUsageBroadcaster;
import org.apache.cassandra.service.disk.usage.DiskUsageMonitor;
import org.apache.cassandra.service.disk.usage.DiskUsageState;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.mockito.Mockito;

import static org.apache.cassandra.service.disk.usage.DiskUsageState.FULL;
import static org.apache.cassandra.service.disk.usage.DiskUsageState.NOT_AVAILABLE;
import static org.apache.cassandra.service.disk.usage.DiskUsageState.SPACIOUS;
import static org.apache.cassandra.service.disk.usage.DiskUsageState.STUFFED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests the guardrails for disk usage, {@link Guardrails#localDataDiskUsage} and {@link Guardrails#replicaDiskUsage}.
 */
@RunWith(BMUnitRunner.class)
@BMRule(name = "Always returns a physical disk size of 1000TiB",
targetClass = "DiskUsageMonitor",
targetMethod = "totalDiskSpace",
action = "return " + (1000L * 1024 * 1024 * 1024 * 1024) + "L;") // 1000TiB
public class GuardrailDiskUsageTest extends GuardrailTester
{
    private static int defaultDataDiskUsagePercentageWarnThreshold;
    private static int defaultDataDiskUsagePercentageFailThreshold;

    @BeforeClass
    public static void beforeClass()
    {
        defaultDataDiskUsagePercentageWarnThreshold = Guardrails.instance.getDataDiskUsagePercentageWarnThreshold();
        defaultDataDiskUsagePercentageFailThreshold = Guardrails.instance.getDataDiskUsagePercentageFailThreshold();

        Guardrails.instance.setDataDiskUsagePercentageThreshold(-1, -1);
    }

    @AfterClass
    public static void afterClass()
    {
        Guardrails.instance.setDataDiskUsagePercentageThreshold(defaultDataDiskUsagePercentageWarnThreshold,
                                                                defaultDataDiskUsagePercentageFailThreshold);
    }

    @Test
    public void testConfigValidation()
    {
        assertConfigValid(x -> x.setDataDiskUsageMaxDiskSize(null));
        assertNull(guardrails().getDataDiskUsageMaxDiskSize());

        assertConfigFails(x -> x.setDataDiskUsageMaxDiskSize("0B"), "0 is not allowed");
        assertConfigFails(x -> x.setDataDiskUsageMaxDiskSize("0KiB"), "0 is not allowed");
        assertConfigFails(x -> x.setDataDiskUsageMaxDiskSize("0MiB"), "0 is not allowed");
        assertConfigFails(x -> x.setDataDiskUsageMaxDiskSize("0GiB"), "0 is not allowed");

        assertConfigValid(x -> x.setDataDiskUsageMaxDiskSize("10B"));
        assertEquals("10B", guardrails().getDataDiskUsageMaxDiskSize());

        assertConfigValid(x -> x.setDataDiskUsageMaxDiskSize("20KiB"));
        assertEquals("20KiB", guardrails().getDataDiskUsageMaxDiskSize());

        assertConfigValid(x -> x.setDataDiskUsageMaxDiskSize("30MiB"));
        assertEquals("30MiB", guardrails().getDataDiskUsageMaxDiskSize());

        assertConfigValid(x -> x.setDataDiskUsageMaxDiskSize("40GiB"));
        assertEquals("40GiB", guardrails().getDataDiskUsageMaxDiskSize());

        long diskSize = DiskUsageMonitor.totalDiskSpace();
        String message = String.format("only %s are actually available on disk", FileUtils.stringifyFileSize(diskSize));
        assertConfigValid(x -> x.setDataDiskUsageMaxDiskSize(diskSize + "B"));
        assertConfigFails(x -> x.setDataDiskUsageMaxDiskSize(diskSize + 1 + "B"), message);
        // We want to test with very big number, Long.MAX_VALUE is not allowed so it was easy to use Intger.MAX_VALUE
        assertConfigFails(x -> x.setDataDiskUsageMaxDiskSize(Integer.MAX_VALUE + "GiB"), message);

        // warn threshold smaller than lower bound
        assertConfigFails(x -> x.setDataDiskUsagePercentageThreshold(0, 80), "0 is not allowed");

        // fail threshold bigger than upper bound
        assertConfigFails(x -> x.setDataDiskUsagePercentageThreshold(1, 110), "maximum allowed value is 100");

        // warn threshold larger than fail threshold
        assertConfigFails(x -> x.setDataDiskUsagePercentageThreshold(60, 50),
                          "The warn threshold 60 for data_disk_usage_percentage_warn_threshold should be lower than the fail threshold 50");
    }

    @Test
    public void testDiskUsageState()
    {
        guardrails().setDataDiskUsagePercentageThreshold(50, 90);

        // under usage
        assertEquals(SPACIOUS, DiskUsageMonitor.instance.getState(10));
        assertEquals(SPACIOUS, DiskUsageMonitor.instance.getState(50));

        // exceed warning threshold
        assertEquals(STUFFED, DiskUsageMonitor.instance.getState(51));
        assertEquals(STUFFED, DiskUsageMonitor.instance.getState(56));
        assertEquals(STUFFED, DiskUsageMonitor.instance.getState(90));

        // exceed fail threshold
        assertEquals(FULL, DiskUsageMonitor.instance.getState(91));
        assertEquals(FULL, DiskUsageMonitor.instance.getState(100));

        // shouldn't be possible to go over 100% but just to be sure
        assertEquals(FULL, DiskUsageMonitor.instance.getState(101));
        assertEquals(FULL, DiskUsageMonitor.instance.getState(500));
    }

    @Test
    public void testDiskUsageDetectorWarnDisabled()
    {
        guardrails().setDataDiskUsagePercentageThreshold(-1, 90);

        // under usage
        assertEquals(SPACIOUS, DiskUsageMonitor.instance.getState(0));
        assertEquals(SPACIOUS, DiskUsageMonitor.instance.getState(50));
        assertEquals(SPACIOUS, DiskUsageMonitor.instance.getState(90));

        // exceed fail threshold
        assertEquals(FULL, DiskUsageMonitor.instance.getState(91));
        assertEquals(FULL, DiskUsageMonitor.instance.getState(100));
    }

    @Test
    public void testDiskUsageDetectorFailDisabled()
    {
        guardrails().setDataDiskUsagePercentageThreshold(50, -1);

        // under usage
        assertEquals(SPACIOUS, DiskUsageMonitor.instance.getState(50));

        // exceed warning threshold
        assertEquals(STUFFED, DiskUsageMonitor.instance.getState(51));
        assertEquals(STUFFED, DiskUsageMonitor.instance.getState(80));
        assertEquals(STUFFED, DiskUsageMonitor.instance.getState(100));
    }

    @Test
    public void testDiskUsageGuardrailDisabled()
    {
        guardrails().setDataDiskUsagePercentageThreshold(-1, -1);

        assertEquals(NOT_AVAILABLE, DiskUsageMonitor.instance.getState(0));
        assertEquals(NOT_AVAILABLE, DiskUsageMonitor.instance.getState(60));
        assertEquals(NOT_AVAILABLE, DiskUsageMonitor.instance.getState(100));
    }

    @Test
    public void testMemtableSizeIncluded() throws Throwable
    {
        DiskUsageMonitor monitor = new DiskUsageMonitor();

        createTable(keyspace(), "CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = { 'enabled': false }");

        long memtableSizeBefore = monitor.getAllMemtableSize();
        int rows = 10;
        int mb = 1024 * 1024;

        for (int i = 0; i < rows; i++)
        {
            char[] chars = new char[mb];
            Arrays.fill(chars, (char) i);
            String value = String.copyValueOf(chars);
            execute("INSERT INTO %s (k, v) VALUES(?, ?)", i, value);
        }

        // verify memtables are included
        long memtableSizeAfterInsert = monitor.getAllMemtableSize();
        assertTrue(String.format("Expect at least 10MB more data, but got before: %s and after: %d",
                                 memtableSizeBefore, memtableSizeAfterInsert),
                   memtableSizeAfterInsert - memtableSizeBefore >= rows * mb);

        // verify memtable size are reduced after flush
        flush();
        long memtableSizeAfterFlush = monitor.getAllMemtableSize();
        assertEquals(memtableSizeBefore, memtableSizeAfterFlush, mb);
    }

    @Test
    public void testMonitorLogsOnStateChange()
    {
        guardrails().setDataDiskUsagePercentageThreshold(50, 90);

        Guardrails.localDataDiskUsage.resetLastNotifyTime();

        DiskUsageMonitor monitor = new DiskUsageMonitor();

        // transit to SPACIOUS, no logging
        assertMonitorStateTransition(0.50, SPACIOUS, monitor);

        // transit to STUFFED, expect warning
        assertMonitorStateTransition(0.50001, STUFFED, monitor, true, "Local data disk usage 51%(Stuffed) exceeds warning threshold of 50%");

        // remain as STUFFED, no logging because of min log interval
        assertMonitorStateTransition(0.90, STUFFED, monitor);

        // transit to FULL, expect failure
        assertMonitorStateTransition(0.90001, FULL, monitor, false, "Local data disk usage 91%(Full) exceeds failure threshold of 90%, will stop accepting writes");

        // remain as FULL, no logging because of min log interval
        assertMonitorStateTransition(0.99, FULL, monitor);

        // remain as FULL, no logging because of min log interval
        assertMonitorStateTransition(5.0, FULL, monitor);

        // transit back to STUFFED, no warning  because of min log interval
        assertMonitorStateTransition(0.90, STUFFED, monitor);

        // transit back to FULL, no logging  because of min log interval
        assertMonitorStateTransition(0.900001, FULL, monitor);

        // transit back to STUFFED, no logging  because of min log interval
        assertMonitorStateTransition(0.90, STUFFED, monitor);

        // transit to SPACIOUS, no logging
        assertMonitorStateTransition(0.50, SPACIOUS, monitor);
    }

    @Test
    public void testDiskUsageBroadcaster() throws UnknownHostException
    {
        DiskUsageBroadcaster broadcaster = new DiskUsageBroadcaster(null);
        Gossiper.instance.unregister(broadcaster);

        InetAddressAndPort node1 = InetAddressAndPort.getByName("127.0.0.1");
        InetAddressAndPort node2 = InetAddressAndPort.getByName("127.0.0.2");
        InetAddressAndPort node3 = InetAddressAndPort.getByName("127.0.0.3");

        // initially it's NOT_AVAILABLE
        assertFalse(broadcaster.hasStuffedOrFullNode());
        assertFalse(broadcaster.isFull(node1));
        assertFalse(broadcaster.isFull(node2));
        assertFalse(broadcaster.isFull(node3));

        // adding 1st node: Spacious, cluster has no Full node
        broadcaster.onChange(node1, ApplicationState.DISK_USAGE, value(SPACIOUS));
        assertFalse(broadcaster.hasStuffedOrFullNode());
        assertFalse(broadcaster.isFull(node1));

        // adding 2nd node with wrong ApplicationState
        broadcaster.onChange(node2, ApplicationState.RACK, value(FULL));
        assertFalse(broadcaster.hasStuffedOrFullNode());
        assertFalse(broadcaster.isFull(node2));

        // adding 2nd node: STUFFED
        broadcaster.onChange(node2, ApplicationState.DISK_USAGE, value(STUFFED));
        assertTrue(broadcaster.hasStuffedOrFullNode());
        assertTrue(broadcaster.isStuffed(node2));

        // adding 3rd node: FULL
        broadcaster.onChange(node3, ApplicationState.DISK_USAGE, value(FULL));
        assertTrue(broadcaster.hasStuffedOrFullNode());
        assertTrue(broadcaster.isFull(node3));

        // remove 2nd node, cluster has Full node
        broadcaster.onRemove(node2);
        assertTrue(broadcaster.hasStuffedOrFullNode());
        assertFalse(broadcaster.isStuffed(node2));

        // remove 3nd node, cluster has no Full node
        broadcaster.onRemove(node3);
        assertFalse(broadcaster.hasStuffedOrFullNode());
        assertFalse(broadcaster.isFull(node3));
    }

    @Test
    public void testDiskUsageCalculationWithMaxDiskSize() throws IOException
    {
        Directories.DataDirectory directory = mock(Directories.DataDirectory.class);
        when(directory.getRawSize()).thenReturn(new DataStorageSpec.LongBytesBound("5GiB").toBytes());

        FileStore store = mock(FileStore.class);
        when(store.getUsableSpace()).thenReturn(new DataStorageSpec.LongBytesBound("95GiB").toBytes()); // 100GiB disk - 5GiB

        Multimap<FileStore, Directories.DataDirectory> directories = HashMultimap.create();
        directories.put(store, directory);
        DiskUsageMonitor monitor = spy(new DiskUsageMonitor(() -> directories));

        doCallRealMethod().when(monitor).getDiskUsage();
        doReturn(0L).when(monitor).getAllMemtableSize();

        guardrails().setDataDiskUsageMaxDiskSize(null);
        assertThat(monitor.getDiskUsage()).isEqualTo(0.05);

        // 5G are used of 10G
        guardrails().setDataDiskUsageMaxDiskSize("10GiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(0.5);

        // max disk size = space used
        guardrails().setDataDiskUsageMaxDiskSize("5GiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(1.0);

        // max disk size < space used
        guardrails().setDataDiskUsageMaxDiskSize("1GiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(5.0);
    }

    @Test
    public void testDiskUsageCalculationWithMaxDiskSizeAndSmallUnits() throws IOException
    {
        // 5GiB used out of 100GiB disk
        long freeDiskSizeInBytes = new DataStorageSpec.LongBytesBound("100GiB").toBytes() - new DataStorageSpec.LongBytesBound("5MiB").toBytes();

        FileStore store = mock(FileStore.class);
        when(store.getUsableSpace()).thenReturn(new DataStorageSpec.LongBytesBound(freeDiskSizeInBytes + "B").toBytes()); // 100GiB disk

        Directories.DataDirectory directory = mock(Directories.DataDirectory.class);
        when(directory.getRawSize()).thenReturn(new DataStorageSpec.LongBytesBound("5MiB").toBytes());

        Multimap<FileStore, Directories.DataDirectory> directories = HashMultimap.create();
        directories.put(store, directory);
        DiskUsageMonitor monitor = spy(new DiskUsageMonitor(() -> directories));

        doCallRealMethod().when(monitor).getDiskUsage();
        doReturn(0L).when(monitor).getAllMemtableSize();

        guardrails().setDataDiskUsageMaxDiskSize(null);
        assertThat(monitor.getDiskUsage()).isEqualTo(0.00005);

        // 5MiB are used of 10MiB
        guardrails().setDataDiskUsageMaxDiskSize("10MiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(0.5);

        // max disk size = space used
        guardrails().setDataDiskUsageMaxDiskSize("5MiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(1.0);

        // max disk size < space used
        guardrails().setDataDiskUsageMaxDiskSize("1MiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(5.0);
    }

    @Test
    public void testDiskUsageCalculationWithMaxDiskSizeAndMultipleVolumes() throws IOException
    {
        Mockito.reset();

        Multimap<FileStore, Directories.DataDirectory> directories = HashMultimap.create();

        Directories.DataDirectory directory1 = mock(Directories.DataDirectory.class);
        FileStore store1 = mock(FileStore.class);
        when(directory1.getRawSize()).thenReturn(new DataStorageSpec.LongBytesBound("5GiB").toBytes());
        when(store1.getUsableSpace()).thenReturn(new DataStorageSpec.LongBytesBound("95GiB").toBytes()); // 100 GiB disk - 5 GiB
        directories.put(store1, directory1);

        Directories.DataDirectory directory2 = mock(Directories.DataDirectory.class);
        FileStore store2 = mock(FileStore.class);
        when(directory2.getRawSize()).thenReturn(new DataStorageSpec.LongBytesBound("25GiB").toBytes());
        when(store2.getUsableSpace()).thenReturn(new DataStorageSpec.LongBytesBound("75GiB").toBytes()); // 100 GiB disk - 25 GiB
        directories.put(store2, directory2);

        Directories.DataDirectory directory3 = mock(Directories.DataDirectory.class);
        FileStore store3 = mock(FileStore.class);
        when(directory3.getRawSize()).thenReturn(new DataStorageSpec.LongBytesBound("20GiB").toBytes());
        when(store3.getUsableSpace()).thenReturn(new DataStorageSpec.LongBytesBound("80GiB").toBytes()); // 100 GiB disk - 20 GiB
        directories.put(store3, directory3);

        DiskUsageMonitor monitor = spy(new DiskUsageMonitor(() -> directories));

        doCallRealMethod().when(monitor).getDiskUsage();
        doReturn(0L).when(monitor).getAllMemtableSize();

        // 50G/300G as each disk has a capacity of 100G
        guardrails().setDataDiskUsageMaxDiskSize(null);
        assertThat(monitor.getDiskUsage()).isEqualTo(0.16667);

        // 50G/100G
        guardrails().setDataDiskUsageMaxDiskSize("100GiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(0.5);

        // 50G/75G
        guardrails().setDataDiskUsageMaxDiskSize("75GiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(0.66667);

        // 50G/50G
        guardrails().setDataDiskUsageMaxDiskSize("50GiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(1.0);

        // 50G/49G
        guardrails().setDataDiskUsageMaxDiskSize("49GiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(1.02041);
    }

    @Test
    public void testDiskUsageCalculationWithMaxDiskSizeAndMultipleDirectories() throws IOException
    {
        Mockito.reset();

        Directories.DataDirectory directory1 = mock(Directories.DataDirectory.class);
        when(directory1.getRawSize()).thenReturn(new DataStorageSpec.LongBytesBound("5GiB").toBytes());

        Directories.DataDirectory directory2 = mock(Directories.DataDirectory.class);
        when(directory2.getRawSize()).thenReturn(new DataStorageSpec.LongBytesBound("25GiB").toBytes());

        Directories.DataDirectory directory3 = mock(Directories.DataDirectory.class);
        when(directory3.getRawSize()).thenReturn(new DataStorageSpec.LongBytesBound("20GiB").toBytes());

        FileStore store = mock(FileStore.class);
        when(store.getUsableSpace()).thenReturn(new DataStorageSpec.LongBytesBound("250GiB").toBytes()); // 100 GiB disk (300 - 5 - 25 - 20)

        Multimap<FileStore, Directories.DataDirectory> directories = HashMultimap.create();
        directories.putAll(store, ImmutableSet.of(directory1, directory2, directory3));

        DiskUsageMonitor monitor = spy(new DiskUsageMonitor(() -> directories));

        doCallRealMethod().when(monitor).getDiskUsage();
        doReturn(0L).when(monitor).getAllMemtableSize();

        // 50G/300G as disk has a capacity of 300G
        guardrails().setDataDiskUsageMaxDiskSize(null);
        assertThat(monitor.getDiskUsage()).isEqualTo(0.16667);

        // 50G/100G
        guardrails().setDataDiskUsageMaxDiskSize("100GiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(0.5);

        // 50G/75G
        guardrails().setDataDiskUsageMaxDiskSize("75GiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(0.66667);

        // 50G/50G
        guardrails().setDataDiskUsageMaxDiskSize("50GiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(1.0);

        // 50G/49G
        guardrails().setDataDiskUsageMaxDiskSize("49GiB");
        assertThat(monitor.getDiskUsage()).isEqualTo(1.02041);
    }

    @Test
    public void testWriteRequests() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        InetAddressAndPort node1 = InetAddressAndPort.getByName("127.0.0.11");
        InetAddressAndPort node2 = InetAddressAndPort.getByName("127.0.0.21");
        InetAddressAndPort node3 = InetAddressAndPort.getByName("127.0.0.31");

        Guardrails.replicaDiskUsage.resetLastNotifyTime();
        guardrails().setDataDiskUsagePercentageThreshold(98, 99);

        ConsistencyLevel cl = ConsistencyLevel.LOCAL_QUORUM;
        String select = "SELECT * FROM %s";
        String insert = "INSERT INTO %s (k, v) VALUES (0, 0)";
        String batch = "BEGIN BATCH " +
                       "INSERT INTO %s (k, v) VALUES (1, 1);" +
                       "INSERT INTO %<s (k, v) VALUES (2, 2); " +
                       "APPLY BATCH";
        CheckedFunction userSelect = () -> execute(userClientState, select, cl);
        CheckedFunction userInsert = () -> execute(userClientState, insert, cl);
        CheckedFunction userBatch = () -> execute(userClientState, batch, cl);

        // default state, write request works fine
        assertValid(userSelect);
        assertValid(userInsert);
        assertValid(userBatch);

        // verify node1 NOT_AVAILABLE won't affect writes
        setDiskUsageState(node1, NOT_AVAILABLE);
        assertValid(userSelect);
        assertValid(userInsert);
        assertValid(userBatch);

        // verify node2 Spacious won't affect writes
        setDiskUsageState(node2, SPACIOUS);
        assertValid(userSelect);
        assertValid(userInsert);
        assertValid(userBatch);

        // verify node3 STUFFED won't trigger warning as it's not write replica
        setDiskUsageState(node3, STUFFED);
        assertValid(userSelect);
        assertValid(userInsert);
        assertValid(userBatch);

        // verify node3 Full won't affect writes as it's not write replica
        setDiskUsageState(node3, FULL);
        assertValid(userSelect);
        assertValid(userInsert);
        assertValid(userBatch);

        // verify local node STUFF, will log warning
        setDiskUsageState(local, STUFFED);
        assertValid(userSelect);
        assertWarns(userInsert);
        assertWarns(userBatch);

        // verify local node Full, will reject writes
        setDiskUsageState(local, FULL);
        assertValid(userSelect);
        assertFails(userInsert);
        assertFails(userBatch);

        // excluded users can write to FULL cluster
        useSuperUser();
        Guardrails.replicaDiskUsage.resetLastNotifyTime();
        for (ClientState excludedUser : Arrays.asList(systemClientState, superClientState))
        {
            assertValid(() -> execute(excludedUser, select, cl));
            assertValid(() -> execute(excludedUser, insert, cl));
            assertValid(() -> execute(excludedUser, batch, cl));
        }

        // verify local node STUFFED won't reject writes
        setDiskUsageState(local, STUFFED);
        assertValid(userSelect);
        assertWarns(userInsert);
        assertWarns(userBatch);
    }

    @Override
    protected void assertValid(CheckedFunction function) throws Throwable
    {
        Guardrails.replicaDiskUsage.resetLastNotifyTime();
        super.assertValid(function);
    }

    protected void assertWarns(CheckedFunction function) throws Throwable
    {
        Guardrails.replicaDiskUsage.resetLastNotifyTime();
        super.assertWarns(function, "Replica disk usage exceeds warning threshold");
    }

    protected void assertFails(CheckedFunction function) throws Throwable
    {
        Guardrails.replicaDiskUsage.resetLastNotifyTime();
        super.assertFails(function, "Write request failed because disk usage exceeds failure threshold");
    }

    private static void setDiskUsageState(InetAddressAndPort endpoint, DiskUsageState state)
    {
        DiskUsageBroadcaster.instance.onChange(endpoint, ApplicationState.DISK_USAGE, value(state));
    }

    private static VersionedValue value(DiskUsageState state)
    {
        return StorageService.instance.valueFactory.diskUsage(state.name());
    }

    private void assertMonitorStateTransition(double usageRatio, DiskUsageState state, DiskUsageMonitor monitor)
    {
        assertMonitorStateTransition(usageRatio, state, monitor, false, null);
    }

    private void assertMonitorStateTransition(double usageRatio, DiskUsageState state, DiskUsageMonitor monitor,
                                              boolean isWarn, String msg)
    {
        boolean stateChanged = state != monitor.state();
        Consumer<DiskUsageState> notifier = newState -> {
            if (stateChanged)
                assertEquals(state, newState);
            else
                fail("Expect no notification if state remains the same");
        };

        monitor.updateLocalState(usageRatio, notifier);
        assertEquals(state, monitor.state());

        if (msg == null)
        {
            listener.assertNotFailed();
            listener.assertNotWarned();
        }
        else if (isWarn)
        {
            listener.assertWarned(msg);
            listener.assertNotFailed();
        }
        else
        {
            listener.assertFailed(msg);
            listener.assertNotWarned();
        }

        listener.clear();
    }
}
