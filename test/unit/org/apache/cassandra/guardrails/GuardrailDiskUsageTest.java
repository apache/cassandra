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

package org.apache.cassandra.guardrails;


import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.disk.usage.DiskUsageBroadcaster;
import org.apache.cassandra.service.disk.usage.DiskUsageMonitor;
import org.apache.cassandra.service.disk.usage.DiskUsageState;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.service.disk.usage.DiskUsageState.FULL;
import static org.apache.cassandra.service.disk.usage.DiskUsageState.NOT_AVAILABLE;
import static org.apache.cassandra.service.disk.usage.DiskUsageState.SPACIOUS;
import static org.apache.cassandra.service.disk.usage.DiskUsageState.STUFFED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GuardrailDiskUsageTest extends GuardrailTester
{
    private static Integer defaultDiskUsagePercentageWarnThreshold;
    private static Integer defaultDiskUsagePercentageFailThreshold;

    @BeforeClass
    public static void beforeClass()
    {
        defaultDiskUsagePercentageWarnThreshold = DatabaseDescriptor.getGuardrailsConfig().disk_usage_percentage_warn_threshold;
        defaultDiskUsagePercentageFailThreshold = DatabaseDescriptor.getGuardrailsConfig().disk_usage_percentage_failure_threshold;

        DatabaseDescriptor.getGuardrailsConfig().disk_usage_percentage_warn_threshold = -1;
        DatabaseDescriptor.getGuardrailsConfig().disk_usage_percentage_failure_threshold = -1;
    }

    @AfterClass
    public static void afterClass()
    {
        DatabaseDescriptor.getGuardrailsConfig().disk_usage_percentage_warn_threshold = defaultDiskUsagePercentageWarnThreshold;
        DatabaseDescriptor.getGuardrailsConfig().disk_usage_percentage_failure_threshold = defaultDiskUsagePercentageFailThreshold;
    }

    @Test
    public void testConfigValidation()
    {
        GuardrailsConfig config = DatabaseDescriptor.getGuardrailsConfig();

        // warn threshold smaller than lower bound
        config.disk_usage_percentage_warn_threshold = 0;
        config.disk_usage_percentage_failure_threshold = 80;
        assertConfigFails(config::validateDiskUsageThreshold, "0 is not allowed");

        // fail threshold bigger than upper bound
        config.disk_usage_percentage_warn_threshold = 1;
        config.disk_usage_percentage_failure_threshold = 110;
        assertConfigFails(config::validateDiskUsageThreshold, "maximum allowed value is 100");

        // warn threshold larger than fail threshold
        config.disk_usage_percentage_warn_threshold = 60;
        config.disk_usage_percentage_failure_threshold = 50;
        assertConfigFails(config::validateDiskUsageThreshold, "60 for the disk_usage_percentage guardrail should be" +
                                                              " lower than the failure threshold 50");

        // disabled warn
        config.disk_usage_percentage_warn_threshold = -1;
        config.disk_usage_percentage_failure_threshold = 100;
        config.validateDiskUsageThreshold();
        assertTrue(GuardrailsConfig.diskUsageGuardrailDisabled(config.disk_usage_percentage_warn_threshold));
        assertFalse(GuardrailsConfig.diskUsageGuardrailDisabled(config.disk_usage_percentage_failure_threshold));

        // disabled fail
        config.disk_usage_percentage_warn_threshold = 20;
        config.disk_usage_percentage_failure_threshold = -1;
        config.validateDiskUsageThreshold();
        assertFalse(GuardrailsConfig.diskUsageGuardrailDisabled(config.disk_usage_percentage_warn_threshold));
        assertTrue(GuardrailsConfig.diskUsageGuardrailDisabled(config.disk_usage_percentage_failure_threshold));

        // disabled disk usage guardrail
        config.disk_usage_percentage_warn_threshold = -1;
        config.disk_usage_percentage_failure_threshold = -1;
        config.validateDiskUsageThreshold();
        assertTrue(GuardrailsConfig.diskUsageGuardrailDisabled(config.disk_usage_percentage_warn_threshold));
        assertTrue(GuardrailsConfig.diskUsageGuardrailDisabled(config.disk_usage_percentage_failure_threshold));
    }

    @Test
    public void testDiskUsageState()
    {
        GuardrailsConfig config = DatabaseDescriptor.getGuardrailsConfig();
        config.disk_usage_percentage_warn_threshold = 50;
        config.disk_usage_percentage_failure_threshold = 90;

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
    }

    @Test
    public void testDiskUsageDetectorWarnDisabled()
    {
        GuardrailsConfig config = DatabaseDescriptor.getGuardrailsConfig();
        config.disk_usage_percentage_warn_threshold = -1;
        config.disk_usage_percentage_failure_threshold = 90;

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
        GuardrailsConfig config = DatabaseDescriptor.getGuardrailsConfig();
        config.disk_usage_percentage_warn_threshold = 50;
        config.disk_usage_percentage_failure_threshold = -1;

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
        GuardrailsConfig config = DatabaseDescriptor.getGuardrailsConfig();
        config.disk_usage_percentage_warn_threshold = -1;
        config.disk_usage_percentage_failure_threshold = -1;

        assertEquals(NOT_AVAILABLE, DiskUsageMonitor.instance.getState(0));
        assertEquals(NOT_AVAILABLE, DiskUsageMonitor.instance.getState(60));
        assertEquals(NOT_AVAILABLE, DiskUsageMonitor.instance.getState(100));
    }

    @Test
    public void testMemtableSizeIncluded() throws Throwable
    {
        DiskUsageMonitor monitor = new DiskUsageMonitor();

        createTable(keyspace(), "CREATE TABLE %s (key text primary key, value text) with compression = { 'enabled' : false };");

        long memtableSizeBefore = monitor.getAllMemtableSize();
        int rows = 10;
        int mb = 1024 * 1024;

        for (int i = 0; i < rows; i++)
        {
            char[] chars = new char[mb];
            Arrays.fill(chars, (char) i);
            String value = String.copyValueOf(chars);
            execute("INSERT INTO %s (key, value) VALUES(?, ?)", i, value);
        }

        // verify memtables are included
        long memtableSizeAfterInsert = monitor.getAllMemtableSize();
        assertTrue("Expect at least 10MB more data, but got before: " + memtableSizeBefore + " and after: " + memtableSizeAfterInsert,
                   memtableSizeAfterInsert - memtableSizeBefore >= rows * mb);

        // verify memtable size are reduced after flush
        flush();
        long memtableSizeAfterFlush = monitor.getAllMemtableSize();
        assertEquals(memtableSizeBefore, memtableSizeAfterFlush, mb);
    }

    @Test
    public void testMonitorLogsOnStateChange()
    {
        GuardrailsConfig config = DatabaseDescriptor.getGuardrailsConfig();
        config.disk_usage_percentage_warn_threshold = 50;
        config.disk_usage_percentage_failure_threshold = 90;

        Guardrails.localDiskUsage.resetLastNotifyTime();

        DiskUsageMonitor monitor = new DiskUsageMonitor();

        // transit to SPACIOUS, no logging
        assertMonitorStateTransition(0.50, SPACIOUS, monitor);

        // transit to STUFFED, expect warning
        assertMonitorStateTransition(0.50001, STUFFED, monitor, true, "Local disk usage 51%(Stuffed) exceeds warn threshold of 50%");

        // remain as STUFFED, no logging because of min log interval
        assertMonitorStateTransition(0.90, STUFFED, monitor);

        // transit to FULL, expect failure
        assertMonitorStateTransition(0.90001, FULL, monitor, false, "Local disk usage 91%(Full) exceeds failure threshold of 90%, will stop accepting writes");

        // remain as FULL, no logging because of min log interval
        assertMonitorStateTransition(0.99, FULL, monitor);

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
    public void testWriteRequests() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (key int primary key, value int)");

        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        InetAddressAndPort node1 = InetAddressAndPort.getByName("127.0.0.11");
        InetAddressAndPort node2 = InetAddressAndPort.getByName("127.0.0.21");
        InetAddressAndPort node3 = InetAddressAndPort.getByName("127.0.0.31");

        // avoid noise due to test machines
        Guardrails.replicaDiskUsage.resetLastNotifyTime();
        GuardrailsConfig config = DatabaseDescriptor.getGuardrailsConfig();
        config.disk_usage_percentage_warn_threshold = 98;
        config.disk_usage_percentage_failure_threshold = 99;

        String warnMessage = "Replica disk usage exceeds warn threshold";
        String errorMessage = "Write request failed because disk usage exceeds failure threshold";

        CheckedFunction select = () -> {
            Statement statement = new SimpleStatement("SELECT * FROM " + keyspace() + "." + table);
            statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            executeNet(statement);
        };
        CheckedFunction insert = () -> {
            Statement statement = new SimpleStatement("INSERT INTO " + keyspace() + "." + table + " (key, value) VALUES(0, 0)");
            statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            executeNet(statement);
        };
        CheckedFunction batch = () -> {
            BatchStatement batchStatement = new BatchStatement();
            batchStatement.add(new SimpleStatement("INSERT INTO " + keyspace() + "." + table + " (key, value) VALUES(1, 1)"));
            batchStatement.add(new SimpleStatement("INSERT INTO " + keyspace() + "." + table + " (key, value) VALUES(2, 2)"));
            batchStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            executeNet(batchStatement);
        };

        // default state, write request works fine
        assertTrue(Guardrails.enabled());
        assertValid(select);
        assertValid(insert);
        assertValid(batch);

        // verify node1 NOT_AVAILABLE won't affect writes
        DiskUsageBroadcaster.instance.onChange(node1, ApplicationState.DISK_USAGE, value(NOT_AVAILABLE));
        assertValid(select);
        assertValid(insert);
        assertValid(batch);

        // verify node2 Spacious won't affect writes
        DiskUsageBroadcaster.instance.onChange(node2, ApplicationState.DISK_USAGE, value(SPACIOUS));
        assertValid(select);
        assertValid(insert);
        assertValid(batch);

        // verify node3 STUFFED won't trigger warning as it's not write replica
        DiskUsageBroadcaster.instance.onChange(node3, ApplicationState.DISK_USAGE, value(STUFFED));
        assertValid(select);
        assertValid(insert);
        assertValid(batch);

        // verify node3 Full won't affect writes as it's not write replica
        DiskUsageBroadcaster.instance.onChange(node3, ApplicationState.DISK_USAGE, value(FULL));
        assertValid(select);
        assertValid(insert);
        assertValid(batch);

        // verify local node STUFF, will log warning
        DiskUsageBroadcaster.instance.onChange(local, ApplicationState.DISK_USAGE, value(STUFFED));
        assertValid(select);
        Guardrails.replicaDiskUsage.resetLastNotifyTime();
        assertWarns(insert, warnMessage);
        Guardrails.replicaDiskUsage.resetLastNotifyTime();
        assertWarns(batch, warnMessage);

        // verify local node Full, will reject writes
        DiskUsageBroadcaster.instance.onChange(local, ApplicationState.DISK_USAGE, value(FULL));
        assertValid(select);
        Guardrails.replicaDiskUsage.resetLastNotifyTime();
        assertFails(insert, errorMessage);
        Guardrails.replicaDiskUsage.resetLastNotifyTime();
        assertFails(batch, errorMessage);

        // super user can insert to Full cluster
        useSuperUser();
        Guardrails.replicaDiskUsage.resetLastNotifyTime();
        assertValid(select);
        assertValid(insert);
        assertValid(batch);
        useUser(USERNAME, PASSWORD);

        // verify local node STUFFED won't reject writes
        DiskUsageBroadcaster.instance.onChange(local, ApplicationState.DISK_USAGE, value(STUFFED));
        assertValid(select);
        Guardrails.replicaDiskUsage.resetLastNotifyTime();
        assertWarns(insert, warnMessage);
        Guardrails.replicaDiskUsage.resetLastNotifyTime();
        assertWarns(batch, warnMessage);
    }

    private VersionedValue value(DiskUsageState state)
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


    protected void assertConfigFails(Runnable runnable, String message)
    {
        try
        {
            runnable.run();
            fail("Expected failure");
        }
        catch (ConfigurationException e)
        {
            String actualMessage = e.getMessage();
            assertTrue(String.format("Failure message '%s' does not contain expected message '%s'", actualMessage, message),
                       actualMessage.contains(message));
        }
    }
}