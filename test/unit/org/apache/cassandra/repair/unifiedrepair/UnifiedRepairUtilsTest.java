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

package org.apache.cassandra.repair.unifiedrepair;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.unifiedrepair.UnifiedRepairConfig.RepairType;
import org.apache.cassandra.repair.unifiedrepair.UnifiedRepairUtils.UnifiedRepairHistory;
import org.apache.cassandra.repair.unifiedrepair.UnifiedRepairUtils.CurrentRepairStatus;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.apache.cassandra.Util.setUnifiedRepairEnabled;
import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_DEFAULT_RF;
import static org.apache.cassandra.repair.unifiedrepair.UnifiedRepairUtils.COL_DELETE_HOSTS;
import static org.apache.cassandra.repair.unifiedrepair.UnifiedRepairUtils.COL_FORCE_REPAIR;
import static org.apache.cassandra.repair.unifiedrepair.UnifiedRepairUtils.COL_REPAIR_FINISH_TS;
import static org.apache.cassandra.repair.unifiedrepair.UnifiedRepairUtils.COL_REPAIR_PRIORITY;
import static org.apache.cassandra.repair.unifiedrepair.UnifiedRepairUtils.COL_REPAIR_START_TS;
import static org.apache.cassandra.repair.unifiedrepair.UnifiedRepairUtils.COL_REPAIR_TURN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class UnifiedRepairUtilsTest extends CQLTester
{
    static RepairType repairType = RepairType.incremental;
    static UUID hostId;

    static InetAddressAndPort localEndpoint;

    @Mock
    static IEndpointSnitch snitchMock;

    static IEndpointSnitch defaultSnitch;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
        setUnifiedRepairEnabled(true);
        requireNetwork();
        defaultSnitch = DatabaseDescriptor.getEndpointSnitch();
        localEndpoint = FBUtilities.getBroadcastAddressAndPort();
        hostId = StorageService.instance.getHostIdForEndpoint(localEndpoint);
        StorageService.instance.doUnifiedRepairSetup();
    }

    @Before
    public void setup()
    {
        SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
        QueryProcessor.executeInternal(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", "ks"));
        QueryProcessor.executeInternal(String.format("CREATE TABLE %s.%s (k text, s text static, i int, v text, primary key(k,i))", "ks", "tbl"));

        UnifiedRepair.SLEEP_IF_REPAIR_FINISHES_QUICKLY = new DurationSpec.IntSecondsBound("0s");
        MockitoAnnotations.initMocks(this);
        DatabaseDescriptor.setEndpointSnitch(defaultSnitch);
        QueryProcessor.executeInternal(String.format(
        "TRUNCATE %s.%s",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY));
        QueryProcessor.executeInternal(String.format(
        "TRUNCATE %s.%s",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_PRIORITY));
    }

    @Test
    public void testSetForceRepair()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, host_id, force_repair) VALUES ('%s', %s, false)",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId));

        UnifiedRepairUtils.setForceRepair(repairType, ImmutableSet.of(localEndpoint));

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT force_repair FROM %s.%s WHERE repair_type = '%s' AND host_id = %s",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId));
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.one().getBoolean(COL_FORCE_REPAIR));
    }

    @Test
    public void testSetForceRepairNewNode()
    {
        UnifiedRepairUtils.setForceRepairNewNode(repairType);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT force_repair FROM %s.%s WHERE repair_type = '%s' AND host_id = %s",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId));
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.one().getBoolean(COL_FORCE_REPAIR));
    }


    @Test
    public void testClearDeleteHosts()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, host_id, delete_hosts, delete_hosts_update_time) VALUES ('%s', %s, { %s }, toTimestamp(now()))",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId, hostId));

        UnifiedRepairUtils.clearDeleteHosts(repairType, hostId);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT delete_hosts FROM %s.%s WHERE repair_type = '%s' AND host_id = %s",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId));
        assertNotNull(result);
        assertEquals(1, result.size());
        Set<UUID> deleteHosts = result.one().getSet(COL_DELETE_HOSTS, UUIDType.instance);
        assertNull(deleteHosts);
    }

    @Test
    public void testGetUnifiedRepairHistoryForLocalGroup()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, host_id, force_repair) VALUES ('%s', %s, false)",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId));

        List<UnifiedRepairHistory> history = UnifiedRepairUtils.getUnifiedRepairHistory(repairType);
        assertNotNull(history);
        assertEquals(1, history.size());
        assertEquals(hostId, history.get(0).hostId);
    }

    @Test
    public void testGetUnifiedRepairHistoryForLocalGroup_empty_history()
    {
        List<UnifiedRepairHistory> history = UnifiedRepairUtils.getUnifiedRepairHistory(repairType);

        assertNull(history);
    }

    @Test
    public void testGetCurrentRepairStatus()
    {
        UUID forceRepair = UUID.randomUUID();
        UUID regularRepair = UUID.randomUUID();
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, host_id) VALUES ('%s', %s)",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId));
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, host_id, force_repair, repair_start_ts) VALUES ('%s', %s, true, toTimestamp(now()))",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), forceRepair));
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, host_id, repair_start_ts) VALUES ('%s', %s, toTimestamp(now()))",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), regularRepair));
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, repair_priority) VALUES ('%s', { %s })",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_PRIORITY,
        repairType.toString(), regularRepair));

        CurrentRepairStatus status = UnifiedRepairUtils.getCurrentRepairStatus(repairType);

        assertNotNull(status);
        assertEquals(1, status.historiesWithoutOnGoingRepair.size());
        assertEquals(hostId, status.historiesWithoutOnGoingRepair.get(0).hostId);
        assertEquals(1, status.hostIdsWithOnGoingRepair.size());
        assertTrue(status.hostIdsWithOnGoingRepair.contains(regularRepair));
        assertEquals(1, status.hostIdsWithOnGoingForceRepair.size());
        assertTrue(status.hostIdsWithOnGoingForceRepair.contains(forceRepair));
        assertEquals(1, status.priority.size());
        assertTrue(status.priority.contains(regularRepair));
    }

    @Test
    public void testGetHostIdsInCurrentRing()
    {
        TreeSet<UUID> hosts = UnifiedRepairUtils.getHostIdsInCurrentRing(repairType);

        assertNotNull(hosts);
        assertEquals(1, hosts.size());
        assertTrue(hosts.contains(hostId));
    }

    @Test
    public void testGetHostIdsInCurrentRing_multiple_nodes()
    {
        InetAddressAndPort ignoredEndpoint = localEndpoint.withPort(localEndpoint.getPort() + 1);
        InetAddressAndPort deadEndpoint = localEndpoint.withPort(localEndpoint.getPort() + 2);
        DatabaseDescriptor.getUnifiedRepairConfig().setIgnoreDCs(repairType, ImmutableSet.of("dc2"));
        DatabaseDescriptor.setEndpointSnitch(snitchMock);
        when(snitchMock.getDatacenter(localEndpoint)).thenReturn("dc1");
        when(snitchMock.getDatacenter(ignoredEndpoint)).thenReturn("dc2");
        when(snitchMock.getDatacenter(deadEndpoint)).thenReturn("dc1");

        TreeSet<UUID> hosts = UnifiedRepairUtils.getHostIdsInCurrentRing(repairType, ImmutableSet.of(new NodeAddresses(localEndpoint), new NodeAddresses(ignoredEndpoint), new NodeAddresses(deadEndpoint)));

        assertNotNull(hosts);
        assertEquals(1, hosts.size());
        assertTrue(hosts.contains(hostId));
    }

    @Test
    public void testGetHostWithLongestUnrepairTime()
    {
        UUID otherHostId = UUID.randomUUID();
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, host_id) VALUES ('%s', %s)",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId));
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, host_id, repair_finish_ts) VALUES ('%s', %s, toTimestamp(now()))",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), otherHostId));

        UnifiedRepairHistory history = UnifiedRepairUtils.getHostWithLongestUnrepairTime(repairType);

        assertEquals(hostId, history.hostId);
    }

    @Test
    public void testGetMaxNumberOfNodeRunUnifiedRepairInGroup_0_group_size()
    {
        DatabaseDescriptor.getUnifiedRepairConfig().setParallelRepairCount(repairType, 2);

        int count = UnifiedRepairUtils.getMaxNumberOfNodeRunUnifiedRepair(repairType, 0);

        assertEquals(2, count);
    }


    @Test
    public void testGetMaxNumberOfNodeRunUnifiedRepairInGroup_percentage()
    {
        DatabaseDescriptor.getUnifiedRepairConfig().setParallelRepairCount(repairType, 2);
        DatabaseDescriptor.getUnifiedRepairConfig().setParallelRepairPercentage(repairType, 50);


        int count = UnifiedRepairUtils.getMaxNumberOfNodeRunUnifiedRepair(repairType, 10);

        assertEquals(5, count);
    }

    @Test
    public void testDeleteUnifiedRepairHistory()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, host_id) VALUES ('%s', %s)",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId));

        UnifiedRepairUtils.deleteUnifiedRepairHistory(repairType, hostId);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT * FROM %s.%s WHERE repair_type = '%s' AND host_id = %s",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId));
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testUpdateStartUnifiedRepairHistory()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, host_id) VALUES ('%s', %s)",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId));

        UnifiedRepairUtils.updateStartUnifiedRepairHistory(repairType, hostId, 123, UnifiedRepairUtils.RepairTurn.MY_TURN);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT repair_start_ts, repair_turn FROM %s.%s WHERE repair_type = '%s' AND host_id = %s",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId));
        assertNotNull(result);
        assertEquals(1, result.size());
        UntypedResultSet.Row row = result.one();
        assertEquals(123, row.getLong(COL_REPAIR_START_TS, 0));
        assertEquals(UnifiedRepairUtils.RepairTurn.MY_TURN.toString(), row.getString(COL_REPAIR_TURN));
    }

    @Test
    public void testUpdateFinishUnifiedRepairHistory()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, host_id) VALUES ('%s', %s)",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId));

        UnifiedRepairUtils.updateFinishUnifiedRepairHistory(repairType, hostId, 123);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT repair_finish_ts FROM %s.%s WHERE repair_type = '%s' AND host_id = %s",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), hostId));
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(123, result.one().getLong(COL_REPAIR_FINISH_TS, 0));
    }

    @Test
    public void testAddHostIdToDeleteHosts()
    {
        UUID otherHostId = UUID.randomUUID();
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, host_id) VALUES ('%s', %s)",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), otherHostId));

        UnifiedRepairUtils.addHostIdToDeleteHosts(repairType, hostId, otherHostId);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT * FROM %s.%s WHERE repair_type = '%s' AND host_id = %s",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_HISTORY,
        repairType.toString(), otherHostId));
        assertNotNull(result);
        assertEquals(1, result.size());
        Set<UUID> deleteHosts = result.one().getSet(COL_DELETE_HOSTS, UUIDType.instance);
        assertNotNull(deleteHosts);
        assertEquals(1, deleteHosts.size());
        assertTrue(deleteHosts.contains(hostId));
    }

    @Test
    public void testAddPriorityHost()
    {
        UnifiedRepairUtils.addPriorityHosts(repairType, ImmutableSet.of(localEndpoint));

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT * FROM %s.%s WHERE repair_type = '%s'",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_PRIORITY,
        repairType.toString()));
        assertNotNull(result);
        assertEquals(1, result.size());
        Set<UUID> repairPriority = result.one().getSet(COL_REPAIR_PRIORITY, UUIDType.instance);
        assertNotNull(repairPriority);
        assertEquals(1, repairPriority.size());
        assertTrue(repairPriority.contains(hostId));
    }

    @Test
    public void testRemovePriorityStatus()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, repair_priority) VALUES ('%s', { %s })",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_PRIORITY,
        repairType.toString(), hostId));

        UnifiedRepairUtils.removePriorityStatus(repairType, hostId);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT * FROM %s.%s WHERE repair_type = '%s'",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_PRIORITY,
        repairType.toString()));
        assertNotNull(result);
        assertEquals(1, result.size());
        Set<UUID> repairPriority = result.one().getSet(COL_REPAIR_PRIORITY, UUIDType.instance);
        assertNull(repairPriority);
    }

    @Test
    public void testGetPriorityHosts()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, repair_priority) VALUES ('%s', { %s })",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.UNIFIED_REPAIR_PRIORITY,
        repairType.toString(), hostId));

        Set<InetAddressAndPort> hosts = UnifiedRepairUtils.getPriorityHosts(repairType);

        assertNotNull(hosts);
        assertEquals(1, hosts.size());
        assertTrue(hosts.contains(localEndpoint));
    }

    @Test
    public void testCheckNodeContainsKeyspaceReplica()
    {
        Keyspace ks = Keyspace.open("ks");

        assertTrue(UnifiedRepairUtils.checkNodeContainsKeyspaceReplica(ks));
    }

    @Test
    public void testTableMaxRepairTimeExceeded()
    {
        DatabaseDescriptor.getUnifiedRepairConfig().setUnifiedRepairTableMaxRepairTime(repairType, "0s");

        assertTrue(UnifiedRepairUtils.tableMaxRepairTimeExceeded(repairType, 0));
    }

    @Test
    public void testKeyspaceMaxRepairTimeExceeded()
    {
        DatabaseDescriptor.getUnifiedRepairConfig().setUnifiedRepairTableMaxRepairTime(repairType, "0s");

        assertTrue(UnifiedRepairUtils.keyspaceMaxRepairTimeExceeded(repairType, 0, 1));
    }

    @Test
    public void testGetLastRepairFinishTime()
    {
        UnifiedRepairHistory history = new UnifiedRepairHistory(UUID.randomUUID(), "", 0, 0, null, 0, false);

        assertEquals(0, history.getLastRepairFinishTime());

        history.lastRepairFinishTime = 100;

        assertEquals(100, history.getLastRepairFinishTime());
    }

    @Test
    public void testMyTurnToRunRepairShouldReturnMyTurnWhenRepairOngoing()
    {
        UUID myID = UUID.randomUUID();
        UUID otherID = UUID.randomUUID();
        DatabaseDescriptor.getUnifiedRepairConfig().setParallelRepairCount(repairType, 5);
        long currentMillis = System.currentTimeMillis();
        // finish time less than start time means that repair is ongoing
        UnifiedRepairUtils.insertNewRepairHistory(repairType, myID, currentMillis, currentMillis - 100);
        // finish time is larger than start time means that repair for other node is finished
        UnifiedRepairUtils.insertNewRepairHistory(repairType, otherID, currentMillis, currentMillis + 100);

        assertEquals(UnifiedRepairUtils.RepairTurn.MY_TURN, UnifiedRepairUtils.myTurnToRunRepair(repairType, myID));
    }

    @Test
    public void testLocalStrategyAndNetworkKeyspace()
    {
        assertFalse(UnifiedRepairUtils.checkNodeContainsKeyspaceReplica(Keyspace.open("system")));
        assertTrue(UnifiedRepairUtils.checkNodeContainsKeyspaceReplica(Keyspace.open(KEYSPACE)));
    }

    @Test
    public void testGetLastRepairTimeForNode()
    {
        UUID myID = UUID.randomUUID();
        UUID otherID = UUID.randomUUID();
        long currentMillis = System.currentTimeMillis();
        UnifiedRepairUtils.insertNewRepairHistory(repairType, myID, currentMillis, currentMillis - 100);
        UnifiedRepairUtils.insertNewRepairHistory(repairType, otherID, currentMillis, currentMillis + 100);

        assertEquals(currentMillis - 100, UnifiedRepairUtils.getLastRepairTimeForNode(repairType, myID));
    }

    @Test
    public void testGetLastRepairTimeForNodeWhenHistoryIsEmpty()
    {
        UUID myID = UUID.randomUUID();

        assertEquals(0, UnifiedRepairUtils.getLastRepairTimeForNode(repairType, myID));
    }
}
