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

package org.apache.cassandra.repair.autorepair;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils.AutoRepairHistory;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils.CurrentRepairStatus;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.COL_DELETE_HOSTS;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.COL_FORCE_REPAIR;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.COL_REPAIR_FINISH_TS;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.COL_REPAIR_PRIORITY;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.COL_REPAIR_START_TS;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.COL_REPAIR_TURN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class AutoRepairUtilsTest extends CQLTester
{
    static RepairType repairType = RepairType.incremental;
    static int pid = "".hashCode();
    static UUID hostId;

    static InetAddressAndPort localEndpoint;

    @Mock
    static IEndpointSnitch snitchMock;

    static IEndpointSnitch defaultSnitch;


    @BeforeClass
    public static void setupClass() throws Exception
    {
        setAutoRepairEnabled(true);
        requireNetwork();
        defaultSnitch = DatabaseDescriptor.getEndpointSnitch();
        localEndpoint = FBUtilities.getBroadcastAddressAndPort();
        hostId = Gossiper.instance.getHostId(localEndpoint);
        AutoRepairUtils.setup();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.create(false,
                                                                ImmutableMap.of("class", "NetworkTopologyStrategy", "datacenter1", "1")),
                                    TableMetadata.builder("ks", "tbl")
                                                 .addPartitionKeyColumn("k", UTF8Type.instance)
                                                 .build());
    }

    @Before
    public void setup()
    {
        MockitoAnnotations.initMocks(this);
        DatabaseDescriptor.setEndpointSnitch(defaultSnitch);
        DatabaseDescriptor.getAutoRepairConfig().setDCGroups(repairType, new HashSet<>());
        QueryProcessor.executeInternal(String.format(
        "TRUNCATE %s.%s",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2));
        QueryProcessor.executeInternal(String.format(
        "TRUNCATE %s.%s",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_PRIORITY_V2));
    }

    @Test
    public void testSetForceRepair()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, host_id, force_repair) VALUES ('%s', %s, %s, false)",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId));

        AutoRepairUtils.setForceRepair(repairType, ImmutableSet.of(localEndpoint));

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT force_repair FROM %s.%s WHERE repair_type = '%s' AND pid = %s AND host_id = %s",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId));
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.one().getBoolean(COL_FORCE_REPAIR));
    }

    @Test
    public void testSetForceRepairNewNode()
    {
        AutoRepairUtils.setForceRepairNewNode(repairType);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT force_repair FROM %s.%s WHERE repair_type = '%s' AND pid = %s AND host_id = %s",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId));
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.one().getBoolean(COL_FORCE_REPAIR));
    }


    @Test
    public void testClearDeleteHosts()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, host_id, delete_hosts, delete_hosts_update_time) VALUES ('%s', %s, %s, { %s }, toTimestamp(now()))",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId, hostId));

        AutoRepairUtils.clearDeleteHosts(repairType, hostId);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT delete_hosts FROM %s.%s WHERE repair_type = '%s' AND pid = %s AND host_id = %s",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId));
        assertNotNull(result);
        assertEquals(1, result.size());
        Set<UUID> deleteHosts = result.one().getSet(COL_DELETE_HOSTS, UUIDType.instance);
        assertNull(deleteHosts);
    }

    @Test
    public void testGetAutoRepairHistoryForLocalGroup()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, host_id, force_repair) VALUES ('%s', %s, %s, false)",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId));

        List<AutoRepairHistory> history = AutoRepairUtils.getAutoRepairHistoryForLocalGroup(repairType);

        assertNotNull(history);
        assertEquals(1, history.size());
        assertEquals(hostId, history.get(0).hostId);
    }

    @Test
    public void testGetAutoRepairHistoryForLocalGroup_empty_history()
    {
        List<AutoRepairHistory> history = AutoRepairUtils.getAutoRepairHistoryForLocalGroup(repairType);

        assertNull(history);
    }

    @Test
    public void testGetLocalDCGroup()
    {
        DatabaseDescriptor.getAutoRepairConfig().setDCGroups(repairType, ImmutableSet.of(
        String.format("%s|dc2", DatabaseDescriptor.getLocalDataCenter()), "dc3"));

        Set<String> dcGroup = AutoRepairUtils.getLocalDCGroup(repairType);

        assertEquals(ImmutableSet.of(DatabaseDescriptor.getLocalDataCenter(), "dc2"), dcGroup);
    }

    @Test
    public void testGetCurrentRepairStatus()
    {
        UUID forceRepair = UUID.randomUUID();
        UUID regularRepair = UUID.randomUUID();
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, host_id) VALUES ('%s', %s, %s)",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId));
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, host_id, force_repair, repair_start_ts) VALUES ('%s', %s, %s, true, toTimestamp(now()))",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, forceRepair));
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, host_id, repair_start_ts) VALUES ('%s', %s, %s, toTimestamp(now()))",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, regularRepair));
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, repair_priority) VALUES ('%s', %s, { %s })",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_PRIORITY_V2,
        repairType.toString(), pid, regularRepair));

        CurrentRepairStatus status = AutoRepairUtils.getCurrentRepairStatus(repairType);

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
    public void testProcessNodesByGroup()
    {
        InetAddressAndPort otherEndpoint = localEndpoint.withPort(localEndpoint.getPort() + 1);
        InetAddressAndPort ignoredEndpoint = localEndpoint.withPort(localEndpoint.getPort() + 2);
        DatabaseDescriptor.getAutoRepairConfig().setDCGroups(repairType,
                                                             ImmutableSet.of(String.format("%s|dc2", DatabaseDescriptor.getLocalDataCenter())));
        DatabaseDescriptor.setEndpointSnitch(snitchMock);
        when(snitchMock.getDatacenter(localEndpoint)).thenReturn(DatabaseDescriptor.getLocalDataCenter());
        when(snitchMock.getDatacenter(otherEndpoint)).thenReturn("dc2");
        when(snitchMock.getDatacenter(ignoredEndpoint)).thenReturn("dc3");

        Set<InetAddressAndPort> nodesInCurrentRing = AutoRepairUtils.processNodesByGroup(repairType,
                                                                                         ImmutableSet.of(localEndpoint, ignoredEndpoint, otherEndpoint));

        assertNotNull(nodesInCurrentRing);
        assertEquals(2, nodesInCurrentRing.size());
        assertTrue(nodesInCurrentRing.contains(localEndpoint));
        assertTrue(nodesInCurrentRing.contains(otherEndpoint));
    }

    @Test
    public void testGetHostIdsInCurrentRing()
    {
        TreeSet<UUID> hosts = AutoRepairUtils.getHostIdsInCurrentRing(repairType);

        assertNotNull(hosts);
        assertEquals(1, hosts.size());
        assertTrue(hosts.contains(hostId));
    }

    @Test
    public void testGetHostIdsInCurrentRing_multiple_nodes()
    {
        InetAddressAndPort ignoredEndpoint = localEndpoint.withPort(localEndpoint.getPort() + 1);
        InetAddressAndPort deadEndpoint = localEndpoint.withPort(localEndpoint.getPort() + 2);
        DatabaseDescriptor.getAutoRepairConfig().setIgnoreDCs(repairType, ImmutableSet.of("dc2"));
        DatabaseDescriptor.setEndpointSnitch(snitchMock);
        when(snitchMock.getDatacenter(localEndpoint)).thenReturn("dc1");
        when(snitchMock.getDatacenter(ignoredEndpoint)).thenReturn("dc2");
        when(snitchMock.getDatacenter(deadEndpoint)).thenReturn("dc1");

        TreeSet<UUID> hosts = AutoRepairUtils.getHostIdsInCurrentRing(repairType, ImmutableSet.of(localEndpoint, ignoredEndpoint, deadEndpoint));

        assertNotNull(hosts);
        assertEquals(1, hosts.size());
        assertTrue(hosts.contains(hostId));
    }

    @Test
    public void testGetHostWithLongestUnrepairTime()
    {
        UUID otherHostId = UUID.randomUUID();
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, host_id) VALUES ('%s', %s, %s)",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId));
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, host_id, repair_finish_ts) VALUES ('%s', %s, %s, toTimestamp(now()))",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, otherHostId));

        AutoRepairHistory history = AutoRepairUtils.getHostWithLongestUnrepairTime(repairType);

        assertEquals(hostId, history.hostId);
    }

    @Test
    public void testGetMaxNumberOfNodeRunAutoRepairInGroup_0_group_size()
    {
        DatabaseDescriptor.getAutoRepairConfig().setParallelRepairCountInGroup(repairType, 2);

        int count = AutoRepairUtils.getMaxNumberOfNodeRunAutoRepairInGroup(repairType, 0);

        assertEquals(2, count);
    }


    @Test
    public void testGetMaxNumberOfNodeRunAutoRepairInGroup_percentage()
    {
        DatabaseDescriptor.getAutoRepairConfig().setParallelRepairCountInGroup(repairType, 2);
        DatabaseDescriptor.getAutoRepairConfig().setParallelRepairPercentageInGroup(repairType, 50);


        int count = AutoRepairUtils.getMaxNumberOfNodeRunAutoRepairInGroup(repairType, 10);

        assertEquals(5, count);
    }

    @Test
    public void testDeleteAutoRepairHistory()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, host_id) VALUES ('%s', %s, %s)",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId));

        AutoRepairUtils.deleteAutoRepairHistory(repairType, hostId);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT * FROM %s.%s WHERE repair_type = '%s' AND pid = %s AND host_id = %s",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId));
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testUpdateStartAutoRepairHistory()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, host_id) VALUES ('%s', %s, %s)",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId));

        AutoRepairUtils.updateStartAutoRepairHistory(repairType, hostId, 123, AutoRepairUtils.RepairTurn.MY_TURN);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT repair_start_ts, repair_turn FROM %s.%s WHERE repair_type = '%s' AND pid = %s AND host_id = %s",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId));
        assertNotNull(result);
        assertEquals(1, result.size());
        UntypedResultSet.Row row = result.one();
        assertEquals(123, row.getLongOrDefault(COL_REPAIR_START_TS, 0));
        assertEquals(AutoRepairUtils.RepairTurn.MY_TURN.toString(), row.getString(COL_REPAIR_TURN));
    }

    @Test
    public void testUpdateFinishAutoRepairHistory()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, host_id) VALUES ('%s', %s, %s)",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId));

        AutoRepairUtils.updateFinishAutoRepairHistory(repairType, hostId, 123);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT repair_finish_ts FROM %s.%s WHERE repair_type = '%s' AND pid = %s AND host_id = %s",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, hostId));
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(123, result.one().getLongOrDefault(COL_REPAIR_FINISH_TS, 0));
    }

    @Test
    public void testAddHostIdToDeleteHosts()
    {
        UUID otherHostId = UUID.randomUUID();
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, host_id) VALUES ('%s', %s, %s)",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, otherHostId));

        AutoRepairUtils.addHostIdToDeleteHosts(repairType, hostId, otherHostId);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT * FROM %s.%s WHERE repair_type = '%s' AND pid = %s AND host_id = %s",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2,
        repairType.toString(), pid, otherHostId));
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
        AutoRepairUtils.addPriorityHosts(repairType, ImmutableSet.of(localEndpoint));

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT * FROM %s.%s WHERE repair_type = '%s' AND pid = %s",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_PRIORITY_V2,
        repairType.toString(), pid));
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
        "INSERT INTO %s.%s (repair_type, pid, repair_priority) VALUES ('%s', %s, { %s })",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_PRIORITY_V2,
        repairType.toString(), pid, hostId));

        AutoRepairUtils.removePriorityStatus(repairType, hostId);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format(
        "SELECT * FROM %s.%s WHERE repair_type = '%s' AND pid = %s",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_PRIORITY_V2,
        repairType.toString(), pid));
        assertNotNull(result);
        assertEquals(1, result.size());
        Set<UUID> repairPriority = result.one().getSet(COL_REPAIR_PRIORITY, UUIDType.instance);
        assertNull(repairPriority);
    }

    @Test
    public void testGetPriorityHosts()
    {
        QueryProcessor.executeInternal(String.format(
        "INSERT INTO %s.%s (repair_type, pid, repair_priority) VALUES ('%s', %s, { %s })",
        SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_PRIORITY_V2,
        repairType.toString(), pid, hostId));

        Set<InetAddressAndPort> hosts = AutoRepairUtils.getPriorityHosts(repairType);

        assertNotNull(hosts);
        assertEquals(1, hosts.size());
        assertTrue(hosts.contains(localEndpoint));
    }

    @Test
    public void testCheckNodeContainsKeyspaceReplica()
    {
        Keyspace ks = Keyspace.open("ks");

        assertTrue(AutoRepairUtils.checkNodeContainsKeyspaceReplica(ks));
    }

    @Test
    public void testTableMaxRepairTimeExceeded()
    {
        DatabaseDescriptor.getAutoRepairConfig().setAutoRepairTableMaxRepairTimeInSec(repairType, 0);

        assertTrue(AutoRepairUtils.tableMaxRepairTimeExceeded(repairType, 0));
    }

    @Test
    public void testKeyspaceMaxRepairTimeExceeded()
    {
        DatabaseDescriptor.getAutoRepairConfig().setAutoRepairTableMaxRepairTimeInSec(repairType, 0);

        assertTrue(AutoRepairUtils.keyspaceMaxRepairTimeExceeded(repairType, 0, 1));
    }

    @Test
    public void testGetLastRepairFinishTime()
    {
        AutoRepairHistory history = new AutoRepairHistory(UUID.randomUUID(), "", 0, 0, null, 0, false);

        assertEquals(0, history.getLastRepairFinishTime());

        history.lastRepairFinishTime = 100;

        assertEquals(100, history.getLastRepairFinishTime());
    }

    @Test
    public void testMyTurnToRunRepairShouldReturnMyTurnWhenRepairOngoing()
    {
        UUID myID = UUID.randomUUID();
        UUID otherID = UUID.randomUUID();
        DatabaseDescriptor.getAutoRepairConfig().setParallelRepairCountInGroup(repairType, 5);
        long currentMillis = System.currentTimeMillis();
        // finish time less than start time means that repair is ongoing
        AutoRepairUtils.insertNewRepairHistory(repairType, myID, currentMillis, currentMillis - 100);
        // finish time is larger than start time means that repair for other node is finished
        AutoRepairUtils.insertNewRepairHistory(repairType, otherID, currentMillis, currentMillis + 100);

        assertEquals(AutoRepairUtils.RepairTurn.MY_TURN, AutoRepairUtils.myTurnToRunRepair(repairType, myID));
    }
}
