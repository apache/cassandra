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
package org.apache.cassandra.repair;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.cassandra.repair.AutoRepairUtils.RepairTurn.*;

/**
 * This class servers as utility class for AutoRepair. It contains various helper APIs
 * to store/retrieve repair status, decide whose turn is next, etc.
 */
public class AutoRepairUtils
{
    private static final Logger logger = LoggerFactory.getLogger(AutoRepairUtils.class);
    static final String COL_PID = "pid";  // this value is used to store the group id of the row.
    static final String COL_HOST_ID = "host_id";
    static final String COL_REPAIR_STATUS = "repair_status";
    static final String COL_REPAIR_TS = "repair_ts";
    static final String COL_REPAIR_START_TS = "repair_start_ts";
    static final String COL_REPAIR_FINISH_TS = "repair_finish_ts";
    static final String COL_REPAIR_PRIORITY = "repair_priority";
    static final String COL_DELETE_HOSTS = "delete_hosts";  // this set stores the host ids which think the row should be deleted
    static final String COL_REPAIR_TURN = "repair_turn";  // this record the last repair turn. Normal turn or turn due to priority
    static final String COL_DELETE_HOSTS_UPDATE_TIME = "delete_hosts_update_time"; // the time when delete hosts are upated

    final static String INSERT_REPAIR_STATUS = String.format(
    "INSERT INTO %s.%s (%s, %s, %s, %s) values (?, ?, ?, ?)"
            , SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_STATUS, COL_PID, COL_HOST_ID, COL_REPAIR_STATUS, COL_REPAIR_TS);
    final static String SELECT_REPAIR_STATUS = String.format(
    "SELECT %s, %s, %s, %s FROM %s.%s WHERE pid = ?"
            , COL_HOST_ID, COL_REPAIR_STATUS, COL_REPAIR_TS, COL_REPAIR_PRIORITY, SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_STATUS);
    final static String SELECT_REPAIR_HISTORY = String.format(
    "SELECT * FROM %s.%s WHERE pid = ?", SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY);
    final static String DEL_REPAIR_STATUS = String.format(
    "DELETE FROM %s.%s WHERE pid = ?", SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_STATUS);
    final static String DEL_REPAIR_PRIORITY = String.format(
    "DELETE %s[?] FROM %s.%s WHERE pid = ?", COL_REPAIR_PRIORITY, SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_STATUS);
    final static String ADD_PRIORITY_HOST = String.format(
    "UPDATE %s.%s SET %s = %s + ?  WHERE pid = ?", SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_STATUS,
    COL_REPAIR_PRIORITY, COL_REPAIR_PRIORITY);

    final static String INSERT_NEW_REPAIR_HISTORY = String.format(
    "INSERT INTO %s.%s (%s, %s, %s, %s, %s, %s) values (?, ?, ? ,?, {}, ?) IF NOT EXISTS"
            , SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY, COL_PID, COL_HOST_ID, COL_REPAIR_START_TS, COL_REPAIR_FINISH_TS, COL_DELETE_HOSTS, COL_DELETE_HOSTS_UPDATE_TIME
    );

    final static String ADD_HOST_ID_TO_DELETE_HOSTS = String.format(
    "UPDATE %s.%s SET %s = %s + ?, %s = ? WHERE %s = ? AND %s = ?"
            , SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY, COL_DELETE_HOSTS, COL_DELETE_HOSTS, COL_DELETE_HOSTS_UPDATE_TIME, COL_PID, COL_HOST_ID
    );

    final static String DEL_AUTO_REPAIR_HISTORY = String.format(
    "DELETE FROM %s.%s WHERE %s = ? AND %s = ?"
            , SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY, COL_PID, COL_HOST_ID
    );

    final static String RECORD_START_REPAIR_HISTORY = String.format(
    "UPDATE %s.%s SET %s= ?, repair_turn = ? WHERE %s = ? AND %s = ?"
            , SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY,  COL_REPAIR_START_TS, COL_PID, COL_HOST_ID
    );

    final static String RECORD_FINISH_REPAIR_HISTORY = String.format(
    "UPDATE %s.%s SET %s= ? WHERE %s = ? AND %s = ?"
    , SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY,  COL_REPAIR_FINISH_TS, COL_PID, COL_HOST_ID
    );

    final static String CLEAR_DELETE_HOSTS = String.format(
    "UPDATE %s.%s SET %s= {} WHERE %s = ? AND %s = ?"
    , SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY,  COL_DELETE_HOSTS, COL_PID, COL_HOST_ID
    );

    static ModificationStatement modificationStatementRepairStatus;
    static ModificationStatement delStatementRepairStatus;
    static ModificationStatement delStatementRepairHistory;
    static SelectStatement selectStatementRepairStatus;
    static SelectStatement selectStatementRepairHistory;
    static ModificationStatement delStatementPriorityStatus;
    static ModificationStatement addPriorityHost;
    static ModificationStatement insertNewRepairHistoryStatement;
    static ModificationStatement recordStartRepairHistoryStatement;
    static ModificationStatement recordFinishRepairHistoryStatement;
    static ModificationStatement addHostIDToDeleteHostsStatement;
    static ModificationStatement clearDeleteHostsStatement;
    static ConsistencyLevel internalQueryCL;

    enum RepairTurn
    {
        MY_TURN,
        NOT_MY_TURN,
        MY_TURN_DUE_TO_PRIORITY
    }

    public static void setup()
    {
        modificationStatementRepairStatus = (ModificationStatement) QueryProcessor.getStatement(INSERT_REPAIR_STATUS, ClientState
                .forInternalCalls());
        selectStatementRepairStatus = (SelectStatement) QueryProcessor.getStatement(SELECT_REPAIR_STATUS, ClientState
                .forInternalCalls());
        selectStatementRepairHistory = (SelectStatement) QueryProcessor.getStatement(SELECT_REPAIR_HISTORY, ClientState
        .forInternalCalls());
        delStatementRepairStatus = (ModificationStatement) QueryProcessor.getStatement(DEL_REPAIR_STATUS, ClientState
                .forInternalCalls());
        delStatementPriorityStatus = (ModificationStatement) QueryProcessor.getStatement(DEL_REPAIR_PRIORITY, ClientState
                .forInternalCalls());
        addPriorityHost = (ModificationStatement) QueryProcessor.getStatement(ADD_PRIORITY_HOST, ClientState
                .forInternalCalls());
        insertNewRepairHistoryStatement = (ModificationStatement) QueryProcessor.getStatement(INSERT_NEW_REPAIR_HISTORY, ClientState
                .forInternalCalls());
        recordStartRepairHistoryStatement = (ModificationStatement) QueryProcessor.getStatement(RECORD_START_REPAIR_HISTORY, ClientState
                .forInternalCalls());
        recordFinishRepairHistoryStatement = (ModificationStatement) QueryProcessor.getStatement(RECORD_FINISH_REPAIR_HISTORY, ClientState
                .forInternalCalls());
        addHostIDToDeleteHostsStatement = (ModificationStatement) QueryProcessor.getStatement(ADD_HOST_ID_TO_DELETE_HOSTS, ClientState
        .forInternalCalls());
        clearDeleteHostsStatement = (ModificationStatement) QueryProcessor.getStatement(CLEAR_DELETE_HOSTS, ClientState
        .forInternalCalls());
        delStatementRepairHistory = (ModificationStatement) QueryProcessor.getStatement(DEL_AUTO_REPAIR_HISTORY, ClientState
        .forInternalCalls());
        Keyspace autoRepairKS = Schema.instance.getKeyspaceInstance(SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME);
        internalQueryCL = autoRepairKS.getReplicationStrategy().getClass() == NetworkTopologyStrategy.class ?
                ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.ONE;
    }

    static class AutoRepairHistory
    {
        UUID hostId;
        String repairTurn;
        long lastRepairStartTime;
        long lastRepairFinishTime;
        Set<UUID> deleteHosts;
        long deleteHostsUpdateTime;

        AutoRepairHistory(UUID hostId, String repairTurn, long lastRepairStartTime, long lastRepairFinishTime, Set<UUID> deleteHosts, long deleteHostsUpateTime)
        {
            this.hostId = hostId;
            this.repairTurn = repairTurn;
            this.lastRepairStartTime = lastRepairStartTime;
            this.lastRepairFinishTime = lastRepairFinishTime;
            this.deleteHosts = deleteHosts;
            if (this.deleteHosts == null) {
                this.deleteHosts = new HashSet<>();
            }
            this.deleteHostsUpdateTime = deleteHostsUpateTime;
        }

        public String toString()
        {
            return MoreObjects.toStringHelper(this).
                              add("hostId", hostId).
                              add("repairTurn", repairTurn).
                              add("lastRepairStartTime", lastRepairStartTime).
                              add("lastRepairFinishTime", lastRepairFinishTime).
                              add("deleteHosts", deleteHosts).
                              toString();
        }
    }

    static class CurrentRepairStatus
    {
        UUID hostIdWithOnGoingRepair;
        int currentRepairStatus;
        long repairFinishedTs;
        Set<UUID> priority;

        CurrentRepairStatus(UUID hostIdWithOnGoingRepair, int currentRepairStatus, long repairFinishedTs, Set<UUID> priority)
        {
            this.hostIdWithOnGoingRepair = hostIdWithOnGoingRepair;
            this.currentRepairStatus = currentRepairStatus;
            this.repairFinishedTs = repairFinishedTs;
            this.priority = priority;
        }

        public String toString()
        {
            return MoreObjects.toStringHelper(this).
                    add("hostIdWithOnGoingRepair", hostIdWithOnGoingRepair).
                    add("currentRepairStatus", currentRepairStatus).
                    add("repairFinishedTs", repairFinishedTs).
                    add("priority", priority).
                    toString();
        }
    }

    public static List<AutoRepairHistory> getAutoRepairHistoryForLocalGroup()
    {
        UntypedResultSet repairHistoryResult;
        ResultMessage.Rows repairStatusRows = selectStatementRepairHistory.execute(QueryState.forInternalCalls(), QueryOptions
        .forInternalCalls(internalQueryCL, Lists.newArrayList(ByteBufferUtil.bytes(getLocalDCGroup().hashCode()))), Dispatcher.RequestTime.forImmediateExecution());
        repairHistoryResult = UntypedResultSet.create(repairStatusRows.result);

        List<AutoRepairHistory> repairHistories = new ArrayList<>();
        if (repairHistoryResult.size() > 0) {
            for (UntypedResultSet.Row row : repairHistoryResult) {
                UUID hostId = row.getUUID(COL_HOST_ID);
                String repairTurn = null;
                if (row.has(COL_REPAIR_TURN))
                    repairTurn = row.getString(COL_REPAIR_TURN);
                long lastRepairStartTime = row.getLong(COL_REPAIR_START_TS);
                long lastRepairFinishTime = row.getLong(COL_REPAIR_FINISH_TS);
                Set<UUID> deleteHosts = row.getSet(COL_DELETE_HOSTS, UUIDType.instance);
                long deleteHostsUpdateTime = 0;
                if (row.has(COL_DELETE_HOSTS_UPDATE_TIME)) {
                    deleteHostsUpdateTime = row.getLong(COL_DELETE_HOSTS_UPDATE_TIME);
                }
                repairHistories.add(new AutoRepairHistory(hostId, repairTurn, lastRepairStartTime, lastRepairFinishTime, deleteHosts, deleteHostsUpdateTime));
            }
            return repairHistories;
        }
        logger.info("No repair history found for pid = " + getLocalDCGroup().hashCode());
        return null;
    }

    // A host may add itself in delete hosts for some other hosts due to restart or some temp gossip issue. If a node's record
    // delete_hosts is not growing for more than 2 hours, we consider it as a normal node so we clear the delete_hosts for that node
    public static void clearDeleteHosts(UUID hostId) {
        clearDeleteHostsStatement.execute(QueryState.forInternalCalls(),
                                                  QueryOptions.forInternalCalls(internalQueryCL,
                                                                                Lists.newArrayList(ByteBufferUtil.bytes(getLocalDCGroup().hashCode()),
                                                                                                   ByteBufferUtil.bytes(hostId))), Dispatcher.RequestTime.forImmediateExecution());

    }

    public static CurrentRepairStatus getCurrentRepairStatus()
    {
        //get current repair status
        logger.info("Getting repair status for pid = " + getLocalDCGroup().hashCode());
        ResultMessage.Rows repairStatusRows = selectStatementRepairStatus.execute(QueryState.forInternalCalls(), QueryOptions
                .forInternalCalls(internalQueryCL, Lists.newArrayList(ByteBufferUtil.bytes(getLocalDCGroup().hashCode()))), Dispatcher.RequestTime.forImmediateExecution());
        UntypedResultSet repairStatusResult = UntypedResultSet.create(repairStatusRows.result);

        if (repairStatusResult.size() > 0)
        {
            // we will always get one row returned because the query is selecting with primary key
            UntypedResultSet.Row row = repairStatusResult.one();
            UUID hostIdWithOnGoingRepair = row.getUUID(COL_HOST_ID);
            int currentRepairStatus = row.getInt(COL_REPAIR_STATUS);
            long repairFinishedTs = row.getLong(COL_REPAIR_TS);
            Set<UUID> priority = row.getSet(COL_REPAIR_PRIORITY, UUIDType.instance);
            logger.info("Latest repair status hostIdWithOnGoingRepair {}, currentRepairStatus {}, " +
                         "repair_finished_ts {}", hostIdWithOnGoingRepair,
                         currentRepairStatus, repairFinishedTs);
            CurrentRepairStatus status = new CurrentRepairStatus(hostIdWithOnGoingRepair, currentRepairStatus,
                                                                 repairFinishedTs, priority);

            return status;
        }
        logger.info("No repair status for pid = " + getLocalDCGroup().hashCode() + " found!");
        return null;
    }

    public static Set<String> getLocalDCGroup() {
        String localDataCenter = DatabaseDescriptor.getLocalDataCenter();
        Set<String> localGroup = new HashSet<>();
        for (Set<String> group : AutoRepairService.instance.getDCGroups()) {
            if (group.contains(localDataCenter)) {
                localGroup = group;
                break;
            }
        }
        return localGroup;
    }

    // if dc groups is empty(not set), return the input value. If groups are set, only return the nodes in the same group
    public static Set<InetAddressAndPort> processNodesByGroup(Set<InetAddressAndPort> allNodesInRing) {
        Set<Set<String>> dcGroups = AutoRepairService.instance.getDCGroups();
        if (dcGroups == null || dcGroups.isEmpty()) {
            logger.info("No data center groups is defined, will use all nodes in ring as one group.");
            return allNodesInRing;
        }
        Set<String> localGroup = getLocalDCGroup();
        logger.info("Auto repair local group is " + localGroup.toString());

        Set<InetAddressAndPort> localGroupNodes = new HashSet<>();
        for (InetAddressAndPort node : allNodesInRing) {
            if (localGroup.contains(DatabaseDescriptor.getEndpointSnitch().getDatacenter(node))) {
                localGroupNodes.add(node);
            }
        }
        logger.info("Total number of nodes in group {} is {}, local nodes: {}.", localGroup.toString(), localGroupNodes.size(), localGroupNodes);
        return localGroupNodes;
    }

    private static TreeSet<UUID> getHostIdsInCurrentRing(Set<InetAddressAndPort> allNodesInRing)
    {
        TreeSet<UUID> hostIdsInCurrentRing = new TreeSet<>();
        allNodesInRing = processNodesByGroup(allNodesInRing);
        for (InetAddressAndPort node : allNodesInRing)
        {
            String nodeDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(node);
            if (AutoRepairService.instance.getIgnoreDCs().contains(nodeDC))
            {
                logger.info("Ignore node {} because its datacenter is {}", node, nodeDC);
                continue;
            }
            /** Check if endpoint state exists in gossip or not. If it
             * does not then this maybe a ghost node so ignore it
             */
            if (Gossiper.instance.isAlive(node))
            {
                UUID hostId = Gossiper.instance.getHostId(node);
                hostIdsInCurrentRing.add(hostId);
            } else {
                logger.info("Node is not present in Gossipe cache node {}, node data center {}", node, nodeDC);
            }
        }
        return hostIdsInCurrentRing;
    }

    public static TreeSet<UUID> getHostIdsInCurrentRing()
    {
        if (!AutoRepairService.instance.isAutoRepairEnabled()) {
            return new TreeSet<>();
        }
        Set<InetAddressAndPort> allNodesInRing = StorageService.instance.getTokenMetadata().getAllEndpoints();
        return getHostIdsInCurrentRing(allNodesInRing);
    }

    // This function will return the host ID for the node which has not been repaired for longest time
    public static AutoRepairHistory getHostIDWithLongestUnrepairTime() {
        List<AutoRepairHistory> autoRepairHistories = getAutoRepairHistoryForLocalGroup();
        return getHostIDWithLongestUnrepairTime(autoRepairHistories);
    }
    private static AutoRepairHistory getHostIDWithLongestUnrepairTime(List<AutoRepairHistory> autoRepairHistories) {
        if (autoRepairHistories == null) {
            return null;
        }
        AutoRepairHistory rst = null;
        long oldestTimestamp = Long.MAX_VALUE;
        for (AutoRepairHistory autoRepairHistory : autoRepairHistories) {
            if (autoRepairHistory.lastRepairFinishTime < oldestTimestamp) {
                rst = autoRepairHistory;
                oldestTimestamp = autoRepairHistory.lastRepairFinishTime;
            }
        }
        return rst;
    }

    @VisibleForTesting
    public static RepairTurn myTurnToRunRepair(UUID myId)
    {
        try
        {
            Set<InetAddressAndPort> allNodesInRing = StorageService.instance.getTokenMetadata().getAllEndpoints();
            logger.info("Total nodes in ring {}", allNodesInRing.size());
            TreeSet<UUID> hostIdsInCurrentRing = getHostIdsInCurrentRing(allNodesInRing);
            logger.info("Total nodes qualified for repair {}", hostIdsInCurrentRing.size());

            List<AutoRepairHistory> autoRepairHistories = getAutoRepairHistoryForLocalGroup();
            int localGroup = getLocalDCGroup().hashCode();

            //get current repair status
            CurrentRepairStatus currentRepairStatus = getCurrentRepairStatus();
            if (currentRepairStatus != null) {
                logger.info("Latest repair status {}", currentRepairStatus);
            }

            // no ongoing repair, check if I'm the new one
            if (currentRepairStatus == null || currentRepairStatus.currentRepairStatus == AutoRepair.RepairCurrentStatus.REPAIR_DONE.ordinal()) {
                Set<UUID> autoRepairHistoryIds = new HashSet<>();

                // 1. Remove any node that is not part of group based on goissip info
                if (autoRepairHistories != null) {
                    for (AutoRepairHistory nodeHistory : autoRepairHistories) {
                        autoRepairHistoryIds.add(nodeHistory.hostId);
                        // clear delete_hosts if the node's delete hosts is not growing for more than two hours
                        if (nodeHistory.deleteHosts.size() > 0 && AutoRepairService.instance.getAutoRepairHistoryClearDeleteHostsBufferInSec() < TimeUnit.MILLISECONDS.toSeconds(
                        System.currentTimeMillis() - nodeHistory.deleteHostsUpdateTime
                        )) {
                            clearDeleteHosts(nodeHistory.hostId);
                            logger.info("Delete hosts for {} has not been updated for more than {} seconds. Delete hosts has been cleared. Delete hosts before clear {}"
                            , nodeHistory.hostId, AutoRepairService.instance.getAutoRepairHistoryClearDeleteHostsBufferInSec(), nodeHistory.deleteHosts);
                        }
                        else if (!hostIdsInCurrentRing.contains(nodeHistory.hostId)) {
                            if (nodeHistory.deleteHosts.size() > Math.max(2, hostIdsInCurrentRing.size() * 0.5)) {
                                // More than half of the groups thinks the record should be deleted
                                logger.info("{} think {} is orphan node, will delete auto repair history.", nodeHistory.deleteHosts, nodeHistory.hostId);
                                deleteAutoRepairHistory(nodeHistory.hostId);
                            } else {
                                // I think this host should be deleted
                                logger.info("I({}) think {} is not part of ring, vote to delete it.", myId, nodeHistory.hostId);
                                addHostIdToDeleteHosts(myId, nodeHistory.hostId);
                            }
                        }
                    }
                }

                // 2. Add node to auto repair history table if a node is in gossip info
                for (UUID hostId : hostIdsInCurrentRing) {
                    if (!autoRepairHistoryIds.contains(hostId)) {
                        logger.info("{} doesn't exist in the auto repair history table, insert a new record.", hostId);
                        insertNewRepairHistory(hostId, System.currentTimeMillis(), System.currentTimeMillis());
                    }
                }

                if (autoRepairHistories != null) {
                    logger.info("Auto repair history table has {} records for group {}", autoRepairHistories.size(), localGroup);
                } else {
                    // try to fetch again
                    autoRepairHistories = getAutoRepairHistoryForLocalGroup();
                    if (autoRepairHistories == null) {
                        logger.error("No record found for group id {}", localGroup);
                        return NOT_MY_TURN;
                    }
                }

                AutoRepairHistory defaultNodeToBeRepaired = getHostIDWithLongestUnrepairTime(autoRepairHistories);
                //check who is next, which is helpful for debugging
                logger.info("Next node to be repaired by default: {}", defaultNodeToBeRepaired);

                if (currentRepairStatus != null) {
                    UUID priorityHostId = null;
                    if (currentRepairStatus.priority != null)
                    {
                        for (UUID priorityID : currentRepairStatus.priority) {
                            // remove ids doesn't belong to this ring
                            if (!hostIdsInCurrentRing.contains(priorityID)) {
                                logger.info("{} is not part of the current ring, will be removed from priority list.", priorityID);
                                removePriorityStatus(priorityID);
                            } else {
                                priorityHostId = priorityID;
                                break;
                            }
                        }
                    }

                    if (!hostIdsInCurrentRing.contains(currentRepairStatus.hostIdWithOnGoingRepair))
                    {
                        //host is no longer part of the ring, could happen if host is replaced while it was doing repair
                        // (rare case but still possible)
                        logger.info("Host is no longer part of the ring hence removing its repair status " +
                                    "hostIdWithOnGoingRepair {}, currentRepairStatus {}, repair_finished_ts {}",
                                    currentRepairStatus.hostIdWithOnGoingRepair, currentRepairStatus, currentRepairStatus.repairFinishedTs);
                        delStatementRepairStatus.execute(QueryState.forInternalCalls(),
                                                         QueryOptions.forInternalCalls(internalQueryCL,
                                                                                       Lists.newArrayList(ByteBufferUtil.bytes(getLocalDCGroup().hashCode()))), Dispatcher.RequestTime.forImmediateExecution());
                        return NOT_MY_TURN;
                    }

                    if (priorityHostId != null && !myId.equals(priorityHostId)) {
                        logger.info("Priority list is not empty and I'm not the first node in the list, not my turn." +
                                    "First node in priority list is {}", StorageService.instance.getTokenMetadata().getEndpointForHostId(priorityHostId));
                        return NOT_MY_TURN;
                    }

                    if (myId.equals(priorityHostId))
                    {
                        //I have a priority for repair hence its my turn now
                        return MY_TURN_DUE_TO_PRIORITY;
                    }
                }

                return defaultNodeToBeRepaired.hostId.equals(myId) ? MY_TURN : NOT_MY_TURN;
            }
            else {
                // repair not done
                //for some reason I was not done with the repair hence resume (maybe node restart in-between, etc.)
                return currentRepairStatus.hostIdWithOnGoingRepair.equals(myId) ? MY_TURN : NOT_MY_TURN;
            }
        }
        catch (Exception e)
        {
            logger.error("Exception while deciding node's turn:", e);
        }
        return NOT_MY_TURN;
    }

    static void updateRepairStatus(UUID myId, AutoRepair.RepairCurrentStatus repairStatus)
    {
        //mark current hostId as repaired
        modificationStatementRepairStatus.execute(QueryState.forInternalCalls(),
                                                  QueryOptions.forInternalCalls(internalQueryCL,
                                                                                Lists.newArrayList(ByteBufferUtil.bytes(getLocalDCGroup().hashCode()),
                                                                                                   ByteBufferUtil.bytes(myId),
                                                                                                   ByteBufferUtil.bytes(repairStatus.ordinal()),
                                                                                                   ByteBufferUtil.bytes(System.currentTimeMillis()))),
                                                 Dispatcher.RequestTime.forImmediateExecution());

    }

    static void deleteAutoRepairHistory(UUID hostId)
    {
        //delete the given hostId from current local group
        delStatementRepairHistory.execute(QueryState.forInternalCalls(),
                                                  QueryOptions.forInternalCalls(internalQueryCL,
                                                                                Lists.newArrayList(ByteBufferUtil.bytes(getLocalDCGroup().hashCode()),
                                                                                                   ByteBufferUtil.bytes(hostId))), Dispatcher.RequestTime.forImmediateExecution());
    }

    static void updateStartAutoRepairHistory(UUID myId, long timestamp, RepairTurn turn) {
        recordStartRepairHistoryStatement.execute(QueryState.forInternalCalls(),
                                                  QueryOptions.forInternalCalls(internalQueryCL,
                                                                                Lists.newArrayList(ByteBufferUtil.bytes(timestamp),
                                                                                                   ByteBufferUtil.bytes(turn.name()),
                                                                                                   ByteBufferUtil.bytes(getLocalDCGroup().hashCode()),
                                                                                                   ByteBufferUtil.bytes(myId)
                                                                                                   )), Dispatcher.RequestTime.forImmediateExecution());

    }

    static void updateFinishAutoRepairHistory(UUID myId, long timestamp) {
        recordFinishRepairHistoryStatement.execute(QueryState.forInternalCalls(),
                                                  QueryOptions.forInternalCalls(internalQueryCL,
                                                                                Lists.newArrayList(ByteBufferUtil.bytes(timestamp),
                                                                                                   ByteBufferUtil.bytes(getLocalDCGroup().hashCode()),
                                                                                                   ByteBufferUtil.bytes(myId)
                                                                                )), Dispatcher.RequestTime.forImmediateExecution());

    }

    public static void insertNewRepairHistory(UUID hostId, int pid, long startTime, long finishTime) {
        try
        {
            Keyspace autoRepairKS = Schema.instance.getKeyspaceInstance(SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME);
            ConsistencyLevel cl =  autoRepairKS.getReplicationStrategy().getClass() == NetworkTopologyStrategy.class ?
                              ConsistencyLevel.LOCAL_SERIAL : null;

            UntypedResultSet resultSet;
            ResultMessage.Rows resultMessage = (ResultMessage.Rows) insertNewRepairHistoryStatement.execute(
            QueryState.forInternalCalls(), QueryOptions.create(internalQueryCL, Lists.newArrayList(
            ByteBufferUtil.bytes(pid),
            ByteBufferUtil.bytes(hostId),
            ByteBufferUtil.bytes(startTime),
            ByteBufferUtil.bytes(finishTime),
            ByteBufferUtil.bytes(System.currentTimeMillis())
            ), false, -1, null, cl, ProtocolVersion.CURRENT, SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME),
            Dispatcher.RequestTime.forImmediateExecution());
            resultSet = UntypedResultSet.create(resultMessage.result);
            boolean applied = resultSet.one().getBoolean(ModificationStatement.CAS_RESULT_COLUMN.toString());
            if (applied) {
                logger.info("Successfully inserted a new auto repair history record for host id: {} in pid: {}", hostId, pid);
            } else {
                logger.info("Record exists, no need to insert again for host id: {} in pid: {}", hostId, pid);
            }

        } catch (Exception e)
        {
            logger.error("Exception in inserting new repair history:", e);
        }
    }

    public static void insertNewRepairHistory(UUID hostId, long startTime, long finishTime) {
        int pid = getLocalDCGroup().hashCode();
        insertNewRepairHistory(hostId, pid, startTime, finishTime);
    }

    public static void insertNewRepairHistory(long startTime, long finishTime) {
        UUID hostId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
        insertNewRepairHistory(hostId, startTime, finishTime);
    }

    public static void addHostIdToDeleteHosts(UUID myID, UUID hostToBeDeleted) {
        SetSerializer<UUID> serializer = SetSerializer.getInstance(UUIDSerializer.instance, UTF8Type.instance.comparatorSet);
        addHostIDToDeleteHostsStatement.execute(QueryState.forInternalCalls(),
                                                   QueryOptions.forInternalCalls(internalQueryCL,
                                                                                 Lists.newArrayList(serializer.serialize(new HashSet<>(Arrays.asList(myID))),
                                                                                                    ByteBufferUtil.bytes(System.currentTimeMillis()),
                                                                                                    ByteBufferUtil.bytes(getLocalDCGroup().hashCode()),
                                                                                                    ByteBufferUtil.bytes(hostToBeDeleted)
                                                                                 )), Dispatcher.RequestTime.forImmediateExecution());
    }

    public static void addPriorityHost(Set<InetAddressAndPort> hosts)
    {
        Set<UUID> hostIds = new HashSet<>();
        for (InetAddressAndPort host : hosts)
        {
            //find hostId from IP address
            UUID hostId = StorageService.instance.getTokenMetadata().getHostId(host);
            hostIds.add(hostId);
            if (hostId != null)
            {
                logger.info("Add host {} to the priority list", hostId);
            }
        }
        if (hostIds.size() > 0)
        {
            SetSerializer<UUID> serializer = SetSerializer.getInstance(UUIDSerializer.instance, UTF8Type.instance.comparatorSet);
            addPriorityHost.execute(QueryState.forInternalCalls(),
                    QueryOptions.forInternalCalls(internalQueryCL,
                            Lists.newArrayList(serializer.serialize(hostIds),
                                               ByteBufferUtil.bytes(getLocalDCGroup().hashCode()))), Dispatcher.RequestTime.forImmediateExecution());
        }
    }

    static void removePriorityStatus(UUID hostId)
    {
        logger.info("Remove host {} from priority list", hostId);
        delStatementPriorityStatus.execute(QueryState.forInternalCalls(),
                QueryOptions.forInternalCalls(internalQueryCL,
                        Lists.newArrayList(ByteBufferUtil.bytes(hostId),
                                ByteBufferUtil.bytes(getLocalDCGroup().hashCode()))), Dispatcher.RequestTime.forImmediateExecution());
    }

    public static Set<InetAddress> getPriorityHosts()
    {
        if (!AutoRepairService.instance.isAutoRepairEnabled()) {
            return Collections.emptySet();
        }
        CurrentRepairStatus status = getCurrentRepairStatus();
        if (status != null && status.priority != null)
        {
            //convert UUID to InetAddress
            Set<InetAddress> hosts = new HashSet<>();
            for (UUID hostId : status.priority)
            {
                hosts.add(StorageService.instance.getTokenMetadata().getEndpointForHostId(hostId).getAddress());
            }
            return hosts;
        }
        return Collections.emptySet();
    }

    public static boolean shouldRepair(String keyspace)
    {
        if (AutoRepairService.instance.getRepairOnlyKeyspaces() != null)
        {
            return AutoRepairService.instance.getRepairOnlyKeyspaces().matcher(keyspace).matches();
        }
        else if (AutoRepairService.instance.getRepairIgnoreKeyspaces() != null)
        {
            return !AutoRepairService.instance.getRepairIgnoreKeyspaces().matcher(keyspace).matches();
        }
        return true;
    }

    public static boolean checkNodeContainsKeyspaceReplica(Keyspace ks)
    {
        AbstractReplicationStrategy replicationStrategy = ks.getReplicationStrategy();
        boolean ksReplicaOnNode = true;
        if (replicationStrategy instanceof NetworkTopologyStrategy)
        {
            Set<String> datacenters = ((NetworkTopologyStrategy) replicationStrategy).getDatacenters();
            String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort());
            if (!datacenters.contains(localDC))
            {
                ksReplicaOnNode = false;
            }
        }
        return ksReplicaOnNode;
    }


    public static boolean tableMaxRepairTimeExceeded(long startTime)
    {
        long tableRepairTimeSoFar = TimeUnit.MILLISECONDS.toSeconds
                (System.currentTimeMillis() - startTime);
        return AutoRepairService.instance.getAutoRepairTableMaxRepairTimeInSec() < tableRepairTimeSoFar;
    }

    public static boolean keyspaceMaxRepairTimeExceeded(long startTime, int numOfTablesToBeRepaired) {
        long keyspaceRepairTimeSoFar = TimeUnit.MILLISECONDS.toSeconds( (System.currentTimeMillis() - startTime));
        return AutoRepairService.instance.getAutoRepairTableMaxRepairTimeInSec() * numOfTablesToBeRepaired < keyspaceRepairTimeSoFar;
    }
}
