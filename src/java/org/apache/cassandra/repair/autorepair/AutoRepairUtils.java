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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import org.apache.cassandra.locator.LocalStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;

import static org.apache.cassandra.repair.autorepair.AutoRepairConfig.Options.DC_GROUP_SEPARATOR;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.RepairTurn.MY_TURN;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.RepairTurn.MY_TURN_DUE_TO_PRIORITY;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.RepairTurn.NOT_MY_TURN;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.RepairTurn.MY_TURN_FORCE_REPAIR;

/**
 * This class serves as a utility class for AutoRepairV2. It contains various helper APIs
 * to store/retrieve repair status, decide whose turn is next, etc.
 */
public class AutoRepairUtils
{
    private static final Logger logger = LoggerFactory.getLogger(AutoRepairUtils.class);
    static final String COL_REPAIR_TYPE = "repair_type";
    static final String COL_PID = "pid";  // this value is used to store the group id of the row.
    static final String COL_HOST_ID = "host_id";
    static final String COL_REPAIR_START_TS = "repair_start_ts";
    static final String COL_REPAIR_FINISH_TS = "repair_finish_ts";
    static final String COL_REPAIR_PRIORITY = "repair_priority";
    static final String COL_DELETE_HOSTS = "delete_hosts";  // this set stores the host ids which think the row should be deleted
    static final String COL_REPAIR_TURN = "repair_turn";  // this record the last repair turn. Normal turn or turn due to priority
    static final String COL_DELETE_HOSTS_UPDATE_TIME = "delete_hosts_update_time"; // the time when delete hosts are upated
    static final String COL_FORCE_REPAIR = "force_repair";  // if set to true, the node will do non-primary range rapair

    final static String SELECT_REPAIR_HISTORY = String.format(
    "SELECT * FROM %s.%s WHERE %s = ? AND %s = ?", SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME,
    AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2, COL_REPAIR_TYPE, COL_PID);
    final static String SELECT_REPAIR_PRIORITY = String.format(
    "SELECT * FROM %s.%s WHERE %s = ? AND %s = ?", SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME,
    AutoRepairKeyspace.AUTO_REPAIR_PRIORITY_V2, COL_REPAIR_TYPE, COL_PID);
    final static String DEL_REPAIR_PRIORITY = String.format(
    "DELETE %s[?] FROM %s.%s WHERE %s = ? AND %s = ?", COL_REPAIR_PRIORITY, SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME,
    AutoRepairKeyspace.AUTO_REPAIR_PRIORITY_V2, COL_REPAIR_TYPE, COL_PID);
    final static String ADD_PRIORITY_HOST = String.format(
    "UPDATE %s.%s SET %s = %s + ?  WHERE %s = ? AND %s = ?", SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME,
    AutoRepairKeyspace.AUTO_REPAIR_PRIORITY_V2, COL_REPAIR_PRIORITY, COL_REPAIR_PRIORITY, COL_REPAIR_TYPE, COL_PID);

    final static String INSERT_NEW_REPAIR_HISTORY = String.format(
    "INSERT INTO %s.%s (%s, %s, %s, %s, %s, %s, %s) values (?, ?, ? ,?, ?, {}, ?) IF NOT EXISTS",
    SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2, COL_REPAIR_TYPE, COL_PID,
    COL_HOST_ID, COL_REPAIR_START_TS, COL_REPAIR_FINISH_TS, COL_DELETE_HOSTS, COL_DELETE_HOSTS_UPDATE_TIME);

    final static String ADD_HOST_ID_TO_DELETE_HOSTS = String.format(
    "UPDATE %s.%s SET %s = %s + ?, %s = ? WHERE %s = ? AND %s = ? AND %s = ? IF EXISTS"
    , SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2, COL_DELETE_HOSTS,
    COL_DELETE_HOSTS, COL_DELETE_HOSTS_UPDATE_TIME, COL_REPAIR_TYPE, COL_PID, COL_HOST_ID);

    final static String DEL_AUTO_REPAIR_HISTORY = String.format(
    "DELETE FROM %s.%s WHERE %s = ? AND %s = ? AND %s = ?"
    , SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2, COL_REPAIR_TYPE, COL_PID,
    COL_HOST_ID);

    final static String RECORD_START_REPAIR_HISTORY = String.format(
    "UPDATE %s.%s SET %s= ?, repair_turn = ? WHERE %s = ? AND %s = ? AND %s = ?"
    , SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2, COL_REPAIR_START_TS,
    COL_REPAIR_TYPE, COL_PID, COL_HOST_ID);

    final static String RECORD_FINISH_REPAIR_HISTORY = String.format(

    "UPDATE %s.%s SET %s= ?, %s=false WHERE %s = ? AND %s = ? AND %s = ?"
    , SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2, COL_REPAIR_FINISH_TS,
    COL_FORCE_REPAIR, COL_REPAIR_TYPE, COL_PID, COL_HOST_ID);

    final static String CLEAR_DELETE_HOSTS = String.format(
    "UPDATE %s.%s SET %s= {} WHERE %s = ? AND %s = ? AND %s = ?"
    , SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2, COL_DELETE_HOSTS,
    COL_REPAIR_TYPE, COL_PID, COL_HOST_ID);

    final static String SET_FORCE_REPAIR = String.format(
    "UPDATE %s.%s SET %s=true  WHERE %s = ? AND %s = ? AND %s = ?"
    , SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY_V2, COL_FORCE_REPAIR,
    COL_REPAIR_TYPE, COL_PID, COL_HOST_ID);

    static ModificationStatement delStatementRepairHistory;
    static SelectStatement selectStatementRepairHistory;
    static ModificationStatement delStatementPriorityStatus;
    static SelectStatement selectStatementRepairPriority;
    static ModificationStatement addPriorityHost;
    static ModificationStatement insertNewRepairHistoryStatement;
    static ModificationStatement recordStartRepairHistoryStatement;
    static ModificationStatement recordFinishRepairHistoryStatement;
    static ModificationStatement addHostIDToDeleteHostsStatement;
    static ModificationStatement clearDeleteHostsStatement;
    static ModificationStatement setForceRepairStatement;
    static ConsistencyLevel internalQueryCL;

    public enum RepairTurn
    {
        MY_TURN,
        NOT_MY_TURN,
        MY_TURN_DUE_TO_PRIORITY,
        MY_TURN_FORCE_REPAIR
    }

    public static void setup()
    {
        selectStatementRepairHistory = (SelectStatement) QueryProcessor.getStatement(SELECT_REPAIR_HISTORY, ClientState
                                                                                                            .forInternalCalls());
        selectStatementRepairPriority = (SelectStatement) QueryProcessor.getStatement(SELECT_REPAIR_PRIORITY, ClientState
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
        setForceRepairStatement = (ModificationStatement) QueryProcessor.getStatement(SET_FORCE_REPAIR, ClientState
                                                                                                        .forInternalCalls());
        clearDeleteHostsStatement = (ModificationStatement) QueryProcessor.getStatement(CLEAR_DELETE_HOSTS, ClientState
                                                                                                            .forInternalCalls());
        delStatementRepairHistory = (ModificationStatement) QueryProcessor.getStatement(DEL_AUTO_REPAIR_HISTORY, ClientState
                                                                                                                 .forInternalCalls());
        Keyspace autoRepairKS = Schema.instance.getKeyspaceInstance(SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME);
        internalQueryCL = autoRepairKS.getReplicationStrategy().getClass() == NetworkTopologyStrategy.class ?
                          ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.ONE;
    }

    public static class AutoRepairHistory
    {
        UUID hostId;
        String repairTurn;
        long lastRepairStartTime;
        long lastRepairFinishTime;
        Set<UUID> deleteHosts;
        long deleteHostsUpdateTime;
        boolean forceRepair;

        public AutoRepairHistory(UUID hostId, String repairTurn, long lastRepairStartTime, long lastRepairFinishTime,
                                 Set<UUID> deleteHosts, long deleteHostsUpateTime, boolean forceRepair)
        {
            this.hostId = hostId;
            this.repairTurn = repairTurn;
            this.lastRepairStartTime = lastRepairStartTime;
            this.lastRepairFinishTime = lastRepairFinishTime;
            this.deleteHosts = deleteHosts;
            if (this.deleteHosts == null)
            {
                this.deleteHosts = new HashSet<>();
            }
            this.deleteHostsUpdateTime = deleteHostsUpateTime;
            this.forceRepair = forceRepair;
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

        public boolean isRepairRunning()
        {
            // if a repair history record has start time laster than finish time, it means the repair is running
            return lastRepairStartTime > lastRepairFinishTime;
        }

        public long getLastRepairFinishTime()
        {
            return lastRepairFinishTime;
        }
    }

    public static class CurrentRepairStatus
    {
        public Set<UUID> hostIdsWithOnGoingRepair;  // hosts that is running repair
        public Set<UUID> hostIdsWithOnGoingForceRepair; // hosts that is running repair because of force repair
        Set<UUID> priority;
        List<AutoRepairHistory> historiesWithoutOnGoingRepair;  // hosts that is NOT running repair

        public CurrentRepairStatus(List<AutoRepairHistory> repairHistories, Set<UUID> priority)
        {
            hostIdsWithOnGoingRepair = new HashSet<>();
            hostIdsWithOnGoingForceRepair = new HashSet<>();
            historiesWithoutOnGoingRepair = new ArrayList<>();

            for (AutoRepairHistory history : repairHistories)
            {
                if (history.isRepairRunning())
                {
                    if (history.forceRepair)
                    {
                        hostIdsWithOnGoingForceRepair.add(history.hostId);
                    }
                    else
                    {
                        hostIdsWithOnGoingRepair.add(history.hostId);
                    }
                }
                else
                {
                    historiesWithoutOnGoingRepair.add(history);
                }
            }
            this.priority = priority;
        }

        public String toString()
        {
            return MoreObjects.toStringHelper(this).
                              add("hostIdsWithOnGoingRepair", hostIdsWithOnGoingRepair).
                              add("hostIdsWithOnGoingForceRepair", hostIdsWithOnGoingForceRepair).
                              add("historiesWithoutOnGoingRepair", historiesWithoutOnGoingRepair).
                              add("priority", priority).
                              toString();
        }
    }

    @VisibleForTesting
    public static List<AutoRepairHistory> getAutoRepairHistoryByGroupID(RepairType repairType, int groupHash)
    {
        UntypedResultSet repairHistoryResult;

        ResultMessage.Rows repairStatusRows = selectStatementRepairHistory.execute(QueryState.forInternalCalls(),
                                                                                   QueryOptions.forInternalCalls(internalQueryCL, Lists.newArrayList(ByteBufferUtil.bytes(repairType.toString()),
                                                                                                                                                     ByteBufferUtil.bytes(groupHash))), System.nanoTime());
        repairHistoryResult = UntypedResultSet.create(repairStatusRows.result);

        List<AutoRepairHistory> repairHistories = new ArrayList<>();
        if (repairHistoryResult.size() > 0)
        {
            for (UntypedResultSet.Row row : repairHistoryResult)
            {
                UUID hostId = row.getUUID(COL_HOST_ID);
                String repairTurn = null;
                if (row.has(COL_REPAIR_TURN))
                    repairTurn = row.getString(COL_REPAIR_TURN);
                long lastRepairStartTime = row.getLongOrDefault(COL_REPAIR_START_TS, 0);
                long lastRepairFinishTime = row.getLongOrDefault(COL_REPAIR_FINISH_TS, 0);
                Set<UUID> deleteHosts = row.getSet(COL_DELETE_HOSTS, UUIDType.instance);
                long deleteHostsUpdateTime = row.getLongOrDefault(COL_DELETE_HOSTS_UPDATE_TIME, 0);
                Boolean forceRepair = row.has(COL_FORCE_REPAIR) ? row.getBoolean(COL_FORCE_REPAIR) : false;
                repairHistories.add(new AutoRepairHistory(hostId, repairTurn, lastRepairStartTime, lastRepairFinishTime,
                                                          deleteHosts, deleteHostsUpdateTime, forceRepair));
            }
            return repairHistories;
        }
        logger.info("No repair history found for pid = " + groupHash);
        return null;
    }

    public static List<AutoRepairHistory> getAutoRepairHistoryForLocalGroup(RepairType repairType)
    {
        return getAutoRepairHistoryByGroupID(repairType, getLocalDCGroup(repairType).hashCode());
    }

    // A host may add itself in delete hosts for some other hosts due to restart or some temp gossip issue. If a node's record
    // delete_hosts is not growing for more than 2 hours, we consider it as a normal node so we clear the delete_hosts for that node
    public static void clearDeleteHosts(RepairType repairType, UUID hostId)
    {
        clearDeleteHostsStatement.execute(QueryState.forInternalCalls(),
                                          QueryOptions.forInternalCalls(internalQueryCL,
                                                                        Lists.newArrayList(ByteBufferUtil.bytes(repairType.toString()),
                                                                                           ByteBufferUtil.bytes(getLocalDCGroup(repairType).hashCode()),
                                                                                           ByteBufferUtil.bytes(hostId))), System.nanoTime());
    }

    public static void setForceRepairNewNode(RepairType repairType)
    {
        // this function will be called when a node bootstrap finished
        int pid = getLocalDCGroup(repairType).hashCode();
        UUID hostId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
        // insert the data first
        insertNewRepairHistory(repairType, System.currentTimeMillis(), System.currentTimeMillis());
        setForceRepair(repairType, pid, hostId);
    }

    public static void setForceRepair(RepairType repairType, Set<InetAddressAndPort> hosts)
    {
        // this function is used by nodetool
        int pid = getLocalDCGroup(repairType).hashCode();
        for (InetAddressAndPort host : hosts)
        {
            UUID hostId = Gossiper.instance.getHostId(host);
            setForceRepair(repairType, pid, hostId);
        }
    }

    public static void setForceRepair(RepairType repairType, int pid, UUID hostId)
    {
        setForceRepairStatement.execute(QueryState.forInternalCalls(),
                                        QueryOptions.forInternalCalls(internalQueryCL,
                                                                      Lists.newArrayList(ByteBufferUtil.bytes(repairType.toString()),
                                                                                         ByteBufferUtil.bytes(pid),
                                                                                         ByteBufferUtil.bytes(hostId))),
                                        System.nanoTime());

        logger.info("Set force repair repair type: {}, pid: {}, node: {}", repairType, pid, hostId);
    }

    public static CurrentRepairStatus getCurrentRepairStatus(RepairType repairType)
    {
        List<AutoRepairHistory> autoRepairHistories = getAutoRepairHistoryForLocalGroup(repairType);
        return getCurrentRepairStatus(repairType, autoRepairHistories);
    }

    public static CurrentRepairStatus getCurrentRepairStatus(RepairType repairType, List<AutoRepairHistory> autoRepairHistories)
    {
        if (autoRepairHistories != null)
        {
            CurrentRepairStatus status = new CurrentRepairStatus(autoRepairHistories, getPriorityHostIds(repairType));

            return status;
        }
        return null;
    }

    public static Set<String> getLocalDCGroup(RepairType repairType)
    {
        String localDataCenter = DatabaseDescriptor.getLocalDataCenter();
        Set<String> localGroup = new HashSet<>();
        for (String groupStr : AutoRepairService.instance.getAutoRepairConfig().getDCGroups(repairType))
        {
            Set<String> group = Arrays.stream(groupStr.split(DC_GROUP_SEPARATOR)).collect(Collectors.toSet());
            if (group.contains(localDataCenter))
            {
                localGroup = group;
                break;
            }
        }
        return localGroup;
    }

    // if dc groups is empty(not set), return the input value. If groups are set, only return the nodes in the local group
    public static Set<InetAddressAndPort> processNodesByGroup(RepairType repairType, Set<InetAddressAndPort> allNodesInRing)
    {
        Set<String> dcGroups = AutoRepairService.instance.getAutoRepairConfig().getDCGroups(repairType);
        if (dcGroups == null || dcGroups.isEmpty())
        {
            logger.info("No data center groups is defined, will use all nodes in ring as one group.");
            return allNodesInRing;
        }
        Set<String> localGroup = getLocalDCGroup(repairType);
        logger.info("Auto repair local group for repair type {} is {}", repairType, localGroup);

        Set<InetAddressAndPort> localGroupNodes = new HashSet<>();
        for (InetAddressAndPort node : allNodesInRing)
        {
            if (localGroup.contains(DatabaseDescriptor.getEndpointSnitch().getDatacenter(node)))
            {
                localGroupNodes.add(node);
            }
        }
        logger.info("Total number of nodes in group {} for repair type {} is {}, local nodes: {}.", localGroup, repairType, localGroupNodes.size(), localGroupNodes);
        return localGroupNodes;
    }

    @VisibleForTesting
    protected static TreeSet<UUID> getHostIdsInCurrentRing(RepairType repairType, Set<InetAddressAndPort> allNodesInRing)
    {
        TreeSet<UUID> hostIdsInCurrentRing = new TreeSet<>();
        allNodesInRing = processNodesByGroup(repairType, allNodesInRing);
        for (InetAddressAndPort node : allNodesInRing)
        {
            String nodeDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(node);
            if (AutoRepairService.instance.getAutoRepairConfig().getIgnoreDCs(repairType).contains(nodeDC))
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
            }
            else
            {
                logger.info("Node is not present in Gossipe cache node {}, node data center {}", node, nodeDC);
            }
        }
        return hostIdsInCurrentRing;
    }

    public static TreeSet<UUID> getHostIdsInCurrentRing(RepairType repairType)
    {
        Set<InetAddressAndPort> allNodesInRing = StorageService.instance.getTokenMetadata().getAllEndpoints();
        return getHostIdsInCurrentRing(repairType, allNodesInRing);
    }

    // This function will return the host ID for the node which has not been repaired for longest time
    public static AutoRepairHistory getHostWithLongestUnrepairTime(RepairType repairType)
    {
        List<AutoRepairHistory> autoRepairHistories = getAutoRepairHistoryForLocalGroup(repairType);
        return getHostWithLongestUnrepairTime(autoRepairHistories);
    }

    private static AutoRepairHistory getHostWithLongestUnrepairTime(List<AutoRepairHistory> autoRepairHistories)
    {
        if (autoRepairHistories == null)
        {
            return null;
        }
        AutoRepairHistory rst = null;
        long oldestTimestamp = Long.MAX_VALUE;
        for (AutoRepairHistory autoRepairHistory : autoRepairHistories)
        {
            if (autoRepairHistory.lastRepairFinishTime < oldestTimestamp)
            {
                rst = autoRepairHistory;
                oldestTimestamp = autoRepairHistory.lastRepairFinishTime;
            }
        }
        return rst;
    }

    public static int getMaxNumberOfNodeRunAutoRepairInGroup(RepairType repairType, int groupSize)
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        if (groupSize == 0)
        {
            return Math.max(config.getParallelRepairCountInGroup(repairType), 1);
        }
        // we will use the max number from config between auto_repair_parallel_repair_count_in_group and auto_repair_parallel_repair_percentage_in_group
        int value = Math.max(groupSize * config.getParallelRepairPercentageInGroup(repairType) / 100,
                             config.getParallelRepairCountInGroup(repairType));
        // make sure at least one node getting repaired
        return Math.max(1, value);
    }

    // TODO: this method has become very convoluted over the years, we should try to refactor it (SO-29818)
    @VisibleForTesting
    public static RepairTurn myTurnToRunRepair(RepairType repairType, UUID myId)
    {
        try
        {
            Set<InetAddressAndPort> allNodesInRing = StorageService.instance.getTokenMetadata().getAllEndpoints();
            logger.info("Total nodes in ring {}", allNodesInRing.size());
            TreeSet<UUID> hostIdsInCurrentRing = getHostIdsInCurrentRing(repairType, allNodesInRing);
            logger.info("Total nodes qualified for repair {}", hostIdsInCurrentRing.size());

            List<AutoRepairHistory> autoRepairHistories = getAutoRepairHistoryForLocalGroup(repairType);
            int localGroup = getLocalDCGroup(repairType).hashCode();
            Set<UUID> autoRepairHistoryIds = new HashSet<>();

            // 1. Remove any node that is not part of group based on goissip info
            if (autoRepairHistories != null)
            {
                for (AutoRepairHistory nodeHistory : autoRepairHistories)
                {
                    autoRepairHistoryIds.add(nodeHistory.hostId);
                    // clear delete_hosts if the node's delete hosts is not growing for more than two hours
                    AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
                    if (nodeHistory.deleteHosts.size() > 0
                        && config.getAutoRepairHistoryClearDeleteHostsBufferInSec() < TimeUnit.MILLISECONDS.toSeconds(
                    System.currentTimeMillis() - nodeHistory.deleteHostsUpdateTime
                    ))
                    {
                        clearDeleteHosts(repairType, nodeHistory.hostId);
                        logger.info("Delete hosts for {} for repair type {} has not been updated for more than {} seconds. Delete hosts has been cleared. Delete hosts before clear {}"
                        , nodeHistory.hostId, repairType, config.getAutoRepairHistoryClearDeleteHostsBufferInSec(), nodeHistory.deleteHosts);
                    }
                    else if (!hostIdsInCurrentRing.contains(nodeHistory.hostId))
                    {
                        if (nodeHistory.deleteHosts.size() > Math.max(2, hostIdsInCurrentRing.size() * 0.5))
                        {
                            // More than half of the groups thinks the record should be deleted
                            logger.info("{} think {} is orphan node, will delete auto repair history for repair type {}.", nodeHistory.deleteHosts, nodeHistory.hostId, repairType);
                            deleteAutoRepairHistory(repairType, nodeHistory.hostId);
                        }
                        else
                        {
                            // I think this host should be deleted
                            logger.info("I({}) think {} is not part of ring, vote to delete it for repair type {}.", myId, nodeHistory.hostId, repairType);
                            addHostIdToDeleteHosts(repairType, myId, nodeHistory.hostId);
                        }
                    }
                }
            }

            // 2. Add node to auto repair history table if a node is in gossip info
            for (UUID hostId : hostIdsInCurrentRing)
            {
                if (!autoRepairHistoryIds.contains(hostId))
                {
                    logger.info("{} for repair type {} doesn't exist in the auto repair history table, insert a new record.", repairType, hostId);
                    insertNewRepairHistory(repairType, hostId, System.currentTimeMillis(), System.currentTimeMillis());
                }
            }

            //get current repair status
            CurrentRepairStatus currentRepairStatus = getCurrentRepairStatus(repairType, autoRepairHistories);
            if (currentRepairStatus != null)
            {
                logger.info("Latest repair status {}", currentRepairStatus);
                //check if I am forced to run repair
                for (AutoRepairHistory history : currentRepairStatus.historiesWithoutOnGoingRepair)
                {
                    if (history.forceRepair && history.hostId.equals(myId))
                    {
                        return MY_TURN_FORCE_REPAIR;
                    }
                }
            }

            int parallelRepairNumber = getMaxNumberOfNodeRunAutoRepairInGroup(repairType,
                                                                              autoRepairHistories == null ? 0 : autoRepairHistories.size());
            logger.info("Will run repairs concurrently on {} node(s)", parallelRepairNumber);

            if (currentRepairStatus == null || parallelRepairNumber > currentRepairStatus.hostIdsWithOnGoingRepair.size())
            {
                // more repairs can be run, I might be the new one

                if (autoRepairHistories != null)
                {
                    logger.info("Auto repair history table has {} records for group {}", autoRepairHistories.size(), localGroup);
                }
                else
                {
                    // try to fetch again
                    autoRepairHistories = getAutoRepairHistoryForLocalGroup(repairType);
                    currentRepairStatus = getCurrentRepairStatus(repairType, autoRepairHistories);
                    if (autoRepairHistories == null || currentRepairStatus == null)
                    {
                        logger.error("No record found for group id {}", localGroup);
                        return NOT_MY_TURN;
                    }
                }

                // get the longest unrepaired node from the nodes which are not running repair
                AutoRepairHistory defaultNodeToBeRepaired = getHostWithLongestUnrepairTime(currentRepairStatus.historiesWithoutOnGoingRepair);
                //check who is next, which is helpful for debugging
                logger.info("Next node to be repaired for repair type {} by default: {}", repairType, defaultNodeToBeRepaired);
                UUID priorityHostId = null;
                if (currentRepairStatus.priority != null)
                {
                    for (UUID priorityID : currentRepairStatus.priority)
                    {
                        // remove ids doesn't belong to this ring
                        if (!hostIdsInCurrentRing.contains(priorityID))
                        {
                            logger.info("{} is not part of the current ring, will be removed from priority list.", priorityID);
                            removePriorityStatus(repairType, priorityID);
                        }
                        else
                        {
                            priorityHostId = priorityID;
                            break;
                        }
                    }
                }

                if (priorityHostId != null && !myId.equals(priorityHostId))
                {
                    logger.info("Priority list is not empty and I'm not the first node in the list, not my turn." +
                                "First node in priority list is {}", StorageService.instance.getTokenMetadata().getEndpointForHostId(priorityHostId));
                    return NOT_MY_TURN;
                }

                if (myId.equals(priorityHostId))
                {
                    //I have a priority for repair hence its my turn now
                    return MY_TURN_DUE_TO_PRIORITY;
                }

                if (defaultNodeToBeRepaired.hostId.equals(myId))
                    return MY_TURN;
            }
            else if (currentRepairStatus.hostIdsWithOnGoingForceRepair.contains(myId))
            {
                return MY_TURN_FORCE_REPAIR;
            }
            // for some reason I was not done with the repair hence resume (maybe node restart in-between, etc.)
            return currentRepairStatus.hostIdsWithOnGoingRepair.contains(myId) ? MY_TURN : NOT_MY_TURN;
        }
        catch (Exception e)
        {
            logger.error("Exception while deciding node's turn:", e);
        }
        return NOT_MY_TURN;
    }

    static void deleteAutoRepairHistory(RepairType repairType, UUID hostId)
    {
        //delete the given hostId from current local group
        delStatementRepairHistory.execute(QueryState.forInternalCalls(),
                                          QueryOptions.forInternalCalls(internalQueryCL,
                                                                        Lists.newArrayList(ByteBufferUtil.bytes(repairType.toString()),
                                                                                           ByteBufferUtil.bytes(getLocalDCGroup(repairType).hashCode()),
                                                                                           ByteBufferUtil.bytes(hostId))), System.nanoTime());
    }

    static void updateStartAutoRepairHistory(RepairType repairType, UUID myId, long timestamp, RepairTurn turn)
    {
        recordStartRepairHistoryStatement.execute(QueryState.forInternalCalls(),
                                                  QueryOptions.forInternalCalls(internalQueryCL,
                                                                                Lists.newArrayList(ByteBufferUtil.bytes(timestamp),
                                                                                                   ByteBufferUtil.bytes(turn.name()),
                                                                                                   ByteBufferUtil.bytes(repairType.toString()),
                                                                                                   ByteBufferUtil.bytes(getLocalDCGroup(repairType).hashCode()),
                                                                                                   ByteBufferUtil.bytes(myId)
                                                                                )), System.nanoTime());
    }

    static void updateFinishAutoRepairHistory(RepairType repairType, UUID myId, long timestamp)
    {
        recordFinishRepairHistoryStatement.execute(QueryState.forInternalCalls(),
                                                   QueryOptions.forInternalCalls(internalQueryCL,
                                                                                 Lists.newArrayList(ByteBufferUtil.bytes(timestamp),
                                                                                                    ByteBufferUtil.bytes(repairType.toString()),
                                                                                                    ByteBufferUtil.bytes(getLocalDCGroup(repairType).hashCode()),
                                                                                                    ByteBufferUtil.bytes(myId)
                                                                                 )), System.nanoTime());
        // Do not remove beblow log, the log is used by dtest
        logger.info("Auto repair finished for {}", myId);
    }

    public static void insertNewRepairHistory(RepairType repairType, UUID hostId, int pid, long startTime, long finishTime)
    {
        try
        {
            Keyspace autoRepairKS = Schema.instance.getKeyspaceInstance(SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME);
            ConsistencyLevel cl = autoRepairKS.getReplicationStrategy().getClass() == NetworkTopologyStrategy.class ?
                                  ConsistencyLevel.LOCAL_SERIAL : null;

            UntypedResultSet resultSet;
            ResultMessage.Rows resultMessage = (ResultMessage.Rows) insertNewRepairHistoryStatement.execute(
            QueryState.forInternalCalls(), QueryOptions.create(internalQueryCL, Lists.newArrayList(
            ByteBufferUtil.bytes(repairType.toString()),
            ByteBufferUtil.bytes(pid),
            ByteBufferUtil.bytes(hostId),
            ByteBufferUtil.bytes(startTime),
            ByteBufferUtil.bytes(finishTime),
            ByteBufferUtil.bytes(System.currentTimeMillis())
            ), false, -1, null, cl, ProtocolVersion.CURRENT, SchemaConstants.AUTO_REPAIR_KEYSPACE_NAME),
            System.nanoTime());
            resultSet = UntypedResultSet.create(resultMessage.result);
            boolean applied = resultSet.one().getBoolean(ModificationStatement.CAS_RESULT_COLUMN.toString());
            if (applied)
            {
                logger.info("Successfully inserted a new auto repair history record for host id: {} in pid: {}", hostId, pid);
            }
            else
            {
                logger.info("Record exists, no need to insert again for host id: {} in pid: {}", hostId, pid);
            }
        }
        catch (Exception e)
        {
            logger.error("Exception in inserting new repair history:", e);
        }
    }

    public static void insertNewRepairHistory(RepairType repairType, UUID hostId, long startTime, long finishTime)
    {
        int pid = getLocalDCGroup(repairType).hashCode();
        insertNewRepairHistory(repairType, hostId, pid, startTime, finishTime);
    }

    public static void insertNewRepairHistory(RepairType repairType, long startTime, long finishTime)
    {
        UUID hostId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
        insertNewRepairHistory(repairType, hostId, startTime, finishTime);
    }

    public static void addHostIdToDeleteHosts(RepairType repairType, UUID myID, UUID hostToBeDeleted)
    {
        SetSerializer<UUID> serializer = SetSerializer.getInstance(UUIDSerializer.instance, UTF8Type.instance.comparatorSet);
        addHostIDToDeleteHostsStatement.execute(QueryState.forInternalCalls(),
                                                QueryOptions.forInternalCalls(internalQueryCL,
                                                                              Lists.newArrayList(serializer.serialize(new HashSet<>(Arrays.asList(myID))),
                                                                                                 ByteBufferUtil.bytes(System.currentTimeMillis()),
                                                                                                 ByteBufferUtil.bytes(repairType.toString()),
                                                                                                 ByteBufferUtil.bytes(getLocalDCGroup(repairType).hashCode()),
                                                                                                 ByteBufferUtil.bytes(hostToBeDeleted)
                                                                              )), System.nanoTime());
    }

    public static void addPriorityHosts(RepairType repairType, Set<InetAddressAndPort> hosts)
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
                                                                                     ByteBufferUtil.bytes(repairType.toString()),
                                                                                     ByteBufferUtil.bytes(getLocalDCGroup(repairType).hashCode()))),
                                    System.nanoTime());
        }
    }

    static void removePriorityStatus(RepairType repairType, UUID hostId)
    {
        logger.info("Remove host {} from priority list", hostId);
        delStatementPriorityStatus.execute(QueryState.forInternalCalls(),
                                           QueryOptions.forInternalCalls(internalQueryCL,
                                                                         Lists.newArrayList(ByteBufferUtil.bytes(hostId),
                                                                                            ByteBufferUtil.bytes(repairType.toString()),
                                                                                            ByteBufferUtil.bytes(getLocalDCGroup(repairType).hashCode()))),
                                           System.nanoTime());
    }

    public static Set<UUID> getPriorityHostIds(RepairType repairType)
    {
        return getPriorityHostIds(repairType, getLocalDCGroup(repairType).hashCode());
    }

    public static Set<UUID> getPriorityHostIds(RepairType repairType, int groupHash)
    {
        UntypedResultSet repairPriorityResult;

        ResultMessage.Rows repairPriorityRows = selectStatementRepairPriority.execute(QueryState.forInternalCalls(),
                                                                                      QueryOptions.forInternalCalls(internalQueryCL, Lists.newArrayList(ByteBufferUtil.bytes(repairType.toString()),
                                                                                                                                                        ByteBufferUtil.bytes(groupHash))), System.nanoTime());
        repairPriorityResult = UntypedResultSet.create(repairPriorityRows.result);

        Set<UUID> priorities = null;
        if (repairPriorityResult.size() > 0)
        {
            // there should be only one row
            UntypedResultSet.Row row = repairPriorityResult.one();
            priorities = row.getSet(COL_REPAIR_PRIORITY, UUIDType.instance);
        }
        if (priorities != null)
        {
            return priorities;
        }
        return Collections.emptySet();
    }

    public static Set<InetAddressAndPort> getPriorityHosts(RepairType repairType)
    {
        Set<InetAddressAndPort> hosts = new HashSet<>();
        for (UUID hostId : getPriorityHostIds(repairType))
        {
            hosts.add(StorageService.instance.getTokenMetadata().getEndpointForHostId(hostId));
        }
        return hosts;
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
        if (replicationStrategy instanceof LocalStrategy)
        {
            ksReplicaOnNode = false;
        }
        return ksReplicaOnNode;
    }


    public static boolean tableMaxRepairTimeExceeded(RepairType repairType, long startTime)
    {
        long tableRepairTimeSoFar = TimeUnit.MILLISECONDS.toSeconds
                                                         (System.currentTimeMillis() - startTime);
        return AutoRepairService.instance.getAutoRepairConfig().getAutoRepairTableMaxRepairTimeInSec(repairType) <
               tableRepairTimeSoFar;
    }

    public static boolean keyspaceMaxRepairTimeExceeded(RepairType repairType, long startTime, int numOfTablesToBeRepaired)
    {
        long keyspaceRepairTimeSoFar = TimeUnit.MILLISECONDS.toSeconds((System.currentTimeMillis() - startTime));
        return AutoRepairService.instance.getAutoRepairConfig().getAutoRepairTableMaxRepairTimeInSec(repairType) *
               numOfTablesToBeRepaired < keyspaceRepairTimeSoFar;
    }

    public static List<String> getAllMVs(RepairType repairType, Keyspace keyspace, TableMetadata tableMetadata)
    {
        List<String> allMvs = new ArrayList<>();
        if (AutoRepairService.instance.getAutoRepairConfig().getMVRepairEnabled(repairType) && keyspace.getMetadata().views != null)
        {
            Iterator<ViewMetadata> views = keyspace.getMetadata().views.forTable(tableMetadata.id).iterator();
            while (views.hasNext())
            {
                String viewName = views.next().name();
                logger.info("Adding MV to the list {}.{}.{}", keyspace.getName(), tableMetadata.name, viewName);
                allMvs.add(viewName);
            }
        }
        return allMvs;
    }
}
