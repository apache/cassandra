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
import com.google.common.collect.Iterables;
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
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
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
    static final String KEYSPACE_NAME = "system_auto_repair";
    static final String REPAIR_STATUS = "cur_repair_status";
    static final String COL_PID = "pid";
    static final String COL_HOST_ID = "host_id";
    static final String COL_REPAIR_STATUS = "repair_status";
    static final String COL_REPAIR_TS = "repair_ts";
    static final String COL_REPAIR_PRIORITY = "repair_priority";
    static final String REPAIR_SCHEMA =
            String.format("CREATE TABLE " + REPAIR_STATUS + "("
                            + "pid int,"
                            + "%s uuid,"
                            + "%s int,"
                            + "%s timestamp,"
                            + "%s set<uuid>,"
                            + "PRIMARY KEY (pid))", COL_HOST_ID, COL_REPAIR_STATUS,
                    COL_REPAIR_TS, COL_REPAIR_PRIORITY);

    final static String INSERT_REPAIR_STATUS = String.format(
            "INSERT INTO %s.%s (%s, %s, %s, %s) values (?, ?, ?, ?)"
            , KEYSPACE_NAME, REPAIR_STATUS, COL_PID, COL_HOST_ID, COL_REPAIR_STATUS, COL_REPAIR_TS);
    final static String SELECT_REPAIR_STATUS = String.format(
            "SELECT %s, %s, %s, %s FROM %s.%s WHERE pid = 0"
            , COL_HOST_ID, COL_REPAIR_STATUS, COL_REPAIR_TS, COL_REPAIR_PRIORITY, KEYSPACE_NAME, REPAIR_STATUS);
    final static String DEL_REPAIR_STATUS = String.format(
            "DELETE FROM %s.%s WHERE pid = ?", KEYSPACE_NAME, REPAIR_STATUS);
    final static String DEL_REPAIR_PRIORITY = String.format(
            "DELETE %s[?] FROM %s.%s WHERE pid = ?", COL_REPAIR_PRIORITY, KEYSPACE_NAME, REPAIR_STATUS);
    final static String ADD_PRIORITY_HOST = String.format(
            "UPDATE %s.%s SET %s = %s + ?  WHERE pid = 0", KEYSPACE_NAME, REPAIR_STATUS, COL_REPAIR_PRIORITY, COL_REPAIR_PRIORITY);

    static ModificationStatement modificationStatementRepairStatus;
    static ModificationStatement delStatementRepairStatus;
    static SelectStatement selectStatementRepairStatus;
    static ModificationStatement delStatementPriorityStatus;
    static ModificationStatement addPriorityHost;
    static ConsistencyLevel internalQueryCL;

    enum RepairTurn
    {
        MY_TURN,
        NOT_MY_TURN,
        MY_TURN_DUE_TO_PRIORITY
    }

    private static KeyspaceMetadata getAutoRepairSchema()
    {
        TableMetadata currentRepairSchema = CreateTableStatement.parse(REPAIR_SCHEMA, KEYSPACE_NAME)
                                                                .comment("current repair status details")
                                                                .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90))
                                                                .build();
        return KeyspaceMetadata.create(KEYSPACE_NAME, KeyspaceParams.simple(1), Tables.of(currentRepairSchema));
    }

    public static void setup()
    {
        KeyspaceMetadata autoRepairSchema = getAutoRepairSchema();
        try
        {
            Schema.instance.transform(SchemaTransformations.updateSystemKeyspace(autoRepairSchema, 0));
        }
        catch (AlreadyExistsException e)
        {
            logger.debug("Attempted to create new keyspace {}, but it already exists", autoRepairSchema.name);
        }

        modificationStatementRepairStatus = (ModificationStatement) QueryProcessor.getStatement(INSERT_REPAIR_STATUS, ClientState
                .forInternalCalls());
        selectStatementRepairStatus = (SelectStatement) QueryProcessor.getStatement(SELECT_REPAIR_STATUS, ClientState
                .forInternalCalls());
        delStatementRepairStatus = (ModificationStatement) QueryProcessor.getStatement(DEL_REPAIR_STATUS, ClientState
                .forInternalCalls());
        delStatementPriorityStatus = (ModificationStatement) QueryProcessor.getStatement(DEL_REPAIR_PRIORITY, ClientState
                .forInternalCalls());
        addPriorityHost = (ModificationStatement) QueryProcessor.getStatement(ADD_PRIORITY_HOST, ClientState
                .forInternalCalls());

        Keyspace autoRepairKS = Schema.instance.getKeyspaceInstance(KEYSPACE_NAME);
        internalQueryCL = autoRepairKS.getReplicationStrategy().getClass() == NetworkTopologyStrategy.class ?
                ConsistencyLevel.LOCAL_ONE : ConsistencyLevel.ONE;
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

    private static CurrentRepairStatus getCurrentRepairStatus()
    {
        //get current repair status
        ResultMessage.Rows repairStatusRows = selectStatementRepairStatus.execute(QueryState.forInternalCalls(), QueryOptions
                .forInternalCalls(internalQueryCL, null), Dispatcher.RequestTime.forImmediateExecution());
        UntypedResultSet repairStatusResult = UntypedResultSet.create(repairStatusRows.result);

        if (repairStatusResult.size() > 0)
        {
            UntypedResultSet.Row row = repairStatusResult.one();
            UUID hostIdWithOnGoingRepair = row.getUUID(COL_HOST_ID);
            int currentRepairStatus = row.getInt(COL_REPAIR_STATUS);
            long repairFinishedTs = row.getLong(COL_REPAIR_TS);
            Set<UUID> priority = row.getSet(COL_REPAIR_PRIORITY, UUIDType.instance);
            logger.debug("Latest repair status hostIdWithOnGoingRepair {}, currentRepairStatus {}, " +
                            "repair_finished_ts {}", hostIdWithOnGoingRepair,
                    currentRepairStatus, repairFinishedTs);
            CurrentRepairStatus status = new CurrentRepairStatus(hostIdWithOnGoingRepair, currentRepairStatus,
                    repairFinishedTs, priority);

            return status;
        }
        return null;
    }

    @VisibleForTesting
    public static RepairTurn myTurnToRunRepair(UUID myId)
    {
        RepairTurn myTurn = NOT_MY_TURN;
        try
        {
            Set<InetAddressAndPort> allNodesInRing = StorageService.instance.getTokenMetadata().getAllEndpoints();
            logger.info("Total nodes in ring {}", allNodesInRing.size());
            TreeSet<UUID> hostIdsInCurrentRing = new TreeSet<>();
            for (InetAddressAndPort node : allNodesInRing)
            {
                String nodeDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(node);
                if (AutoRepairService.instance.getIgnoreDCs().contains(nodeDC))
                {
                    logger.debug("Ignore node {} because its datacenter is {}", node, nodeDC);
                    continue;
                }
                /** Check if endpoint state exists in gossip or not. If it
                 * does not then this maybe a ghost node so ignore it
                 */
                if (Gossiper.instance.getEndpointStateForEndpoint(node) !=  null)
                {
                    UUID hostId = Gossiper.instance.getHostId(node);
                    hostIdsInCurrentRing.add(hostId);
                }
            }
            logger.info("Total nodes qualified for repair {}", hostIdsInCurrentRing.size());

            //get my previous neighbour
            UUID myNeighbourHostId = null;
            boolean firstInTheRing = false;
            for (UUID hostId : hostIdsInCurrentRing)
            {
                if (hostId.equals(myId))
                {
                    break;
                }
                myNeighbourHostId = hostId;
            }

            if (myNeighbourHostId == null)
            {
                firstInTheRing = true;
                //i.e. I am the first one in the ring, check the last node's repair status
                myNeighbourHostId = hostIdsInCurrentRing.last();
            }

            //get current repair status
            CurrentRepairStatus currentRepairStatus = getCurrentRepairStatus();
            if (currentRepairStatus != null)
            {
                logger.info("Latest repair status {}", currentRepairStatus.toString());
                UUID priorityHostId = null;
                if (currentRepairStatus.priority != null)
                {
                    priorityHostId = Iterables.getFirst(currentRepairStatus.priority, null);
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
                                    Lists.newArrayList(ByteBufferUtil.bytes(0))), Dispatcher.RequestTime.forImmediateExecution());
                    return NOT_MY_TURN;
                }

                if (myId.equals(priorityHostId) && currentRepairStatus.currentRepairStatus == AutoRepair.RepairCurrentStatus.REPAIR_DONE
                        .ordinal())
                {
                    //I have a priority for repair hence its my turn now
                    myTurn = MY_TURN_DUE_TO_PRIORITY;
                }
                else if (myNeighbourHostId != null && myNeighbourHostId.equals(currentRepairStatus.hostIdWithOnGoingRepair) &&
                        (currentRepairStatus.currentRepairStatus == AutoRepair.RepairCurrentStatus.REPAIR_DONE.ordinal()))
                {
                    //my neighbour is done with repair, its my turn now
                    myTurn = MY_TURN;
                }
                else if (currentRepairStatus.hostIdWithOnGoingRepair.equals(myId) && (currentRepairStatus.currentRepairStatus == AutoRepair.RepairCurrentStatus
                        .REPAIR_NOT_DONE.ordinal()))
                {
                    //for some reason I was not done with the repair hence resume (maybe node restart in-between, etc.)
                    myTurn = MY_TURN;
                }

                if (myTurn != MY_TURN)
                {
                    //check who is next, which is helpful for debugging
                    UUID nextNode = null;
                    boolean currentNodeFound = false;
                    for (UUID hostId : hostIdsInCurrentRing)
                    {
                        if (nextNode == null)
                        {
                            nextNode = hostId;
                        }
                        if (currentNodeFound)
                        {
                            nextNode = hostId;
                            break;
                        }
                        if (hostId.equals(currentRepairStatus.hostIdWithOnGoingRepair))
                        {
                            currentNodeFound = true;
                        }
                    }
                    logger.info("Next node in sequence is {}", StorageService.instance.getTokenMetadata().getEndpointForHostId(nextNode));
                }

            }
            else if (firstInTheRing)
            {
                //I am the first one in the ring to start repair
                myTurn = MY_TURN;
            }
        }
        catch (Exception e)
        {
            logger.error("Exception while deciding node's turn:", e);
        }
        return myTurn;
    }

    static void updateRepairStatus(UUID myId, AutoRepair.RepairCurrentStatus repairStatus)
    {
        //mark current hostId as repaired
        modificationStatementRepairStatus.execute(QueryState.forInternalCalls(),
                                                  QueryOptions.forInternalCalls(internalQueryCL,
                        Lists.newArrayList(ByteBufferUtil.bytes(0),
                                ByteBufferUtil.bytes(myId),
                                ByteBufferUtil.bytes(repairStatus.ordinal()),
                                ByteBufferUtil.bytes(System.currentTimeMillis()))), Dispatcher.RequestTime.forImmediateExecution());

    }

    public static void addPriorityHost(Set<InetAddress> hosts)
    {
        Set<UUID> hostIds = new HashSet<>();
        for (InetAddress host : hosts)
        {
            //find hostId from IP address
            UUID hostId = StorageService.instance.getTokenMetadata().getHostId(InetAddressAndPort.getByAddress(host));
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
                            Lists.newArrayList(serializer.serialize(hostIds))), Dispatcher.RequestTime.forImmediateExecution());
        }
    }

    static void removePriorityStatus(UUID hostId)
    {
        logger.info("Remove host {} from priority list", hostId);
        delStatementPriorityStatus.execute(QueryState.forInternalCalls(),
                QueryOptions.forInternalCalls(internalQueryCL,
                        Lists.newArrayList(ByteBufferUtil.bytes(hostId),
                                ByteBufferUtil.bytes(0))), Dispatcher.RequestTime.forImmediateExecution());
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

    public static boolean tableMaxRepairTimeExceeded(long startTime)
    {
        long tableRepairTimeSoFar = TimeUnit.MILLISECONDS.toSeconds
                (System.currentTimeMillis() - startTime);
        return AutoRepairService.instance.getAutoRepairTableMaxRepairTimeInSec() < tableRepairTimeSoFar;
    }
}
