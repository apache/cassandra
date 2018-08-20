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

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.metrics.AutoRepairMetrics;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.ProgressListener;

import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

/**
 * This class is designed to run start automatic repair on Cassandra cluster where repair was not in past and now we
 * would like to start repair w/o impacting production workload. Here is how it works:
 * a. It sorts all the nodes's uuid in the ring and checks whose turn it is to run repair
 * b. After node makes a decision to run repair, full sub-range repair is triggered one table at a time
 * If at any point #of unrepaired sstable count goes beyond certain threshold then simply ignore such table as
 * there maybe some challenges to repair such a large table
 * <p>
 * Idea here is to not impact production workload, just try best effort full sub-range repair
 */
public class AutoRepair
{
    private static final Logger logger = LoggerFactory.getLogger(AutoRepair.class);

    private static final String KEYSPACE_NAME = "system_auto_repair";
    private static final String REPAIR_STATUS = "cur_repair_status";
    private static final String REPAIR_SCHEMA =
            "CREATE TABLE " + REPAIR_STATUS + "("
                    + "pid int,"
                    + "host_id uuid,"
                    + "repair_status int,"
                    + "repair_ts timestamp,"
                    + "PRIMARY KEY (pid))";

    private final static String INSERT_REPAIR_STATUS = String.format(
            "INSERT INTO %s.%s (pid, host_id, repair_status, repair_ts) values (?, ?, ?, ?)"
            , KEYSPACE_NAME, REPAIR_STATUS);
    private final static String SELECT_REPAIR_STATUS = String.format(
            "SELECT host_id, repair_status, repair_ts FROM %s.%s WHERE pid = 0"
            , KEYSPACE_NAME, REPAIR_STATUS);
    private final static String DEL_REPAIR_STATUS = String.format(
            "DELETE FROM %s.%s WHERE pid = ?", KEYSPACE_NAME, REPAIR_STATUS);

    private static ModificationStatement modificationStatementRepairStatus;
    private static ModificationStatement delStatementRepairStatus;
    private static SelectStatement selectStatementRepairStatus;

    enum REPAIR_CUR_STATUS
    {
        REPAIR_DONE,
        REPAIR_NOT_DONE
    }

    private static ConsistencyLevel internalQueryCL;
    private static int numberOfSubranges = 1;
    private static int repairThreads = 1;
    private static int sstableHigherThreshold = 10000;
    private static int minRepairFrequencyInHours = 24;
    private static int totalTablesConsideredForRepair = 0;
    private static long lastRepairTimeInMs;
    private static Set<String> ignoreKeyspaces = new HashSet<>(Arrays.asList("system", "system_auth",
            "system_schema", "system_distributed", "system_traces", "system_monitor", "system_auto_repair",
            "pingless"));
    private static Set<String> ignoreDCs = new HashSet<>();

    public static AutoRepair instance = new AutoRepair();

    private KeyspaceMetadata getAutoRepairSchema()
    {
        TableMetadata currentRepairSchema = CreateTableStatement.parse(REPAIR_SCHEMA, KEYSPACE_NAME)
                                                                .comment("current repair status details")
                                                                .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90))
                                                                .build();
        return KeyspaceMetadata.create(KEYSPACE_NAME, KeyspaceParams.simple(1), Tables.of(currentRepairSchema));
    }

    @VisibleForTesting
    public long getLastRepairTime()
    {
        return lastRepairTimeInMs;
    }

    @VisibleForTesting
    public void setMinRepairFrequencyInHours(int minRepairFrequencyInHours)
    {
        this.minRepairFrequencyInHours = minRepairFrequencyInHours;
    }

    @VisibleForTesting
    public int getTotalTablesConsideredForRepair()
    {
        return totalTablesConsideredForRepair;
    }

    public void setup()
    {
        numberOfSubranges = DatabaseDescriptor.getAutoRepairNumberOfSubRanges();
        repairThreads = DatabaseDescriptor.getAutoRepairNumberOfRepairThreads();
        sstableHigherThreshold = DatabaseDescriptor.getAutoRepairSSTableUpperThreshold();
        minRepairFrequencyInHours = DatabaseDescriptor.getAutoRepairMinRepairFrequencyInHours();
        if (DatabaseDescriptor.getAutoRepairIgnoreKeyspaces().length() > 0)
        {
            ignoreKeyspaces.clear();
            for (String keyspaceToIgnore : DatabaseDescriptor.getAutoRepairIgnoreKeyspaces().split(","))
            {
                ignoreKeyspaces.add(keyspaceToIgnore);
            }
        }
        if (DatabaseDescriptor.getAutoRepairIgnoreDC().length() > 0)
        {
            ignoreDCs.clear();
            for (String dcToIgnore : DatabaseDescriptor.getAutoRepairIgnoreDC().split(","))
            {
                ignoreDCs.add(dcToIgnore);
            }
        }

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

        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> repair(true),
                30,
                DatabaseDescriptor.getAutoRepairCheckInterval(),
                TimeUnit.SECONDS);
    }

    @VisibleForTesting
    public static boolean myTurnToRunRepair(UUID myId)
    {
        if (internalQueryCL == null)
        {
            Keyspace autoRepairKS = Schema.instance.getKeyspaceInstance(KEYSPACE_NAME);
            internalQueryCL = autoRepairKS.getReplicationStrategy().getClass() == NetworkTopologyStrategy.class ?
                    ConsistencyLevel.LOCAL_ONE : ConsistencyLevel.ONE;
        }

        boolean myTurn = false;
        try
        {
            Set<InetAddressAndPort> allNodesInRing = StorageService.instance.getTokenMetadata().getAllEndpoints();
            logger.info("Total nodes in ring {}", allNodesInRing.size());
            TreeSet<UUID> hostIdsInCurrentRing = new TreeSet<>();
            for (InetAddressAndPort node : allNodesInRing)
            {
                String nodeDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(node);
                if (ignoreDCs.contains(nodeDC))
                {
                    logger.debug("Ignore node {} because its datacenter is {}", node, nodeDC);
                    continue;
                }
                UUID hostId = Gossiper.instance.getHostId(node);
                hostIdsInCurrentRing.add(hostId);
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
            ResultMessage.Rows repairStatusRows = selectStatementRepairStatus.execute(QueryState.forInternalCalls(), QueryOptions
                    .forInternalCalls(internalQueryCL, null), Dispatcher.RequestTime.forImmediateExecution());
            UntypedResultSet repairStatusResult = UntypedResultSet.create(repairStatusRows.result);

            if (repairStatusResult.size() > 0)
            {
                UntypedResultSet.Row row = repairStatusResult.one();
                UUID hostIdWithOnGoingRepair = row.getUUID("host_id");
                int currentRepairStatus = row.getInt("repair_status");
                long repair_finished_ts = row.getLong("repair_ts");
                logger.info("Latest repair status hostIdWithOnGoingRepair {}, currentRepairStatus {}, " +
                                "repair_finished_ts {}", hostIdWithOnGoingRepair,
                        currentRepairStatus, repair_finished_ts);

                if (!hostIdsInCurrentRing.contains(hostIdWithOnGoingRepair))
                {
                    //host is no longer part of the ring, could happen if host is replaced while it was doing repair
                    // (rare case but still possible)
                    logger.info("Host is no longer part of the ring hence removing its repair status " +
                                    "hostIdWithOnGoingRepair {}, currentRepairStatus {}, repair_finished_ts {}",
                            hostIdWithOnGoingRepair, currentRepairStatus, repair_finished_ts);
                    delStatementRepairStatus.execute(QueryState.forInternalCalls(),
                                                     QueryOptions.forInternalCalls(internalQueryCL,
                                    Lists.newArrayList(ByteBufferUtil.bytes(0))), Dispatcher.RequestTime.forImmediateExecution());
                    return false;
                }

                if (myNeighbourHostId != null && myNeighbourHostId.equals(hostIdWithOnGoingRepair) &&
                        (currentRepairStatus == REPAIR_CUR_STATUS.REPAIR_DONE.ordinal()))
                {
                    //my neighbour is done with repair, its my turn now
                    myTurn = true;
                }
                else if (hostIdWithOnGoingRepair.equals(myId) && (currentRepairStatus == REPAIR_CUR_STATUS
                        .REPAIR_NOT_DONE.ordinal()))
                {
                    //for some reason I was not done with the repair hence resume (maybe node restart in-between, etc.)
                    myTurn = true;
                }
            }
            else if (firstInTheRing)
            {
                //I am the first one in the ring to start repair
                myTurn = true;
            }
        }
        catch (Exception e)
        {
            logger.error("Exception while deciding node's turn:", e);
        }
        return myTurn;
    }


    @VisibleForTesting
    public static void repair(boolean doTransportCheck)
    {
        if (!StorageService.instance.isAutoRepairStarted())
        {
            logger.info("AutoRepair is stopped hence not running repair");
            return;
        }

        //only for utest we don't want to check transport hence this flag
        if (doTransportCheck)
        {
            //wait for C* to initialize fully before continue with repair
            if (!StorageService.instance.isNativeTransportRunning())
            {
                logger.info("Native transport is not yet running, wait and retry...");
                return;
            }
        }

        try
        {
            String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddressAndPort());
            if (ignoreDCs.contains(localDC))
            {
                logger.info("Not running repair as this node belongs to datacenter {}", localDC);
                return;
            }

            //consistency level to use for local query
            UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
            if (myTurnToRunRepair(myId))
            {
                totalTablesConsideredForRepair = 0;
                if (lastRepairTimeInMs != 0)
                {
                    /** check if it is too soon to run repair. one of the reason we
                     * should not run frequent repair is because repair triggers
                     * memtable flush
                     */
                    long timeElapsedSinceLastRepairInHours = TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis() - lastRepairTimeInMs);
                    if (timeElapsedSinceLastRepairInHours < minRepairFrequencyInHours)
                    {
                        logger.info("Too soon to run repair, last repair was done {} hour(s) ago",
                                timeElapsedSinceLastRepairInHours);
                        return;
                    }
                }

                Stopwatch stopWatch = Stopwatch.createStarted();
                logger.info("My turn to run repair...");

                //todo: add grafana metrics so we can see which node is running repair, etc.
                //mark repair as started
                modificationStatementRepairStatus.execute(QueryState.forInternalCalls(),
                        QueryOptions.forInternalCalls(internalQueryCL,
                                Lists.newArrayList(ByteBufferUtil.bytes(0),
                                        ByteBufferUtil.bytes(myId),
                                        ByteBufferUtil.bytes(REPAIR_CUR_STATUS.REPAIR_NOT_DONE.ordinal()),
                                        ByteBufferUtil.bytes(System.currentTimeMillis()))), Dispatcher.RequestTime.forImmediateExecution());

                int repairKeyspaceCount = 0;
                int repairTableSuccessCount = 0;
                int repairTableFailureCount = 0;
                int repairTableSkipCount = 0;
                AutoRepairMetrics.repairsInProgress.inc();
                for (Keyspace keyspace : Keyspace.all())
                {
                    Tables tables = keyspace.getMetadata().tables;
                    Iterator<TableMetadata> iter = tables.iterator();
                    String keyspaceName = keyspace.getName();
                    if (ignoreKeyspaces.contains(keyspaceName))
                    {
                        continue;
                    }

                    repairKeyspaceCount++;
                    while (iter.hasNext())
                    {
                        try
                        {
                            if (!StorageService.instance.isAutoRepairStarted())
                            {
                                logger.error("AutoRepair is disabled hence not running repair");
                                AutoRepairMetrics.repairsInProgress.dec();
                                return;
                            }
                            totalTablesConsideredForRepair++;
                            String tableName = iter.next().name;

                            ColumnFamilyStore columnFamilyStore = keyspace.getColumnFamilyStore(tableName);
                            // this is done to make autorepair safe as running repair on table with more sstables
                            // may have its own challenges
                            if (columnFamilyStore.getLiveSSTables().size() > sstableHigherThreshold)
                            {
                                logger.info("Too many SSTables for repair, not doing repair on table {}.{} " +
                                        "totalSSTables {}", keyspaceName, tableName, columnFamilyStore.getLiveSSTables().size());
                                repairTableSkipCount++;
                                continue;
                            }

                            logger.info("Repair table {}.{}", keyspaceName, tableName);
                            //now run full repair on this table
                            Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(keyspaceName);
                            boolean tableRepairSuccess = true;
                            Set<Range<Token>> ranges = new HashSet<>();
                            int totalSubRanges = tokens.size() * numberOfSubranges;
                            int totalProcessedSubRanges = 0;
                            for (Range<Token> token : tokens)
                            {
                                Murmur3Partitioner.LongToken l = (Murmur3Partitioner.LongToken) (token.left);
                                Murmur3Partitioner.LongToken r = (Murmur3Partitioner.LongToken) (token.right);
                                Token parentStartToken = StorageService.instance.getTokenMetadata()
                                        .partitioner.getTokenFactory().fromString("" + l.getTokenValue());
                                Token parentEndToken = StorageService.instance.getTokenMetadata()
                                        .partitioner.getTokenFactory().fromString("" + r.getTokenValue());
                                logger.debug("Parent Token Left side {}, right side {}", parentStartToken.toString(),
                                        parentEndToken.toString());

                                long left = (Long) l.getTokenValue();
                                long right = (Long) r.getTokenValue();
                                long repairTokenWidth = (right - left) / numberOfSubranges;
                                if ((right - left) < numberOfSubranges)
                                {
                                    logger.warn("Too many sub-ranges are given {}", numberOfSubranges);
                                    numberOfSubranges = (int) (right - left) == 0 ? 1 : (int) (right - left);
                                    repairTokenWidth = 1;
                                    totalSubRanges = tokens.size() * numberOfSubranges;
                                }
                                for (int i = 0; i < numberOfSubranges; i++)
                                {
                                    long curLeft = left + (i * repairTokenWidth);
                                    long curRight = curLeft + repairTokenWidth;

                                    if ((i + 1) == numberOfSubranges)
                                    {
                                        curRight = right;
                                    }

                                    Token childStartToken = StorageService.instance.getTokenMetadata()
                                            .partitioner.getTokenFactory().fromString("" + curLeft);
                                    Token childEndToken = StorageService.instance.getTokenMetadata()
                                            .partitioner.getTokenFactory().fromString("" + curRight);
                                    logger.debug("Current Token Left side {}, right side {}", childStartToken
                                            .toString(), childEndToken.toString());

                                    ranges.add(new Range<>(childStartToken, childEndToken));
                                    totalProcessedSubRanges++;
                                    if ((totalProcessedSubRanges % repairThreads == 0) ||
                                            (totalProcessedSubRanges == totalSubRanges))
                                    {
                                        RepairOption options = new RepairOption(RepairParallelism.PARALLEL, true, false,
                                                                                false, repairThreads, ranges, !ranges.isEmpty(), false,
                                                                                false, PreviewKind.NONE, false, true,
                                                                                false, false);
                                        options.getColumnFamilies().add(tableName);
                                        int repairCmdId = StorageService.instance.nextRepairCommand.incrementAndGet();
                                        RepairRunnable task = new RepairRunnable(StorageService.instance, repairCmdId, options, keyspaceName);
                                        RepairStatus rs = new RepairStatus();
                                        task.addProgressListener(rs);
                                        new Thread(NamedThreadFactory.createAnonymousThread(new FutureTask<>(task, null))).start();
                                        try
                                        {
                                            rs.waitForRepairToComplete();
                                        }
                                        catch (InterruptedException e)
                                        {
                                            logger.error("Exception in cond await:", e);
                                        }
                                        //check repair status
                                        if (rs.success)
                                        {
                                            logger.debug("Repair completed for range {}-{} for {}.{}", childStartToken
                                            .toString(), childEndToken.toString(), keyspaceName, tableName);
                                        }
                                        else
                                        {
                                            tableRepairSuccess = false;
                                            //in future we can add retry, etc.
                                            logger.info("Repair failed for range {}-{} for {}.{}", childStartToken
                                            .toString(), childEndToken.toString(), keyspaceName, tableName);
                                        }
                                        ranges.clear();
                                    }
                                }
                            }
                            if (tableRepairSuccess)
                            {
                                repairTableSuccessCount++;
                            }
                            else
                            {
                                repairTableFailureCount++;
                            }
                            logger.debug("Repair completed for {}.{}", keyspaceName, tableName);
                        }
                        catch (Exception e)
                        {
                            logger.error("Exception while repairing keyspace {}:", keyspaceName, e);
                        }
                    }
                }
                //mark current hostId as repaired
                modificationStatementRepairStatus.execute(QueryState.forInternalCalls(),
                        QueryOptions.forInternalCalls(internalQueryCL,
                                Lists.newArrayList(ByteBufferUtil.bytes(0),
                                        ByteBufferUtil.bytes(myId),
                                        ByteBufferUtil.bytes(REPAIR_CUR_STATUS.REPAIR_DONE.ordinal()),
                                        ByteBufferUtil.bytes(System.currentTimeMillis()))), Dispatcher.RequestTime.forImmediateExecution());

                logger.info("Local repair time {} hour(s), stats: repairKeyspaceCount {}, " +
                                "repairTableSuccessCount {}, repairTableFailureCount {}, " +
                                "repairTableSkipCount {}", stopWatch.elapsed(TimeUnit.HOURS), repairKeyspaceCount,
                        repairTableSuccessCount,
                        repairTableFailureCount,
                        repairTableSkipCount);
                if (lastRepairTimeInMs != 0)
                {
                    logger.info("Cluster repair time {} hour(s)",
                            TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis() - lastRepairTimeInMs));
                }
                lastRepairTimeInMs = System.currentTimeMillis();
                AutoRepairMetrics.repairsInProgress.dec();
            }
            else
            {
                logger.info("Waiting for my turn...");
            }
        }
        catch (Exception e)
        {
            logger.error("Exception in autorepair:", e);
        }
    }
}

class RepairStatus implements ProgressListener
{
    private static final Logger logger = LoggerFactory.getLogger(RepairStatus.class);
    private final Condition condition = newOneTimeCondition();
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    boolean success = true;

    public void waitForRepairToComplete() throws InterruptedException
    {
        //if for some reason we don't hear back on repair progress for sometime
        success = condition.await(12, TimeUnit.HOURS);
    }

    @Override
    public void progress(String tag, ProgressEvent event)
    {
        ProgressEventType type = event.getType();
        String message = String.format("[%s] %s", format.format(System.currentTimeMillis()), event.getMessage());
        if (type == ProgressEventType.ERROR)
        {
            logger.error("Repair failure {}", message);
            success = false;
            condition.signalAll();
        }
        if (type == ProgressEventType.PROGRESS)
        {
            message = message + " (progress: " + (int) event.getProgressPercentage() + "%)";
            logger.debug("Repair progress {}", message);
        }
        if (type == ProgressEventType.COMPLETE)
        {
            condition.signalAll();
        }
    }
}
