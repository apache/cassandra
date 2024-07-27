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
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.repair.RepairCoordinator;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils.RepairTurn;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.RepairTurn.MY_TURN;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.RepairTurn.MY_TURN_DUE_TO_PRIORITY;
import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.RepairTurn.MY_TURN_FORCE_REPAIR;

public class AutoRepair
{
    // Initial delay for repair session to start after setup
    final static long INITIAL_REPAIR_DELAY_SEC = 30;

    private static final Logger logger = LoggerFactory.getLogger(AutoRepair.class);

    @VisibleForTesting
    protected static Supplier<Long> timeFunc = System::currentTimeMillis;

    public static AutoRepair instance = new AutoRepair();

    @VisibleForTesting
    protected final Map<AutoRepairConfig.RepairType, ScheduledExecutorPlus> repairExecutors;

    protected final Map<AutoRepairConfig.RepairType, ScheduledExecutorPlus> repairRunnableExecutors;

    @VisibleForTesting
    protected final Map<AutoRepairConfig.RepairType, AutoRepairState> repairStates;

    protected final Map<AutoRepairConfig.RepairType, IAutoRepairTokenRangeSplitter> tokenRangeSplitters = new EnumMap<>(AutoRepairConfig.RepairType.class);


    @VisibleForTesting
    protected AutoRepair()
    {
        AutoRepairConfig config = DatabaseDescriptor.getAutoRepairConfig();
        repairExecutors = new EnumMap<>(AutoRepairConfig.RepairType.class);
        repairRunnableExecutors = new EnumMap<>(AutoRepairConfig.RepairType.class);
        repairStates = new EnumMap<>(AutoRepairConfig.RepairType.class);
        for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values())
        {
            repairExecutors.put(repairType, executorFactory().scheduled(false, "AutoRepair-Repair-" + repairType, Thread.NORM_PRIORITY));
            repairRunnableExecutors.put(repairType, executorFactory().scheduled(false, "AutoRepair-RepairRunnable-" + repairType, Thread.NORM_PRIORITY));
            repairStates.put(repairType, AutoRepairConfig.RepairType.getAutoRepairState(repairType));
            tokenRangeSplitters.put(repairType, FBUtilities.newAutoRepairTokenRangeSplitter(config.getTokenRangeSplitter(repairType)));
        }
    }

    public void setup()
    {
        verifyIsSafeToEnable();

        AutoRepairConfig config = DatabaseDescriptor.getAutoRepairConfig();
        AutoRepairService.setup();
        AutoRepairUtils.setup();

        for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values())
        {
            repairExecutors.get(repairType).scheduleWithFixedDelay(
            () -> repair(repairType, 60000),
            INITIAL_REPAIR_DELAY_SEC,
            config.getRepairCheckInterval().toSeconds(),
            TimeUnit.SECONDS);
        }
    }

    @VisibleForTesting
    protected void verifyIsSafeToEnable()
    {
        AutoRepairConfig config = DatabaseDescriptor.getAutoRepairConfig();
        if (config.isAutoRepairEnabled(AutoRepairConfig.RepairType.incremental) &&
            (DatabaseDescriptor.getMaterializedViewsEnabled() || DatabaseDescriptor.isCDCEnabled()))
            throw new ConfigurationException("Cannot enable incremental repair with materialized views or CDC enabled");
    }

    // repairAsync runs a repair session of the given type asynchronously.
    public void repairAsync(AutoRepairConfig.RepairType repairType, long millisToWait)
    {
        repairExecutors.get(repairType).submit(() -> repair(repairType, millisToWait));
    }

    // repair runs a repair session of the given type synchronously.
    public void repair(AutoRepairConfig.RepairType repairType, long millisToWait)
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        if (!config.isAutoRepairEnabled(repairType))
        {
            logger.debug("Auto-repair is disabled for repair type {}", repairType);
            return;
        }
        AutoRepairState repairState = repairStates.get(repairType);
        try
        {
            String localDC = DatabaseDescriptor.getLocalDataCenter();
            if (config.getIgnoreDCs(repairType).contains(localDC))
            {
                logger.info("Not running repair as this node belongs to datacenter {}", localDC);
                return;
            }

            // refresh the longest unrepaired node
            repairState.setLongestUnrepairedNode(AutoRepairUtils.getHostWithLongestUnrepairTime(repairType));

            //consistency level to use for local query
            UUID myId = StorageService.instance.getHostIdForEndpoint(FBUtilities.getBroadcastAddressAndPort());
            RepairTurn turn = AutoRepairUtils.myTurnToRunRepair(repairType, myId);
            if (turn == MY_TURN || turn == MY_TURN_DUE_TO_PRIORITY || turn == MY_TURN_FORCE_REPAIR)
            {
                repairState.recordTurn(turn);
                // For normal auto repair, we will use primary range only repairs (Repair with -pr option).
                // For some cases, we may set the auto_repair_primary_token_range_only flag to false then we will do repair
                // without -pr. We may also do force repair for certain node that we want to repair all the data on one node
                // When doing force repair, we want to repair without -pr.
                boolean primaryRangeOnly = config.getRepairPrimaryTokenRangeOnly(repairType)
                                           && turn != MY_TURN_FORCE_REPAIR;
                repairState.setTotalTablesConsideredForRepair(0);
                if (tooSoonToRunRepair(repairType, repairState, config))
                {
                    return;
                }

                long startTime = timeFunc.get();
                logger.info("My host id: {}, my turn to run repair...repair primary-ranges only? {}", myId,
                            config.getRepairPrimaryTokenRangeOnly(repairType));
                AutoRepairUtils.updateStartAutoRepairHistory(repairType, myId, timeFunc.get(), turn);

                repairState.setRepairKeyspaceCount(0);
                repairState.setRepairTableSuccessCount(0);
                repairState.setRepairFailedTablesCount(0);
                repairState.setRepairSkippedTablesCount(0);
                repairState.setRepairInProgress(true);
                repairState.setTotalMVTablesConsideredForRepair(0);
                for (Keyspace keyspace : Keyspace.all())
                {
                    if (!AutoRepairUtils.checkNodeContainsKeyspaceReplica(keyspace))
                    {
                        continue;
                    }

                    repairState.setRepairKeyspaceCount(repairState.getRepairKeyspaceCount() + 1);
                    List<String> tablesToBeRepaired = retrieveTablesToBeRepaired(keyspace, repairType, repairState);
                    for (String tableName : tablesToBeRepaired)
                    {
                        String keyspaceName = keyspace.getName();
                        try
                        {
                            ColumnFamilyStore columnFamilyStore = keyspace.getColumnFamilyStore(tableName);
                            if (!columnFamilyStore.metadata().params.automatedRepair.get(repairType).repairEnabled())
                            {
                                logger.info("Repair is disabled for keyspace {} for tables: {}", keyspaceName, tableName);
                                repairState.setTotalDisabledTablesRepairCount(repairState.getTotalDisabledTablesRepairCount() + 1);
                                continue;
                            }
                            // this is done to make autorepair safe as running repair on table with more sstables
                            // may have its own challenges
                            int size = columnFamilyStore.getLiveSSTables().size();
                            if (size > config.getRepairSSTableCountHigherThreshold(repairType))
                            {
                                logger.info("Too many SSTables for repair, not doing repair on table {}.{} " +
                                            "totalSSTables {}", keyspaceName, tableName, columnFamilyStore.getLiveSSTables().size());
                                repairState.setRepairSkippedTablesCount(repairState.getRepairSkippedTablesCount() + 1);
                                continue;
                            }

                            if (config.getRepairByKeyspace(repairType))
                            {
                                logger.info("Repair keyspace {} for tables: {}", keyspaceName, tablesToBeRepaired);
                            }
                            else
                            {
                                logger.info("Repair table {}.{}", keyspaceName, tableName);
                            }
                            long tableStartTime = timeFunc.get();
                            boolean repairSuccess = true;
                            Set<Range<Token>> ranges = new HashSet<>();
                            List<Pair<Token, Token>> subRangesToBeRepaired = tokenRangeSplitters.get(repairType).getRange(repairType, primaryRangeOnly, keyspaceName, tableName);
                            int totalSubRanges = subRangesToBeRepaired.size();
                            int totalProcessedSubRanges = 0;
                            for (Pair<Token, Token> token : subRangesToBeRepaired)
                            {
                                if (!config.isAutoRepairEnabled(repairType))
                                {
                                    logger.error("Auto-repair for type {} is disabled hence not running repair", repairType);
                                    repairState.setRepairInProgress(false);
                                    return;
                                }

                                if (config.getRepairByKeyspace(repairType))
                                {
                                    if (AutoRepairUtils.keyspaceMaxRepairTimeExceeded(repairType, tableStartTime, tablesToBeRepaired.size()))
                                    {
                                        repairState.setRepairSkippedTablesCount(repairState.getRepairSkippedTablesCount() + tablesToBeRepaired.size());
                                        logger.info("Keyspace took too much time to repair hence skipping it {}",
                                                    keyspaceName);
                                        break;
                                    }
                                }
                                else
                                {
                                    if (AutoRepairUtils.tableMaxRepairTimeExceeded(repairType, tableStartTime))
                                    {
                                        repairState.setRepairSkippedTablesCount(repairState.getRepairSkippedTablesCount() + 1);
                                        logger.info("Table took too much time to repair hence skipping it {}.{}",
                                                    keyspaceName, tableName);
                                        break;
                                    }
                                }
                                Token childStartToken = token.left;
                                Token childEndToken = token.right;
                                logger.debug("Current Token Left side {}, right side {}", childStartToken
                                                                                          .toString(), childEndToken.toString());

                                ranges.add(new Range<>(childStartToken, childEndToken));
                                totalProcessedSubRanges++;
                                if ((totalProcessedSubRanges % config.getRepairThreads(repairType) == 0) ||
                                    (totalProcessedSubRanges == totalSubRanges))
                                {
                                    RepairCoordinator task = repairState.getRepairRunnable(keyspaceName,
                                                                                        config.getRepairByKeyspace(repairType) ? tablesToBeRepaired : ImmutableList.of(tableName),
                                                                                           ranges, primaryRangeOnly);
                                    repairState.resetWaitCondition();
                                    Future<?> f = repairRunnableExecutors.get(repairType).submit(task);
                                    try
                                    {
                                        repairState.waitForRepairToComplete();
                                    }
                                    catch (InterruptedException e)
                                    {
                                        logger.error("Exception in cond await:", e);
                                    }

                                    //check repair status
                                    if (repairState.isSuccess())
                                    {
                                        logger.info("Repair completed for range {}-{} for {}.{}, total subranges: {}," +
                                                    "processed subranges: {}", childStartToken, childEndToken,
                                                    keyspaceName, config.getRepairByKeyspace(repairType) ? tablesToBeRepaired : tableName, totalSubRanges, totalProcessedSubRanges);
                                    }
                                    else
                                    {
                                        repairSuccess = false;
                                        boolean cancellationStatus = f.cancel(true);
                                        //in future we can add retry, etc.
                                        logger.info("Repair failed for range {}-{} for {}.{} total subranges: {}," +
                                                    "processed subranges: {}, cancellationStatus: {}", childStartToken, childEndToken,
                                                    keyspaceName, config.getRepairByKeyspace(repairType) ? tablesToBeRepaired : tableName, totalSubRanges, totalProcessedSubRanges, cancellationStatus);
                                    }
                                    ranges.clear();
                                }
                            }
                            int touchedTables = config.getRepairByKeyspace(repairType) ? tablesToBeRepaired.size() : 1;
                            if (repairSuccess)
                            {
                                repairState.setRepairTableSuccessCount(repairState.getRepairTableSuccessCount() + touchedTables);
                            }
                            else
                            {
                                repairState.setRepairFailedTablesCount(repairState.getRepairFailedTablesCount() + touchedTables);
                            }
                            if (config.getRepairByKeyspace(repairType))
                            {
                                logger.info("Repair completed for keyspace {}, tables: {}", keyspaceName, tablesToBeRepaired);
                                break;
                            }
                            else
                            {
                                logger.info("Repair completed for {}.{}", keyspaceName, tableName);
                            }
                        }
                        catch (Exception e)
                        {
                            logger.error("Exception while repairing keyspace {}:", keyspaceName, e);
                        }
                    }
                }
                cleanupAndUpdateStats(turn, repairType, repairState, myId, startTime, millisToWait);
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

    private boolean tooSoonToRunRepair(AutoRepairConfig.RepairType repairType, AutoRepairState repairState, AutoRepairConfig config)
    {
        if (repairState.getLastRepairTime() != 0)
        {
            /** check if it is too soon to run repair. one of the reason we
             * should not run frequent repair is that repair triggers
             * memtable flush
             */
            long timeElapsedSinceLastRepair = TimeUnit.MILLISECONDS.toSeconds(timeFunc.get() - repairState.getLastRepairTime());
            if (timeElapsedSinceLastRepair < config.getRepairMinInterval(repairType).toSeconds())
            {
                logger.info("Too soon to run repair, last repair was done {} seconds ago",
                            timeElapsedSinceLastRepair);
                return true;
            }
        }
        return false;
    }

    private List<String> retrieveTablesToBeRepaired(Keyspace keyspace, AutoRepairConfig.RepairType repairType, AutoRepairState repairState)
    {
        Tables tables = keyspace.getMetadata().tables;
        List<String> tablesToBeRepaired = new ArrayList<>();
        Iterator<TableMetadata> iter = tables.iterator();
        while (iter.hasNext())
        {
            repairState.setTotalTablesConsideredForRepair(repairState.getTotalTablesConsideredForRepair() + 1);
            TableMetadata tableMetadata = iter.next();
            String tableName = tableMetadata.name;
            tablesToBeRepaired.add(tableName);

            // See if we should repair MVs as well that are associated with this given table
            List<String> mvs = AutoRepairUtils.getAllMVs(repairType, keyspace, tableMetadata);
            if (!mvs.isEmpty())
            {
                tablesToBeRepaired.addAll(mvs);
                repairState.setTotalMVTablesConsideredForRepair(repairState.getTotalMVTablesConsideredForRepair() + mvs.size());
            }
        }
        return tablesToBeRepaired;
    }

    private void cleanupAndUpdateStats(RepairTurn turn, AutoRepairConfig.RepairType repairType, AutoRepairState repairState, UUID myId,
                                       long startTime, long millisToWait) throws InterruptedException
    {
        //if it was due to priority then remove it now
        if (turn == MY_TURN_DUE_TO_PRIORITY)
        {
            logger.info("Remove current host from priority list");
            AutoRepairUtils.removePriorityStatus(repairType, myId);
        }

        repairState.setNodeRepairTimeInSec((int) TimeUnit.MILLISECONDS.toSeconds(timeFunc.get() - startTime));
        long timeInHours = TimeUnit.SECONDS.toHours(repairState.getNodeRepairTimeInSec());
        logger.info("Local {} repair time {} hour(s), stats: repairKeyspaceCount {}, " +
                    "repairTableSuccessCount {}, repairTableFailureCount {}, " +
                    "repairTableSkipCount {}", repairType, timeInHours, repairState.getRepairKeyspaceCount(),
                    repairState.getRepairTableSuccessCount(), repairState.getRepairFailedTablesCount(),
                    repairState.getRepairSkippedTablesCount());
        if (repairState.getLastRepairTime() != 0)
        {
            repairState.setClusterRepairTimeInSec((int) TimeUnit.MILLISECONDS.toSeconds(timeFunc.get() -
                                                                                        repairState.getLastRepairTime()));
            logger.info("Cluster repair time for repair type {}: {} day(s)", repairType,
                        TimeUnit.SECONDS.toDays(repairState.getClusterRepairTimeInSec()));
        }
        repairState.setLastRepairTime(timeFunc.get());
        if (timeInHours == 0 && millisToWait > 0)
        {
            //If repair finished quickly, happens for an empty instance, in such case
            //wait for a minute so that the JMX metrics can detect the repairInProgress
            logger.info("Wait for {} milliseconds for repair type {}.", millisToWait, repairType);
            Thread.sleep(millisToWait);
        }
        repairState.setRepairInProgress(false);
        AutoRepairUtils.updateFinishAutoRepairHistory(repairType, myId, timeFunc.get());
    }

    public AutoRepairState getRepairState(AutoRepairConfig.RepairType repairType)
    {
        return repairStates.get(repairType);
    }
}
