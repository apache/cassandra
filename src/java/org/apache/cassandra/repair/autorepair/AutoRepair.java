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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.RepairRunnable;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.repair.autorepair.IAutoRepairTokenRangeSplitter.RepairAssignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
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
    private static final Logger logger = LoggerFactory.getLogger(AutoRepair.class);

    @VisibleForTesting
    protected static Supplier<Long> timeFunc = Clock.Global::currentTimeMillis;

    public static AutoRepair instance = new AutoRepair();

    // Sleep for 5 seconds if repair finishes quickly to flush JMX metrics; it happens only for Cassandra nodes with tiny amount of data.
    public static DurationSpec.IntSecondsBound SLEEP_IF_REPAIR_FINISHES_QUICKLY = new DurationSpec.IntSecondsBound("5s");

    @VisibleForTesting
    protected final Map<AutoRepairConfig.RepairType, ScheduledExecutorPlus> repairExecutors;

    protected final Map<AutoRepairConfig.RepairType, ScheduledExecutorPlus> repairRunnableExecutors;

    @VisibleForTesting
    protected final Map<AutoRepairConfig.RepairType, AutoRepairState> repairStates;

    @VisibleForTesting
    protected static Consumer<List<?>> shuffleFunc = java.util.Collections::shuffle;

    @VisibleForTesting
    protected static BiConsumer<Long, TimeUnit> sleepFunc = Uninterruptibles::sleepUninterruptibly;

    protected final Map<AutoRepairConfig.RepairType, IAutoRepairTokenRangeSplitter> tokenRangeSplitters = new EnumMap<>(AutoRepairConfig.RepairType.class);

    private boolean isSetupDone = false;

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
        // Ensure setup is done only once; this is only for unit tests
        // For production, this method should be called only once.
        synchronized (this)
        {
            if (isSetupDone)
            {
                return;
            }
            AutoRepairConfig config = DatabaseDescriptor.getAutoRepairConfig();
            AutoRepairUtils.setup();

            for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values())
            {
                if (config.isAutoRepairEnabled(repairType))
                    AutoRepairService.instance.checkCanRun(repairType);

                repairExecutors.get(repairType).scheduleWithFixedDelay(
                () -> repair(repairType),
                config.getInitialSchedulerDelay(repairType).toSeconds(),
                config.getRepairCheckInterval().toSeconds(),
                TimeUnit.SECONDS);
            }
            isSetupDone = true;
        }
    }

    // repairAsync runs a repair session of the given type asynchronously.
    public void repairAsync(AutoRepairConfig.RepairType repairType)
    {
        if (!AutoRepairService.instance.getAutoRepairConfig().isAutoRepairEnabled(repairType))
        {
            throw new ConfigurationException("Auto-repair is disabled for repair type " + repairType);
        }
        repairExecutors.get(repairType).submit(() -> repair(repairType));
    }

    // repair runs a repair session of the given type synchronously.
    public void repair(AutoRepairConfig.RepairType repairType)
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        if (!config.isAutoRepairEnabled(repairType))
        {
            logger.debug("Auto-repair is disabled for repair type {}", repairType);
            return;
        }
        AutoRepairService.instance.checkCanRun(repairType);
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
            UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
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
                if (tooSoonToRunRepair(repairType, repairState, config, myId))
                {
                    return;
                }

                long startTime = timeFunc.get();
                logger.info("My host id: {}, my turn to run repair...repair primary-ranges only? {}", myId,
                            config.getRepairPrimaryTokenRangeOnly(repairType));
                AutoRepairUtils.updateStartAutoRepairHistory(repairType, myId, timeFunc.get(), turn);

                repairState.setRepairKeyspaceCount(0);
                repairState.setRepairInProgress(true);
                repairState.setTotalTablesConsideredForRepair(0);
                repairState.setTotalMVTablesConsideredForRepair(0);

                CollectectedRepairStats collectectedRepairStats = new CollectectedRepairStats();

                List<Keyspace> keyspaces = new ArrayList<>();
                Keyspace.all().forEach(keyspaces::add);
                // Auto-repair is likely to be run on multiple nodes independently, we want to avoid running multiple repair
                // sessions on overlapping datasets at the same time. Shuffling keyspaces reduces the likelihood of this happening.
                shuffleFunc.accept(keyspaces);

                for (Keyspace keyspace : keyspaces)
                {
                    if (!AutoRepairUtils.checkNodeContainsKeyspaceReplica(keyspace))
                    {
                        continue;
                    }

                    repairState.setRepairKeyspaceCount(repairState.getRepairKeyspaceCount() + 1);
                    List<String> tablesToBeRepairedList = retrieveTablesToBeRepaired(keyspace, config, repairType, repairState, collectectedRepairStats);
                    shuffleFunc.accept(tablesToBeRepairedList);
                    String keyspaceName = keyspace.getName();
                    List<RepairAssignment> repairAssignments = tokenRangeSplitters.get(repairType).getRepairAssignments(repairType, primaryRangeOnly, keyspaceName, tablesToBeRepairedList);

                    int totalRepairAssignments = repairAssignments.size();
                    long keyspaceStartTime = timeFunc.get();
                    RepairAssignment previousAssignment = null;
                    long tableStartTime = timeFunc.get();
                    int totalProcessedAssignments = 0;
                    Set<Range<Token>> ranges = new HashSet<>();
                    for (RepairAssignment curRepairAssignment : repairAssignments)
                    {
                        try
                        {
                            totalProcessedAssignments++;
                            boolean repairOneTableAtATime = !config.getRepairByKeyspace(repairType);
                            if (previousAssignment != null && repairOneTableAtATime && !previousAssignment.tableNames.equals(curRepairAssignment.tableNames))
                            {
                                // In the repair assignment, all the tables are appended sequnetially.
                                // Check if we have a different table, and if so, we should reset the table start time.
                                tableStartTime = timeFunc.get();
                            }
                            previousAssignment = curRepairAssignment;
                            if (!config.isAutoRepairEnabled(repairType))
                            {
                                logger.error("Auto-repair for type {} is disabled hence not running repair", repairType);
                                repairState.setRepairInProgress(false);
                                return;
                            }
                            if (AutoRepairUtils.keyspaceMaxRepairTimeExceeded(repairType, keyspaceStartTime, tablesToBeRepairedList.size()))
                            {
                                collectectedRepairStats.skippedTokenRanges += totalRepairAssignments - totalProcessedAssignments;
                                logger.info("Keyspace took too much time to repair hence skipping it {}",
                                            keyspaceName);
                                break;
                            }
                            if (repairOneTableAtATime && AutoRepairUtils.tableMaxRepairTimeExceeded(repairType, tableStartTime))
                            {
                                collectectedRepairStats.skippedTokenRanges += 1;
                                logger.info("Table took too much time to repair hence skipping it table name {}.{}, token range {}",
                                            keyspaceName, curRepairAssignment.tableNames, curRepairAssignment.tokenRange);
                                continue;
                            }

                            Range<Token> tokenRange = curRepairAssignment.getTokenRange();
                            logger.debug("Current Token Left side {}, right side {}",
                                         tokenRange.left.toString(),
                                         tokenRange.right.toString());

                            ranges.add(curRepairAssignment.getTokenRange());
                            if ((totalProcessedAssignments % config.getRepairThreads(repairType) == 0) ||
                                (totalProcessedAssignments == totalRepairAssignments))
                            {
                                int retryCount = 0;
                                Future<?> f = null;
                                while (retryCount <= config.getRepairMaxRetries())
                                {
                                    RepairRunnable task = repairState.getRepairRunnable(keyspaceName,
                                                                                        Lists.newArrayList(curRepairAssignment.getTableNames()),
                                                                                        ranges, primaryRangeOnly);
                                    repairState.resetWaitCondition();
                                    f = repairRunnableExecutors.get(repairType).submit(task);
                                    try
                                    {
                                        repairState.waitForRepairToComplete(config.getRepairSessionTimeout(repairType));
                                    }
                                    catch (InterruptedException e)
                                    {
                                        logger.error("Exception in cond await:", e);
                                    }
                                    if (repairState.isSuccess())
                                    {
                                        break;
                                    }
                                    else if (retryCount < config.getRepairMaxRetries())
                                    {
                                        boolean cancellationStatus = f.cancel(true);
                                        logger.warn("Repair failed for range {}-{} for {} tables {} with cancellationStatus: {} retrying after {} seconds...",
                                                    tokenRange.left, tokenRange.right,
                                                    keyspaceName, curRepairAssignment.getTableNames(),
                                                    cancellationStatus, config.getRepairRetryBackoff().toSeconds());
                                        sleepFunc.accept(config.getRepairRetryBackoff().toSeconds(), TimeUnit.SECONDS);
                                    }
                                    retryCount++;
                                }
                                //check repair status
                                if (repairState.isSuccess())
                                {
                                    logger.info("Repair completed for range {}-{} for {} tables {}, total assignments: {}," +
                                                "processed assignments: {}", tokenRange.left, tokenRange.right,
                                                keyspaceName, curRepairAssignment.getTableNames(), totalRepairAssignments, totalProcessedAssignments);
                                    collectectedRepairStats.succeededTokenRanges += ranges.size();
                                }
                                else
                                {
                                    boolean cancellationStatus = true;
                                    if (f != null)
                                    {
                                        cancellationStatus = f.cancel(true);
                                    }
                                    //in the future we can add retry, etc.
                                    logger.error("Repair failed for range {}-{} for {} tables {} after {} retries, total assignments: {}," +
                                                 "processed assignments: {}, cancellationStatus: {}", tokenRange.left, tokenRange.right, keyspaceName,
                                                 curRepairAssignment.getTableNames(), retryCount, totalRepairAssignments, totalProcessedAssignments, cancellationStatus);
                                    collectectedRepairStats.failedTokenRanges += ranges.size();
                                }
                                ranges.clear();
                            }
                            logger.info("Repair completed for {} tables {}, range {}", keyspaceName, curRepairAssignment.getTableNames(), curRepairAssignment.getTokenRange());
                        }
                        catch (Exception e)
                        {
                            logger.error("Exception while repairing keyspace {}:", keyspaceName, e);
                        }
                    }
                }
                cleanupAndUpdateStats(turn, repairType, repairState, myId, startTime, collectectedRepairStats);
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

    private boolean tooSoonToRunRepair(AutoRepairConfig.RepairType repairType, AutoRepairState repairState, AutoRepairConfig config, UUID myId)
    {
        if (repairState.getLastRepairTime() == 0)
        {
            // the node has either just boooted or has not run repair before,
            // we should check for the node's repair history in the DB
            repairState.setLastRepairTime(AutoRepairUtils.getLastRepairTimeForNode(repairType, myId));
        }
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
        return false;
    }

    private List<String> retrieveTablesToBeRepaired(Keyspace keyspace, AutoRepairConfig config, AutoRepairConfig.RepairType repairType, AutoRepairState repairState, CollectectedRepairStats collectectedRepairStats)
    {
        Tables tables = keyspace.getMetadata().tables;
        List<String> tablesToBeRepaired = new ArrayList<>();
        Iterator<TableMetadata> iter = tables.iterator();
        while (iter.hasNext())
        {
            repairState.setTotalTablesConsideredForRepair(repairState.getTotalTablesConsideredForRepair() + 1);
            TableMetadata tableMetadata = iter.next();
            String tableName = tableMetadata.name;

            ColumnFamilyStore columnFamilyStore = keyspace.getColumnFamilyStore(tableName);
            if (!columnFamilyStore.metadata().params.automatedRepair.get(repairType).repairEnabled())
            {
                logger.info("Repair is disabled for keyspace {} for tables: {}", keyspace.getName(), tableName);
                repairState.setTotalDisabledTablesRepairCount(repairState.getTotalDisabledTablesRepairCount() + 1);
                collectectedRepairStats.skippedTables++;
                continue;
            }

            // this is done to make autorepair safe as running repair on table with more sstables
            // may have its own challenges
            int totalSSTables = columnFamilyStore.getLiveSSTables().size();
            if (totalSSTables > config.getRepairSSTableCountHigherThreshold(repairType))
            {
                logger.info("Too many SSTables for repair for table {}.{}" +
                            "totalSSTables {}", keyspace.getName(), tableName, totalSSTables);
                collectectedRepairStats.skippedTables++;
                continue;
            }

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
                                       long startTime, CollectectedRepairStats collectectedRepairStats) throws InterruptedException
    {
        //if it was due to priority then remove it now
        if (turn == MY_TURN_DUE_TO_PRIORITY)
        {
            logger.info("Remove current host from priority list");
            AutoRepairUtils.removePriorityStatus(repairType, myId);
        }

        repairState.setFailedTokenRangesCount(collectectedRepairStats.failedTokenRanges);
        repairState.setSucceededTokenRangesCount(collectectedRepairStats.succeededTokenRanges);
        repairState.setSkippedTokenRangesCount(collectectedRepairStats.skippedTokenRanges);
        repairState.setSkippedTablesCount(collectectedRepairStats.skippedTables);
        repairState.setNodeRepairTimeInSec((int) TimeUnit.MILLISECONDS.toSeconds(timeFunc.get() - startTime));
        long timeInHours = TimeUnit.SECONDS.toHours(repairState.getNodeRepairTimeInSec());
        logger.info("Local {} repair time {} hour(s), stats: repairKeyspaceCount {}, " +
                    "repairTokenRangesSuccessCount {}, repairTokenRangesFailureCount {}, " +
                    "repairTokenRangesSkipCount {}, repairTablesSkipCount {}", repairType, timeInHours, repairState.getRepairKeyspaceCount(),
                    repairState.getSucceededTokenRangesCount(), repairState.getFailedTokenRangesCount(),
                    repairState.getSkippedTokenRangesCount(), repairState.getSkippedTablesCount());
        if (repairState.getLastRepairTime() != 0)
        {
            repairState.setClusterRepairTimeInSec((int) TimeUnit.MILLISECONDS.toSeconds(timeFunc.get() -
                                                                                        repairState.getLastRepairTime()));
            logger.info("Cluster repair time for repair type {}: {} day(s)", repairType,
                        TimeUnit.SECONDS.toDays(repairState.getClusterRepairTimeInSec()));
        }
        repairState.setLastRepairTime(timeFunc.get());
        if (timeInHours == 0 && SLEEP_IF_REPAIR_FINISHES_QUICKLY.toSeconds() > 0)
        {
            //If repair finished quickly, happens for an empty instance, in such case
            //wait for some duration so that the JMX metrics can detect the repairInProgress
            logger.info("Wait for {} for repair type {}.", SLEEP_IF_REPAIR_FINISHES_QUICKLY, repairType);
            Thread.sleep(SLEEP_IF_REPAIR_FINISHES_QUICKLY.toMilliseconds());
        }
        repairState.setRepairInProgress(false);
        AutoRepairUtils.updateFinishAutoRepairHistory(repairType, myId, timeFunc.get());
    }

    public AutoRepairState getRepairState(AutoRepairConfig.RepairType repairType)
    {
        return repairStates.get(repairType);
    }

    static class CollectectedRepairStats
    {
        int failedTokenRanges = 0;
        int succeededTokenRanges = 0;
        int skippedTokenRanges = 0;
        int skippedTables = 0;
    }
}
