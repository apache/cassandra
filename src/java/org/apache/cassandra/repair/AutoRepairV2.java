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
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.apache.cassandra.metrics.AutoRepairMetricsV2;
import org.apache.cassandra.repair.state.AutoRepairState;
import org.apache.cassandra.repair.state.AutoRepairStateFactory;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.repair.AutoRepairUtilsV2.RepairTurn;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.ProgressListener;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.repair.AutoRepairUtilsV2.RepairTurn.MY_TURN;
import static org.apache.cassandra.repair.AutoRepairUtilsV2.RepairTurn.MY_TURN_DUE_TO_PRIORITY;
import static org.apache.cassandra.repair.AutoRepairUtilsV2.RepairTurn.MY_TURN_FORCE_REPAIR;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;


// TODO: add class documentation (SO-28898)
public class AutoRepairV2
{
    private static final Logger logger = LoggerFactory.getLogger(AutoRepairV2.class);
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    @VisibleForTesting
    protected static Supplier<Long> timeFunc = Clock.Global::currentTimeMillis;
    @VisibleForTesting
    protected final Map<AutoRepairConfig.RepairType, ScheduledExecutorPlus> repairExecutors;
    @VisibleForTesting
    protected final Map<AutoRepairConfig.RepairType, AutoRepairState> repairStates;
    @VisibleForTesting
    protected static Consumer<List<?>> shuffleFunc = java.util.Collections::shuffle;
    @VisibleForTesting
    protected static BiConsumer<Long, TimeUnit> sleepFunc = Uninterruptibles::sleepUninterruptibly;
    public static AutoRepairV2 instance = new AutoRepairV2();

    public boolean isSetupDone = false;

    @VisibleForTesting
    protected AutoRepairV2()
    {
        repairExecutors = new EnumMap<>(AutoRepairConfig.RepairType.class);
        repairStates = new EnumMap<>(AutoRepairConfig.RepairType.class);
        for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values())
        {
            repairExecutors.put(repairType, executorFactory().scheduled(false, "AutoRepair-Repair-" + repairType, Thread.NORM_PRIORITY));
            repairStates.put(repairType, AutoRepairStateFactory.getAutoRepairState(repairType));
        }
    }

    public void setup()
    {
        // Ensure setup is done only once
        synchronized (this)
        {
            if (isSetupDone)
            {
                return;
            }
            AutoRepairConfig config = DatabaseDescriptor.getAutoRepairConfig();
            AutoRepairService.setup();
            AutoRepairUtilsV2.setup();

            for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values())
            {
                if (config.isAutoRepairEnabled(repairType))
                    AutoRepairService.instance.checkCanRun(repairType);

                repairExecutors.get(repairType).scheduleWithFixedDelay(
                () -> repair(repairType, 60000),
                config.getInitialSchedulerDelayInSec(repairType),
                config.getRepairCheckIntervalInSec(),
                TimeUnit.SECONDS);
            }
            isSetupDone = true;
        }
    }

    // repairAsync runs a repair session of the given type asynchronously.
    public void repairAsync(AutoRepairConfig.RepairType repairType, long millisToWait)
    {
        if (!AutoRepairService.instance.getAutoRepairConfig().isAutoRepairEnabled(repairType))
        {
            throw new ConfigurationException("Auto-repair is disabled for repair type " + repairType);
        }

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

        AutoRepairService.instance.checkCanRun(repairType);

        AutoRepairState repairState = repairStates.get(repairType);
        AutoRepairMetricsV2 metrics = AutoRepairMetricsManager.getMetrics(repairType);

        try
        {
            metrics.repairEligilityCheck.inc();
            String localDC = DatabaseDescriptor.getLocalDataCenter();
            if (config.getIgnoreDCs(repairType).contains(localDC))
            {
                logger.info("Not running repair as this node belongs to datacenter {}", localDC);
                metrics.ineligibleForRepairDueToDCLimits.inc();
                return;
            }

            // Table operations are not allowed during bootstrap
            if (!AutoRepairUtilsV2.isBootstrapRepair())
            {
                // refresh the longest unrepaired node
                repairState.setLongestUnrepairedNode(AutoRepairUtilsV2.getHostWithLongestUnrepairTime(repairType));
            }

            //consistency level to use for local query
            UUID myId = Gossiper.instance.getHostId(FBUtilities.getBroadcastAddressAndPort());
            RepairTurn turn = repairState.calcRepairTurn(myId);
            repairState.recordTurn(turn);
            if (turn == MY_TURN || turn == MY_TURN_DUE_TO_PRIORITY || turn == MY_TURN_FORCE_REPAIR)
            {
                // For normal auto repair, we will use primary range only repairs (Repair with -pr option).
                // For some cases, we may set the primary_token_range_only flag to false then we will do repair
                // without -pr. We may also do force repair for certain node that we want to repair all the data on one node
                // When doing force repair, we want to repair without -pr.
                boolean primaryRangeOnly = config.getRepairPrimaryTokenRangeOnly(repairType)
                                           && turn != MY_TURN_FORCE_REPAIR;

                // Table operations are not allowed during bootstrap
                if (repairState.getLastRepairTime() == 0 && !AutoRepairUtilsV2.isBootstrapRepair())
                {
                    // the node has either just boooted or has not run repair before,
                    // we should check for the node's repair history in the DB
                    repairState.setLastRepairTime(AutoRepairUtilsV2.getLastRepairTimeForNode(repairType, myId));
                }

                /** check if it is too soon to run repair. one of the reason we
                 * should not run frequent repair is because repair triggers
                 * memtable flush
                 */
                long timeElapsedSinceLastRepairInHours = TimeUnit.MILLISECONDS.toHours(timeFunc.get() - repairState.getLastRepairTime());
                if (timeElapsedSinceLastRepairInHours < config.getRepairMinIntervalInHours(repairType))
                {
                    logger.info("Too soon to run repair, last repair was done {} hour(s) ago",
                                timeElapsedSinceLastRepairInHours);
                    metrics.ineligibleForRepairDueToRepairCooldown.inc();
                    return;
                }

                long startTime = timeFunc.get();
                logger.info("My host id: {}, my turn to run repair...repair primary-ranges only? {}", myId,
                            config.getRepairPrimaryTokenRangeOnly(repairType));
                // Table operations are not allowed during bootstrap
                if (!AutoRepairUtilsV2.isBootstrapRepair())
                {
                    AutoRepairUtilsV2.updateStartAutoRepairHistory(repairType, myId, timeFunc.get(), turn);
                }

                repairState.setRepairKeyspaceCount(0);
                repairState.setRepairInProgress(true);
                repairState.setTotalTablesConsideredForRepair(0);
                repairState.setTotalMVTablesConsideredForRepair(0);
                int failedTokenRanges = 0;
                int succeededTokenRanges = 0;
                int skippedTokenRanges = 0;

                List<Keyspace> keyspaces = new ArrayList<>();
                Keyspace.all().forEach(keyspaces::add);
                // Auto-repair is likely to be run on multiple nodes independently, we want to avoid running multiple repair
                // sessions on overlapping datasets at the same time. Shuffling keyspaces reduces the likelihood of this happening.
                shuffleFunc.accept(keyspaces);

                for (Keyspace keyspace : keyspaces)
                {
                    Tables tables = keyspace.getMetadata().tables;
                    Iterator<TableMetadata> iter = tables.iterator();
                    String keyspaceName = keyspace.getName();
                    if (!AutoRepairUtilsV2.shouldRepair(repairType, keyspace) ||
                        !AutoRepairUtilsV2.checkNodeContainsKeyspaceReplica(keyspace))
                    {
                        continue;
                    }

                    repairState.setRepairKeyspaceCount(repairState.getRepairKeyspaceCount() + 1);
                    List<String> tablesToBeRepaired = new ArrayList<>();
                    while (iter.hasNext())
                    {
                        repairState.setTotalTablesConsideredForRepair(repairState.getTotalTablesConsideredForRepair() + 1);
                        TableMetadata tableMetadata = iter.next();
                        String tableName = tableMetadata.name;
                        tablesToBeRepaired.add(tableName);

                        // See if we should repair MVs as well that are associated with this given table
                        List<String> mvs = AutoRepairUtilsV2.getAllMVs(repairType, keyspace, tableMetadata);
                        if (mvs.size() > 0)
                        {
                            tablesToBeRepaired.addAll(mvs);
                            repairState.setTotalMVTablesConsideredForRepair(repairState.getTotalMVTablesConsideredForRepair() + mvs.size());
                        }
                    }

                    shuffleFunc.accept(tablesToBeRepaired);
                    for (String tableName : tablesToBeRepaired)
                    {
                        try
                        {
                            // by default run repair for the token range that this node owns
                            InetAddressAndPort repairTokenRangesForNode = FBUtilities.getBroadcastAddressAndPort();
                            if (AutoRepairUtilsV2.isBootstrapRepair())
                            {
                                // this is useful if we want to run repair for the token range owned
                                // by some other node
                                // TODO: maybe add a metric for this
                                repairTokenRangesForNode = DatabaseDescriptor.getReplaceAddress();
                                logger.info("Repair token ranges for node {}", repairTokenRangesForNode);
                            }
                            Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRangesForEndpoint(keyspaceName, repairTokenRangesForNode);
                            if (!primaryRangeOnly)
                            {
                                // if we need to repair non-primary token ranges, then change the tokens accrodingly
                                tokens = StorageService.instance.getLocalReplicasEndpoint(keyspaceName, repairTokenRangesForNode).ranges();
                            }
                            int numberOfSubranges = config.getRepairSubRangeNum(repairType);
                            int totalSubRanges = tokens.size() * numberOfSubranges;
                            ColumnFamilyStore columnFamilyStore = keyspace.getColumnFamilyStore(tableName);
                            // this is done to make autorepair safe as running repair on table with more sstables
                            // may have its own challenges
                            int size = columnFamilyStore.getLiveSSTables().size();
                            if (size > config.getRepairSSTableCountHigherThreshold(repairType))
                            {
                                logger.info("Too many SSTables for repair, not doing repair on table {}.{} " +
                                            "totalSSTables {}", keyspaceName, tableName, columnFamilyStore.getLiveSSTables().size());
                                skippedTokenRanges += totalSubRanges;
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
                            //now run repair on this table
                            Set<Range<Token>> ranges = new HashSet<>();
                            int totalProcessedSubRanges = 0;
                            for (Range<Token> token : tokens)
                            {
                                if (!config.isAutoRepairEnabled(repairType))
                                {
                                    logger.error("Auto-repair for type {} is disabled hence not running repair", repairType);
                                    repairState.setRepairInProgress(false);
                                    return;
                                }

                                if (config.getRepairByKeyspace(repairType))
                                {
                                    if (AutoRepairUtilsV2.keyspaceMaxRepairTimeExceeded(repairType, tableStartTime, tablesToBeRepaired.size()))
                                    {
                                        skippedTokenRanges += totalSubRanges - totalProcessedSubRanges;
                                        logger.info("Keyspace took too much time to repair hence skipping it {}",
                                                    keyspaceName);
                                        break;
                                    }
                                }
                                else
                                {
                                    if (AutoRepairUtilsV2.tableMaxRepairTimeExceeded(repairType, tableStartTime))
                                    {
                                        skippedTokenRanges += totalSubRanges - totalProcessedSubRanges;
                                        logger.info("Table took too much time to repair hence skipping it {}.{}",
                                                    keyspaceName, tableName);
                                        break;
                                    }
                                }

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
                                    if ((totalProcessedSubRanges % config.getRepairThreads(repairType) == 0) ||
                                        (totalProcessedSubRanges == totalSubRanges))
                                    {
                                        boolean success = false;
                                        int retryCount = 0;
                                        while(retryCount <= config.getRepairMaxRetries())
                                        {
                                            RepairRunnable task = repairState.getRepairRunnable(keyspaceName,
                                                                                                config.getRepairByKeyspace(repairType) ? tablesToBeRepaired : ImmutableList.of(tableName),
                                                                                                ranges, primaryRangeOnly, AutoRepairUtilsV2.getLocalDCGroup(repairType));

                                            RepairProgressListener listener = new RepairProgressListener(repairType);
                                            task.addProgressListener(listener);
                                            new Thread(NamedThreadFactory.createAnonymousThread(new FutureTask<>(task, null))).start();
                                            try
                                            {
                                                long jobStartTime = timeFunc.get();
                                                listener.await();
                                                success = listener.isSuccess();
                                                soakAfterRepair(jobStartTime, config.getRepairTaskMinDuration().toMilliseconds());
                                            }
                                            catch (InterruptedException e)
                                            {
                                                logger.error("Exception in cond await:", e);
                                            }
                                            if (success)
                                            {
                                                break;
                                            }
                                            else if (retryCount < config.getRepairMaxRetries())
                                            {
                                                logger.warn("Repair failed for range {}-{} for {}.{} retrying after {} seconds...",
                                                            childStartToken, childEndToken,
                                                            keyspaceName, config.getRepairByKeyspace(repairType) ? tablesToBeRepaired : tableName,
                                                            config.getRepairRetryBackoffInSec());
                                                sleepFunc.accept(config.getRepairRetryBackoffInSec(), TimeUnit.SECONDS);
                                            }
                                            retryCount++;
                                        }
                                        //check repair outcome
                                        if (success)
                                        {
                                            logger.info("Repair completed for range {}-{} for {}.{}, total subranges: {}," +
                                                        "processed subranges: {}", childStartToken.toString(), childEndToken.toString(),
                                                        keyspaceName, config.getRepairByKeyspace(repairType) ? tablesToBeRepaired : tableName, totalSubRanges, totalProcessedSubRanges);
                                            succeededTokenRanges += ranges.size();
                                        }
                                        else
                                        {
                                            logger.error("Repair failed for range {}-{} for {}.{} after {} retries, total subranges: {}," +
                                                        "processed subranges: {}", childStartToken.toString(), childEndToken.toString(), keyspaceName,
                                                         config.getRepairByKeyspace(repairType) ? tablesToBeRepaired : tableName, retryCount, totalSubRanges, totalProcessedSubRanges);
                                            failedTokenRanges += ranges.size();
                                        }
                                        ranges.clear();
                                    }
                                }
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

                //if it was due to priority then remove it now
                if (turn == MY_TURN_DUE_TO_PRIORITY)
                {
                    logger.info("Remove current host from priority list");
                    AutoRepairUtilsV2.removePriorityStatus(repairType, myId);
                }

                repairState.setFailedTokenRangesCount(failedTokenRanges);
                repairState.setSucceededTokenRangesCount(succeededTokenRanges);
                repairState.setSkippedTokenRangesCount(skippedTokenRanges);
                long timeInHours = TimeUnit.SECONDS.toHours(repairState.getNodeRepairTimeInSec());
                logger.info("Local {} repair time {} hour(s), stats: repairKeyspaceCount {}, " +
                            "repairTokenRangesSuccessCount {}, repairTokenRangesFailureCount {}, " +
                            "repairTokenRangesSkipCount {}", repairType, timeInHours, repairState.getRepairKeyspaceCount(),
                            repairState.getSucceededTokenRangesCount(), repairState.getFailedTokenRangesCount(),
                            repairState.getSkippedTokenRangesCount());
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
                repairState.setNodeRepairTimeInSec((int) TimeUnit.MILLISECONDS.toSeconds(timeFunc.get() - startTime));
                repairState.setRepairInProgress(false);
                if (!AutoRepairUtilsV2.isBootstrapRepair())
                {
                    AutoRepairUtilsV2.updateFinishAutoRepairHistory(repairType, myId, timeFunc.get());
                }
            }
            else
            {
                logger.info("Waiting for my turn...");
                metrics.ineligibleForRepairDueToNodeOrder.inc();
            }
        }
        catch (Exception e)
        {
            logger.error("Exception in autorepair:", e);
        }
    }

    public AutoRepairState getRepairState(AutoRepairConfig.RepairType repairType)
    {
        return repairStates.get(repairType);
    }

    private void soakAfterRepair(long startTimeMilis, long minDurationMilis)
    {
        long currentTime = timeFunc.get();
        long timeElapsed = currentTime - startTimeMilis;
        if (timeElapsed < minDurationMilis)
        {
            long timeToSoak = minDurationMilis - timeElapsed;
            logger.info("Soaking for {} ms after repair", timeToSoak);
            sleepFunc.accept(timeToSoak, TimeUnit.MILLISECONDS);
        }
    }

    @VisibleForTesting
    protected static class RepairProgressListener implements ProgressListener
    {
        private final AutoRepairConfig.RepairType repairType;
        @VisibleForTesting
        protected boolean success;
        @VisibleForTesting
        protected final Condition condition = newOneTimeCondition();

        public RepairProgressListener(AutoRepairConfig.RepairType repairType)
        {
            this.repairType = repairType;
        }

        public void await() throws InterruptedException
        {
            //if for some reason we don't hear back on repair progress for sometime
            if (!condition.await(12, TimeUnit.HOURS))
            {
                success = false;
            }
        }

        public boolean isSuccess()
        {
            return success;
        }

        @Override
        public void progress(String tag, ProgressEvent event)
        {
            ProgressEventType type = event.getType();
            String message = String.format("[%s] %s", format.format(System.currentTimeMillis()), event.getMessage());
            if (type == ProgressEventType.ERROR)
            {
                logger.error("Repair failure for repair {}: {}", repairType.toString(), message);
                success = false;
                condition.signalAll();
            }
            if (type == ProgressEventType.PROGRESS)
            {
                message = message + " (progress: " + (int) event.getProgressPercentage() + "%)";
                logger.debug("Repair progress for repair {}: {}", repairType.toString(), message);
            }
            if (type == ProgressEventType.COMPLETE)
            {
                logger.debug("Repair completed for repair {}: {}", repairType.toString(), message);
                success = true;
                condition.signalAll();
            }
        }
    }
}
