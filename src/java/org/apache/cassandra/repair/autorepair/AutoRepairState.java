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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.view.TableViews;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.apache.cassandra.metrics.AutoRepairMetrics;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils.AutoRepairHistory;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.RepairRunnable;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.ProgressListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

// AutoRepairState represents the state of automated repair for a given repair type.
public abstract class AutoRepairState implements ProgressListener
{
    protected static final Logger logger = LoggerFactory.getLogger(AutoRepairState.class);
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    @VisibleForTesting
    protected static Supplier<Long> timeFunc = Clock.Global::currentTimeMillis;

    @VisibleForTesting
    protected final RepairType repairType;
    @VisibleForTesting
    protected int totalTablesConsideredForRepair = 0;
    @VisibleForTesting
    protected long lastRepairTimeInMs;
    @VisibleForTesting
    protected int nodeRepairTimeInSec = 0;
    @VisibleForTesting
    protected int clusterRepairTimeInSec = 0;
    @VisibleForTesting
    protected boolean repairInProgress = false;
    @VisibleForTesting
    protected int repairKeyspaceCount = 0;
    @VisibleForTesting
    protected int totalMVTablesConsideredForRepair = 0;
    @VisibleForTesting
    protected int totalDisabledTablesRepairCount = 0;

    @VisibleForTesting
    protected int failedTokenRangesCount = 0;
    @VisibleForTesting
    protected int succeededTokenRangesCount = 0;
    @VisibleForTesting
    protected int skippedTokenRangesCount = 0;

    @VisibleForTesting
    protected int skippedTablesCount = 0;

    @VisibleForTesting
    protected AutoRepairHistory longestUnrepairedNode;
    @VisibleForTesting
    protected Condition condition = newOneTimeCondition();
    @VisibleForTesting
    protected boolean success = true;
    protected final AutoRepairMetrics metrics;

    protected AutoRepairState(RepairType repairType)
    {
        metrics = AutoRepairMetricsManager.getMetrics(repairType);
        this.repairType = repairType;
    }

    public abstract RepairRunnable getRepairRunnable(String keyspace, List<String> tables, Set<Range<Token>> ranges, boolean primaryRangeOnly);

    protected RepairRunnable getRepairRunnable(String keyspace, RepairOption options)
    {
        RepairRunnable task = new RepairRunnable(StorageService.instance, StorageService.nextRepairCommand.incrementAndGet(),
                                                 options, keyspace);

        task.addProgressListener(this);

        return task;
    }

    @Override
    public void progress(String tag, ProgressEvent event)
    {
        ProgressEventType type = event.getType();
        String message = String.format("[%s] %s", format.format(currentTimeMillis()), event.getMessage());
        if (type == ProgressEventType.ERROR)
        {
            logger.error("Repair failure for {} repair: {}", repairType.toString(), message);
            success = false;
            condition.signalAll();
        }
        if (type == ProgressEventType.PROGRESS)
        {
            message = message + " (progress: " + (int) event.getProgressPercentage() + "%)";
            logger.debug("Repair progress for {} repair: {}", repairType.toString(), message);
        }
        if (type == ProgressEventType.COMPLETE)
        {
            success = true;
            condition.signalAll();
        }
    }

    public void waitForRepairToComplete(DurationSpec.IntSecondsBound repairSessionTimeout) throws InterruptedException
    {
        //if for some reason we don't hear back on repair progress for sometime
        if (!condition.await(repairSessionTimeout.toSeconds(), TimeUnit.SECONDS))
        {
            success = false;
        }
    }

    public long getLastRepairTime()
    {
        return lastRepairTimeInMs;
    }

    public void setTotalTablesConsideredForRepair(int count)
    {
        totalTablesConsideredForRepair = count;
    }

    public int getTotalTablesConsideredForRepair()
    {
        return totalTablesConsideredForRepair;
    }

    public void setLastRepairTime(long lastRepairTime)
    {
        lastRepairTimeInMs = lastRepairTime;
    }

    public int getClusterRepairTimeInSec()
    {
        return clusterRepairTimeInSec;
    }

    public int getNodeRepairTimeInSec()
    {
        return nodeRepairTimeInSec;
    }

    public void setRepairInProgress(boolean repairInProgress)
    {
        this.repairInProgress = repairInProgress;
    }

    public boolean isRepairInProgress()
    {
        return repairInProgress;
    }

    public int getLongestUnrepairedSec()
    {
        if (longestUnrepairedNode == null)
        {
            return 0;
        }
        return (int) TimeUnit.MILLISECONDS.toSeconds(timeFunc.get() - longestUnrepairedNode.getLastRepairFinishTime());
    }

    public void setTotalMVTablesConsideredForRepair(int count)
    {
        totalMVTablesConsideredForRepair = count;
    }

    public int getTotalMVTablesConsideredForRepair()
    {
        return totalMVTablesConsideredForRepair;
    }

    public void setNodeRepairTimeInSec(int elapsed)
    {
        nodeRepairTimeInSec = elapsed;
    }

    public void setClusterRepairTimeInSec(int seconds)
    {
        clusterRepairTimeInSec = seconds;
    }

    public void setRepairKeyspaceCount(int count)
    {
        repairKeyspaceCount = count;
    }

    public int getRepairKeyspaceCount()
    {
        return repairKeyspaceCount;
    }

    public void setLongestUnrepairedNode(AutoRepairHistory longestUnrepairedNode)
    {
        this.longestUnrepairedNode = longestUnrepairedNode;
    }

    public void setFailedTokenRangesCount(int count)
    {
        failedTokenRangesCount = count;
    }

    public int getFailedTokenRangesCount()
    {
        return failedTokenRangesCount;
    }

    public void setSucceededTokenRangesCount(int count)
    {
        succeededTokenRangesCount = count;
    }

    public int getSucceededTokenRangesCount()
    {
        return succeededTokenRangesCount;
    }

    public void setSkippedTokenRangesCount(int count)
    {
        skippedTokenRangesCount = count;
    }

    public int getSkippedTokenRangesCount()
    {
        return skippedTokenRangesCount;
    }

    public void setSkippedTablesCount(int count)
    {
        skippedTablesCount = count;
    }

    public int getSkippedTablesCount()
    {
        return skippedTablesCount;
    }

    public boolean isSuccess()
    {
        return success;
    }

    public void recordTurn(AutoRepairUtils.RepairTurn turn)
    {
        metrics.recordTurn(turn);
    }

    public void setTotalDisabledTablesRepairCount(int count)
    {
        totalDisabledTablesRepairCount = count;
    }

    public int getTotalDisabledTablesRepairCount()
    {
        return totalDisabledTablesRepairCount;
    }

    public void resetWaitCondition()
    {
        condition = newOneTimeCondition();
    }
}

class IncrementalRepairState extends AutoRepairState
{
    public IncrementalRepairState()
    {
        super(RepairType.incremental);
    }

    @Override
    public RepairRunnable getRepairRunnable(String keyspace, List<String> tables, Set<Range<Token>> ranges, boolean primaryRangeOnly)
    {
        RepairOption option = new RepairOption(RepairParallelism.PARALLEL, primaryRangeOnly, true, false,
                                               AutoRepairService.instance.getAutoRepairConfig().getRepairThreads(repairType), ranges,
                                               !ranges.isEmpty(), false, false, PreviewKind.NONE, true, true, false, false);

        option.getColumnFamilies().addAll(filterOutUnsafeTables(keyspace, tables));

        return getRepairRunnable(keyspace, option);
    }

    @VisibleForTesting
    protected List<String> filterOutUnsafeTables(String keyspaceName, List<String> tables)
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);

        return tables.stream()
                     .filter(table -> {
                         ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(table);
                         TableViews views = keyspace.viewManager.forTable(cfs.metadata().id);
                         if (views != null && !views.isEmpty())
                         {
                             logger.debug("Skipping incremental repair for {}.{} as it has materialized views", keyspaceName, table);
                             return false;
                         }

                         if (cfs.metadata().params != null && cfs.metadata().params.cdc)
                         {
                             logger.debug("Skipping incremental repair for {}.{} as it has CDC enabled", keyspaceName, table);
                             return false;
                         }

                         return true;
                     }).collect(Collectors.toList());
    }
}

class FullRepairState extends AutoRepairState
{
    public FullRepairState()
    {
        super(RepairType.full);
    }

    @Override
    public RepairRunnable getRepairRunnable(String keyspace, List<String> tables, Set<Range<Token>> ranges, boolean primaryRangeOnly)
    {
        RepairOption option = new RepairOption(RepairParallelism.PARALLEL, primaryRangeOnly, false, false,
                                               AutoRepairService.instance.getAutoRepairConfig().getRepairThreads(repairType), ranges,
                                               !ranges.isEmpty(), false, false, PreviewKind.NONE, true, true, false, false);

        option.getColumnFamilies().addAll(tables);

        return getRepairRunnable(keyspace, option);
    }
}
