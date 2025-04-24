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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.view.TableViews;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.apache.cassandra.metrics.AutoRepairMetrics;
import org.apache.cassandra.repair.RepairCoordinator;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils.AutoRepairHistory;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * AutoRepairState represents the state of automated repair for a given repair type.
 */
public abstract class AutoRepairState
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
    protected final AutoRepairMetrics metrics;

    protected AutoRepairState(RepairType repairType)
    {
        metrics = AutoRepairMetricsManager.getMetrics(repairType);
        this.repairType = repairType;
    }

    public abstract RepairCoordinator getRepairRunnable(String keyspace, List<String> tables, Set<Range<Token>> ranges, boolean primaryRangeOnly);

    protected RepairCoordinator getRepairRunnable(String keyspace, RepairOption options)
    {
        return new RepairCoordinator(StorageService.instance, StorageService.nextRepairCommand.incrementAndGet(),
                                                 options, keyspace);
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
}

class PreviewRepairedState extends AutoRepairState
{
    public PreviewRepairedState()
    {
        super(RepairType.PREVIEW_REPAIRED);
    }

    @Override
    public RepairCoordinator getRepairRunnable(String keyspace, List<String> tables, Set<Range<Token>> ranges, boolean primaryRangeOnly)
    {
        RepairOption option = new RepairOption(RepairParallelism.PARALLEL, primaryRangeOnly, false, false,
                AutoRepairService.instance.getAutoRepairConfig().getRepairThreads(repairType), ranges, false, false, PreviewKind.REPAIRED, false, true, true, false, false, false);

        option.getColumnFamilies().addAll(tables);

        return getRepairRunnable(keyspace, option);
    }
}

class IncrementalRepairState extends AutoRepairState
{
    public IncrementalRepairState()
    {
        super(RepairType.INCREMENTAL);
    }

    @Override
    public RepairCoordinator getRepairRunnable(String keyspace, List<String> tables, Set<Range<Token>> ranges, boolean primaryRangeOnly)
    {
        RepairOption option = new RepairOption(RepairParallelism.PARALLEL, primaryRangeOnly, true, false,
                                               AutoRepairService.instance.getAutoRepairConfig().getRepairThreads(repairType), ranges,
                                               false, false, PreviewKind.NONE, true, true, true, false, false, false);

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
                         TableViews views = keyspace.viewManager.forTable(cfs.metadata());
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
        super(RepairType.FULL);
    }

    @Override
    public RepairCoordinator getRepairRunnable(String keyspace, List<String> tables, Set<Range<Token>> ranges, boolean primaryRangeOnly)
    {
        RepairOption option = new RepairOption(RepairParallelism.PARALLEL, primaryRangeOnly, false, false,
                                               AutoRepairService.instance.getAutoRepairConfig().getRepairThreads(repairType), ranges,
                                               false, false, PreviewKind.NONE, true, true, true, false, false, false);

        option.getColumnFamilies().addAll(tables);

        return getRepairRunnable(keyspace, option);
    }
}
