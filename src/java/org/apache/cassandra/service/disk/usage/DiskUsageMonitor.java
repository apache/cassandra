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
package org.apache.cassandra.service.disk.usage;


import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.guardrails.Guardrails;
import org.apache.cassandra.guardrails.GuardrailsConfig;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.Schema;

/**
 * Schedule periodic task to monitor local disk usage and notify {@link DiskUsageBroadcaster} if local state changed.
 */
public class DiskUsageMonitor
{
    private static final Logger logger = LoggerFactory.getLogger(DiskUsageMonitor.class);

    private static final int MONITOR_INTERVAL = Integer.parseInt(System.getProperty(Config.PROPERTY_PREFIX + "disk_usage.monitor_interval_ms", Integer.valueOf(30 * 1000).toString()));

    public static DiskUsageMonitor instance = new DiskUsageMonitor();

    private final GuardrailsConfig config;
    private final LongSupplier warnValueSupplier;
    private final LongSupplier failValueSupplier;
    private final Supplier<Directories.DataDirectories> dataDirectoriesSupplier;

    private volatile DiskUsageState localState = DiskUsageState.NOT_AVAILABLE;

    @VisibleForTesting
    public DiskUsageMonitor()
    {
        this.config = DatabaseDescriptor.getGuardrailsConfig();
        this.warnValueSupplier = () -> config.disk_usage_percentage_warn_threshold;
        this.failValueSupplier = () -> config.disk_usage_percentage_failure_threshold;
        this.dataDirectoriesSupplier = () -> Directories.dataDirectories;
    }

    /**
     * Start monitoring local disk usage and call notifier when local disk usage state changed.
     */
    public void start(Consumer<DiskUsageState> notifier)
    {
        // start the scheduler regardless guardrail is enabled, so we can enable it later without a restart
        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(() -> {

            if (!Guardrails.localDiskUsage.enabled())
                return;

            updateLocalState(getDiskUsage(), notifier);
        }, 0, MONITOR_INTERVAL, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    public void updateLocalState(double usageRatio, Consumer<DiskUsageState> notifier)
    {
        double percentage = usageRatio * 100;
        long percentageCeiling = (long) Math.ceil(percentage);

        DiskUsageState state = getState(percentageCeiling);

        Guardrails.localDiskUsage.guard(percentageCeiling, state.toString(), false);

        // if state remains unchanged, no need to notify peers
        if (state == localState)
            return;

        localState = state;
        notifier.accept(state);
    }

    /**
     * @return local node disk usage state
     */
    @VisibleForTesting
    public DiskUsageState state()
    {
        return localState;
    }

    /**
     * @return disk usage (including all memtable sizes) ratio
     */
    private double getDiskUsage()
    {
        // using BigDecimal to handle large file system
        BigDecimal used = BigDecimal.ZERO;
        BigDecimal total = BigDecimal.ZERO;

        for (Directories.DataDirectory dir : dataDirectoriesSupplier.get())
        {
            used = used.add(BigDecimal.valueOf(dir.getSpaceUsed()));
            total = total.add(BigDecimal.valueOf(dir.getTotalSpace()));
        }

        used = used.add(BigDecimal.valueOf(getAllMemtableSize()));

        if (logger.isTraceEnabled())
            logger.trace("Disk Usage Guardrail: current disk usage = {}, total disk usage = {}.",
                         FileUtils.stringifyFileSize(used.doubleValue()),
                         FileUtils.stringifyFileSize(total.doubleValue()));

        return used.divide(total, 5, BigDecimal.ROUND_UP).doubleValue();
    }

    @VisibleForTesting
    public long getAllMemtableSize()
    {
        long size = 0;

        for (String keyspaceName : Schema.instance.getKeyspaces())
        {
            Keyspace keyspace = Keyspace.open(keyspaceName);

            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                for (Memtable memtable : cfs.getTracker().getView().getAllMemtables())
                {
                    size += memtable.getLiveDataSize();
                }
            }
        }

        return size;
    }

    @VisibleForTesting
    public DiskUsageState getState(long usagePercentage)
    {
        long warnValue = warnValueSupplier.getAsLong();
        long failValue = failValueSupplier.getAsLong();

        boolean warnDisabled = GuardrailsConfig.diskUsageGuardrailDisabled(warnValue);
        boolean failDisabled = GuardrailsConfig.diskUsageGuardrailDisabled(failValue);

        if (failDisabled && warnDisabled)
            return DiskUsageState.NOT_AVAILABLE;

        if (!failDisabled && usagePercentage > failValue)
            return DiskUsageState.FULL;

        if (!warnDisabled && usagePercentage > warnValue)
            return DiskUsageState.STUFFED;

        return DiskUsageState.SPACIOUS;
    }
}
