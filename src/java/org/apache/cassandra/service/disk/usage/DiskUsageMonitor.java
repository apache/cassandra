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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.GuardrailsConfig;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Schedule periodic task to monitor local disk usage and notify {@link DiskUsageBroadcaster} if local state changed.
 */
public class DiskUsageMonitor
{
    private static final Logger logger = LoggerFactory.getLogger(DiskUsageMonitor.class);

    public static DiskUsageMonitor instance = new DiskUsageMonitor();

    private final Supplier<GuardrailsConfig> guardrailsConfigSupplier = () -> Guardrails.CONFIG_PROVIDER.getOrCreate(null);
    private final Supplier<Multimap<FileStore, Directories.DataDirectory>> dataDirectoriesSupplier;

    private volatile DiskUsageState localState = DiskUsageState.NOT_AVAILABLE;

    @VisibleForTesting
    public DiskUsageMonitor()
    {
        this.dataDirectoriesSupplier = DiskUsageMonitor::dataDirectoriesGroupedByFileStore;
    }

    @VisibleForTesting
    public DiskUsageMonitor(Supplier<Multimap<FileStore, Directories.DataDirectory>> dataDirectoriesSupplier)
    {
        this.dataDirectoriesSupplier = dataDirectoriesSupplier;
    }

    /**
     * Start monitoring local disk usage and call notifier when local disk usage state changed.
     */
    public void start(Consumer<DiskUsageState> notifier)
    {
        // start the scheduler regardless guardrail is enabled, so we can enable it later without a restart
        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(() -> {

            if (!Guardrails.localDataDiskUsage.enabled(null))
                return;

            updateLocalState(getDiskUsage(), notifier);
        }, 0, CassandraRelevantProperties.DISK_USAGE_MONITOR_INTERVAL_MS.getLong(), TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    public void updateLocalState(double usageRatio, Consumer<DiskUsageState> notifier)
    {
        double percentage = usageRatio * 100;
        long percentageCeiling = (long) Math.ceil(percentage);

        DiskUsageState state = getState(percentageCeiling);

        Guardrails.localDataDiskUsage.guard(percentageCeiling, state.toString(), false, null);

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
     * @return The current disk usage (including all memtable sizes) ratio. This is the ratio between the space taken by
     * all the data directories and the addition of that same space and the free available space on disk. The space
     * taken by the data directories is the addition of the actual space on disk plus the size of the memtables.
     * Memtables are included in that calculation because they are expected to be eventually flushed to disk.
     */
    @VisibleForTesting
    public double getDiskUsage()
    {
        // using BigInteger to handle large file system
        BigInteger used = BigInteger.ZERO; // space used by data directories
        BigInteger usable = BigInteger.ZERO; // free space on disks

        for (Map.Entry<FileStore, Collection<Directories.DataDirectory>> e : dataDirectoriesSupplier.get().asMap().entrySet())
        {
            usable = usable.add(BigInteger.valueOf(usableSpace(e.getKey())));

            for (Directories.DataDirectory dir : e.getValue())
                used = used.add(BigInteger.valueOf(dir.getRawSize()));
        }

        // The total disk size for data directories is the space that is actually used by those directories plus the
        // free space on disk that might be used for storing those directories in the future.
        BigInteger total = used.add(usable);

        // That total space can be limited by the config property data_disk_usage_max_disk_size.
        DataStorageSpec.LongBytesBound diskUsageMaxSize = guardrailsConfigSupplier.get().getDataDiskUsageMaxDiskSize();
        if (diskUsageMaxSize != null)
            total = total.min(BigInteger.valueOf(diskUsageMaxSize.toBytes()));

        // Add memtables size to the amount of used space because those memtables will be flushed to data directories.
        used = used.add(BigInteger.valueOf(getAllMemtableSize()));

        if (logger.isTraceEnabled())
            logger.trace("Disk Usage Guardrail: current disk usage = {}, total disk usage = {}.",
                         FileUtils.stringifyFileSize(used.doubleValue()),
                         FileUtils.stringifyFileSize(total.doubleValue()));

        return new BigDecimal(used).divide(new BigDecimal(total), 5, RoundingMode.UP).doubleValue();
    }

    @VisibleForTesting
    public long getAllMemtableSize()
    {
        long size = 0;

        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            for (Memtable memtable : cfs.getTracker().getView().getAllMemtables())
            {
                size += memtable.getLiveDataSize();
            }
        }

        return size;
    }

    @VisibleForTesting
    public DiskUsageState getState(long usagePercentage)
    {
        if (!Guardrails.localDataDiskUsage.enabled())
            return DiskUsageState.NOT_AVAILABLE;

        if (Guardrails.localDataDiskUsage.failsOn(usagePercentage, null))
            return DiskUsageState.FULL;

        if (Guardrails.localDataDiskUsage.warnsOn(usagePercentage, null))
            return DiskUsageState.STUFFED;

        return DiskUsageState.SPACIOUS;
    }

    private static Multimap<FileStore, Directories.DataDirectory> dataDirectoriesGroupedByFileStore()
    {
        Multimap<FileStore, Directories.DataDirectory> directories = HashMultimap.create();
        try
        {
            for (Directories.DataDirectory dir : Directories.dataDirectories.getAllDirectories())
            {
                FileStore store = Files.getFileStore(dir.location.toPath());
                directories.put(store, dir);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Cannot get data directories grouped by file store", e);
        }
        return directories;
    }

    public static long totalDiskSpace()
    {
        BigInteger size = dataDirectoriesGroupedByFileStore().keys()
                                                             .stream()
                                                             .map(DiskUsageMonitor::totalSpace)
                                                             .map(BigInteger::valueOf)
                                                             .reduce(BigInteger.ZERO, BigInteger::add);

        return size.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) >= 0
               ? Long.MAX_VALUE
               : size.longValue();
    }

    public static long totalSpace(FileStore store)
    {
        try
        {
            long size = store.getTotalSpace();
            return size < 0 ? Long.MAX_VALUE : size;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Cannot get total space of file store", e);
        }
    }

    public static long usableSpace(FileStore store)
    {
        try
        {
            long size = store.getUsableSpace();
            return size < 0 ? Long.MAX_VALUE : size;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Cannot get usable size of file store", e);
        }
    }
}

