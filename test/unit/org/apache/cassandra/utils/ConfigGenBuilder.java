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

package org.apache.cassandra.utils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;

public class ConfigGenBuilder
{
    Gen<IPartitioner> partitionerGen = Generators.toGen(CassandraGenerators.nonLocalPartitioners());
    Gen<Config.DiskAccessMode> commitLogDiskAccessModeGen = Gens.enums().all(Config.DiskAccessMode.class)
                                                                .filter(m -> m != Config.DiskAccessMode.standard && m != Config.DiskAccessMode.mmap_index_only);
    Gen<Config.DiskAccessMode> diskAccessModeGen = Gens.enums().all(Config.DiskAccessMode.class).filter(m -> m != Config.DiskAccessMode.direct);
    Gen<String> sstableFormatGen = Generators.toGen(CassandraGenerators.sstableFormat()).map(SSTableFormat::name);
    Gen<Config.MemtableAllocationType> memtableAllocationTypeGen = Gens.enums().all(Config.MemtableAllocationType.class);
    Gen<Config.CommitLogSync> commitLogSyncGen = Gens.enums().all(Config.CommitLogSync.class);
    Gen<DurationSpec.IntMillisecondsBound> commitLogSyncPeriodGen = rs -> {
        // how long?
        long periodMillis;
        switch (rs.nextInt(0, 2))
        {
            case 0: // millis
                periodMillis = rs.nextLong(1, 20);
                break;
            case 1: // seconds
                periodMillis = TimeUnit.SECONDS.toMillis(rs.nextLong(1, 20));
                break;
            default:
                throw new AssertionError();
        }
        return new DurationSpec.IntMillisecondsBound(periodMillis);
    };
    // group blocks each and every write for X milliseconds which cause tests to take a lot of time,
    // for this reason the period must be "short"
    Gen<DurationSpec.IntMillisecondsBound> commitlogSyncGroupWindowGen = Gens.longs().between(1, 20).map(l -> new DurationSpec.IntMillisecondsBound(l));

    public ConfigGenBuilder withPartitioner(IPartitioner instance)
    {
        this.partitionerGen = ignore -> instance;
        return this;
    }

    public ConfigGenBuilder withCommitLogSync(Config.CommitLogSync commitLogSync)
    {
        this.commitLogSyncGen = ignore -> commitLogSync;
        return this;
    }

    public Gen<Map<String, Object>> build()
    {
        return rs -> {
            Map<String, Object> config = new LinkedHashMap<>();

            updateConfigPartitioner(rs, config);
            updateConfigCommitLog(rs, config);
            updateConfigMemtable(rs, config);
            updateConfigSSTables(rs, config);
            updateConfigDisk(rs, config);
            return config;
        };
    }

    private void updateConfigPartitioner(RandomSource rs, Map<String, Object> config)
    {
        IPartitioner partitioner = partitionerGen.next(rs);
        config.put("partitioner", partitioner.getClass().getSimpleName());
    }

    private void updateConfigCommitLog(RandomSource rs, Map<String, Object> config)
    {
        Config.CommitLogSync commitlog_sync = commitLogSyncGen.next(rs);
        config.put("commitlog_sync", commitlog_sync);
        switch (commitlog_sync)
        {
            case batch:
                break;
            case periodic:
                config.put("commitlog_sync_period", commitLogSyncPeriodGen.next(rs).toString());
            break;
            case group:
                config.put("commitlog_sync_group_window", commitlogSyncGroupWindowGen.next(rs).toString());
            break;
            default:
                throw new AssertionError(commitlog_sync.name());
        }
        config.put("commitlog_disk_access_mode", commitLogDiskAccessModeGen.next(rs));
    }

    private void updateConfigMemtable(RandomSource rs, Map<String, Object> config)
    {
        config.put("memtable_allocation_type", memtableAllocationTypeGen.next(rs));
    }

    private void updateConfigSSTables(RandomSource rs, Map<String, Object> config)
    {
        config.put("sstable", ImmutableMap.of("selected_format", sstableFormatGen.next(rs)));
    }

    private void updateConfigDisk(RandomSource rs, Map<String, Object> config)
    {
        config.put("disk_access_mode", diskAccessModeGen.next(rs));
    }
}
