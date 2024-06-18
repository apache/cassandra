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

import java.util.*;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.dht.IPartitioner;

public class ConfigGenBuilder
{
    public enum Memtable
    {SkipListMemtable, TrieMemtable, ShardedSkipListMemtable}

    private static boolean validCommitLogDiskAccessMode(Config c)
    {
        Config.DiskAccessMode m = c.commitlog_disk_access_mode;

        return null != c.commitlog_compression
            ? Config.DiskAccessMode.standard == m
            : Config.DiskAccessMode.mmap == m || Config.DiskAccessMode.direct == m;
    }

    @Nullable
    Gen<IPartitioner> partitionerGen = Generators.toGen(CassandraGenerators.nonLocalPartitioners());

    Gen<Config.DiskAccessMode> commitLogDiskAccessModeGen = Gens.enums().all(Config.DiskAccessMode.class)
                                                                .filter(m -> m != Config.DiskAccessMode.standard
                                                                             && m != Config.DiskAccessMode.mmap_index_only
                                                                             && m != Config.DiskAccessMode.direct); // don't allow direct as not every filesystem supports it, making the config environment specific

    Gen<Config.DiskAccessMode> diskAccessModeGen = Gens.enums().all(Config.DiskAccessMode.class).filter(m -> m != Config.DiskAccessMode.direct);
    Gen<String> sstableFormatGen = Generators.toGen(CassandraGenerators.sstableFormatNames());
    Gen<Config.MemtableAllocationType> memtableAllocationTypeGen = Gens.enums().all(Config.MemtableAllocationType.class);
    Gen<Memtable> memtableGen = Gens.enums().all(Memtable.class);
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

    /**
     * When loading the {@link Config} from a yaml its possible that some configs set will conflict with the configs that get generated here, to avoid that set them to a good default
     */
    public static Config prepare(Config config)
    {
        Config defaults = new Config();
        config.commitlog_sync = defaults.commitlog_sync;
        config.commitlog_sync_group_window = defaults.commitlog_sync_group_window;
        config.commitlog_sync_period = defaults.commitlog_sync_period;
        return config;
    }

    /**
     * After loading from yaml, sanitize the resulting config to avoid fast-failures on illegal combinations
     */
    public static Config sanitize(Config config)
    {
        // if commitlog_disk_access_mode has been generated to something other than standard, make sure compression is disabled
        if (null != config.commitlog_compression && Config.DiskAccessMode.standard != config.commitlog_disk_access_mode)
            config.commitlog_compression = null;

        return config;
    }

    public ConfigGenBuilder withPartitioner(IPartitioner instance)
    {
        this.partitionerGen = ignore -> instance;
        return this;
    }

    public ConfigGenBuilder withPartitionerGen(@Nullable Gen<IPartitioner> gen)
    {
        this.partitionerGen = gen;
        return this;
    }

    public ConfigGenBuilder withCommitLogSync(Config.CommitLogSync commitLogSync)
    {
        this.commitLogSyncGen = ignore -> commitLogSync;
        return this;
    }

    public ConfigGenBuilder withCommitLogSyncPeriod(DurationSpec.IntMillisecondsBound value)
    {
        Objects.requireNonNull(value);
        commitLogSyncPeriodGen = ignore -> value;
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
        if (partitionerGen == null) return;;
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

    public Gen<Map<String, Object>> buildMemtable()
    {
        return rs -> {
            LinkedHashMap<String, Object> config = new LinkedHashMap<>();
            updateConfigMemtable(rs, config);
            return config;
        };
    }

    private enum MemtableConfigShape { Single, Simple, Nested}
    public void updateConfigMemtable(RandomSource rs, Map<String, Object> config)
    {
        config.put("memtable_allocation_type", memtableAllocationTypeGen.next(rs));
        Memtable defaultMemtable = memtableGen.next(rs);
        LinkedHashMap<String, Map<String, Object>> memtables = new LinkedHashMap<>();
        switch (rs.pick(MemtableConfigShape.values()))
        {
            case Single:
                // define inline
                memtables.put("default", createConfig(defaultMemtable).next(rs));
                break;
            case Simple:
                // use inherits
                for (Memtable m : Memtable.values())
                    memtables.put(m.name(), createConfig(m).next(rs));
                memtables.put("default", ImmutableMap.of("inherits", defaultMemtable.name()));
                break;
            case Nested:
            {
                for (Memtable m : Memtable.values())
                {
                    int levels = rs.nextInt(0, 4);
                    String prev = m.name() + "_base";
                    memtables.put(prev, createConfig(m).next(rs));
                    for (int i = 0; i < levels; i++)
                    {
                        String next = m.name() + '_' + i;
                        memtables.put(next, ImmutableMap.of("inherits", prev));
                        prev = next;
                    }
                    memtables.put(m.name(), ImmutableMap.of("inherits", prev));
                }
                memtables.put("default", ImmutableMap.of("inherits", defaultMemtable.name()));
            }
            break;
            default:
                throw new UnsupportedOperationException();
        }
        config.put("memtable", ImmutableMap.of("configurations", scrable(rs, memtables)));
    }

    private static <K, V> LinkedHashMap<K, V> scrable(RandomSource rs, LinkedHashMap<K, V> map)
    {
        List<K> keys = new ArrayList<>(map.keySet());
        Collections.shuffle(keys, rs.asJdkRandom());
        LinkedHashMap<K, V> copy = new LinkedHashMap<>();
        for (K key : keys)
            copy.put(key, map.get(key));
        return copy;
    }

    private static Gen<Map<String, Object>> createConfig(Memtable type)
    {
        return rs -> {
            ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
            builder.put("class_name", type.name());
            ImmutableMap.Builder<String, Object> parametersBuilder = ImmutableMap.builder();
            switch (type)
            {
                case TrieMemtable:
                {
                    if (rs.nextBoolean())
                        parametersBuilder.put("shards", rs.nextInt(1, 64));
                }
                break;
                case ShardedSkipListMemtable:
                {
                    if (rs.nextBoolean())
                        parametersBuilder.put("serialize_writes", rs.nextBoolean());
                    if (rs.nextBoolean())
                        parametersBuilder.put("shards", rs.nextInt(1, 64));
                }
                break;
            }
            ImmutableMap<String, Object> params = parametersBuilder.build();
            if (!params.isEmpty())
                builder.put("parameters", params);
            return builder.build();
        };
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
