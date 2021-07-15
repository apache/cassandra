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
package org.apache.cassandra.schema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.db.compaction.CompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionStrategyOptions;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;

public final class CompactionParams
{
    public enum Option
    {
        CLASS,
        ENABLED,
        MIN_THRESHOLD,
        MAX_THRESHOLD,
        PROVIDE_OVERLAPPING_TOMBSTONES;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    public enum TombstoneOption
    {
        NONE,
        ROW,
        CELL;

        private static final TombstoneOption[] copyOfValues = values();

        public static Optional<TombstoneOption> forName(String name)
        {
            return Arrays.stream(copyOfValues).filter(x -> x.name().equals(name)).findFirst();
        }
    }

    public static final boolean DEFAULT_ENABLED = true;
    public static final TombstoneOption DEFAULT_PROVIDE_OVERLAPPING_TOMBSTONES =
            TombstoneOption.valueOf(System.getProperty("default.provide.overlapping.tombstones", TombstoneOption.NONE.toString()).toUpperCase());

    public static final Map<String, String> DEFAULT_THRESHOLDS =
        ImmutableMap.of(Option.MIN_THRESHOLD.toString(), Integer.toString(CompactionStrategyOptions.DEFAULT_MIN_THRESHOLD),
                        Option.MAX_THRESHOLD.toString(), Integer.toString(CompactionStrategyOptions.DEFAULT_MAX_THRESHOLD));

    public static final CompactionParams DEFAULT =
        new CompactionParams(SizeTieredCompactionStrategy.class, DEFAULT_THRESHOLDS, DEFAULT_ENABLED, DEFAULT_PROVIDE_OVERLAPPING_TOMBSTONES);

    private final CompactionStrategyOptions strategyOptions;
    private final boolean isEnabled;
    private final TombstoneOption tombstoneOption;

    private CompactionParams(Class<? extends CompactionStrategy> klass, Map<String, String> options, boolean isEnabled, TombstoneOption tombstoneOption)
    {
        this.strategyOptions = new CompactionStrategyOptions(klass, options, true);
        this.isEnabled = isEnabled;
        this.tombstoneOption = tombstoneOption;
    }

    public static CompactionParams create(Class<? extends CompactionStrategy> klass, Map<String, String> options)
    {
        boolean isEnabled = options.containsKey(Option.ENABLED.toString())
                          ? Boolean.parseBoolean(options.get(Option.ENABLED.toString()))
                          : DEFAULT_ENABLED;
        String overlappingTombstoneParm = options.getOrDefault(Option.PROVIDE_OVERLAPPING_TOMBSTONES.toString(),
                                                               DEFAULT_PROVIDE_OVERLAPPING_TOMBSTONES.toString()).toUpperCase();
        Optional<TombstoneOption> tombstoneOptional = TombstoneOption.forName(overlappingTombstoneParm);
        if (!tombstoneOptional.isPresent())
        {
            throw new ConfigurationException(format("Invalid value %s for 'provide_overlapping_tombstones' compaction sub-option - must be one of the following [%s].",
                                                    overlappingTombstoneParm,
                                                    StringUtils.join(TombstoneOption.values(), ", ")));
        }
        TombstoneOption tombstoneOption = tombstoneOptional.get();

        return new CompactionParams(klass, new HashMap<>(options), isEnabled, tombstoneOption);
    }

    public static CompactionParams stcs(Map<String, String> options)
    {
        return create(SizeTieredCompactionStrategy.class, options);
    }

    public static CompactionParams lcs(Map<String, String> options)
    {
        return create(LeveledCompactionStrategy.class, options);
    }

    public static CompactionParams twcs(Map<String, String> options)
    {
        return create(TimeWindowCompactionStrategy.class, options);
    }

    public static CompactionParams ucs(Map<String, String> options)
    {
        return create(UnifiedCompactionStrategy.class, options);
    }

    public int minCompactionThreshold()
    {
        return strategyOptions.minCompactionThreshold();
    }

    public int maxCompactionThreshold()
    {
        return strategyOptions.maxCompactionThreshold();
    }

    public TombstoneOption tombstoneOption()
    {
        return tombstoneOption;
    }

    double defaultBloomFilterFbChance()
    {
        return klass().equals(LeveledCompactionStrategy.class) ? 0.1 : 0.01;
    }

    public Class<? extends CompactionStrategy> klass()
    {
        return strategyOptions.klass();
    }

    /**
     * All strategy options - excluding 'class'.
     */
    public Map<String, String> options()
    {
        return strategyOptions.getOptions();
    }

    public boolean isEnabled()
    {
        return isEnabled;
    }

    public static CompactionParams fromMap(Map<String, String> map)
    {
        Map<String, String> options = new HashMap<>(map);

        String className = options.remove(Option.CLASS.toString());
        if (className == null)
        {
            throw new ConfigurationException(format("Missing sub-option '%s' for the '%s' option",
                                                    Option.CLASS,
                                                    TableParams.Option.COMPACTION));
        }

        return create(classFromName(className), options);
    }

    public static Class<? extends CompactionStrategy> classFromName(String name)
    {
        String className = name.contains(".")
                         ? name
                         : "org.apache.cassandra.db.compaction." + name;
        Class<CompactionStrategy> strategyClass = FBUtilities.classForName(className, "compaction strategy");

        if (!CompactionStrategy.class.isAssignableFrom(strategyClass))
        {
            throw new ConfigurationException(format("Compaction strategy class %s is not derived from AbstractReplicationStrategy",
                                                    className));
        }

        return strategyClass;
    }

    public Map<String, String> asMap()
    {
        Map<String, String> map = new HashMap<>(options());
        map.put(Option.CLASS.toString(), klass().getName());
        return map;
    }

    @Override
    public String toString()
    {
        return strategyOptions.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CompactionParams))
            return false;

        CompactionParams cp = (CompactionParams) o;

        return strategyOptions.equals(cp.strategyOptions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(strategyOptions);
    }
}
