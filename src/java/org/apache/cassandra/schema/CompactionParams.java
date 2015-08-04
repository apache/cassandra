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

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;

public final class CompactionParams
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionParams.class);

    public enum Option
    {
        CLASS,
        ENABLED,
        MIN_THRESHOLD,
        MAX_THRESHOLD;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    public static final int DEFAULT_MIN_THRESHOLD = 4;
    public static final int DEFAULT_MAX_THRESHOLD = 32;

    public static final boolean DEFAULT_ENABLED = true;

    public static final Map<String, String> DEFAULT_THRESHOLDS =
        ImmutableMap.of(Option.MIN_THRESHOLD.toString(), Integer.toString(DEFAULT_MIN_THRESHOLD),
                        Option.MAX_THRESHOLD.toString(), Integer.toString(DEFAULT_MAX_THRESHOLD));

    public static final CompactionParams DEFAULT =
        new CompactionParams(SizeTieredCompactionStrategy.class, DEFAULT_THRESHOLDS, DEFAULT_ENABLED);

    private final Class<? extends AbstractCompactionStrategy> klass;
    private final ImmutableMap<String, String> options;
    private final boolean isEnabled;

    private CompactionParams(Class<? extends AbstractCompactionStrategy> klass, Map<String, String> options, boolean isEnabled)
    {
        this.klass = klass;
        this.options = ImmutableMap.copyOf(options);
        this.isEnabled = isEnabled;
    }

    public static CompactionParams create(Class<? extends AbstractCompactionStrategy> klass, Map<String, String> options)
    {
        boolean isEnabled = options.containsKey(Option.ENABLED.toString())
                          ? Boolean.parseBoolean(options.get(Option.ENABLED.toString()))
                          : DEFAULT_ENABLED;

        Map<String, String> allOptions = new HashMap<>(options);
        if (supportsThresholdParams(klass))
        {
            allOptions.putIfAbsent(Option.MIN_THRESHOLD.toString(), Integer.toString(DEFAULT_MIN_THRESHOLD));
            allOptions.putIfAbsent(Option.MAX_THRESHOLD.toString(), Integer.toString(DEFAULT_MAX_THRESHOLD));
        }

        return new CompactionParams(klass, allOptions, isEnabled);
    }

    public static CompactionParams scts(Map<String, String> options)
    {
        return create(SizeTieredCompactionStrategy.class, options);
    }

    public static CompactionParams lcs(Map<String, String> options)
    {
        return create(LeveledCompactionStrategy.class, options);
    }

    public int minCompactionThreshold()
    {
        String threshold = options.get(Option.MIN_THRESHOLD.toString());
        return threshold == null
             ? DEFAULT_MIN_THRESHOLD
             : Integer.parseInt(threshold);
    }

    public int maxCompactionThreshold()
    {
        String threshold = options.get(Option.MAX_THRESHOLD.toString());
        return threshold == null
             ? DEFAULT_MAX_THRESHOLD
             : Integer.parseInt(threshold);
    }

    public void validate()
    {
        try
        {
            Map<?, ?> unknownOptions = (Map) klass.getMethod("validateOptions", Map.class).invoke(null, options);
            if (!unknownOptions.isEmpty())
            {
                throw new ConfigurationException(format("Properties specified %s are not understood by %s",
                                                        unknownOptions.keySet(),
                                                        klass.getSimpleName()));
            }
        }
        catch (NoSuchMethodException e)
        {
            logger.warn("Compaction strategy {} does not have a static validateOptions method. Validation ignored",
                        klass.getName());
        }
        catch (InvocationTargetException e)
        {
            if (e.getTargetException() instanceof ConfigurationException)
                throw (ConfigurationException) e.getTargetException();

            Throwable cause = e.getCause() == null
                            ? e
                            : e.getCause();

            throw new ConfigurationException(format("%s.validateOptions() threw an error: %s %s",
                                                    klass.getName(),
                                                    cause.getClass().getName(),
                                                    cause.getMessage()),
                                             e);
        }
        catch (IllegalAccessException e)
        {
            throw new ConfigurationException("Cannot access method validateOptions in " + klass.getName(), e);
        }

        String minThreshold = options.get(Option.MIN_THRESHOLD.toString());
        if (minThreshold != null && !StringUtils.isNumeric(minThreshold))
        {
            throw new ConfigurationException(format("Invalid value %s for '%s' compaction sub-option - must be an integer",
                                                    minThreshold,
                                                    Option.MIN_THRESHOLD));
        }

        String maxThreshold = options.get(Option.MAX_THRESHOLD.toString());
        if (maxThreshold != null && !StringUtils.isNumeric(maxThreshold))
        {
            throw new ConfigurationException(format("Invalid value %s for '%s' compaction sub-option - must be an integer",
                                                    maxThreshold,
                                                    Option.MAX_THRESHOLD));
        }

        if (minCompactionThreshold() <= 0 || maxCompactionThreshold() <= 0)
        {
            throw new ConfigurationException("Disabling compaction by setting compaction thresholds to 0 has been removed,"
                                             + " set the compaction option 'enabled' to false instead.");
        }

        if (minCompactionThreshold() <= 1)
        {
            throw new ConfigurationException(format("Min compaction threshold cannot be less than 2 (got %d)",
                                                    minCompactionThreshold()));
        }

        if (minCompactionThreshold() > maxCompactionThreshold())
        {
            throw new ConfigurationException(format("Min compaction threshold (got %d) cannot be greater than max compaction threshold (got %d)",
                                                    minCompactionThreshold(),
                                                    maxCompactionThreshold()));
        }
    }

    double defaultBloomFilterFbChance()
    {
        return klass.equals(LeveledCompactionStrategy.class) ? 0.1 : 0.01;
    }

    public Class<? extends AbstractCompactionStrategy> klass()
    {
        return klass;
    }

    /**
     * All strategy options - excluding 'class'.
     */
    public Map<String, String> options()
    {
        return options;
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

    private static Class<? extends AbstractCompactionStrategy> classFromName(String name)
    {
        String className = name.contains(".")
                         ? name
                         : "org.apache.cassandra.db.compaction." + name;
        Class<AbstractCompactionStrategy> strategyClass = FBUtilities.classForName(className, "compaction strategy");

        if (!AbstractCompactionStrategy.class.isAssignableFrom(strategyClass))
        {
            throw new ConfigurationException(format("Compaction strategy class %s is not derived from AbstractReplicationStrategy",
                                                    className));
        }

        return strategyClass;
    }

    /*
     * LCS doesn't, STCS and DTCS do
     */
    @SuppressWarnings("unchecked")
    public static boolean supportsThresholdParams(Class<? extends AbstractCompactionStrategy> klass)
    {
        try
        {
            Map<String, String> unrecognizedOptions =
                (Map<String, String>) klass.getMethod("validateOptions", Map.class)
                                           .invoke(null, DEFAULT_THRESHOLDS);

            return unrecognizedOptions.isEmpty();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public Map<String, String> asMap()
    {
        Map<String, String> map = new HashMap<>(options());
        map.put(Option.CLASS.toString(), klass.getName());
        return map;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("class", klass.getName())
                          .add("options", options)
                          .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CompactionParams))
            return false;

        CompactionParams cp = (CompactionParams) o;

        return klass.equals(cp.klass) && options.equals(cp.options);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(klass, options);
    }
}
