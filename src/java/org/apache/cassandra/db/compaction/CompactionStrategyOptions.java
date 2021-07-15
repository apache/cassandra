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

package org.apache.cassandra.db.compaction;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.MoreObjects;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Throwables;

import static java.lang.String.format;

/**
 * This class contains all compaction options that are shared by all strategies.
 */
public class CompactionStrategyOptions
{
    public static final int DEFAULT_MIN_THRESHOLD = 4;
    public static final int DEFAULT_MAX_THRESHOLD = 32;
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyOptions.class);

    public static final Map<String, String> DEFAULT_THRESHOLDS =
    ImmutableMap.of(CompactionParams.Option.MIN_THRESHOLD.toString(), Integer.toString(DEFAULT_MIN_THRESHOLD),
                    CompactionParams.Option.MAX_THRESHOLD.toString(), Integer.toString(DEFAULT_MAX_THRESHOLD));

    public static final String ONLY_PURGE_REPAIRED_TOMBSTONES = "only_purge_repaired_tombstones";

    public static final String DEFAULT_TOMBSTONE_THRESHOLD = "0.2";
    // minimum interval needed to perform tombstone removal compaction in seconds, default 86400 or 1 day.
    public static final String DEFAULT_TOMBSTONE_COMPACTION_INTERVAL = "86400";
    public static final String DEFAULT_UNCHECKED_TOMBSTONE_COMPACTION_OPTION = "false";
    public static final String DEFAULT_LOG_ALL_OPTION = "false";

    public static final String TOMBSTONE_THRESHOLD_OPTION = "tombstone_threshold";
    public static final String TOMBSTONE_COMPACTION_INTERVAL_OPTION = "tombstone_compaction_interval";
    // disable range overlap check when deciding if an SSTable is candidate for tombstone compaction (CASSANDRA-6563)
    public static final String UNCHECKED_TOMBSTONE_COMPACTION_OPTION = "unchecked_tombstone_compaction";
    public static final String LOG_ALL_OPTION = "log_all";
    public static final String COMPACTION_ENABLED = "enabled";

    private final Class<? extends CompactionStrategy> klass;
    private final Map<String, String> options;
    private final float tombstoneThreshold;
    private final long tombstoneCompactionInterval;
    private final boolean uncheckedTombstoneCompaction;
    private boolean disableTombstoneCompactions = false;
    private final boolean logAll;

    public CompactionStrategyOptions(Class<? extends CompactionStrategy> klass, Map<String, String> options, boolean throwOnInvalidOption)
    {
        this.klass = klass;
        this.options = copyOptions(klass, options);

        boolean useDefault = false;
        try
        {
            validate(); // will throw ConfigurationException if the options are invalid
        }
        catch (ConfigurationException e)
        {
            // when called from CompactionParams we throw but when called from AbstractCompactionStrategy we use defaults
            // could probably not bother with the latter (?)
            if (throwOnInvalidOption)
            {
                throw e;
            }
            else
            {
                logger.warn("Error setting compaction strategy options ({}), defaults will be used", e.getMessage());
                useDefault = true;
            }
        }

        tombstoneThreshold = Float.parseFloat(getOption(TOMBSTONE_THRESHOLD_OPTION, useDefault, DEFAULT_TOMBSTONE_THRESHOLD));
        tombstoneCompactionInterval = Long.parseLong(getOption(TOMBSTONE_COMPACTION_INTERVAL_OPTION, useDefault, DEFAULT_TOMBSTONE_COMPACTION_INTERVAL));
        uncheckedTombstoneCompaction = Boolean.parseBoolean(getOption(UNCHECKED_TOMBSTONE_COMPACTION_OPTION, useDefault, DEFAULT_UNCHECKED_TOMBSTONE_COMPACTION_OPTION));
        logAll = Boolean.parseBoolean(getOption(LOG_ALL_OPTION, useDefault, DEFAULT_LOG_ALL_OPTION));
    }

    private Map<String, String> copyOptions(Class<? extends CompactionStrategy> klass, Map<String, String> options)
    {
        Map<String, String> newOptions = new HashMap<>(options);

        // For legacy compatibility reasons, for some compaction strategies we want to see the default min and max threshold
        // in the compaction parameters that can be seen in CQL when retrieving the table from the schema tables so for
        // these strategies we need to add these options when they have not been specified by the user
        if (supportsThresholdParams(klass))
        {
            newOptions.putIfAbsent(CompactionParams.Option.MIN_THRESHOLD.toString(), Integer.toString(DEFAULT_MIN_THRESHOLD));
            newOptions.putIfAbsent(CompactionParams.Option.MAX_THRESHOLD.toString(), Integer.toString(DEFAULT_MAX_THRESHOLD));
        }

        return newOptions;
    }

    /**
     * All strategies except {@link UnifiedCompactionStrategy} support the minimum and maximum thresholds
     */
    @SuppressWarnings("unchecked")
    public static boolean supportsThresholdParams(Class<? extends CompactionStrategy> klass)
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
            throw Throwables.cleaned(e);
        }
    }

    private String getOption(String optionName, boolean useDefault, String defaultValue)
    {
        if (useDefault)
            return defaultValue;

        String optionValue = options.get(optionName);
        if (optionValue == null)
            return defaultValue;

        return optionValue;
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

        if (!(o instanceof CompactionStrategyOptions))
            return false;

        CompactionStrategyOptions that = (CompactionStrategyOptions) o;

        return klass.equals(that.klass) && options.equals(that.options);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(klass, options);
    }

    private Map<String, String> validate()
    {
        try
        {
            // Each strategy currently implements a static validateOptions() method for custom validation, the default behavior
            // is to simply call validateOptions() below, through AbstractCompactionStrategy.validateOptions(), we could simplify
            // all this assuming we don't need to support any user-defined compaction strategy
            Map<String, String> unknownOptions = (Map<String, String>) klass.getMethod("validateOptions", Map.class).invoke(null, options);
            if (!unknownOptions.isEmpty())
            {
                throw new ConfigurationException(format("Properties specified %s are not understood by %s",
                                                        unknownOptions.keySet(),
                                                        klass.getSimpleName()));
            }

            return unknownOptions;
        }
        catch (NoSuchMethodException e)
        {
            logger.warn("Compaction strategy {} does not have a static validateOptions method. Validation ignored", klass.getName());
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

        return options;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        String minThreshold = options.get(CompactionParams.Option.MIN_THRESHOLD.toString());
        if (minThreshold != null && !StringUtils.isNumeric(minThreshold))
        {
            throw new ConfigurationException(format("Invalid value %s for '%s' compaction sub-option - must be an integer",
                                                    minThreshold,
                                                    CompactionParams.Option.MIN_THRESHOLD));
        }

        String maxThreshold = options.get(CompactionParams.Option.MAX_THRESHOLD.toString());
        if (maxThreshold != null && !StringUtils.isNumeric(maxThreshold))
        {
            throw new ConfigurationException(format("Invalid value %s for '%s' compaction sub-option - must be an integer",
                                                    maxThreshold,
                                                    CompactionParams.Option.MAX_THRESHOLD));
        }

        String threshold = options.get(TOMBSTONE_THRESHOLD_OPTION);
        if (threshold != null)
        {
            try
            {
                float thresholdValue = Float.parseFloat(threshold);
                if (thresholdValue < 0)
                {
                    throw new ConfigurationException(String.format("%s must be greater than 0, but was %f", TOMBSTONE_THRESHOLD_OPTION, thresholdValue));
                }
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", threshold, TOMBSTONE_THRESHOLD_OPTION), e);
            }
        }

        String interval = options.get(TOMBSTONE_COMPACTION_INTERVAL_OPTION);
        if (interval != null)
        {
            try
            {
                long tombstoneCompactionInterval = Long.parseLong(interval);
                if (tombstoneCompactionInterval < 0)
                {
                    throw new ConfigurationException(String.format("%s must be greater than 0, but was %d", TOMBSTONE_COMPACTION_INTERVAL_OPTION, tombstoneCompactionInterval));
                }
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", interval, TOMBSTONE_COMPACTION_INTERVAL_OPTION), e);
            }
        }

        String unchecked = options.get(UNCHECKED_TOMBSTONE_COMPACTION_OPTION);
        if (unchecked != null && !unchecked.equalsIgnoreCase("true") && !unchecked.equalsIgnoreCase("false"))
        {
            throw new ConfigurationException(String.format("'%s' should be either 'true' or 'false', not '%s'", UNCHECKED_TOMBSTONE_COMPACTION_OPTION, unchecked));
        }

        String logAll = options.get(LOG_ALL_OPTION);
        if (logAll != null && !logAll.equalsIgnoreCase("true") && !logAll.equalsIgnoreCase("false"))
        {
            throw new ConfigurationException(String.format("'%s' should either be 'true' or 'false', not %s", LOG_ALL_OPTION, logAll));
        }

        String compactionEnabled = options.get(COMPACTION_ENABLED);
        if (compactionEnabled != null && !compactionEnabled.equalsIgnoreCase("true") && !compactionEnabled.equalsIgnoreCase("false"))
        {
            throw new ConfigurationException(String.format("enabled should either be 'true' or 'false', not %s", compactionEnabled));
        }

        Map<String, String> uncheckedOptions = new HashMap<>(options);
        uncheckedOptions.remove(TOMBSTONE_THRESHOLD_OPTION);
        uncheckedOptions.remove(TOMBSTONE_COMPACTION_INTERVAL_OPTION);
        uncheckedOptions.remove(UNCHECKED_TOMBSTONE_COMPACTION_OPTION);
        uncheckedOptions.remove(LOG_ALL_OPTION);
        uncheckedOptions.remove(COMPACTION_ENABLED);
        uncheckedOptions.remove(ONLY_PURGE_REPAIRED_TOMBSTONES);
        uncheckedOptions.remove(CompactionParams.Option.PROVIDE_OVERLAPPING_TOMBSTONES.toString());
        return uncheckedOptions;
    }

    public int minCompactionThreshold()
    {
        String threshold = options.get(CompactionParams.Option.MIN_THRESHOLD.toString());
        return threshold == null
               ? DEFAULT_MIN_THRESHOLD
               : Integer.parseInt(threshold);
    }

    public int maxCompactionThreshold()
    {
        String threshold = options.get(CompactionParams.Option.MAX_THRESHOLD.toString());
        return threshold == null
               ? DEFAULT_MAX_THRESHOLD
               : Integer.parseInt(threshold);
    }

    public Class<? extends CompactionStrategy> klass()
    {
        return klass;
    }

    public Map<String, String> getOptions()
    {
        return options;
    }

    public float getTombstoneThreshold()
    {
        return tombstoneThreshold;
    }

    public long getTombstoneCompactionInterval()
    {
        return tombstoneCompactionInterval;
    }

    public boolean isUncheckedTombstoneCompaction()
    {
        return uncheckedTombstoneCompaction;
    }

    public boolean isDisableTombstoneCompactions()
    {
        return disableTombstoneCompactions;
    }

    /**
     * {@link DateTieredCompactionStrategy} and {@link TimeWindowCompactionStrategy} disable this
     * parameter if other parameters aren't available.
     */
    public void setDisableTombstoneCompactions(boolean disableTombstoneCompactions)
    {
        this.disableTombstoneCompactions = disableTombstoneCompactions;
    }

    public boolean isLogAll()
    {
        return logAll;
    }
}
