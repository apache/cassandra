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

package org.apache.cassandra.config;

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.GuardrailsConfig;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

/**
 * Configuration settings for guardrails populated from the Yaml file.
 *
 * <p>Note that the settings here must only be used to build the {@link GuardrailsConfig} class and not directly by the
 * code checking each guarded constraint. That code should use the higher level abstractions defined in
 * {@link Guardrails}).
 *
 * <p>This contains a main setting, {@code enabled}, controlling if guardrails are globally active or not, and
 * individual settings to control each guardrail.
 *
 * <p>We have 2 variants of guardrails, soft (warn) and hard (abort) limits, each guardrail having either one of the
 * variants or both. Note in particular that hard limits only make sense for guardrails triggering during query
 * execution. For other guardrails, say one triggering during compaction, aborting that compaction does not make sense.
 *
 * <p>Additionally, each individual setting should have a specific value (typically -1 for numeric settings),
 * that allows to disable the corresponding guardrail.
 */
public class GuardrailsOptions implements GuardrailsConfig
{
    private static final String NAME_PREFIX = "guardrails.";
    private static final Logger logger = LoggerFactory.getLogger(GuardrailsOptions.class);

    public volatile boolean enabled = false;
    public final IntThreshold keyspaces = new IntThreshold();
    public final IntThreshold tables = new IntThreshold();
    public final IntThreshold columns_per_table = new IntThreshold();
    public final IntThreshold secondary_indexes_per_table = new IntThreshold();
    public final IntThreshold materialized_views_per_table = new IntThreshold();
    public final TableProperties table_properties = new TableProperties();
    public final IntThreshold page_size = new IntThreshold();
    public final LongKBThreshold coordinator_read_size = new LongKBThreshold();
    public final LongKBThreshold local_read_size = new LongKBThreshold();
    public final IntKBThreshold row_index_size = new IntKBThreshold();

    public volatile boolean user_timestamps_enabled = true;
    public volatile boolean read_before_write_list_operations_enabled = true;

    public void validate()
    {
        keyspaces.init("keyspaces");
        tables.init("tables");
        columns_per_table.init("columns_per_table");
        secondary_indexes_per_table.init("secondary_indexes_per_table");
        materialized_views_per_table.init("materialized_views_per_table");
        table_properties.init("table_properties");
        page_size.init("page_size");
        coordinator_read_size.init("coordinator_read_size");
        local_read_size.init("local_read_size");
        row_index_size.init("row_index_size");
    }

    @Override
    public boolean getEnabled()
    {
        return enabled;
    }

    /**
     * Enable/disable guardrails.
     *
     * @param enabled {@code true} for enabling guardrails, {@code false} for disabling them.
     */
    public void setEnabled(boolean enabled)
    {
        updatePropertyWithLogging(NAME_PREFIX + "enabled", enabled, () -> this.enabled, x -> this.enabled = x);
    }

    @Override
    public IntThreshold getKeyspaces()
    {
        return keyspaces;
    }

    @Override
    public IntThreshold getTables()
    {
        return tables;
    }

    @Override
    public IntThreshold getColumnsPerTable()
    {
        return columns_per_table;
    }

    @Override
    public IntThreshold getSecondaryIndexesPerTable()
    {
        return secondary_indexes_per_table;
    }

    @Override
    public IntThreshold getMaterializedViewsPerTable()
    {
        return materialized_views_per_table;
    }

    @Override
    public TableProperties getTableProperties()
    {
        return table_properties;
    }

    @Override
    public boolean getUserTimestampsEnabled()
    {
        return user_timestamps_enabled;
    }

    @Override
    public IntThreshold getPageSize()
    {
        return page_size;
    }

    @Override
    public LongKBThreshold getCoordinatorReadSize()
    {
        return coordinator_read_size;
    }

    @Override
    public LongKBThreshold getLocalReadSize()
    {
        return local_read_size;
    }

    @Override
    public IntKBThreshold getRowIndexSize()
    {
        return row_index_size;
    }

    public void setUserTimestampsEnabled(boolean enabled)
    {
        updatePropertyWithLogging(NAME_PREFIX + "user_timestamps_enabled",
                                  enabled,
                                  () -> user_timestamps_enabled,
                                  x -> user_timestamps_enabled = x);
    }

    @Override
    public boolean getReadBeforeWriteListOperationsEnabled()
    {
        return read_before_write_list_operations_enabled;
    }

    public void setReadBeforeWriteListOperationsEnabled(boolean enabled)
    {
        updatePropertyWithLogging(NAME_PREFIX + "read_before_write_list_operations_enabled",
                                  enabled,
                                  () -> read_before_write_list_operations_enabled,
                                  x -> read_before_write_list_operations_enabled = x);
    }

    private static <T> void updatePropertyWithLogging(String propertyName, T newValue, Supplier<T> getter, Consumer<T> setter)
    {
        T oldValue = getter.get();
        if (!newValue.equals(oldValue))
        {
            setter.accept(newValue);
            logger.info("Updated {} from {} to {}", propertyName, oldValue, newValue);
        }
    }

    protected static abstract class Config
    {
        protected String name;

        public String getName()
        {
            return name;
        }

        protected void init(String name)
        {
            this.name = NAME_PREFIX + name;
            validate();
        }

        protected abstract void validate();
    }

    public static abstract class Threshold extends Config
    {
        public static final long DISABLED = -1;

        public abstract long maxValue();

        protected void validate(long warn, long abort)
        {
            validateLimits(warn, name + ".warn_threshold");
            validateLimits(abort, name + ".abort_threshold");
            validateWarnLowerThanAbort(warn, abort);
        }

        protected void validateLimits(long value, String name)
        {
            if (value > maxValue())
                throw new IllegalArgumentException(format("Invalid value %d for %s: maximum allowed value is %d",
                                                          value, name, maxValue()));

            if (value == 0)
                throw new IllegalArgumentException(format("Invalid value for %s: 0 is not allowed; " +
                                                          "if attempting to disable use %d",
                                                          name, DISABLED));

            // We allow -1 as a general "disabling" flag. But reject anything lower to avoid mistakes.
            if (value < DISABLED)
                throw new IllegalArgumentException(format("Invalid value %d for %s: negative values are not allowed, " +
                                                          "outside of %d which disables the guardrail",
                                                          value, name, DISABLED));
        }

        private void validateWarnLowerThanAbort(long warn, long abort)
        {
            if (warn == DISABLED || abort == DISABLED)
                return;

            if (abort < warn)
                throw new IllegalArgumentException(format("The warn threshold %d for %s should be lower than the " +
                                                          "abort threshold %d", warn, name, abort));
        }
    }

    public static class IntThreshold extends Threshold implements GuardrailsConfig.IntThreshold
    {
        public volatile int warn_threshold = (int) DISABLED;
        public volatile int abort_threshold = (int) DISABLED;

        @Override
        public int getWarnThreshold()
        {
            return warn_threshold;
        }

        @Override
        public int getAbortThreshold()
        {
            return abort_threshold;
        }

        public void setThresholds(int warn, int abort)
        {
            validate(warn, abort);
            updatePropertyWithLogging(name + ".warn_threshold", warn, () -> warn_threshold, x -> warn_threshold = x);
            updatePropertyWithLogging(name + ".abort_threshold", abort, () -> abort_threshold, x -> abort_threshold = x);
        }

        @Override
        protected void validate()
        {
            validate(warn_threshold, abort_threshold);
        }

        @Override
        public long maxValue()
        {
            return Integer.MAX_VALUE;
        }
    }

    public static class IntKBThreshold extends Threshold implements GuardrailsConfig.IntKBThreshold
    {
        public volatile int warn_threshold_in_kb = (int) DISABLED;
        public volatile int abort_threshold_in_kb = (int) DISABLED;

        @Override
        public int getWarnThresholdInKB()
        {
            return warn_threshold_in_kb;
        }

        @Override
        public int getAbortThresholdInKB()
        {
            return abort_threshold_in_kb;
        }

        public void setThresholdsInKB(int warn, int abort)
        {
            validate(warn, abort);
            updatePropertyWithLogging(name + ".warn_threshold_in_kb", warn, () -> warn_threshold_in_kb, x -> warn_threshold_in_kb = x);
            updatePropertyWithLogging(name + ".abort_threshold_in_kb", abort, () -> abort_threshold_in_kb, x -> abort_threshold_in_kb = x);
        }

        @Override
        protected void validate()
        {
            validate(warn_threshold_in_kb, abort_threshold_in_kb);
        }

        @Override
        public long maxValue()
        {
            return Integer.MAX_VALUE;
        }
    }

    public static class LongKBThreshold extends Threshold implements GuardrailsConfig.LongKBThreshold
    {
        public volatile long warn_threshold_in_kb = DISABLED;
        public volatile long abort_threshold_in_kb = DISABLED;

        @Override
        public long getWarnThresholdInKB()
        {
            return warn_threshold_in_kb;
        }

        @Override
        public long getAbortThresholdInKB()
        {
            return abort_threshold_in_kb;
        }

        public void setThresholdsInKB(long warn, long abort)
        {
            validate(warn, abort);
            updatePropertyWithLogging(name + ".warn_threshold_in_kb", warn, () -> warn_threshold_in_kb, x -> warn_threshold_in_kb = x);
            updatePropertyWithLogging(name + ".abort_threshold_in_kb", abort, () -> abort_threshold_in_kb, x -> abort_threshold_in_kb = x);
        }

        @Override
        protected void validate()
        {
            validate(warn_threshold_in_kb, abort_threshold_in_kb);
        }

        @Override
        public long maxValue()
        {
            return Long.MAX_VALUE;
        }
    }

    public static class TableProperties extends Config implements GuardrailsConfig.TableProperties
    {
        public volatile Set<String> ignored = Collections.emptySet();
        public volatile Set<String> disallowed = Collections.emptySet();

        @Override
        public Set<String> getIgnored()
        {
            return ignored;
        }

        @Override
        public Set<String> getDisallowed()
        {
            return disallowed;
        }

        public void setIgnored(Set<String> properties)
        {
            updatePropertyWithLogging(name + ".ignored", validateIgnored(properties), () -> ignored, x -> ignored = x);
        }

        public void setDisallowed(Set<String> properties)
        {
            updatePropertyWithLogging(name + ".disallowed", validateDisallowed(properties), () -> disallowed, x -> disallowed = x);
        }

        @Override
        protected void validate()
        {
            validateIgnored(ignored);
            validateDisallowed(disallowed);
        }

        private Set<String> validateIgnored(Set<String> properties)
        {
            return validateTableProperties(properties, name + ".ignored");
        }

        private Set<String> validateDisallowed(Set<String> properties)
        {
            return validateTableProperties(properties, name + ".disallowed");
        }

        private Set<String> validateTableProperties(Set<String> properties, String name)
        {
            if (properties == null)
                throw new IllegalArgumentException(format("Invalid value for %s: null is not allowed", name));

            Set<String> lowerCaseProperties = properties.stream().map(String::toLowerCase).collect(toSet());

            Set<String> diff = Sets.difference(lowerCaseProperties, TableAttributes.allKeywords());

            if (!diff.isEmpty())
                throw new IllegalArgumentException(format("Invalid value for %s: '%s' do not parse as valid table properties",
                                                          name, diff));

            return lowerCaseProperties;
        }
    }
}
