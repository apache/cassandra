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
import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.GuardrailsConfig;
import org.apache.cassandra.db.guardrails.Values;

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
    public volatile boolean enabled = false;
    public final IntThreshold tables = new IntThreshold();
    public final IntThreshold columns_per_table = new IntThreshold();
    public final IntThreshold secondary_indexes_per_table = new IntThreshold();
    public final IntThreshold materialized_views_per_table = new IntThreshold();
    public final TableProperties table_properties = new TableProperties();
    public final IntThreshold page_size = new IntThreshold();

    public volatile boolean user_timestamps_enabled = true;
    public volatile boolean read_before_write_list_operations_enabled = true;

    public void validate()
    {
        tables.validate("guardrails.tables");
        columns_per_table.validate("guardrails.columns_per_table");
        secondary_indexes_per_table.validate("guardrails.secondary_indexes_per_table");
        materialized_views_per_table.validate("guardrails.materialized_views_per_table");
        table_properties.validate("guardrails.table_properties");
        page_size.validate("guardrails.page_size");
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
        this.enabled = enabled;
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

    public void setUserTimestampsEnabled(boolean enabled)
    {
        user_timestamps_enabled = enabled;
    }

    @Override
    public boolean getReadBeforeWriteListOperationsEnabled()
    {
        return read_before_write_list_operations_enabled;
    }

    public void setReadBeforeWriteListOperationsEnabled(boolean enabled)
    {
        read_before_write_list_operations_enabled = enabled;
    }

    public static abstract class Threshold implements org.apache.cassandra.db.guardrails.Threshold.Config
    {
        public static final long DISABLED = -1;

        public volatile long warn_threshold = DISABLED;
        public volatile long abort_threshold = DISABLED;

        @Override
        public long getWarnThreshold()
        {
            return warn_threshold;
        }

        @Override
        public long getAbortThreshold()
        {
            return abort_threshold;
        }

        public void setThresholds(long warn, long abort)
        {
            validateStrictlyPositive(warn, "warn threshold");
            validateStrictlyPositive(abort, "abort threshold");
            validateWarnLowerThanAbort(warn, abort, null);
            warn_threshold = warn;
            abort_threshold = abort;
        }

        public void validate(String name)
        {
            validateStrictlyPositive(warn_threshold, name + ".warn_threshold");
            validateStrictlyPositive(abort_threshold, name + ".abort_threshold");
            validateWarnLowerThanAbort(warn_threshold, abort_threshold, name);
        }

        public abstract long maxValue();
        public abstract boolean allowZero();

        private void validateStrictlyPositive(long value, String name)
        {
            if (value > maxValue())
                throw new IllegalArgumentException(format("Invalid value %d for %s: maximum allowed value is %d",
                                                          value, name, maxValue()));

            if (value == 0 && !allowZero())
                throw new IllegalArgumentException(format("Invalid value for %s: 0 is not allowed; " +
                                                          "if attempting to disable use %d",
                                                          name, DISABLED));

            // We allow -1 as a general "disabling" flag. But reject anything lower to avoid mistakes.
            if (value < DISABLED)
                throw new IllegalArgumentException(format("Invalid value %d for %s: negative values are not allowed, " +
                                                          "outside of %d which disables the guardrail",
                                                          value, name, DISABLED));
        }

        private void validateWarnLowerThanAbort(long warnValue, long abortValue, @Nullable String name)
        {
            if (warnValue == DISABLED || abortValue == DISABLED)
                return;

            if (abortValue < warnValue)
                throw new IllegalArgumentException(format("The warn threshold %d%s should be lower than the abort " +
                                                          "threshold %d",
                                                          warnValue,
                                                          name == null ? "" : " for " + name,
                                                          abortValue));
        }
    }

    public static class IntThreshold extends Threshold
    {
        @Override
        public long maxValue()
        {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean allowZero()
        {
            return false;
        }
    }

    public static class TableProperties implements Values.Config<String>
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

        public void setIgnoredValues(Set<String> values)
        {
            this.ignored = validateTableProperties(values, "ignored properties");
        }

        public void setDisallowedValues(Set<String> values)
        {
            this.disallowed = validateTableProperties(values, "disallowed properties");
        }

        private Set<String> validateTableProperties(Set<String> properties, String name)
        {
            if (properties == null)
                throw new IllegalArgumentException(format("Invalid value for %s: null is not allowed", name));

            Set<String> lowerCaseProperties = properties.stream().map(String::toLowerCase).collect(toSet());

            Set<String> diff = Sets.difference(lowerCaseProperties, TableAttributes.allKeywords());

            if (!diff.isEmpty())
                throw new IllegalArgumentException(format("Invalid value for %s: '%s' do not parse as valid table properties",
                                                          name, diff.toString()));

            return lowerCaseProperties;
        }

        public void validate(String name)
        {
            validateTableProperties(ignored, name + ".ignored");
            validateTableProperties(disallowed, name + ".disallowed");
        }
    }
}
