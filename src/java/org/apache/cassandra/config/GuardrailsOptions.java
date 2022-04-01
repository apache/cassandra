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
import org.apache.cassandra.db.ConsistencyLevel;
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
 * <p>We have 2 variants of guardrails, soft (warn) and hard (fail) limits, each guardrail having either one of the
 * variants or both. Note in particular that hard limits only make sense for guardrails triggering during query
 * execution. For other guardrails, say one triggering during compaction, aborting that compaction does not make sense.
 *
 * <p>Additionally, each individual setting should have a specific value (typically -1 for numeric settings),
 * that allows to disable the corresponding guardrail.
 */
public class GuardrailsOptions implements GuardrailsConfig
{
    private static final Logger logger = LoggerFactory.getLogger(GuardrailsOptions.class);

    private final Config config;

    public GuardrailsOptions(Config config)
    {
        this.config = config;
        validateIntThreshold(config.keyspaces_warn_threshold, config.keyspaces_fail_threshold, "keyspaces");
        validateIntThreshold(config.tables_warn_threshold, config.tables_fail_threshold, "tables");
        validateIntThreshold(config.columns_per_table_warn_threshold, config.columns_per_table_fail_threshold, "columns_per_table");
        validateIntThreshold(config.secondary_indexes_per_table_warn_threshold, config.secondary_indexes_per_table_fail_threshold, "secondary_indexes_per_table");
        validateIntThreshold(config.materialized_views_per_table_warn_threshold, config.materialized_views_per_table_fail_threshold, "materialized_views_per_table");
        config.table_properties_warned = validateTableProperties(config.table_properties_warned, "table_properties_warned");
        config.table_properties_ignored = validateTableProperties(config.table_properties_ignored, "table_properties_ignored");
        config.table_properties_disallowed = validateTableProperties(config.table_properties_disallowed, "table_properties_disallowed");
        validateIntThreshold(config.page_size_warn_threshold, config.page_size_fail_threshold, "page_size");
        validateIntThreshold(config.partition_keys_in_select_warn_threshold,
                             config.partition_keys_in_select_fail_threshold, "partition_keys_in_select");
        validateIntThreshold(config.in_select_cartesian_product_warn_threshold, config.in_select_cartesian_product_fail_threshold, "in_select_cartesian_product");
        config.read_consistency_levels_warned = validateConsistencyLevels(config.read_consistency_levels_warned, "read_consistency_levels_warned");
        config.read_consistency_levels_disallowed = validateConsistencyLevels(config.read_consistency_levels_disallowed, "read_consistency_levels_disallowed");
        config.write_consistency_levels_warned = validateConsistencyLevels(config.write_consistency_levels_warned, "write_consistency_levels_warned");
        config.write_consistency_levels_disallowed = validateConsistencyLevels(config.write_consistency_levels_disallowed, "write_consistency_levels_disallowed");
        validateSizeThreshold(config.collection_size_warn_threshold, config.collection_size_fail_threshold, "collection_size");
        validateIntThreshold(config.items_per_collection_warn_threshold, config.items_per_collection_fail_threshold, "items_per_collection");
        validateIntThreshold(config.fields_per_udt_warn_threshold, config.fields_per_udt_fail_threshold, "fields_per_udt");
    }

    @Override
    public boolean getEnabled()
    {
        return config.guardrails_enabled;
    }

    /**
     * Enable/disable guardrails.
     *
     * @param enabled {@code true} for enabling guardrails, {@code false} for disabling them.
     */
    public void setEnabled(boolean enabled)
    {
        updatePropertyWithLogging("guardrails_enabled",
                                  enabled,
                                  () -> config.guardrails_enabled,
                                  x -> config.guardrails_enabled = x);
    }

    @Override
    public int getKeyspacesWarnThreshold()
    {
        return config.keyspaces_warn_threshold;
    }

    @Override
    public int getKeyspacesFailThreshold()
    {
        return config.keyspaces_fail_threshold;
    }

    public void setKeyspacesThreshold(int warn, int fail)
    {
        validateIntThreshold(warn, fail, "keyspaces");
        updatePropertyWithLogging("keyspaces_warn_threshold",
                                  warn,
                                  () -> config.keyspaces_warn_threshold,
                                  x -> config.keyspaces_warn_threshold = x);
        updatePropertyWithLogging("keyspaces_fail_threshold",
                                  fail,
                                  () -> config.keyspaces_fail_threshold,
                                  x -> config.keyspaces_fail_threshold = x);
    }

    @Override
    public int getTablesWarnThreshold()
    {
        return config.tables_warn_threshold;
    }

    @Override
    public int getTablesFailThreshold()
    {
        return config.tables_fail_threshold;
    }

    public void setTablesThreshold(int warn, int fail)
    {
        validateIntThreshold(warn, fail, "tables");
        updatePropertyWithLogging("tables_warn_threshold",
                                  warn,
                                  () -> config.tables_warn_threshold,
                                  x -> config.tables_warn_threshold = x);
        updatePropertyWithLogging("tables_fail_threshold",
                                  fail,
                                  () -> config.tables_fail_threshold,
                                  x -> config.tables_fail_threshold = x);
    }

    @Override
    public int getColumnsPerTableWarnThreshold()
    {
        return config.columns_per_table_warn_threshold;
    }

    @Override
    public int getColumnsPerTableFailThreshold()
    {
        return config.columns_per_table_fail_threshold;
    }

    public void setColumnsPerTableThreshold(int warn, int fail)
    {
        validateIntThreshold(warn, fail, "columns_per_table");
        updatePropertyWithLogging("columns_per_table_warn_threshold",
                                  warn,
                                  () -> config.columns_per_table_warn_threshold,
                                  x -> config.columns_per_table_warn_threshold = x);
        updatePropertyWithLogging("columns_per_table_fail_threshold",
                                  fail,
                                  () -> config.columns_per_table_fail_threshold,
                                  x -> config.columns_per_table_fail_threshold = x);
    }

    @Override
    public int getSecondaryIndexesPerTableWarnThreshold()
    {
        return config.secondary_indexes_per_table_warn_threshold;
    }

    @Override
    public int getSecondaryIndexesPerTableFailThreshold()
    {
        return config.secondary_indexes_per_table_fail_threshold;
    }

    public void setSecondaryIndexesPerTableThreshold(int warn, int fail)
    {
        validateIntThreshold(warn, fail, "secondary_indexes_per_table");
        updatePropertyWithLogging("secondary_indexes_per_table_warn_threshold",
                                  warn,
                                  () -> config.secondary_indexes_per_table_warn_threshold,
                                  x -> config.secondary_indexes_per_table_warn_threshold = x);
        updatePropertyWithLogging("secondary_indexes_per_table_fail_threshold",
                                  fail,
                                  () -> config.secondary_indexes_per_table_fail_threshold,
                                  x -> config.secondary_indexes_per_table_fail_threshold = x);
    }

    @Override
    public int getMaterializedViewsPerTableWarnThreshold()
    {
        return config.materialized_views_per_table_warn_threshold;
    }

    @Override
    public int getPartitionKeysInSelectWarnThreshold()
    {
        return config.partition_keys_in_select_warn_threshold;
    }

    @Override
    public int getPartitionKeysInSelectFailThreshold()
    {
        return config.partition_keys_in_select_fail_threshold;
    }

    public void setPartitionKeysInSelectThreshold(int warn, int fail)
    {
        validateIntThreshold(warn, fail, "partition_keys_in_select");
        updatePropertyWithLogging("partition_keys_in_select_warn_threshold",
                                  warn,
                                  () -> config.partition_keys_in_select_warn_threshold,
                                  x -> config.partition_keys_in_select_warn_threshold = x);
        updatePropertyWithLogging("partition_keys_in_select_fail_threshold",
                                  fail,
                                  () -> config.partition_keys_in_select_fail_threshold,
                                  x -> config.partition_keys_in_select_fail_threshold = x);
    }

    @Override
    public int getMaterializedViewsPerTableFailThreshold()
    {
        return config.materialized_views_per_table_fail_threshold;
    }

    public void setMaterializedViewsPerTableThreshold(int warn, int fail)
    {
        validateIntThreshold(warn, fail, "materialized_views_per_table");
        updatePropertyWithLogging("materialized_views_per_table_warn_threshold",
                                  warn,
                                  () -> config.materialized_views_per_table_warn_threshold,
                                  x -> config.materialized_views_per_table_warn_threshold = x);
        updatePropertyWithLogging("materialized_views_per_table_fail_threshold",
                                  fail,
                                  () -> config.materialized_views_per_table_fail_threshold,
                                  x -> config.materialized_views_per_table_fail_threshold = x);
    }

    @Override
    public int getPageSizeWarnThreshold()
    {
        return config.page_size_warn_threshold;
    }

    @Override
    public int getPageSizeFailThreshold()
    {
        return config.page_size_fail_threshold;
    }

    public void setPageSizeThreshold(int warn, int fail)
    {
        validateIntThreshold(warn, fail, "page_size");
        updatePropertyWithLogging("page_size_warn_threshold",
                                  warn,
                                  () -> config.page_size_warn_threshold,
                                  x -> config.page_size_warn_threshold = x);
        updatePropertyWithLogging("page_size_fail_threshold",
                                  fail,
                                  () -> config.page_size_fail_threshold,
                                  x -> config.page_size_fail_threshold = x);
    }

    @Override
    public Set<String> getTablePropertiesWarned()
    {
        return config.table_properties_warned;
    }

    public void setTablePropertiesWarned(Set<String> properties)
    {
        updatePropertyWithLogging("table_properties_warned",
                                  validateTableProperties(properties, "table_properties_warned"),
                                  () -> config.table_properties_warned,
                                  x -> config.table_properties_warned = x);
    }

    @Override
    public Set<String> getTablePropertiesIgnored()
    {
        return config.table_properties_ignored;
    }

    public void setTablePropertiesIgnored(Set<String> properties)
    {
        updatePropertyWithLogging("table_properties_ignored",
                                  validateTableProperties(properties, "table_properties_ignored"),
                                  () -> config.table_properties_ignored,
                                  x -> config.table_properties_ignored = x);
    }

    @Override
    public Set<String> getTablePropertiesDisallowed()
    {
        return config.table_properties_disallowed;
    }

    public void setTablePropertiesDisallowed(Set<String> properties)
    {
        updatePropertyWithLogging("table_properties_disallowed",
                                  validateTableProperties(properties, "table_properties_disallowed"),
                                  () -> config.table_properties_disallowed,
                                  x -> config.table_properties_disallowed = x);
    }

    @Override
    public boolean getUserTimestampsEnabled()
    {
        return config.user_timestamps_enabled;
    }

    public void setUserTimestampsEnabled(boolean enabled)
    {
        updatePropertyWithLogging("user_timestamps_enabled",
                                  enabled,
                                  () -> config.user_timestamps_enabled,
                                  x -> config.user_timestamps_enabled = x);
    }

    @Override
    public boolean getCompactTablesEnabled()
    {
        return config.compact_tables_enabled;
    }

    public void setCompactTablesEnabled(boolean enabled)
    {
        updatePropertyWithLogging("compact_tables_enabled",
                                  enabled,
                                  () -> config.compact_tables_enabled,
                                  x -> config.compact_tables_enabled = x);
    }

    @Override
    public boolean getReadBeforeWriteListOperationsEnabled()
    {
        return config.read_before_write_list_operations_enabled;
    }

    public void setReadBeforeWriteListOperationsEnabled(boolean enabled)
    {
        updatePropertyWithLogging("read_before_write_list_operations_enabled",
                                  enabled,
                                  () -> config.read_before_write_list_operations_enabled,
                                  x -> config.read_before_write_list_operations_enabled = x);
    }

    @Override
    public int getInSelectCartesianProductWarnThreshold()
    {
        return config.in_select_cartesian_product_warn_threshold;
    }

    @Override
    public int getInSelectCartesianProductFailThreshold()
    {
        return config.in_select_cartesian_product_fail_threshold;
    }

    public void setInSelectCartesianProductThreshold(int warn, int fail)
    {
        validateIntThreshold(warn, fail, "in_select_cartesian_product");
        updatePropertyWithLogging("in_select_cartesian_product_warn_threshold",
                                  warn,
                                  () -> config.in_select_cartesian_product_warn_threshold,
                                  x -> config.in_select_cartesian_product_warn_threshold = x);
        updatePropertyWithLogging("in_select_cartesian_product_fail_threshold",
                                  fail,
                                  () -> config.in_select_cartesian_product_fail_threshold,
                                  x -> config.in_select_cartesian_product_fail_threshold = x);
    }

    public Set<ConsistencyLevel> getReadConsistencyLevelsWarned()
    {
        return config.read_consistency_levels_warned;
    }

    public void setReadConsistencyLevelsWarned(Set<ConsistencyLevel> consistencyLevels)
    {
        updatePropertyWithLogging("read_consistency_levels_warned",
                                  validateConsistencyLevels(consistencyLevels, "read_consistency_levels_warned"),
                                  () -> config.read_consistency_levels_warned,
                                  x -> config.read_consistency_levels_warned = x);
    }

    @Override
    public Set<ConsistencyLevel> getReadConsistencyLevelsDisallowed()
    {
        return config.read_consistency_levels_disallowed;
    }

    public void setReadConsistencyLevelsDisallowed(Set<ConsistencyLevel> consistencyLevels)
    {
        updatePropertyWithLogging("read_consistency_levels_disallowed",
                                  validateConsistencyLevels(consistencyLevels, "read_consistency_levels_disallowed"),
                                  () -> config.read_consistency_levels_disallowed,
                                  x -> config.read_consistency_levels_disallowed = x);
    }

    @Override
    public Set<ConsistencyLevel> getWriteConsistencyLevelsWarned()
    {
        return config.write_consistency_levels_warned;
    }

    public void setWriteConsistencyLevelsWarned(Set<ConsistencyLevel> consistencyLevels)
    {
        updatePropertyWithLogging("write_consistency_levels_warned",
                                  validateConsistencyLevels(consistencyLevels, "write_consistency_levels_warned"),
                                  () -> config.write_consistency_levels_warned,
                                  x -> config.write_consistency_levels_warned = x);
    }

    @Override
    public Set<ConsistencyLevel> getWriteConsistencyLevelsDisallowed()
    {
        return config.write_consistency_levels_disallowed;
    }

    public void setWriteConsistencyLevelsDisallowed(Set<ConsistencyLevel> consistencyLevels)
    {
        updatePropertyWithLogging("write_consistency_levels_disallowed",
                                  validateConsistencyLevels(consistencyLevels, "write_consistency_levels_disallowed"),
                                  () -> config.write_consistency_levels_disallowed,
                                  x -> config.write_consistency_levels_disallowed = x);
    }

    public DataStorageSpec getCollectionSizeWarnThreshold()
    {
        return config.collection_size_warn_threshold;
    }

    @Override
    public DataStorageSpec getCollectionSizeFailThreshold()
    {
        return config.collection_size_fail_threshold;
    }

    public void setCollectionSizeThreshold(DataStorageSpec warn, DataStorageSpec fail)
    {
        validateSizeThreshold(warn, fail, "collection_size");
        updatePropertyWithLogging("collection_size_warn_threshold",
                                  warn,
                                  () -> config.collection_size_warn_threshold,
                                  x -> config.collection_size_warn_threshold = x);
        updatePropertyWithLogging("collection_size_fail_threshold",
                                  fail,
                                  () -> config.collection_size_fail_threshold,
                                  x -> config.collection_size_fail_threshold = x);
    }

    @Override
    public int getItemsPerCollectionWarnThreshold()
    {
        return config.items_per_collection_warn_threshold;
    }

    @Override
    public int getItemsPerCollectionFailThreshold()
    {
        return config.items_per_collection_fail_threshold;
    }

    public void setItemsPerCollectionThreshold(int warn, int fail)
    {
        validateIntThreshold(warn, fail, "items_per_collection");
        updatePropertyWithLogging("items_per_collection_warn_threshold",
                                  warn,
                                  () -> config.items_per_collection_warn_threshold,
                                  x -> config.items_per_collection_warn_threshold = x);
        updatePropertyWithLogging("items_per_collection_fail_threshold",
                                  fail,
                                  () -> config.items_per_collection_fail_threshold,
                                  x -> config.items_per_collection_fail_threshold = x);
    }

    @Override
    public int getFieldsPerUDTWarnThreshold()
    {
        return config.fields_per_udt_warn_threshold;
    }

    @Override
    public int getFieldsPerUDTFailThreshold()
    {
        return config.fields_per_udt_fail_threshold;
    }

    public void setFieldsPerUDTThreshold(int warn, int fail)
    {
        validateIntThreshold(warn, fail, "fields_per_udt");
        updatePropertyWithLogging("fields_per_udt_warn_threshold",
                                  warn,
                                  () -> config.fields_per_udt_warn_threshold,
                                  x -> config.fields_per_udt_warn_threshold = x);
        updatePropertyWithLogging("fields_per_udt_fail_threshold",
                                  fail,
                                  () -> config.fields_per_udt_fail_threshold,
                                  x -> config.fields_per_udt_fail_threshold = x);
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

    private static void validatePositiveNumeric(long value, long maxValue, String name)
    {
        if (value == Config.DISABLED_GUARDRAIL)
            return;

        if (value > maxValue)
            throw new IllegalArgumentException(format("Invalid value %d for %s: maximum allowed value is %d",
                                                      value, name, maxValue));

        if (value == 0)
            throw new IllegalArgumentException(format("Invalid value for %s: 0 is not allowed; " +
                                                      "if attempting to disable use %d",
                                                      name, Config.DISABLED_GUARDRAIL));

        // We allow -1 as a general "disabling" flag. But reject anything lower to avoid mistakes.
        if (value <= 0)
            throw new IllegalArgumentException(format("Invalid value %d for %s: negative values are not allowed, " +
                                                      "outside of %d which disables the guardrail",
                                                      value, name, Config.DISABLED_GUARDRAIL));
    }

    private static void validateIntThreshold(int warn, int fail, String name)
    {
        validatePositiveNumeric(warn, Integer.MAX_VALUE, name + "_warn_threshold");
        validatePositiveNumeric(fail, Integer.MAX_VALUE, name + "_fail_threshold");
        validateWarnLowerThanFail(warn, fail, name);
    }

    private static void validateWarnLowerThanFail(long warn, long fail, String name)
    {
        if (warn == Config.DISABLED_GUARDRAIL || fail == Config.DISABLED_GUARDRAIL)
            return;

        if (fail < warn)
            throw new IllegalArgumentException(format("The warn threshold %d for %s_warn_threshold should be lower " +
                                                      "than the fail threshold %d", warn, name, fail));
    }

    private static void validateSizeThreshold(DataStorageSpec warn, DataStorageSpec fail, String name)
    {
        if (warn.equals(Config.DISABLED_SIZE_GUARDRAIL) || fail.equals(Config.DISABLED_SIZE_GUARDRAIL))
            return;

        if (fail.toBytes() < warn.toBytes())
            throw new IllegalArgumentException(format("The warn threshold %s for %s_warn_threshold should be lower " +
                                                      "than the fail threshold %s", warn, name, fail));
    }

    private static Set<String> validateTableProperties(Set<String> properties, String name)
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

    private static Set<ConsistencyLevel> validateConsistencyLevels(Set<ConsistencyLevel> consistencyLevels, String name)
    {
        if (consistencyLevels == null)
            throw new IllegalArgumentException(format("Invalid value for %s: null is not allowed", name));

        return consistencyLevels.isEmpty() ? Collections.emptySet() : Sets.immutableEnumSet(consistencyLevels);
    }
}
