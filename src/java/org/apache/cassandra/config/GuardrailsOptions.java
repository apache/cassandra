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
import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.GuardrailsConfig;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.disk.usage.DiskUsageMonitor;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

/**
 * Configuration settings for guardrails populated from the Yaml file.
 *
 * <p>Note that the settings here must only be used to build the {@link GuardrailsConfig} class and not directly by the
 * code checking each guarded constraint. That code should use the higher level abstractions defined in
 * {@link Guardrails}).
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
        validateMaxIntThreshold(config.keyspaces_warn_threshold, config.keyspaces_fail_threshold, "keyspaces");
        validateMaxIntThreshold(config.tables_warn_threshold, config.tables_fail_threshold, "tables");
        validateMaxIntThreshold(config.columns_per_table_warn_threshold, config.columns_per_table_fail_threshold, "columns_per_table");
        validateMaxIntThreshold(config.secondary_indexes_per_table_warn_threshold, config.secondary_indexes_per_table_fail_threshold, "secondary_indexes_per_table");
        validateMaxIntThreshold(config.materialized_views_per_table_warn_threshold, config.materialized_views_per_table_fail_threshold, "materialized_views_per_table");
        config.table_properties_warned = validateTableProperties(config.table_properties_warned, "table_properties_warned");
        config.table_properties_ignored = validateTableProperties(config.table_properties_ignored, "table_properties_ignored");
        config.table_properties_disallowed = validateTableProperties(config.table_properties_disallowed, "table_properties_disallowed");
        validateMaxIntThreshold(config.page_size_warn_threshold, config.page_size_fail_threshold, "page_size");
        validateMaxIntThreshold(config.partition_keys_in_select_warn_threshold, config.partition_keys_in_select_fail_threshold, "partition_keys_in_select");
        validateMaxIntThreshold(config.in_select_cartesian_product_warn_threshold, config.in_select_cartesian_product_fail_threshold, "in_select_cartesian_product");
        config.read_consistency_levels_warned = validateConsistencyLevels(config.read_consistency_levels_warned, "read_consistency_levels_warned");
        config.read_consistency_levels_disallowed = validateConsistencyLevels(config.read_consistency_levels_disallowed, "read_consistency_levels_disallowed");
        config.write_consistency_levels_warned = validateConsistencyLevels(config.write_consistency_levels_warned, "write_consistency_levels_warned");
        config.write_consistency_levels_disallowed = validateConsistencyLevels(config.write_consistency_levels_disallowed, "write_consistency_levels_disallowed");
        validateSizeThreshold(config.partition_size_warn_threshold, config.partition_size_fail_threshold, false, "partition_size");
        validateMaxLongThreshold(config.partition_tombstones_warn_threshold, config.partition_tombstones_fail_threshold, "partition_tombstones", false);
        validateSizeThreshold(config.column_value_size_warn_threshold, config.column_value_size_fail_threshold, false, "column_value_size");
        validateSizeThreshold(config.collection_size_warn_threshold, config.collection_size_fail_threshold, false, "collection_size");
        validateMaxIntThreshold(config.items_per_collection_warn_threshold, config.items_per_collection_fail_threshold, "items_per_collection");
        validateMaxIntThreshold(config.fields_per_udt_warn_threshold, config.fields_per_udt_fail_threshold, "fields_per_udt");
        validateMaxIntThreshold(config.vector_dimensions_warn_threshold, config.vector_dimensions_fail_threshold, "vector_dimensions");
        validatePercentageThreshold(config.data_disk_usage_percentage_warn_threshold, config.data_disk_usage_percentage_fail_threshold, "data_disk_usage_percentage");
        validateDataDiskUsageMaxDiskSize(config.data_disk_usage_max_disk_size);
        validateMinRFThreshold(config.minimum_replication_factor_warn_threshold, config.minimum_replication_factor_fail_threshold);
        validateMaxRFThreshold(config.maximum_replication_factor_warn_threshold, config.maximum_replication_factor_fail_threshold);
        validateTimestampThreshold(config.maximum_timestamp_warn_threshold, config.maximum_timestamp_fail_threshold, "maximum_timestamp");
        validateTimestampThreshold(config.minimum_timestamp_warn_threshold, config.minimum_timestamp_fail_threshold, "minimum_timestamp");
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
        validateMaxIntThreshold(warn, fail, "keyspaces");
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
        validateMaxIntThreshold(warn, fail, "tables");
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
        validateMaxIntThreshold(warn, fail, "columns_per_table");
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
        validateMaxIntThreshold(warn, fail, "secondary_indexes_per_table");
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
        validateMaxIntThreshold(warn, fail, "partition_keys_in_select");
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
        validateMaxIntThreshold(warn, fail, "materialized_views_per_table");
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
        validateMaxIntThreshold(warn, fail, "page_size");
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
    public boolean getGroupByEnabled()
    {
        return config.group_by_enabled;
    }

    public void setGroupByEnabled(boolean enabled)
    {
        updatePropertyWithLogging("group_by_enabled",
                                  enabled,
                                  () -> config.group_by_enabled,
                                  x -> config.group_by_enabled = x);
    }

    @Override
    public boolean getDropTruncateTableEnabled()
    {
        return config.drop_truncate_table_enabled;
    }

    public void setDropTruncateTableEnabled(boolean enabled)
    {
        updatePropertyWithLogging("drop_truncate_table_enabled",
                                  enabled,
                                  () -> config.drop_truncate_table_enabled,
                                  x -> config.drop_truncate_table_enabled = x);
    }

    @Override
    public boolean getDropKeyspaceEnabled()
    {
        return config.drop_keyspace_enabled;
    }

    public void setDropKeyspaceEnabled(boolean enabled)
    {
        updatePropertyWithLogging("drop_keyspace_enabled",
                                  enabled,
                                  () -> config.drop_keyspace_enabled,
                                  x -> config.drop_keyspace_enabled = x);
    }

    @Override
    public boolean getSecondaryIndexesEnabled()
    {
        return config.secondary_indexes_enabled;
    }

    public void setSecondaryIndexesEnabled(boolean enabled)
    {
        updatePropertyWithLogging("secondary_indexes_enabled",
                                  enabled,
                                  () -> config.secondary_indexes_enabled,
                                  x -> config.secondary_indexes_enabled = x);
    }

    @Override
    public boolean getUncompressedTablesEnabled()
    {
        return config.uncompressed_tables_enabled;
    }

    public void setUncompressedTablesEnabled(boolean enabled)
    {
        updatePropertyWithLogging("uncompressed_tables_enabled",
                                  enabled,
                                  () -> config.uncompressed_tables_enabled,
                                  x -> config.uncompressed_tables_enabled = x);
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
    public boolean getAlterTableEnabled()
    {
        return config.alter_table_enabled;
    }

    public void setAlterTableEnabled(boolean enabled)
    {
        updatePropertyWithLogging("alter_table_enabled",
                                  enabled,
                                  () -> config.alter_table_enabled,
                                  x -> config.alter_table_enabled = x);
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
    public boolean getAllowFilteringEnabled()
    {
        return config.allow_filtering_enabled;
    }

    public void setAllowFilteringEnabled(boolean enabled)
    {
        updatePropertyWithLogging("allow_filtering_enabled",
                                  enabled,
                                  () -> config.allow_filtering_enabled,
                                  x -> config.allow_filtering_enabled = x);
    }

    @Override
    public boolean getSimpleStrategyEnabled()
    {
        return config.simplestrategy_enabled;
    }

    public void setSimpleStrategyEnabled(boolean enabled)
    {
        updatePropertyWithLogging("simplestrategy_enabled",
                                  enabled,
                                  () -> config.simplestrategy_enabled,
                                  x -> config.simplestrategy_enabled = x);
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
        validateMaxIntThreshold(warn, fail, "in_select_cartesian_product");
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

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getPartitionSizeWarnThreshold()
    {
        return config.partition_size_warn_threshold;
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getPartitionSizeFailThreshold()
    {
        return config.partition_size_fail_threshold;
    }

    public void setPartitionSizeThreshold(@Nullable DataStorageSpec.LongBytesBound warn, @Nullable DataStorageSpec.LongBytesBound fail)
    {
        validateSizeThreshold(warn, fail, false, "partition_size");
        updatePropertyWithLogging("partition_size_warn_threshold",
                                  warn,
                                  () -> config.partition_size_warn_threshold,
                                  x -> config.partition_size_warn_threshold = x);
        updatePropertyWithLogging("partition_size_fail_threshold",
                                  fail,
                                  () -> config.partition_size_fail_threshold,
                                  x -> config.partition_size_fail_threshold = x);
    }

    @Override
    public long getPartitionTombstonesWarnThreshold()
    {
        return config.partition_tombstones_warn_threshold;
    }

    @Override
    public long getPartitionTombstonesFailThreshold()
    {
        return config.partition_tombstones_fail_threshold;
    }

    public void setPartitionTombstonesThreshold(long warn, long fail)
    {
        validateMaxLongThreshold(warn, fail, "partition_tombstones", false);
        updatePropertyWithLogging("partition_tombstones_warn_threshold",
                                  warn,
                                  () -> config.partition_tombstones_warn_threshold,
                                  x -> config.partition_tombstones_warn_threshold = x);
        updatePropertyWithLogging("partition_tombstones_fail_threshold",
                                  fail,
                                  () -> config.partition_tombstones_fail_threshold,
                                  x -> config.partition_tombstones_fail_threshold = x);
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getColumnValueSizeWarnThreshold()
    {
        return config.column_value_size_warn_threshold;
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getColumnValueSizeFailThreshold()
    {
        return config.column_value_size_fail_threshold;
    }

    public void setColumnValueSizeThreshold(@Nullable DataStorageSpec.LongBytesBound warn, @Nullable DataStorageSpec.LongBytesBound fail)
    {
        validateSizeThreshold(warn, fail, false, "column_value_size");
        updatePropertyWithLogging("column_value_size_warn_threshold",
                                  warn,
                                  () -> config.column_value_size_warn_threshold,
                                  x -> config.column_value_size_warn_threshold = x);
        updatePropertyWithLogging("column_value_size_fail_threshold",
                                  fail,
                                  () -> config.column_value_size_fail_threshold,
                                  x -> config.column_value_size_fail_threshold = x);
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getCollectionSizeWarnThreshold()
    {
        return config.collection_size_warn_threshold;
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getCollectionSizeFailThreshold()
    {
        return config.collection_size_fail_threshold;
    }

    public void setCollectionSizeThreshold(@Nullable DataStorageSpec.LongBytesBound warn, @Nullable DataStorageSpec.LongBytesBound fail)
    {
        validateSizeThreshold(warn, fail, false, "collection_size");
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
        validateMaxIntThreshold(warn, fail, "items_per_collection");
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
        validateMaxIntThreshold(warn, fail, "fields_per_udt");
        updatePropertyWithLogging("fields_per_udt_warn_threshold",
                                  warn,
                                  () -> config.fields_per_udt_warn_threshold,
                                  x -> config.fields_per_udt_warn_threshold = x);
        updatePropertyWithLogging("fields_per_udt_fail_threshold",
                                  fail,
                                  () -> config.fields_per_udt_fail_threshold,
                                  x -> config.fields_per_udt_fail_threshold = x);
    }

    @Override
    public int getVectorDimensionsWarnThreshold()
    {
        return config.vector_dimensions_warn_threshold;
    }

    @Override
    public int getVectorDimensionsFailThreshold()
    {
        return config.vector_dimensions_fail_threshold;
    }

    public void setVectorDimensionsThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "vector_dimensions");
        updatePropertyWithLogging("vector_dimensions_warn_threshold",
                                  warn,
                                  () -> config.vector_dimensions_warn_threshold,
                                  x -> config.vector_dimensions_warn_threshold = x);
        updatePropertyWithLogging("vector_dimensions_fail_threshold",
                                  fail,
                                  () -> config.vector_dimensions_fail_threshold,
                                  x -> config.vector_dimensions_fail_threshold = x);
    }

    public int getDataDiskUsagePercentageWarnThreshold()
    {
        return config.data_disk_usage_percentage_warn_threshold;
    }

    @Override
    public int getDataDiskUsagePercentageFailThreshold()
    {
        return config.data_disk_usage_percentage_fail_threshold;
    }

    public void setDataDiskUsagePercentageThreshold(int warn, int fail)
    {
        validatePercentageThreshold(warn, fail, "data_disk_usage_percentage");
        updatePropertyWithLogging("data_disk_usage_percentage_warn_threshold",
                                  warn,
                                  () -> config.data_disk_usage_percentage_warn_threshold,
                                  x -> config.data_disk_usage_percentage_warn_threshold = x);
        updatePropertyWithLogging("data_disk_usage_percentage_fail_threshold",
                                  fail,
                                  () -> config.data_disk_usage_percentage_fail_threshold,
                                  x -> config.data_disk_usage_percentage_fail_threshold = x);
    }

    @Override
    public DataStorageSpec.LongBytesBound getDataDiskUsageMaxDiskSize()
    {
        return config.data_disk_usage_max_disk_size;
    }

    public void setDataDiskUsageMaxDiskSize(@Nullable DataStorageSpec.LongBytesBound diskSize)
    {
        validateDataDiskUsageMaxDiskSize(diskSize);
        updatePropertyWithLogging("data_disk_usage_max_disk_size",
                                  diskSize,
                                  () -> config.data_disk_usage_max_disk_size,
                                  x -> config.data_disk_usage_max_disk_size = x);
    }

    @Override
    public int getMinimumReplicationFactorWarnThreshold()
    {
        return config.minimum_replication_factor_warn_threshold;
    }

    @Override
    public int getMinimumReplicationFactorFailThreshold()
    {
        return config.minimum_replication_factor_fail_threshold;
    }

    public void setMinimumReplicationFactorThreshold(int warn, int fail)
    {
        validateMinRFThreshold(warn, fail);
        updatePropertyWithLogging("minimum_replication_factor_warn_threshold",
                                  warn,
                                  () -> config.minimum_replication_factor_warn_threshold,
                                  x -> config.minimum_replication_factor_warn_threshold = x);
        updatePropertyWithLogging("minimum_replication_factor_fail_threshold",
                                  fail,
                                  () -> config.minimum_replication_factor_fail_threshold,
                                  x -> config.minimum_replication_factor_fail_threshold = x);
    }

    @Override
    public int getMaximumReplicationFactorWarnThreshold()
    {
        return config.maximum_replication_factor_warn_threshold;
    }

    @Override
    public int getMaximumReplicationFactorFailThreshold()
    {
        return config.maximum_replication_factor_fail_threshold;
    }

    public void setMaximumReplicationFactorThreshold(int warn, int fail)
    {
        validateMaxRFThreshold(warn, fail);
        updatePropertyWithLogging("maximum_replication_factor_warn_threshold",
                                  warn,
                                  () -> config.maximum_replication_factor_warn_threshold,
                                  x -> config.maximum_replication_factor_warn_threshold = x);
        updatePropertyWithLogging("maximum_replication_factor_fail_threshold",
                                  fail,
                                  () -> config.maximum_replication_factor_fail_threshold,
                                  x -> config.maximum_replication_factor_fail_threshold = x);
    }

    @Override
    public boolean getZeroTTLOnTWCSWarned()
    {
        return config.zero_ttl_on_twcs_warned;
    }

    @Override
    public void setZeroTTLOnTWCSWarned(boolean value)
    {
        updatePropertyWithLogging("zero_ttl_on_twcs_warned",
                                  value,
                                  () -> config.zero_ttl_on_twcs_warned,
                                  x -> config.zero_ttl_on_twcs_warned = x);
    }

    @Override
    public boolean getZeroTTLOnTWCSEnabled()
    {
        return config.zero_ttl_on_twcs_enabled;
    }

    @Override
    public void setZeroTTLOnTWCSEnabled(boolean value)
    {
        updatePropertyWithLogging("zero_ttl_on_twcs_enabled",
                                  value,
                                  () -> config.zero_ttl_on_twcs_enabled,
                                  x -> config.zero_ttl_on_twcs_enabled = x);
    }

    @Override
    public  DurationSpec.LongMicrosecondsBound getMaximumTimestampWarnThreshold()
    {
        return config.maximum_timestamp_warn_threshold;
    }

    @Override
    public DurationSpec.LongMicrosecondsBound getMaximumTimestampFailThreshold()
    {
        return config.maximum_timestamp_fail_threshold;
    }

    @Override
    public void setMaximumTimestampThreshold(@Nullable DurationSpec.LongMicrosecondsBound warn,
                                             @Nullable DurationSpec.LongMicrosecondsBound fail)
    {
        validateTimestampThreshold(warn, fail, "maximum_timestamp");

        updatePropertyWithLogging("maximum_timestamp_warn_threshold",
                                  warn,
                                  () -> config.maximum_timestamp_warn_threshold,
                                  x -> config.maximum_timestamp_warn_threshold = x);

        updatePropertyWithLogging("maximum_timestamp_fail_threshold",
                                  fail,
                                  () -> config.maximum_timestamp_fail_threshold,
                                  x -> config.maximum_timestamp_fail_threshold = x);
    }

    @Override
    public  DurationSpec.LongMicrosecondsBound getMinimumTimestampWarnThreshold()
    {
        return config.minimum_timestamp_warn_threshold;
    }

    @Override
    public DurationSpec.LongMicrosecondsBound getMinimumTimestampFailThreshold()
    {
        return config.minimum_timestamp_fail_threshold;
    }

    @Override
    public void setMinimumTimestampThreshold(@Nullable DurationSpec.LongMicrosecondsBound warn,
                                             @Nullable DurationSpec.LongMicrosecondsBound fail)
    {
        validateTimestampThreshold(warn, fail, "minimum_timestamp");

        updatePropertyWithLogging("minimum_timestamp_warn_threshold",
                                  warn,
                                  () -> config.minimum_timestamp_warn_threshold,
                                  x -> config.minimum_timestamp_warn_threshold = x);

        updatePropertyWithLogging("minimum_timestamp_fail_threshold",
                                  fail,
                                  () -> config.minimum_timestamp_fail_threshold,
                                  x -> config.minimum_timestamp_fail_threshold = x);
    }

    private static <T> void updatePropertyWithLogging(String propertyName, T newValue, Supplier<T> getter, Consumer<T> setter)
    {
        T oldValue = getter.get();
        if (newValue == null || !newValue.equals(oldValue))
        {
            setter.accept(newValue);
            logger.info("Updated {} from {} to {}", propertyName, oldValue, newValue);
        }
    }

    private static void validatePositiveNumeric(long value, long maxValue, String name)
    {
        validatePositiveNumeric(value, maxValue, name, false);
    }

    private static void validatePositiveNumeric(long value, long maxValue, String name, boolean allowZero)
    {
        if (value == -1)
            return;

        if (value > maxValue)
            throw new IllegalArgumentException(format("Invalid value %d for %s: maximum allowed value is %d",
                                                      value, name, maxValue));

        if (!allowZero && value == 0)
            throw new IllegalArgumentException(format("Invalid value for %s: 0 is not allowed; " +
                                                      "if attempting to disable use -1", name));

        // We allow -1 as a general "disabling" flag. But reject anything lower to avoid mistakes.
        if (value < 0)
            throw new IllegalArgumentException(format("Invalid value %d for %s: negative values are not allowed, " +
                                                      "outside of -1 which disables the guardrail", value, name));
    }

    private static void validatePercentage(long value, String name)
    {
        validatePositiveNumeric(value, 100, name);
    }

    private static void validatePercentageThreshold(int warn, int fail, String name)
    {
        validatePercentage(warn, name + "_warn_threshold");
        validatePercentage(fail, name + "_fail_threshold");
        validateWarnLowerThanFail(warn, fail, name);
    }

    private static void validateMaxIntThreshold(int warn, int fail, String name)
    {
        validatePositiveNumeric(warn, Integer.MAX_VALUE, name + "_warn_threshold");
        validatePositiveNumeric(fail, Integer.MAX_VALUE, name + "_fail_threshold");
        validateWarnLowerThanFail(warn, fail, name);
    }

    private static void validateMaxLongThreshold(long warn, long fail, String name, boolean allowZero)
    {
        validatePositiveNumeric(warn, Long.MAX_VALUE, name + "_warn_threshold", allowZero);
        validatePositiveNumeric(fail, Long.MAX_VALUE, name + "_fail_threshold", allowZero);
        validateWarnLowerThanFail(warn, fail, name);
    }

    private static void validateMinIntThreshold(int warn, int fail, String name)
    {
        validatePositiveNumeric(warn, Integer.MAX_VALUE, name + "_warn_threshold");
        validatePositiveNumeric(fail, Integer.MAX_VALUE, name + "_fail_threshold");
        validateWarnGreaterThanFail(warn, fail, name);
    }

    private static void validateMinRFThreshold(int warn, int fail)
    {
        validateMinIntThreshold(warn, fail, "minimum_replication_factor");

        if (fail > DatabaseDescriptor.getDefaultKeyspaceRF())
            throw new IllegalArgumentException(format("minimum_replication_factor_fail_threshold to be set (%d) " +
                                                      "cannot be greater than default_keyspace_rf (%d)",
                                                      fail, DatabaseDescriptor.getDefaultKeyspaceRF()));
    }

    private static void validateMaxRFThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "maximum_replication_factor");

        if (fail != -1 && fail < DatabaseDescriptor.getDefaultKeyspaceRF())
            throw new IllegalArgumentException(format("maximum_replication_factor_fail_threshold to be set (%d) " +
                                                      "cannot be lesser than default_keyspace_rf (%d)",
                                                      fail, DatabaseDescriptor.getDefaultKeyspaceRF()));
    }

    public static void validateTimestampThreshold(DurationSpec.LongMicrosecondsBound warn,
                                                  DurationSpec.LongMicrosecondsBound fail,
                                                  String name)
    {
        // this function is used for both upper and lower thresholds because lower threshold is relative
        // despite using MinThreshold we still want the warn threshold to be less than or equal to
        // the fail threshold.
        validateMaxLongThreshold(warn == null ? -1 : warn.toMicroseconds(),
                                 fail == null ? -1 : fail.toMicroseconds(),
                                 name, true);
    }

    private static void validateWarnLowerThanFail(long warn, long fail, String name)
    {
        if (warn == -1 || fail == -1)
            return;

        if (fail < warn)
            throw new IllegalArgumentException(format("The warn threshold %d for %s_warn_threshold should be lower " +
                                                      "than the fail threshold %d", warn, name, fail));
    }

    private static void validateWarnGreaterThanFail(long warn, long fail, String name)
    {
        if (warn == -1 || fail == -1)
            return;

        if (fail > warn)
            throw new IllegalArgumentException(format("The warn threshold %d for %s_warn_threshold should be greater " +
                                                      "than the fail threshold %d", warn, name, fail));
    }

    private static void validateSize(DataStorageSpec.LongBytesBound size, boolean allowZero, String name)
    {
        if (size == null)
            return;

        if (!allowZero && size.toBytes() == 0)
            throw new IllegalArgumentException(format("Invalid value for %s: 0 is not allowed; " +
                                                      "if attempting to disable use an empty value",
                                                      name));
    }

    private static void validateSizeThreshold(DataStorageSpec.LongBytesBound warn, DataStorageSpec.LongBytesBound fail, boolean allowZero, String name)
    {
        validateSize(warn, allowZero, name + "_warn_threshold");
        validateSize(fail, allowZero, name + "_fail_threshold");
        validateWarnLowerThanFail(warn, fail, name);
    }

    private static void validateWarnLowerThanFail(DataStorageSpec.LongBytesBound warn, DataStorageSpec.LongBytesBound fail, String name)
    {
        if (warn == null || fail == null)
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

    private static void validateDataDiskUsageMaxDiskSize(DataStorageSpec.LongBytesBound maxDiskSize)
    {
        if (maxDiskSize == null)
            return;

        validateSize(maxDiskSize, false, "data_disk_usage_max_disk_size");

        long diskSize = DiskUsageMonitor.totalDiskSpace();

        if (diskSize < maxDiskSize.toBytes())
            throw new IllegalArgumentException(format("Invalid value for data_disk_usage_max_disk_size: " +
                                                      "%s specified, but only %s are actually available on disk",
                                                      maxDiskSize, FileUtils.stringifyFileSize(diskSize)));
    }
}
