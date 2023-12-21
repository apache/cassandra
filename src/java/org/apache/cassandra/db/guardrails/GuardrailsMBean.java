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

package org.apache.cassandra.db.guardrails;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * JMX entrypoint for updating the default guardrails configuration parsed from {@code cassandra.yaml}.
 * <p>
 * This is different to just exposing {@link GuardrailsConfig} in that the methods here should be JMX-friendly.
 *
 * <p>For consistency, guardrails based on a simple numeric threshold should use the naming scheme
 * {@code <whatIsGuarded>WarnThreshold} for soft limits and {@code <whatIsGuarded>FailThreshold} for hard
 * ones, and if the value has a unit, that unit should be added at the end (for instance,
 * {@code <whatIsGuarded>FailThresholdInKb}). For "boolean" guardrails that disable a feature, use
 * {@code <whatIsGuardedEnabled}. Other type of guardrails can use appropriate suffixes but should start with
 * {@code <whatIsGuarded>}.
 */
public interface GuardrailsMBean
{
    /**
     * @return The threshold to warn when creating more user keyspaces than threshold.
     * -1 means disabled.
     */
    int getKeyspacesWarnThreshold();

    /**
     * @return The threshold to prevent creating more user keyspaces than threshold.
     * -1 means disabled.
     */
    int getKeyspacesFailThreshold();

    /**
     * @param warn The threshold to warn when creating more user keyspaces than threshold. -1 means disabled.
     * @param fail The threshold to prevent creating more user keyspaces than threshold. -1 means disabled.
     */
    void setKeyspacesThreshold(int warn, int fail);

    /**
     * @return The threshold to warn when creating more tables than threshold.
     * -1 means disabled.
     */
    int getTablesWarnThreshold();

    /**
     * @return The threshold to prevent creating more tables than threshold.
     * -1 means disabled.
     */
    int getTablesFailThreshold();

    /**
     * @param warn The threshold to warn when creating more tables than threshold. -1 means disabled.
     * @param fail The threshold to prevent creating more tables than threshold. -1 means disabled.
     */
    void setTablesThreshold(int warn, int fail);

    /**
     * @return The threshold to warn when having more columns per table than threshold.
     * -1 means disabled.
     */
    int getColumnsPerTableWarnThreshold();

    /**
     * @return The threshold to prevent having more columns per table than threshold. -1 means disabled.
     */
    int getColumnsPerTableFailThreshold();

    /**
     * @param warn The threshold to warn when having more columns per table than threshold. -1 means disabled.
     * @param fail The threshold to prevent having more columns per table than threshold. -1 means disabled.
     */
    void setColumnsPerTableThreshold(int warn, int fail);

    /**
     * @return The threshold to warn when creating more secondary indexes per table than threshold. -1 means disabled.
     */
    int getSecondaryIndexesPerTableWarnThreshold();

    /**
     * @return The threshold to prevent creating more secondary indexes per table than threshold. -1 means disabled.
     */
    int getSecondaryIndexesPerTableFailThreshold();

    /**
     * @param warn The threshold to warn when creating more secondary indexes per table than threshold. -1 means disabled.
     * @param fail The threshold to prevent creating more secondary indexes per table than threshold. -1 means disabled.
     */
    void setSecondaryIndexesPerTableThreshold(int warn, int fail);

    /**
     * @return Whether secondary index creation is active or not on the node
     */
    boolean getSecondaryIndexesEnabled();

    /**
     * Enables or disables the ability to create secondary indexes
     *
     * @param enabled
     */
    void setSecondaryIndexesEnabled(boolean enabled);

    /**
     * @return The threshold to warn when creating more materialized views per table than threshold.
     * -1 means disabled.
     */
    int getMaterializedViewsPerTableWarnThreshold();

    /**
     * @return The threshold to prevent creating more materialized views per table than threshold.
     * -1 means disabled.
     */
    int getMaterializedViewsPerTableFailThreshold();

    /**
     * @param warn The threshold to warn when creating more materialized views per table than threshold. -1 means disabled.
     * @param fail The threshold to prevent creating more materialized views per table than threshold. -1 means disabled.
     */
    void setMaterializedViewsPerTableThreshold(int warn, int fail);

    /**
     * @return properties that are warned about when creating or altering a table.
     */
    Set<String> getTablePropertiesWarned();

    /**
     * @return Comma-separated list of properties that are warned about when creating or altering a table.
     */
    String getTablePropertiesWarnedCSV();

    /**
     * @param properties properties that are warned about when creating or altering a table.
     */
    void setTablePropertiesWarned(Set<String> properties);

    /**
     * @param properties Comma-separated list of properties that are warned about when creating or altering a table.
     */
    void setTablePropertiesWarnedCSV(String properties);

    /**
     * @return properties that are not allowed when creating or altering a table.
     */
    Set<String> getTablePropertiesDisallowed();

    /**
     * @return Comma-separated list of properties that are not allowed when creating or altering a table.
     */
    String getTablePropertiesDisallowedCSV();

    /**
     * @param properties properties that are not allowed when creating or altering a table.
     */
    void setTablePropertiesDisallowed(Set<String> properties);

    /**
     * @param properties Comma-separated list of properties that are not allowed when creating or altering a table.
     */
    void setTablePropertiesDisallowedCSV(String properties);

    /**
     * @return properties that are ignored when creating or altering a table.
     */
    Set<String> getTablePropertiesIgnored();

    /**
     * @return Comma-separated list of properties that are ignored when creating or altering a table.
     */
    String getTablePropertiesIgnoredCSV();

    /**
     * @param properties properties that are ignored when creating or altering a table.
     */
    void setTablePropertiesIgnored(Set<String> properties);

    /**
     * @param properties Comma-separated list of properties that are ignored when creating or altering a table.
     */
    void setTablePropertiesIgnoredCSV(String properties);

    /**
     * Returns whether user-provided timestamps are allowed.
     *
     * @return {@code true} if user-provided timestamps are allowed, {@code false} otherwise.
     */
    boolean getUserTimestampsEnabled();

    /**
     * Sets whether user-provided timestamps are allowed.
     *
     * @param enabled {@code true} if user-provided timestamps are allowed, {@code false} otherwise.
     */
    void setUserTimestampsEnabled(boolean enabled);

    /**
     * Returns whether ALLOW FILTERING property is allowed.
     *
     * @return {@code true} if ALLOW FILTERING is allowed, {@code false} otherwise.
     */
    boolean getAllowFilteringEnabled();

    /**
     * Sets whether ALLOW FILTERING is allowed.
     *
     * @param enabled {@code true} if ALLOW FILTERING is allowed, {@code false} otherwise.
     */
    void setAllowFilteringEnabled(boolean enabled);

    /**
     * Returns whether SimpleStrategy is allowed on keyspace creation or alteration
     *
     * @return {@code true} if SimpleStrategy is allowed; {@code false} otherwise
     */
    boolean getSimpleStrategyEnabled();

    /**
     * Sets whether SimpleStrategy is allowed on keyspace creation or alteration
     *
     * @param enabled {@code true} if SimpleStrategy is allowed, {@code false} otherwise.
     */
    void setSimpleStrategyEnabled(boolean enabled);

    /**
     * Returns whether users can disable compression on tables
     *
     * @return {@code true} if users can disable compression on a table, {@code false} otherwise.
     */
    boolean getUncompressedTablesEnabled();

    /**
     * Sets whether users can disable compression on tables
     *
     * @param enabled {@code true} if users can disable compression on a table, {@code false} otherwise.
     */
    void setUncompressedTablesEnabled(boolean enabled);

    /**
     * Returns whether users can create new COMPACT STORAGE tables
     *
     * @return {@code true} if allowed, {@code false} otherwise.
     */
    boolean getCompactTablesEnabled();

    /**
     * Sets whether users can create new COMPACT STORAGE tables
     *
     * @param enabled {@code true} if allowed, {@code false} otherwise.
     */
    void setCompactTablesEnabled(boolean enabled);

    /**
     * Gets whether users can use the ALTER TABLE statement to change columns
     *
     * @return {@code true} if ALTER TABLE is allowed, {@code false} otherwise.
     */
    boolean getAlterTableEnabled();

    /**
     * Sets whether users can use the ALTER TABLE statement to change columns
     *
     * @param enabled {@code true} if changing columns is allowed, {@code false} otherwise.
     */
    void setAlterTableEnabled(boolean enabled);

    /**
     * Returns whether GROUP BY queries are allowed.
     *
     * @return {@code true} if allowed, {@code false} otherwise.
     */
    boolean getGroupByEnabled();

    /**
     * Sets whether GROUP BY queries are allowed.
     *
     * @param enabled {@code true} if allowed, {@code false} otherwise.
     */
    void setGroupByEnabled(boolean enabled);

    /**
     * Returns whether users can TRUNCATE or DROP TABLE
     *
     * @return {@code true} if allowed, {@code false} otherwise.
     */
    boolean getDropTruncateTableEnabled();

    /**
     * Sets whether users can TRUNCATE or DROP TABLE
     */
    void setDropTruncateTableEnabled(boolean enabled);

    /**
     * Returns whether users can DROP a keyspace
     *
     * @return {@code true} if allowed, {@code false} otherwise.
     */
    boolean getDropKeyspaceEnabled();

    /**
     * Sets whether users can DROP a keyspace
     */
    void setDropKeyspaceEnabled(boolean enabled);

    /**
     * @return The threshold to warn when requested page size greater than threshold.
     * -1 means disabled.
     */
    int getPageSizeWarnThreshold();

    /**
     * @return The threshold to prevent requesting page with more elements than threshold.
     * -1 means disabled.
     */
    int getPageSizeFailThreshold();

    /**
     * @param warn The threshold to warn when the requested page size is greater than threshold. -1 means disabled.
     * @param fail The threshold to prevent requesting pages with more elements than threshold. -1 means disabled.
     */
    void setPageSizeThreshold(int warn, int fail);

    /**
     * Returns whether list operations that require read before write are allowed.
     *
     * @return {@code true} if list operations that require read before write are allowed, {@code false} otherwise.
     */
    boolean getReadBeforeWriteListOperationsEnabled();

    /**
     * Sets whether list operations that require read before write are allowed.
     *
     * @param enabled {@code true} if list operations that require read before write are allowed, {@code false} otherwise.
     */
    void setReadBeforeWriteListOperationsEnabled(boolean enabled);

    /**
     * @return The threshold to warn when the number of partition keys in a select statement greater than threshold.
     * -1 means disabled.
     */
    int getPartitionKeysInSelectWarnThreshold();

    /**
     * @return The threshold to fail when the number of partition keys in a select statement greater than threshold.
     * -1 means disabled.
     */
    int getPartitionKeysInSelectFailThreshold();

    /**
     * @param warn The threshold to warn when the number of partition keys in a select statement is greater than
     *             threshold -1 means disabled.
     * @param fail The threshold to prevent when the number of partition keys in a select statement is more than
     *             threshold -1 means disabled.
     */
    void setPartitionKeysInSelectThreshold(int warn, int fail);

    /**
     * @return The threshold to warn when an IN query creates a cartesian product with a size exceeding threshold.
     * -1 means disabled.
     */
    int getInSelectCartesianProductWarnThreshold();

    /**
     * @return The threshold to prevent IN queries creating a cartesian product with a size exceeding threshold.
     * -1 means disabled.
     */
    int getInSelectCartesianProductFailThreshold();

    /**
     * @param warn The threshold to warn when an IN query creates a cartesian product with a size exceeding threshold.
     *             -1 means disabled.
     * @param fail The threshold to prevent IN queries creating a cartesian product with a size exceeding threshold.
     *             -1 means disabled.
     */
    void setInSelectCartesianProductThreshold(int warn, int fail);

    /**
     * @return consistency levels that are warned about when reading.
     */
    Set<String> getReadConsistencyLevelsWarned();

    /**
     * @return Comma-separated list of consistency levels that are warned about when reading.
     */
    String getReadConsistencyLevelsWarnedCSV();

    /**
     * @param consistencyLevels consistency levels that are warned about when reading.
     */
    void setReadConsistencyLevelsWarned(Set<String> consistencyLevels);

    /**
     * @param consistencyLevels Comma-separated list of consistency levels that are warned about when reading.
     */
    void setReadConsistencyLevelsWarnedCSV(String consistencyLevels);

    /**
     * @return consistency levels that are not allowed when reading.
     */
    Set<String> getReadConsistencyLevelsDisallowed();

    /**
     * @return Comma-separated list of consistency levels that are not allowed when reading.
     */
    String getReadConsistencyLevelsDisallowedCSV();

    /**
     * @param consistencyLevels consistency levels that are not allowed when reading.
     */
    void setReadConsistencyLevelsDisallowed(Set<String> consistencyLevels);

    /**
     * @param consistencyLevels Comma-separated list of consistency levels that are not allowed when reading.
     */
    void setReadConsistencyLevelsDisallowedCSV(String consistencyLevels);

    /**
     * @return consistency levels that are warned about when writing.
     */
    Set<String> getWriteConsistencyLevelsWarned();

    /**
     * @return Comma-separated list of consistency levels that are warned about when writing.
     */
    String getWriteConsistencyLevelsWarnedCSV();

    /**
     * @param consistencyLevels consistency levels that are warned about when writing.
     */
    void setWriteConsistencyLevelsWarned(Set<String> consistencyLevels);

    /**
     * @param consistencyLevels Comma-separated list of consistency levels that are warned about when writing.
     */
    void setWriteConsistencyLevelsWarnedCSV(String consistencyLevels);

    /**
     * @return consistency levels that are not allowed when writing.
     */
    Set<String> getWriteConsistencyLevelsDisallowed();

    /**
     * @return Comma-separated list of consistency levels that are not allowed when writing.
     */
    String getWriteConsistencyLevelsDisallowedCSV();

    /**
     * @param consistencyLevels consistency levels that are not allowed when writing.
     */
    void setWriteConsistencyLevelsDisallowed(Set<String> consistencyLevels);

    /**
     * @param consistencyLevels Comma-separated list of consistency levels that are not allowed when writing.
     */
    void setWriteConsistencyLevelsDisallowedCSV(String consistencyLevels);

    /**
     * @return The threshold to warn when encountering partitions larger than threshold, as a string formatted as in,
     * for example, {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}. A {@code null} value means disabled.
     */
    @Nullable
    String getPartitionSizeWarnThreshold();

    /**
     * @return The threshold to fail when encountering partitions larger than threshold, as a string formatted as in,
     * for example, {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}. A {@code null} value means disabled.
     * Triggering a failure emits a log message and a diagnostic  event, but it doesn't throw an exception interrupting
     * the offending sstable write.
     */
    @Nullable
    String getPartitionSizeFailThreshold();

    /**
     * @param warnSize The threshold to warn when encountering partitions larger than threshold, as a string formatted
     *                 as in, for example, {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}.
     *                 A {@code null} value means disabled.
     * @param failSize The threshold to fail when encountering partitions larger than threshold, as a string formatted
     *                 as in, for example, {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}.
     *                 A {@code null} value means disabled. Triggering a failure emits a log message and a diagnostic
     *                 event, but it desn't throw an exception interrupting the offending sstable write.
     */
    void setPartitionSizeThreshold(@Nullable String warnSize, @Nullable String failSize);

    /**
     * @return The threshold to warn when encountering partitions with more tombstones than threshold. -1 means disabled.
     */
    long getPartitionTombstonesWarnThreshold();

    /**
     * @return The threshold to fail when encountering partitions with more tombstones than threshold. -1 means disabled.
     * Triggering a failure emits a log message and a diagnostic event, but it doesn't throw an exception interrupting
     * the offending sstable write.
     */
    long getPartitionTombstonesFailThreshold();

    /**
     * @param warn The threshold to warn when encountering partitions with more tombstones than threshold. -1 means disabled.
     * @param fail The threshold to fail when encountering partitions with more tombstones than threshold. -1 means disabled.
     *             Triggering a failure emits a log message and a diagnostic event, but it desn't throw an exception
     *             interrupting the offending sstable write.
     */
    void setPartitionTombstonesThreshold(long warn, long fail);

    /**
     * @return The threshold to warn when encountering column values larger than threshold, as a string  formatted as
     * in, for example, {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}. A {@code null} value means disabled.
     */
    @Nullable
    String getColumnValueSizeWarnThreshold();

    /**
     * @return The threshold to prevent column values larger than threshold, as a string formatted as in, for example,
     * {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}. A {@code null} value means disabled.
     */
    @Nullable
    String getColumnValueSizeFailThreshold();

    /**
     * @param warnSize The threshold to warn when encountering column values larger than threshold, as a string
     *                 formatted as in, for example, {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}.
     *                 A {@code null} value means disabled.
     * @param failSize The threshold to prevent column values larger than threshold, as a string formatted as in, for
     *                 example, {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}.
     *                 A {@code null} value means disabled.
     */
    void setColumnValueSizeThreshold(@Nullable String warnSize, @Nullable String failSize);

    /**
     * @return The threshold to warn when encountering larger size of collection data than threshold, as a string
     * formatted as in, for example, {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}.  A {@code null} value
     * means that the threshold is disabled.
     */
    @Nullable
    String getCollectionSizeWarnThreshold();

    /**
     * @return The threshold to prevent collections with larger data size than threshold, as a string formatted as in,
     * for example, {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}. A {@code null} value means that the
     * threshold is disabled.
     */
    @Nullable
    String getCollectionSizeFailThreshold();

    /**
     * @param warnSize The threshold to warn when encountering larger size of collection data than threshold, as a
     *                 string formatted as in, for example, {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}.
     *                 A {@code null} value means disabled.
     * @param failSize The threshold to prevent collections with larger data size than threshold, as a string formatted
     *                 as in, for example, {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}. A {@code null}
     *                 value means disabled.
     */
    void setCollectionSizeThreshold(@Nullable String warnSize, @Nullable String failSize);

    /**
     * @return The threshold to warn when encountering more elements in a collection than threshold.
     */
    int getItemsPerCollectionWarnThreshold();

    /**
     * @return The threshold to prevent collections with more elements than threshold.
     */
    int getItemsPerCollectionFailThreshold();

    /**
     * @param warn The threshold to warn when encountering more elements in a collection than threshold.
     * @param fail The threshold to prevent collectiosn with more elements than threshold.
     */
    void setItemsPerCollectionThreshold(int warn, int fail);

    /**
     * @return The threshold to warn when creating a UDT with more fields than threshold. -1 means disabled.
     */
    int getFieldsPerUDTWarnThreshold();

    /**
     * @return The threshold to fail when creating a UDT with more fields than threshold. -1 means disabled.
     */
    int getFieldsPerUDTFailThreshold();

    /**
     * @param warn The threshold to warn when creating a UDT with more fields than threshold. -1 means disabled.
     * @param fail The threshold to prevent creating a UDT with more fields than threshold. -1 means disabled.
     */
    void setFieldsPerUDTThreshold(int warn, int fail);

    /**
     * @return The threshold to warn when creating a vector with more dimensions than threshold.
     */
    int getVectorDimensionsWarnThreshold();

    /**
     * @return The threshold to fail when creating a vector with more dimensions than threshold.
     */
    int getVectorDimensionsFailThreshold();

    /**
     * @param warn The threshold to warn when creating a vector with more dimensions than threshold.
     * @param fail The threshold to prevent creating a vector with more dimensions than threshold.
     */
    void setVectorDimensionsThreshold(int warn, int fail);

    /**
     * @return The threshold to warn when local data disk usage percentage exceeds that threshold.
     * Allowed values are in the range {@code [1, 100]}, and -1 means disabled.
     */
    int getDataDiskUsagePercentageWarnThreshold();

    /**
     * @return The threshold to fail when local data disk usage percentage exceeds that threshold.
     * Allowed values are in the range {@code [1, 100]}, and -1 means disabled.
     */
    int getDataDiskUsagePercentageFailThreshold();

    /**
     * @param warn The threshold to warn when local disk usage percentage exceeds that threshold.
     *             Allowed values are in the range {@code [1, 100]}, and -1 means disabled.
     * @param fail The threshold to fail when local disk usage percentage exceeds that threshold.
     *             Allowed values are in the range {@code [1, 100]}, and -1 means disabled.
     */
    public void setDataDiskUsagePercentageThreshold(int warn, int fail);

    /**
     * @return The max disk size of the data directories when calculating disk usage thresholds, as a string formatted
     * as in, for example, {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}. A {@code null} value means
     * disabled.
     */
    @Nullable
    String getDataDiskUsageMaxDiskSize();

    /**
     * @param size The max disk size of the data directories when calculating disk usage thresholds, as a string
     *             formatted as in, for example, {@code 10GiB}, {@code 20MiB}, {@code 30KiB} or {@code 40B}.
     *             A {@code null} value means disabled.
     */
    void setDataDiskUsageMaxDiskSize(@Nullable String size);

    /**
     * @return The threshold to warn when replication factor is lesser than threshold.
     */
    int getMinimumReplicationFactorWarnThreshold();

    /**
     * @return The threshold to fail when replication factor is lesser than threshold.
     */
    int getMinimumReplicationFactorFailThreshold();

    /**
     * @param warn The threshold to warn when the minimum replication factor is lesser than threshold.
     *             -1 means disabled.
     * @param fail The threshold to fail when the minimum replication factor is lesser than threshold.
     *            -1 means disabled.
     */
    void setMinimumReplicationFactorThreshold (int warn, int fail);

    /**
     * @return The threshold to fail when replication factor is greater than threshold.
     */
    int getMaximumReplicationFactorWarnThreshold();

    /**
     * @return The threshold to fail when replication factor is greater than threshold.
     */
    int getMaximumReplicationFactorFailThreshold();

    /**
     * @param warn The threshold to warn when the maximum replication factor is greater than threshold.
     *             -1 means disabled.
     * @param fail The threshold to fail when the maximum replication factor is greater than threshold.
     *             -1 means disabled.
     */
    void setMaximumReplicationFactorThreshold (int warn, int fail);

    /**
     * Returns whether warnings will be emitted when usage of 0 default TTL on a
     * table with TimeWindowCompactionStrategy is detected.
     *
     * @return {@code true} if warnings will be emitted, {@code false} otherwise.
     */
    boolean getZeroTTLOnTWCSWarned();

    /**
     * Sets whether warnings will be emitted when usage of 0 default TTL on a
     * table with TimeWindowCompactionStrategy is detected.
     *
     * @param value {@code true} if warning will be emitted, {@code false} otherwise.
     */
    void setZeroTTLOnTWCSWarned(boolean value);

    /**
     * Returns whether it is allowed to create or alter table to use 0 default TTL with TimeWindowCompactionStrategy.
     * If it is not, such query will fail.
     *
     * @return {@code true} if 0 default TTL is allowed on TWCS table, {@code false} otherwise.
     */
    boolean getZeroTTLOnTWCSEnabled();

    /**
     * Sets whether users can use 0 default TTL on a table with TimeWindowCompactionStrategy.
     *
     * @param value {@code true} if 0 default TTL on TWCS tables is allowed, {@code false} otherwise.
     */
    void setZeroTTLOnTWCSEnabled(boolean value);

    /**
     * @return The highest acceptable difference between now and the written value timestamp before triggering a warning.
     *         Expressed as a string formatted as in, for example, {@code 10s} {@code 20m}, {@code 30h} or {@code 40d}.
     *         A {@code null} value means disabled.
     */
    @Nullable
    String getMaximumTimestampWarnThreshold();

    /**
     * @return The highest acceptable difference between now and the written value timestamp before triggering a failure.
     *         Expressed as a string formatted as in, for example, {@code 10s} {@code 20m}, {@code 30h} or {@code 40d}.
     *         A {@code null} value means disabled.
     */
    @Nullable
    String getMaximumTimestampFailThreshold();

    /**
     * Sets the warning upper bound for user supplied timestamps.
     *
     * @param warnDuration The highest acceptable difference between now and the written value timestamp before
     *                     triggering a warning. Expressed as a string formatted as in, for example, {@code 10s},
     *                     {@code 20m}, {@code 30h} or {@code 40d}. A {@code null} value means disabled.
     * @param failDuration The highest acceptable difference between now and the written value timestamp before
     *                     triggering a failure. Expressed as a string formatted as in, for example, {@code 10s},
     *                     {@code 20m}, {@code 30h} or {@code 40d}. A {@code null} value means disabled.
     */
    void setMaximumTimestampThreshold(@Nullable String warnDuration, @Nullable String failDuration);

    /**
     * @return The lowest acceptable difference between now and the written value timestamp before triggering a warning.
     *         Expressed as a string formatted as in, for example, {@code 10s} {@code 20m}, {@code 30h} or {@code 40d}.
     *         A {@code null} value means disabled.
     */
    @Nullable
    String getMinimumTimestampWarnThreshold();

    /**
     * @return The lowest acceptable difference between now and the written value timestamp before triggering a failure.
     *         Expressed as a string formatted as in, for example, {@code 10s} {@code 20m}, {@code 30h} or {@code 40d}.
     *         A {@code null} value means disabled.
     */
    @Nullable
    String getMinimumTimestampFailThreshold();

    /**
     * Sets the warning lower bound for user supplied timestamps.
     *
     * @param warnDuration The lowest acceptable difference between now and the written value timestamp before
     *                     triggering a warning. Expressed as a string formatted as in, for example, {@code 10s},
     *                     {@code 20m}, {@code 30h} or {@code 40d}. A {@code null} value means disabled.
     * @param failDuration The lowest acceptable difference between now and the written value timestamp before
     *                     triggering a failure. Expressed as a string formatted as in, for example, {@code 10s},
     *                     {@code 20m}, {@code 30h} or {@code 40d}. A {@code null} value means disabled.
     */
    void setMinimumTimestampThreshold(@Nullable String warnDuration, @Nullable String failDuration);

    /**
     * @return the warning threshold for the number of SAI SSTable indexes searched by a query
     */
    int getSaiSSTableIndexesPerQueryWarnThreshold();

    /**
     * @return the failure threshold for the number of SAI SSTable indexes searched by a query
     */
    int getSaiSSTableIndexesPerQueryFailThreshold();

    /**
     * Sets warning and failure thresholds for the number of SAI SSTable indexes searched by a query
     *
     * @param warn value to set for warn threshold
     * @param fail value to set for fail threshold
     */
    void setSaiSSTableIndexesPerQueryThreshold(int warn, int fail);

    /**
     * Returns whether it is possible to execute a query against secondary indexes without specifying
     * any partition key restrictions.
     *
     * @return true if it is possible to execute a query without a partition key, false otherwise
     */
    boolean getNonPartitionRestrictedQueryEnabled();

    /**
     * Sets whether it is possible to execute a query against indexes (secondary or SAI) without specifying
     * any partition key restrictions.
     *
     * @param enabled {@code true} if a query without partition key is enabled or not
     */
    void setNonPartitionRestrictedQueryEnabled(boolean enabled);
}
