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

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.ConsistencyLevel;

/**
 * Configuration settings for guardrails.
 *
 * <p>Note that the settings here must only be used by the {@link Guardrails} class and not directly by the code
 * checking each guarded constraint (which, again, should use the higher level abstractions defined in
 * {@link Guardrails}).
 *
 * <p>We have 2 variants of guardrails, soft (warn) and hard (fail) limits, each guardrail having either one of the
 * variants or both. Note in particular that hard limits only make sense for guardrails triggering during query
 * execution. For other guardrails, say one triggering during compaction, aborting that compaction does not make sense.
 *
 * <p>Additionally, each individual setting should have a specific value (typically -1 for numeric settings),
 * that allows to disable the corresponding guardrail.
 * <p>
 * This configuration is offered as an interface so different implementations of {@link GuardrailsConfigProvider} can
 * provide different implementations of this config. However, this mechanism for guardrails config pluggability is not
 * officially supported and this interface may change in a minor release.
 */
public interface GuardrailsConfig
{
    /**
     * @return The threshold to warn when creating more user keyspaces than threshold.
     */
    int getKeyspacesWarnThreshold();

    /**
     * @return The threshold to fail when creating more user keyspaces than threshold.
     */
    int getKeyspacesFailThreshold();

    /**
     * @return The threshold to warn when creating more user tables than threshold.
     */
    int getTablesWarnThreshold();

    /**
     * @return The threshold to fail when creating more user tables than threshold.
     */
    int getTablesFailThreshold();

    /**
     * @return The threshold to warn when creating more columns per table than threshold.
     */
    int getColumnsPerTableWarnThreshold();

    /**
     * @return The threshold to fail when creating more columns per table than threshold.
     */
    int getColumnsPerTableFailThreshold();

    /**
     * @return The threshold to warn when creating more secondary indexes per table than threshold.
     */
    int getSecondaryIndexesPerTableWarnThreshold();

    /**
     * @return The threshold to fail when creating more secondary indexes per table than threshold.
     */
    int getSecondaryIndexesPerTableFailThreshold();

    /**
     * @return Whether creation of secondary indexes is allowed.
     */
    boolean getSecondaryIndexesEnabled();

    /**
     * @return The threshold to warn when creating more materialized views per table than threshold.
     */
    int getMaterializedViewsPerTableWarnThreshold();

    /**
     * @return The threshold to warn when partition keys in select more than threshold.
     */
    int getPartitionKeysInSelectWarnThreshold();

    /**
     * @return The threshold to fail when partition keys in select more than threshold.
     */
    int getPartitionKeysInSelectFailThreshold();

    /**
     * @return The threshold to fail when creating more materialized views per table than threshold.
     */
    int getMaterializedViewsPerTableFailThreshold();

    /**
     * @return The table properties that are warned about when creating or altering a table.
     */
    Set<String> getTablePropertiesWarned();

    /**
     * @return The table properties that are ignored when creating or altering a table.
     */
    Set<String> getTablePropertiesIgnored();

    /**
     * @return The table properties that are disallowed when creating or altering a table.
     */
    Set<String> getTablePropertiesDisallowed();

    /**
     * Returns whether user-provided timestamps are allowed.
     *
     * @return {@code true} if user-provided timestamps are allowed, {@code false} otherwise.
     */
    boolean getUserTimestampsEnabled();

    /**
     * Returns whether users are allowed access to the ALTER TABLE statement to mutate columns or not
     *
     * @return {@code true} if ALTER TABLE ADD/REMOVE/RENAME is allowed, {@code false} otherwise.
     */
    boolean getAlterTableEnabled();

    /**
     * Returns whether tables can be uncompressed
     *
     * @return {@code true} if user's can disable compression, {@code false} otherwise.
     */
    boolean getUncompressedTablesEnabled();

    /**
     * Returns whether users can create new COMPACT STORAGE tables
     *
     * @return {@code true} if allowed, {@code false} otherwise.
     */
    boolean getCompactTablesEnabled();

    /**
     * Returns whether GROUP BY functionality is allowed
     *
     * @return {@code true} if allowed, {@code false} otherwise.
     */
    boolean getGroupByEnabled();

    /**
     * Returns whether TRUNCATE or DROP table are allowed
     *
     * @return {@code true} if allowed, {@code false} otherwise.
     */
    boolean getDropTruncateTableEnabled();

    /**
     * Returns whether DROP on keyspaces is allowed
     *
     * @return {@code true} if allowed, {@code false} otherwise.
     */
    boolean getDropKeyspaceEnabled();

    /**
     * @return The threshold to warn when page size exceeds given size.
     */
    int getPageSizeWarnThreshold();

    /**
     * @return The threshold to fail when page size exceeds given size.
     */
    int getPageSizeFailThreshold();

    /**
     * Returns whether list operations that require read before write are allowed.
     *
     * @return {@code true} if list operations that require read before write are allowed, {@code false} otherwise.
     */
    boolean getReadBeforeWriteListOperationsEnabled();

    /**
     * Returns whether ALLOW FILTERING property is allowed.
     *
     * @return {@code true} if ALLOW FILTERING is allowed, {@code false} otherwise.
     */
    boolean getAllowFilteringEnabled();

    /**
     * Returns whether setting SimpleStrategy via keyspace creation or alteration is enabled
     *
     * @return {@code true} if SimpleStrategy is allowed, {@code false} otherwise.
     */
    boolean getSimpleStrategyEnabled();

    /**
     * @return The threshold to warn when an IN query creates a cartesian product with a size exceeding threshold.
     * -1 means disabled.
     */
    public int getInSelectCartesianProductWarnThreshold();

    /**
     * @return The threshold to prevent IN queries creating a cartesian product with a size exceeding threshold.
     * -1 means disabled.
     */
    public int getInSelectCartesianProductFailThreshold();

    /**
     * @return The consistency levels that are warned about when reading.
     */
    Set<ConsistencyLevel> getReadConsistencyLevelsWarned();

    /**
     * @return The consistency levels that are disallowed when reading.
     */
    Set<ConsistencyLevel> getReadConsistencyLevelsDisallowed();

    /**
     * @return The consistency levels that are warned about when writing.
     */
    Set<ConsistencyLevel> getWriteConsistencyLevelsWarned();

    /**
     * @return The consistency levels that are disallowed when writing.
     */
    Set<ConsistencyLevel> getWriteConsistencyLevelsDisallowed();

    /**
     * @return The threshold to warn when writing partitions larger than threshold.
     */
    @Nullable
    DataStorageSpec.LongBytesBound getPartitionSizeWarnThreshold();

    /**
     * @return The threshold to fail when writing partitions larger than threshold.
     */
    @Nullable
    DataStorageSpec.LongBytesBound getPartitionSizeFailThreshold();

    /**
     * @return The threshold to warn when writing partitions with more tombstones than threshold.
     */
    long getPartitionTombstonesWarnThreshold();

    /**
     * @return The threshold to fail when writing partitions with more tombstones than threshold.
     */
    long getPartitionTombstonesFailThreshold();

    /**
     * @return The threshold to warn when writing column values larger than threshold.
     */
    @Nullable
    DataStorageSpec.LongBytesBound getColumnValueSizeWarnThreshold();

    /**
     * @return The threshold to prevent writing column values larger than threshold.
     */
    @Nullable
    DataStorageSpec.LongBytesBound getColumnValueSizeFailThreshold();

    /**
     * @return The threshold to warn when encountering a collection with larger data size than threshold.
     */
    @Nullable
    DataStorageSpec.LongBytesBound getCollectionSizeWarnThreshold();

    /**
     * @return The threshold to prevent collections with larger data size than threshold.
     */
    @Nullable
    DataStorageSpec.LongBytesBound getCollectionSizeFailThreshold();

    /**
     * @return The threshold to warn when encountering more elements in a collection than threshold.
     */
    int getItemsPerCollectionWarnThreshold();

    /**
     * @return The threshold to prevent collections with more elements than threshold.
     */
    int getItemsPerCollectionFailThreshold();

    /**
     * @return The threshold to warn when creating a UDT with more fields than threshold.
     */
    int getFieldsPerUDTWarnThreshold();

    /**
     * @return The threshold to fail when creating a UDT with more fields than threshold.
     */
    int getFieldsPerUDTFailThreshold();

    /**
     * @return The threshold to warn when creating a vector with more dimensions than threshold.
     */
    int getVectorDimensionsWarnThreshold();

    /**
     * @return The threshold to fail when creating a vector with more dimensions than threshold.
     */
    int getVectorDimensionsFailThreshold();

    /**
     * @return The threshold to warn when local disk usage percentage exceeds that threshold.
     * Allowed values are in the range {@code [1, 100]}, and -1 means disabled.
     */
    int getDataDiskUsagePercentageWarnThreshold();

    /**
     * @return The threshold to fail when local disk usage percentage exceeds that threshold.
     * Allowed values are in the range {@code [1, 100]}, and -1 means disabled.
     */
    int getDataDiskUsagePercentageFailThreshold();

    /**
     * @return The max disk size of the data directories when calculating disk usage thresholds, {@code null} means
     * disabled.
     */
    @Nullable
    DataStorageSpec.LongBytesBound getDataDiskUsageMaxDiskSize();

    /**
     * @return The threshold to warn when replication factor is lesser than threshold.
     */
    int getMinimumReplicationFactorWarnThreshold();

    /**
     * @return The threshold to fail when replication factor is lesser than threshold.
     */
    int getMinimumReplicationFactorFailThreshold();

    /**
     * @return The threshold to warn when replication factor is greater than threshold.
     */
    int getMaximumReplicationFactorWarnThreshold();

    /**
     * @return The threshold to fail when replication factor is greater than threshold.
     */
    int getMaximumReplicationFactorFailThreshold();

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
     * @return A timestamp that if a user supplied timestamp is after will trigger a warning
     */
    @Nullable
    DurationSpec.LongMicrosecondsBound getMaximumTimestampWarnThreshold();

    /**
     * @return A timestamp that if a user supplied timestamp is after will cause a failure
     */
    @Nullable
    DurationSpec.LongMicrosecondsBound getMaximumTimestampFailThreshold();

    /**
     * Sets the warning upper bound for user supplied timestamps
     *
     * @param warn The highest acceptable difference between now and the written value timestamp before triggering a
     *             warning. {@code null} means disabled.
     * @param fail The highest acceptable difference between now and the written value timestamp before triggering a
     *             failure. {@code null} means disabled.
     */
    void setMaximumTimestampThreshold(@Nullable DurationSpec.LongMicrosecondsBound warn,
                                      @Nullable DurationSpec.LongMicrosecondsBound fail);

    /**
     * @return A timestamp that if a user supplied timestamp is before will trigger a warning
     */
    @Nullable
    DurationSpec.LongMicrosecondsBound getMinimumTimestampWarnThreshold();

    /**
     * @return A timestamp that if a user supplied timestamp is after will trigger a warning
     */
    @Nullable
    DurationSpec.LongMicrosecondsBound getMinimumTimestampFailThreshold();

    /**
     * Sets the warning lower bound for user supplied timestamps
     *
     * @param warn The lowest acceptable difference between now and the written value timestamp before triggering a
     *             warning. {@code null} means disabled.
     * @param fail The lowest acceptable difference between now and the written value timestamp before triggering a
     *             failure. {@code null} means disabled.
     */
    void setMinimumTimestampThreshold(@Nullable DurationSpec.LongMicrosecondsBound warn,
                                      @Nullable DurationSpec.LongMicrosecondsBound fail);

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
