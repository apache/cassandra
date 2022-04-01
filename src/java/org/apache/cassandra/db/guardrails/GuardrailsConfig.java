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

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.db.ConsistencyLevel;

/**
 * Configuration settings for guardrails.
 *
 * <p>Note that the settings here must only be used by the {@link Guardrails} class and not directly by the code
 * checking each guarded constraint (which, again, should use the higher level abstractions defined in
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
 * <p>
 * This configuration is offered as an interface so different implementations of {@link GuardrailsConfigProvider} can
 * provide different implementations of this config. However, this mechanism for guardrails config pluggability is not
 * officially supported and this interface may change in a minor release.
 */
public interface GuardrailsConfig
{
    /**
     * Whether guardrails are enabled or not.
     *
     * @return {@code true} if guardrails are enabled, {@code false} otherwise
     */
    boolean getEnabled();

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
     * Returns whether users can create new COMPACT STORAGE tables
     *
     * @return {@code true} if allowed, {@code false} otherwise.
     */
    boolean getCompactTablesEnabled();

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

    /*
     * @return The threshold to warn when encountering a collection with larger data size than threshold.
     */
    DataStorageSpec getCollectionSizeWarnThreshold();

    /**
     * @return The threshold to prevent collections with larger data size than threshold.
     */
    DataStorageSpec getCollectionSizeFailThreshold();

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
}
