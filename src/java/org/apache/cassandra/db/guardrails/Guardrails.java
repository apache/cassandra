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

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.GuardrailsOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.disk.usage.DiskUsageBroadcaster;
import org.apache.cassandra.utils.MBeanWrapper;

import static java.lang.String.format;

/**
 * Entry point for Guardrails, storing the defined guardrails and providing a few global methods over them.
 */
public final class Guardrails implements GuardrailsMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=Guardrails";

    public static final GuardrailsConfigProvider CONFIG_PROVIDER = GuardrailsConfigProvider.instance;
    private static final GuardrailsOptions DEFAULT_CONFIG = DatabaseDescriptor.getGuardrailsConfig();

    @VisibleForTesting
    static final Guardrails instance = new Guardrails();

    /**
     * Guardrail on the total number of user keyspaces.
     */
    public static final MaxThreshold keyspaces =
    new MaxThreshold("keyspaces",
                     state -> CONFIG_PROVIDER.getOrCreate(state).getKeyspacesWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getKeyspacesFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("Creating keyspace %s, current number of keyspaces %s exceeds warning threshold of %s.",
                                        what, value, threshold)
                               : format("Cannot have more than %s keyspaces, aborting the creation of keyspace %s",
                                        threshold, what));

    /**
     * Guardrail on the total number of tables on user keyspaces.
     */
    public static final MaxThreshold tables =
    new MaxThreshold("tables",
                     state -> CONFIG_PROVIDER.getOrCreate(state).getTablesWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getTablesFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("Creating table %s, current number of tables %s exceeds warning threshold of %s.",
                                        what, value, threshold)
                               : format("Cannot have more than %s tables, aborting the creation of table %s",
                                        threshold, what));

    /**
     * Guardrail on the number of columns per table.
     */
    public static final MaxThreshold columnsPerTable =
    new MaxThreshold("columns_per_table",
                     state -> CONFIG_PROVIDER.getOrCreate(state).getColumnsPerTableWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getColumnsPerTableFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("The table %s has %s columns, this exceeds the warning threshold of %s.",
                                        what, value, threshold)
                               : format("Tables cannot have more than %s columns, but %s provided for table %s",
                                        threshold, value, what));

    public static final MaxThreshold secondaryIndexesPerTable =
    new MaxThreshold("secondary_indexes_per_table",
                     state -> CONFIG_PROVIDER.getOrCreate(state).getSecondaryIndexesPerTableWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getSecondaryIndexesPerTableFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("Creating secondary index %s, current number of indexes %s exceeds warning threshold of %s.",
                                        what, value, threshold)
                               : format("Tables cannot have more than %s secondary indexes, aborting the creation of secondary index %s",
                                        threshold, what));

    /**
     * Guardrail disabling user's ability to create secondary indexes
     */
    public static final DisableFlag createSecondaryIndexesEnabled =
    new DisableFlag("secondary_indexes",
                    state -> !CONFIG_PROVIDER.getOrCreate(state).getSecondaryIndexesEnabled(),
                    "User creation of secondary indexes");

    /**
     * Guardrail on the number of materialized views per table.
     */
    public static final MaxThreshold materializedViewsPerTable =
    new MaxThreshold("materialized_views_per_table",
                     state -> CONFIG_PROVIDER.getOrCreate(state).getMaterializedViewsPerTableWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getMaterializedViewsPerTableFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("Creating materialized view %s, current number of views %s exceeds warning threshold of %s.",
                                        what, value, threshold)
                               : format("Tables cannot have more than %s materialized views, aborting the creation of materialized view %s",
                                        threshold, what));

    /**
     * Guardrail warning about, ignoring or rejecting the usage of certain table properties.
     */
    public static final Values<String> tableProperties =
    new Values<>("table_properties",
                 state -> CONFIG_PROVIDER.getOrCreate(state).getTablePropertiesWarned(),
                 state -> CONFIG_PROVIDER.getOrCreate(state).getTablePropertiesIgnored(),
                 state -> CONFIG_PROVIDER.getOrCreate(state).getTablePropertiesDisallowed(),
                 "Table Properties");

    /**
     * Guardrail disabling user-provided timestamps.
     */
    public static final DisableFlag userTimestampsEnabled =
    new DisableFlag("user_timestamps",
                    state -> !CONFIG_PROVIDER.getOrCreate(state).getUserTimestampsEnabled(),
                    "User provided timestamps (USING TIMESTAMP)");

    public static final DisableFlag groupByEnabled =
    new DisableFlag("group_by",
                    state -> !CONFIG_PROVIDER.getOrCreate(state).getGroupByEnabled(),
                    "GROUP BY functionality");

    public static final DisableFlag dropTruncateTableEnabled =
    new DisableFlag("drop_truncate_table_enabled",
                    state -> !CONFIG_PROVIDER.getOrCreate(state).getDropTruncateTableEnabled(),
                    "DROP and TRUNCATE TABLE functionality");

    /**
     * Guardrail disabling user's ability to turn off compression
     */
    public static final DisableFlag uncompressedTablesEnabled =
    new DisableFlag("uncompressed_tables_enabled",
                    state -> !CONFIG_PROVIDER.getOrCreate(state).getUncompressedTablesEnabled(),
                    "Uncompressed table");

    /**
     * Guardrail disabling the creation of new COMPACT STORAGE tables
     */
    public static final DisableFlag compactTablesEnabled =
    new DisableFlag("compact_tables",
                    state -> !CONFIG_PROVIDER.getOrCreate(state).getCompactTablesEnabled(),
                    "Creation of new COMPACT STORAGE tables");

    /**
     * Guardrail on the number of elements returned within page.
     */
    public static final MaxThreshold pageSize =
    new MaxThreshold("page_size",
                     state -> CONFIG_PROVIDER.getOrCreate(state).getPageSizeWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getPageSizeFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("Query for table %s with page size %s exceeds warning threshold of %s.",
                                        what, value, threshold)
                               : format("Aborting query for table %s, page size %s exceeds fail threshold of %s.",
                                        what, value, threshold));

    /**
     * Guardrail on the number of partition keys in the IN clause.
     */
    public static final MaxThreshold partitionKeysInSelect =
    new MaxThreshold("partition_keys_in_select",
                     state -> CONFIG_PROVIDER.getOrCreate(state).getPartitionKeysInSelectWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getPartitionKeysInSelectFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("Query with partition keys in IN clause on table %s, with number of " +
                                        "partition keys %s exceeds warning threshold of %s.",
                                        what, value, threshold)
                               : format("Aborting query with partition keys in IN clause on table %s, " +
                                        "number of partition keys %s exceeds fail threshold of %s.",
                                        what, value, threshold));

    /**
     * Guardrail disabling operations on lists that require read before write.
     */
    public static final DisableFlag readBeforeWriteListOperationsEnabled =
    new DisableFlag("read_before_write_list_operations",
                    state -> !CONFIG_PROVIDER.getOrCreate(state).getReadBeforeWriteListOperationsEnabled(),
                    "List operation requiring read before write");

    /**
     * Guardrail disabling ALLOW FILTERING statement within a query
     */
    public static final DisableFlag allowFilteringEnabled =
    new DisableFlag("allow_filtering",
                    state -> !CONFIG_PROVIDER.getOrCreate(state).getAllowFilteringEnabled(),
                    "Querying with ALLOW FILTERING");

    /**
     * Guardrail on the number of restrictions created by a cartesian product of a CQL's {@code IN} query.
     */
    public static final MaxThreshold inSelectCartesianProduct =
    new MaxThreshold("in_select_cartesian_product",
                     state -> CONFIG_PROVIDER.getOrCreate(state).getInSelectCartesianProductWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getInSelectCartesianProductFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("The cartesian product of the IN restrictions on %s produces %s values, " +
                                        "this exceeds warning threshold of %s.",
                                        what, value, threshold)
                               : format("Aborting query because the cartesian product of the IN restrictions on %s " +
                                        "produces %s values, this exceeds fail threshold of %s.",
                                        what, value, threshold));

    /**
     * Guardrail on read consistency levels.
     */
    public static final Values<ConsistencyLevel> readConsistencyLevels =
    new Values<>("read_consistency_levels",
                 state -> CONFIG_PROVIDER.getOrCreate(state).getReadConsistencyLevelsWarned(),
                 state -> Collections.emptySet(),
                 state -> CONFIG_PROVIDER.getOrCreate(state).getReadConsistencyLevelsDisallowed(),
                 "read consistency levels");

    /**
     * Guardrail on write consistency levels.
     */
    public static final Values<ConsistencyLevel> writeConsistencyLevels =
    new Values<>("write_consistency_levels",
                 state -> CONFIG_PROVIDER.getOrCreate(state).getWriteConsistencyLevelsWarned(),
                 state -> Collections.emptySet(),
                 state -> CONFIG_PROVIDER.getOrCreate(state).getWriteConsistencyLevelsDisallowed(),
                 "write consistency levels");

    /**
     * Guardrail on the size of a collection.
     */
    public static final MaxThreshold collectionSize =
    new MaxThreshold("collection_size",
                     state -> sizeToBytes(CONFIG_PROVIDER.getOrCreate(state).getCollectionSizeWarnThreshold()),
                     state -> sizeToBytes(CONFIG_PROVIDER.getOrCreate(state).getCollectionSizeFailThreshold()),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("Detected collection %s of size %s, this exceeds the warning threshold of %s.",
                                        what, value, threshold)
                               : format("Detected collection %s of size %s, this exceeds the failure threshold of %s.",
                                        what, value, threshold));

    /**
     * Guardrail on the number of items of a collection.
     */
    public static final MaxThreshold itemsPerCollection =
    new MaxThreshold("items_per_collection",
                     state -> CONFIG_PROVIDER.getOrCreate(state).getItemsPerCollectionWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getItemsPerCollectionFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("Detected collection %s with %s items, this exceeds the warning threshold of %s.",
                                        what, value, threshold)
                               : format("Detected collection %s with %s items, this exceeds the failure threshold of %s.",
                                        what, value, threshold));

    /**
     * Guardrail on the number of fields on each UDT.
     */
    public static final MaxThreshold fieldsPerUDT =
    new MaxThreshold("fields_per_udt",
                     state -> CONFIG_PROVIDER.getOrCreate(state).getFieldsPerUDTWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getFieldsPerUDTFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("The user type %s has %s columns, this exceeds the warning threshold of %s.",
                                        what, value, threshold)
                               : format("User types cannot have more than %s columns, but %s provided for user type %s.",
                                        threshold, value, what));

    /**
     * Guardrail on the data disk usage on the local node, used by a periodic task to calculate and propagate that status.
     * See {@link org.apache.cassandra.service.disk.usage.DiskUsageMonitor} and {@link DiskUsageBroadcaster}.
     */
    public static final PercentageThreshold localDataDiskUsage =
    new PercentageThreshold("local_data_disk_usage",
                            state -> CONFIG_PROVIDER.getOrCreate(state).getDataDiskUsagePercentageWarnThreshold(),
                            state -> CONFIG_PROVIDER.getOrCreate(state).getDataDiskUsagePercentageFailThreshold(),
                            (isWarning, what, value, threshold) ->
                            isWarning ? format("Local data disk usage %s(%s) exceeds warning threshold of %s",
                                               value, what, threshold)
                                      : format("Local data disk usage %s(%s) exceeds failure threshold of %s, " +
                                               "will stop accepting writes",
                                               value, what, threshold));

    /**
     * Guardrail on the data disk usage on replicas, used at write time to verify the status of the involved replicas.
     * See {@link org.apache.cassandra.service.disk.usage.DiskUsageMonitor} and {@link DiskUsageBroadcaster}.
     */
    public static final Predicates<InetAddressAndPort> replicaDiskUsage =
    new Predicates<>("replica_disk_usage",
                     state -> DiskUsageBroadcaster.instance::isStuffed,
                     state -> DiskUsageBroadcaster.instance::isFull,
                     // not using `value` because it represents replica address which should be hidden from client.
                     (isWarning, value) ->
                     isWarning ? "Replica disk usage exceeds warning threshold"
                               : "Write request failed because disk usage exceeds failure threshold");

    static
    {
        // Avoid spamming with notifications about stuffed/full disks
        long minNotifyInterval = CassandraRelevantProperties.DISK_USAGE_NOTIFY_INTERVAL_MS.getLong();
        localDataDiskUsage.minNotifyIntervalInMs(minNotifyInterval);
        replicaDiskUsage.minNotifyIntervalInMs(minNotifyInterval);
    }

    /**
     * Guardrail on the minimum replication factor.
     */
    public static final MinThreshold minimumReplicationFactor =
    new MinThreshold("minimum_replication_factor",
                     state -> CONFIG_PROVIDER.getOrCreate(state).getMinimumReplicationFactorWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getMinimumReplicationFactorFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("The keyspace %s has a replication factor of %s, below the warning threshold of %s.",
                                        what, value, threshold)
                               : format("The keyspace %s has a replication factor of %s, below the failure threshold of %s.",
                                        what, value, threshold));

    private Guardrails()
    {
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    @Override
    public int getKeyspacesWarnThreshold()
    {
        return DEFAULT_CONFIG.getKeyspacesWarnThreshold();
    }

    @Override
    public int getKeyspacesFailThreshold()
    {
        return DEFAULT_CONFIG.getKeyspacesFailThreshold();
    }

    @Override
    public void setKeyspacesThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setKeyspacesThreshold(warn, fail);
    }

    @Override
    public int getTablesWarnThreshold()
    {
        return DEFAULT_CONFIG.getTablesWarnThreshold();
    }

    @Override
    public int getTablesFailThreshold()
    {
        return DEFAULT_CONFIG.getTablesFailThreshold();
    }

    @Override
    public void setTablesThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setTablesThreshold(warn, fail);
    }

    @Override
    public int getColumnsPerTableWarnThreshold()
    {
        return DEFAULT_CONFIG.getColumnsPerTableWarnThreshold();
    }

    @Override
    public int getColumnsPerTableFailThreshold()
    {
        return DEFAULT_CONFIG.getColumnsPerTableFailThreshold();
    }

    @Override
    public void setColumnsPerTableThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setColumnsPerTableThreshold(warn, fail);
    }

    @Override
    public int getSecondaryIndexesPerTableWarnThreshold()
    {
        return DEFAULT_CONFIG.getSecondaryIndexesPerTableWarnThreshold();
    }

    @Override
    public int getSecondaryIndexesPerTableFailThreshold()
    {
        return DEFAULT_CONFIG.getSecondaryIndexesPerTableFailThreshold();
    }

    @Override
    public void setSecondaryIndexesPerTableThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setSecondaryIndexesPerTableThreshold(warn, fail);
    }

    @Override
    public boolean getSecondaryIndexesEnabled()
    {
        return DEFAULT_CONFIG.getSecondaryIndexesEnabled();
    }

    @Override
    public void setSecondaryIndexesEnabled(boolean enabled)
    {
        DEFAULT_CONFIG.setSecondaryIndexesEnabled(enabled);
    }

    @Override
    public int getMaterializedViewsPerTableWarnThreshold()
    {
        return DEFAULT_CONFIG.getMaterializedViewsPerTableWarnThreshold();
    }

    @Override
    public int getMaterializedViewsPerTableFailThreshold()
    {
        return DEFAULT_CONFIG.getMaterializedViewsPerTableFailThreshold();
    }

    @Override
    public void setMaterializedViewsPerTableThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setMaterializedViewsPerTableThreshold(warn, fail);
    }

    @Override
    public Set<String> getTablePropertiesWarned()
    {
        return DEFAULT_CONFIG.getTablePropertiesWarned();
    }

    @Override
    public String getTablePropertiesWarnedCSV()
    {
        return toCSV(DEFAULT_CONFIG.getTablePropertiesWarned());
    }

    public void setTablePropertiesWarned(String... properties)
    {
        setTablePropertiesWarned(ImmutableSet.copyOf(properties));
    }

    @Override
    public void setTablePropertiesWarned(Set<String> properties)
    {
        DEFAULT_CONFIG.setTablePropertiesWarned(properties);
    }

    @Override
    public void setTablePropertiesWarnedCSV(String properties)
    {
        setTablePropertiesWarned(fromCSV(properties));
    }

    @Override
    public Set<String> getTablePropertiesDisallowed()
    {
        return DEFAULT_CONFIG.getTablePropertiesDisallowed();
    }

    @Override
    public String getTablePropertiesDisallowedCSV()
    {
        return toCSV(DEFAULT_CONFIG.getTablePropertiesDisallowed());
    }

    public void setTablePropertiesDisallowed(String... properties)
    {
        setTablePropertiesDisallowed(ImmutableSet.copyOf(properties));
    }

    @Override
    public void setTablePropertiesDisallowed(Set<String> properties)
    {
        DEFAULT_CONFIG.setTablePropertiesDisallowed(properties);
    }

    @Override
    public void setTablePropertiesDisallowedCSV(String properties)
    {
        setTablePropertiesDisallowed(fromCSV(properties));
    }

    @Override
    public Set<String> getTablePropertiesIgnored()
    {
        return DEFAULT_CONFIG.getTablePropertiesIgnored();
    }

    @Override
    public String getTablePropertiesIgnoredCSV()
    {
        return toCSV(DEFAULT_CONFIG.getTablePropertiesIgnored());
    }

    public void setTablePropertiesIgnored(String... properties)
    {
        setTablePropertiesIgnored(ImmutableSet.copyOf(properties));
    }

    @Override
    public void setTablePropertiesIgnored(Set<String> properties)
    {
        DEFAULT_CONFIG.setTablePropertiesIgnored(properties);
    }

    @Override
    public void setTablePropertiesIgnoredCSV(String properties)
    {
        setTablePropertiesIgnored(fromCSV(properties));
    }

    @Override
    public boolean getUserTimestampsEnabled()
    {
        return DEFAULT_CONFIG.getUserTimestampsEnabled();
    }

    @Override
    public void setUserTimestampsEnabled(boolean enabled)
    {
        DEFAULT_CONFIG.setUserTimestampsEnabled(enabled);
    }

    @Override
    public boolean getAllowFilteringEnabled()
    {
        return DEFAULT_CONFIG.getAllowFilteringEnabled();
    }

    @Override
    public void setAllowFilteringEnabled(boolean enabled)
    {
        DEFAULT_CONFIG.setAllowFilteringEnabled(enabled);
    }

    @Override
    public boolean getUncompressedTablesEnabled()
    {
        return DEFAULT_CONFIG.getUncompressedTablesEnabled();
    }

    @Override
    public void setUncompressedTablesEnabled(boolean enabled)
    {
        DEFAULT_CONFIG.setUncompressedTablesEnabled(enabled);
    }

    @Override
    public boolean getCompactTablesEnabled()
    {
        return DEFAULT_CONFIG.getCompactTablesEnabled();
    }

    @Override
    public void setCompactTablesEnabled(boolean enabled)
    {
        DEFAULT_CONFIG.setCompactTablesEnabled(enabled);
    }

    @Override
    public boolean getGroupByEnabled()
    {
        return DEFAULT_CONFIG.getGroupByEnabled();
    }

    @Override
    public void setGroupByEnabled(boolean enabled)
    {
        DEFAULT_CONFIG.setGroupByEnabled(enabled);
    }

    @Override
    public boolean getDropTruncateTableEnabled()
    {
        return DEFAULT_CONFIG.getDropTruncateTableEnabled();
    }

    @Override
    public void setDropTruncateTableEnabled(boolean enabled)
    {
        DEFAULT_CONFIG.setDropTruncateTableEnabled(enabled);
    }

    @Override
    public int getPageSizeWarnThreshold()
    {
        return DEFAULT_CONFIG.getPageSizeWarnThreshold();
    }

    @Override
    public int getPageSizeFailThreshold()
    {
        return DEFAULT_CONFIG.getPageSizeFailThreshold();
    }

    @Override
    public void setPageSizeThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setPageSizeThreshold(warn, fail);
    }

    @Override
    public boolean getReadBeforeWriteListOperationsEnabled()
    {
        return DEFAULT_CONFIG.getReadBeforeWriteListOperationsEnabled();
    }

    @Override
    public void setReadBeforeWriteListOperationsEnabled(boolean enabled)
    {
        DEFAULT_CONFIG.setReadBeforeWriteListOperationsEnabled(enabled);
    }

    @Override
    public int getPartitionKeysInSelectWarnThreshold()
    {
        return DEFAULT_CONFIG.getPartitionKeysInSelectWarnThreshold();
    }

    @Override
    public int getPartitionKeysInSelectFailThreshold()
    {
        return DEFAULT_CONFIG.getPartitionKeysInSelectFailThreshold();
    }

    @Override
    public void setPartitionKeysInSelectThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setPartitionKeysInSelectThreshold(warn, fail);
    }

    @Override
    @Nullable
    public String getCollectionSizeWarnThreshold()
    {
        return sizeToString(DEFAULT_CONFIG.getCollectionSizeWarnThreshold());
    }

    @Override
    @Nullable
    public String getCollectionSizeFailThreshold()
    {
        return sizeToString(DEFAULT_CONFIG.getCollectionSizeFailThreshold());
    }

    @Override
    public void setCollectionSizeThreshold(@Nullable String warnSize, @Nullable String failSize)
    {
        DEFAULT_CONFIG.setCollectionSizeThreshold(sizeFromString(warnSize), sizeFromString(failSize));
    }

    @Override
    public int getItemsPerCollectionWarnThreshold()
    {
        return DEFAULT_CONFIG.getItemsPerCollectionWarnThreshold();
    }

    @Override
    public int getItemsPerCollectionFailThreshold()
    {
        return DEFAULT_CONFIG.getItemsPerCollectionFailThreshold();
    }

    @Override
    public void setItemsPerCollectionThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setItemsPerCollectionThreshold(warn, fail);
    }

    @Override
    public int getInSelectCartesianProductWarnThreshold()
    {
        return DEFAULT_CONFIG.getInSelectCartesianProductWarnThreshold();
    }

    @Override
    public int getInSelectCartesianProductFailThreshold()
    {
        return DEFAULT_CONFIG.getInSelectCartesianProductFailThreshold();
    }

    @Override
    public void setInSelectCartesianProductThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setInSelectCartesianProductThreshold(warn, fail);
    }

    @Override
    public Set<String> getReadConsistencyLevelsWarned()
    {
        return toJmx(DEFAULT_CONFIG.getReadConsistencyLevelsWarned());
    }

    @Override
    public String getReadConsistencyLevelsWarnedCSV()
    {
        return toCSV(DEFAULT_CONFIG.getReadConsistencyLevelsWarned(), ConsistencyLevel::toString);
    }

    @Override
    public void setReadConsistencyLevelsWarned(Set<String> consistencyLevels)
    {
        DEFAULT_CONFIG.setReadConsistencyLevelsWarned(fromJmx(consistencyLevels));
    }

    @Override
    public void setReadConsistencyLevelsWarnedCSV(String consistencyLevels)
    {
        DEFAULT_CONFIG.setReadConsistencyLevelsWarned(fromCSV(consistencyLevels, ConsistencyLevel::fromString));
    }

    @Override
    public Set<String> getReadConsistencyLevelsDisallowed()
    {
        return toJmx(DEFAULT_CONFIG.getReadConsistencyLevelsDisallowed());
    }

    @Override
    public String getReadConsistencyLevelsDisallowedCSV()
    {
        return toCSV(DEFAULT_CONFIG.getReadConsistencyLevelsDisallowed(), ConsistencyLevel::toString);
    }

    @Override
    public void setReadConsistencyLevelsDisallowed(Set<String> consistencyLevels)
    {
        DEFAULT_CONFIG.setReadConsistencyLevelsDisallowed(fromJmx(consistencyLevels));
    }

    @Override
    public void setReadConsistencyLevelsDisallowedCSV(String consistencyLevels)
    {
        DEFAULT_CONFIG.setReadConsistencyLevelsDisallowed(fromCSV(consistencyLevels, ConsistencyLevel::fromString));
    }

    @Override
    public Set<String> getWriteConsistencyLevelsWarned()
    {
        return toJmx(DEFAULT_CONFIG.getWriteConsistencyLevelsWarned());
    }

    @Override
    public String getWriteConsistencyLevelsWarnedCSV()
    {
        return toCSV(DEFAULT_CONFIG.getWriteConsistencyLevelsWarned(), ConsistencyLevel::toString);
    }

    @Override
    public void setWriteConsistencyLevelsWarned(Set<String> consistencyLevels)
    {
        DEFAULT_CONFIG.setWriteConsistencyLevelsWarned(fromJmx(consistencyLevels));
    }

    @Override
    public void setWriteConsistencyLevelsWarnedCSV(String consistencyLevels)
    {
        DEFAULT_CONFIG.setWriteConsistencyLevelsWarned(fromCSV(consistencyLevels, ConsistencyLevel::fromString));
    }

    @Override
    public Set<String> getWriteConsistencyLevelsDisallowed()
    {
        return toJmx(DEFAULT_CONFIG.getWriteConsistencyLevelsDisallowed());
    }

    @Override
    public String getWriteConsistencyLevelsDisallowedCSV()
    {
        return toCSV(DEFAULT_CONFIG.getWriteConsistencyLevelsDisallowed(), ConsistencyLevel::toString);
    }

    @Override
    public void setWriteConsistencyLevelsDisallowed(Set<String> consistencyLevels)
    {
        DEFAULT_CONFIG.setWriteConsistencyLevelsDisallowed(fromJmx(consistencyLevels));
    }

    @Override
    public void setWriteConsistencyLevelsDisallowedCSV(String consistencyLevels)
    {
        DEFAULT_CONFIG.setWriteConsistencyLevelsDisallowed(fromCSV(consistencyLevels, ConsistencyLevel::fromString));
    }

    @Override
    public int getFieldsPerUDTWarnThreshold()
    {
        return DEFAULT_CONFIG.getFieldsPerUDTWarnThreshold();
    }

    @Override
    public int getFieldsPerUDTFailThreshold()
    {
        return DEFAULT_CONFIG.getFieldsPerUDTFailThreshold();
    }

    @Override
    public void setFieldsPerUDTThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setFieldsPerUDTThreshold(warn, fail);
    }

    @Override
    public int getDataDiskUsagePercentageWarnThreshold()
    {
        return DEFAULT_CONFIG.getDataDiskUsagePercentageWarnThreshold();
    }

    @Override
    public int getDataDiskUsagePercentageFailThreshold()
    {
        return DEFAULT_CONFIG.getDataDiskUsagePercentageFailThreshold();
    }

    @Override
    public void setDataDiskUsagePercentageThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setDataDiskUsagePercentageThreshold(warn, fail);
    }

    @Override
    @Nullable
    public String getDataDiskUsageMaxDiskSize()
    {
        return sizeToString(DEFAULT_CONFIG.getDataDiskUsageMaxDiskSize());
    }

    @Override
    public void setDataDiskUsageMaxDiskSize(@Nullable String size)
    {
        DEFAULT_CONFIG.setDataDiskUsageMaxDiskSize(sizeFromString(size));
    }

    @Override
    public int getMinimumReplicationFactorWarnThreshold()
    {
        return DEFAULT_CONFIG.getMinimumReplicationFactorWarnThreshold();
    }

    @Override
    public int getMinimumReplicationFactorFailThreshold()
    {
        return DEFAULT_CONFIG.getMinimumReplicationFactorFailThreshold();
    }

    @Override
    public void setMinimumReplicationFactorThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setMinimumReplicationFactorThreshold(warn, fail);
    }

    private static String toCSV(Set<String> values)
    {
        return values == null || values.isEmpty() ? "" : String.join(",", values);
    }

    private static <T> String toCSV(Set<T> values, Function<T, String> formatter)
    {
        return values == null || values.isEmpty() ? "" : values.stream().map(formatter).collect(Collectors.joining(","));
    }

    private static Set<String> fromCSV(String csv)
    {
        return StringUtils.isEmpty(csv) ? Collections.emptySet() : ImmutableSet.copyOf(csv.split(","));
    }

    private static <T> Set<T> fromCSV(String csv, Function<String, T> parser)
    {
        return StringUtils.isEmpty(csv) ? Collections.emptySet() : fromCSV(csv).stream().map(parser).collect(Collectors.toSet());
    }

    private static Set<String> toJmx(Set<ConsistencyLevel> set)
    {
        if (set == null)
            return null;
        return set.stream().map(ConsistencyLevel::name).collect(Collectors.toSet());
    }

    private static Set<ConsistencyLevel> fromJmx(Set<String> set)
    {
        if (set == null)
            return null;
        return set.stream().map(ConsistencyLevel::valueOf).collect(Collectors.toSet());
    }

    private static Long sizeToBytes(@Nullable DataStorageSpec.LongBytesBound size)
    {
        return size == null ? -1 : size.toBytes();
    }

    private static String sizeToString(@Nullable DataStorageSpec size)
    {
        return size == null ? null : size.toString();
    }

    private static DataStorageSpec.LongBytesBound sizeFromString(@Nullable String size)
    {
        return StringUtils.isEmpty(size) ? null : new DataStorageSpec.LongBytesBound(size);
    }
}
