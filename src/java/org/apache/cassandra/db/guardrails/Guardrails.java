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
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.GuardrailsOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.ClientState;
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
    public static final Guardrails instance = new Guardrails();

    /**
     * Guardrail on the total number of user keyspaces.
     */
    public static final MaxThreshold keyspaces =
    new MaxThreshold("keyspaces",
                     null,
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
                     null,
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
                     null,
                     state -> CONFIG_PROVIDER.getOrCreate(state).getColumnsPerTableWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getColumnsPerTableFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("The table %s has %s columns, this exceeds the warning threshold of %s.",
                                        what, value, threshold)
                               : format("Tables cannot have more than %s columns, but %s provided for table %s",
                                        threshold, value, what));

    public static final MaxThreshold secondaryIndexesPerTable =
    new MaxThreshold("secondary_indexes_per_table",
                     null,
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
    public static final EnableFlag createSecondaryIndexesEnabled =
    new EnableFlag("secondary_indexes",
                   null,
                   state -> CONFIG_PROVIDER.getOrCreate(state).getSecondaryIndexesEnabled(),
                   "User creation of secondary indexes");

    /**
     * Guardrail on the number of materialized views per table.
     */
    public static final MaxThreshold materializedViewsPerTable =
    new MaxThreshold("materialized_views_per_table",
                     null,
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
                 null,
                 state -> CONFIG_PROVIDER.getOrCreate(state).getTablePropertiesWarned(),
                 state -> CONFIG_PROVIDER.getOrCreate(state).getTablePropertiesIgnored(),
                 state -> CONFIG_PROVIDER.getOrCreate(state).getTablePropertiesDisallowed(),
                 "Table Properties");

    /**
     * Guardrail disabling user-provided timestamps.
     */
    public static final EnableFlag userTimestampsEnabled =
    new EnableFlag("user_timestamps",
                   null,
                   state -> CONFIG_PROVIDER.getOrCreate(state).getUserTimestampsEnabled(),
                   "User provided timestamps (USING TIMESTAMP)");

    public static final EnableFlag groupByEnabled =
    new EnableFlag("group_by",
                   null,
                   state -> CONFIG_PROVIDER.getOrCreate(state).getGroupByEnabled(),
                   "GROUP BY functionality");

    /**
     * Guardrail disabling ALTER TABLE column mutation access.
     */
    public static final EnableFlag alterTableEnabled =
    new EnableFlag("alter_table",
                   null,
                   state -> CONFIG_PROVIDER.getOrCreate(state).getAlterTableEnabled(),
                   "User access to ALTER TABLE statement for column mutation");

    /**
     * Guardrail disabling DROP / TRUNCATE TABLE behavior
     */
    public static final EnableFlag dropTruncateTableEnabled =
    new EnableFlag("drop_truncate_table_enabled",
                   null,
                   state -> CONFIG_PROVIDER.getOrCreate(state).getDropTruncateTableEnabled(),
                   "DROP and TRUNCATE TABLE functionality");

    /**
     * Guardrail disabling DROP KEYSPACE behavior
     */
    public static final EnableFlag dropKeyspaceEnabled =
    new EnableFlag("drop_keyspace_enabled",
                    null,
                    state -> CONFIG_PROVIDER.getOrCreate(state).getDropKeyspaceEnabled(),
                    "DROP KEYSPACE functionality");

    /**
     * Guardrail disabling user's ability to turn off compression
     */
    public static final EnableFlag uncompressedTablesEnabled =
    new EnableFlag("uncompressed_tables_enabled",
                   null,
                   state -> CONFIG_PROVIDER.getOrCreate(state).getUncompressedTablesEnabled(),
                   "Uncompressed table");

    /**
     * Guardrail disabling the creation of new COMPACT STORAGE tables
     */
    public static final EnableFlag compactTablesEnabled =
    new EnableFlag("compact_tables",
                   null,
                   state -> CONFIG_PROVIDER.getOrCreate(state).getCompactTablesEnabled(),
                   "Creation of new COMPACT STORAGE tables");

    /**
     * Guardrail to warn or fail a CREATE or ALTER TABLE statement when default_time_to_live is set to 0 and
     * the table is using TimeWindowCompactionStrategy compaction or a subclass of it.
     */
    public static final EnableFlag zeroTTLOnTWCSEnabled =
    new EnableFlag("zero_ttl_on_twcs",
                   "It is suspicious to use default_time_to_live set to 0 with such compaction strategy. " +
                   "Please keep in mind that data will not start to automatically expire after they are older " +
                   "than a respective compaction window unit of a certain size. Please set TTL for your INSERT or UPDATE " +
                   "statements if you expect data to be expired as table settings will not do it. ",
                   state -> CONFIG_PROVIDER.getOrCreate(state).getZeroTTLOnTWCSWarned(),
                   state -> CONFIG_PROVIDER.getOrCreate(state).getZeroTTLOnTWCSEnabled(),
                   "0 default_time_to_live on a table with " + TimeWindowCompactionStrategy.class.getSimpleName() + " compaction strategy");

    /**
     * Guardrail on the number of elements returned within page.
     */
    public static final MaxThreshold pageSize =
    new MaxThreshold("page_size",
                     null,
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
                     null,
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
    public static final EnableFlag readBeforeWriteListOperationsEnabled =
    new EnableFlag("read_before_write_list_operations",
                   null,
                   state -> CONFIG_PROVIDER.getOrCreate(state).getReadBeforeWriteListOperationsEnabled(),
                   "List operation requiring read before write");

    /**
     * Guardrail disabling ALLOW FILTERING statement within a query
     */
    public static final EnableFlag allowFilteringEnabled =
    new EnableFlag("allow_filtering",
                   "ALLOW FILTERING can potentially visit all the data in the table and have unpredictable performance.",
                   state -> CONFIG_PROVIDER.getOrCreate(state).getAllowFilteringEnabled(),
                   "Querying with ALLOW FILTERING");

    /**
     * Guardrail disabling setting SimpleStrategy via keyspace creation or alteration
     */
    public static final EnableFlag simpleStrategyEnabled =
    new EnableFlag("simplestrategy",
                   null,
                   state -> CONFIG_PROVIDER.getOrCreate(state).getSimpleStrategyEnabled(),
                   "SimpleStrategy");

    /**
     * Guardrail on the number of restrictions created by a cartesian product of a CQL's {@code IN} query.
     */
    public static final MaxThreshold inSelectCartesianProduct =
    new MaxThreshold("in_select_cartesian_product",
                     null,
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
                 null,
                 state -> CONFIG_PROVIDER.getOrCreate(state).getReadConsistencyLevelsWarned(),
                 state -> Collections.emptySet(),
                 state -> CONFIG_PROVIDER.getOrCreate(state).getReadConsistencyLevelsDisallowed(),
                 "read consistency levels");

    /**
     * Guardrail on write consistency levels.
     */
    public static final Values<ConsistencyLevel> writeConsistencyLevels =
    new Values<>("write_consistency_levels",
                 null,
                 state -> CONFIG_PROVIDER.getOrCreate(state).getWriteConsistencyLevelsWarned(),
                 state -> Collections.emptySet(),
                 state -> CONFIG_PROVIDER.getOrCreate(state).getWriteConsistencyLevelsDisallowed(),
                 "write consistency levels");

    /**
     * Guardrail on the size of a partition.
     */
    public static final MaxThreshold partitionSize =
    new MaxThreshold("partition_size",
                     "Too large partitions can cause performance problems.",
                     state -> sizeToBytes(CONFIG_PROVIDER.getOrCreate(state).getPartitionSizeWarnThreshold()),
                     state -> sizeToBytes(CONFIG_PROVIDER.getOrCreate(state).getPartitionSizeFailThreshold()),
                     (isWarning, what, value, threshold) ->
                             format("Partition %s has size %s, this exceeds the %s threshold of %s.",
                                    what, value, isWarning ? "warning" : "failure", threshold));

    /**
     * Guardrail on the number of rows of a partition.
     */
    public static final MaxThreshold partitionTombstones =
    new MaxThreshold("partition_tombstones",
                     "Partitions with too many tombstones can cause performance problems.",
                     state -> CONFIG_PROVIDER.getOrCreate(state).getPartitionTombstonesWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getPartitionTombstonesFailThreshold(),
                     (isWarning, what, value, threshold) ->
                             format("Partition %s has %s tombstones, this exceeds the %s threshold of %s.",
                                    what, value, isWarning ? "warning" : "failure", threshold));

    /**
     * Guardrail on the size of a collection.
     */
    public static final MaxThreshold columnValueSize =
    new MaxThreshold("column_value_size",
                     null,
                     state -> sizeToBytes(CONFIG_PROVIDER.getOrCreate(state).getColumnValueSizeWarnThreshold()),
                     state -> sizeToBytes(CONFIG_PROVIDER.getOrCreate(state).getColumnValueSizeFailThreshold()),
                     (isWarning, what, value, threshold) ->
                     format("Value of column %s has size %s, this exceeds the %s threshold of %s.",
                            what, value, isWarning ? "warning" : "failure", threshold));

    /**
     * Guardrail on the size of a collection.
     */
    public static final MaxThreshold collectionSize =
    new MaxThreshold("collection_size",
                     null,
                     state -> sizeToBytes(CONFIG_PROVIDER.getOrCreate(state).getCollectionSizeWarnThreshold()),
                     state -> sizeToBytes(CONFIG_PROVIDER.getOrCreate(state).getCollectionSizeFailThreshold()),
                     (isWarning, what, value, threshold) ->
                     format("Detected collection %s of size %s, this exceeds the %s threshold of %s.",
                            what, value, isWarning ? "warning" : "failure", threshold));

    /**
     * Guardrail on the number of items of a collection.
     */
    public static final MaxThreshold itemsPerCollection =
    new MaxThreshold("items_per_collection",
                     null,
                     state -> CONFIG_PROVIDER.getOrCreate(state).getItemsPerCollectionWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getItemsPerCollectionFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     format("Detected collection %s with %s items, this exceeds the %s threshold of %s.",
                            what, value, isWarning ? "warning" : "failure", threshold));

    /**
     * Guardrail on the number of fields on each UDT.
     */
    public static final MaxThreshold fieldsPerUDT =
    new MaxThreshold("fields_per_udt",
                     null,
                     state -> CONFIG_PROVIDER.getOrCreate(state).getFieldsPerUDTWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getFieldsPerUDTFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     isWarning ? format("The user type %s has %s columns, this exceeds the warning threshold of %s.",
                                        what, value, threshold)
                               : format("User types cannot have more than %s columns, but %s provided for user type %s.",
                                        threshold, value, what));

    /**
     * Guardrail on the number of dimensions of vector columns.
     */
    public static final MaxThreshold vectorDimensions =
    new MaxThreshold("vector_dimensions",
                     null,
                     state -> CONFIG_PROVIDER.getOrCreate(state).getVectorDimensionsWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getVectorDimensionsFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     format("%s has a vector of %s dimensions, this exceeds the %s threshold of %s.",
                            what, value, isWarning ? "warning" : "failure", threshold));

    /**
     * Guardrail on the data disk usage on the local node, used by a periodic task to calculate and propagate that status.
     * See {@link org.apache.cassandra.service.disk.usage.DiskUsageMonitor} and {@link DiskUsageBroadcaster}.
     */
    public static final PercentageThreshold localDataDiskUsage =
    new PercentageThreshold("local_data_disk_usage",
                            null,
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
                     null,
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
                     null,
                     state -> CONFIG_PROVIDER.getOrCreate(state).getMinimumReplicationFactorWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getMinimumReplicationFactorFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     format("The keyspace %s has a replication factor of %s, below the %s threshold of %s.",
                            what, value, isWarning ? "warning" : "failure", threshold));

    /**
     * Guardrail on the maximum replication factor.
     */
    public static final MaxThreshold maximumReplicationFactor =
    new MaxThreshold("maximum_replication_factor",
                     null,
                     state -> CONFIG_PROVIDER.getOrCreate(state).getMaximumReplicationFactorWarnThreshold(),
                     state -> CONFIG_PROVIDER.getOrCreate(state).getMaximumReplicationFactorFailThreshold(),
                     (isWarning, what, value, threshold) ->
                     format("The keyspace %s has a replication factor of %s, above the %s threshold of %s.",
                            what, value, isWarning ? "warning" : "failure", threshold));

    public static final MaxThreshold maximumAllowableTimestamp =
    new MaxThreshold("maximum_timestamp",
                     "Timestamps too far in the future can lead to data that can't be easily overwritten",
                     state -> maximumTimestampAsRelativeMicros(CONFIG_PROVIDER.getOrCreate(state).getMaximumTimestampWarnThreshold()),
                     state -> maximumTimestampAsRelativeMicros(CONFIG_PROVIDER.getOrCreate(state).getMaximumTimestampFailThreshold()),
                     (isWarning, what, value, threshold) ->
                    format("The modification to table %s has a timestamp %s after the maximum allowable %s threshold %s",
                           what, value, isWarning ? "warning" : "failure", threshold));

    public static final MinThreshold minimumAllowableTimestamp =
    new MinThreshold("minimum_timestamp",
                     "Timestamps too far in the past can cause writes can be unexpectedly lost",
                     state -> minimumTimestampAsRelativeMicros(CONFIG_PROVIDER.getOrCreate(state).getMinimumTimestampWarnThreshold()),
                     state -> minimumTimestampAsRelativeMicros(CONFIG_PROVIDER.getOrCreate(state).getMinimumTimestampFailThreshold()),
                     (isWarning, what, value, threshold) ->
                     format("The modification to table %s has a timestamp %s before the minimum allowable %s threshold %s",
                            what, value, isWarning ? "warning" : "failure", threshold));

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
    public boolean getAlterTableEnabled()
    {
        return DEFAULT_CONFIG.getAlterTableEnabled();
    }

    @Override
    public void setAlterTableEnabled(boolean enabled)
    {
        DEFAULT_CONFIG.setAlterTableEnabled(enabled);
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
    public boolean getSimpleStrategyEnabled()
    {
        return DEFAULT_CONFIG.getSimpleStrategyEnabled();
    }

    @Override
    public void setSimpleStrategyEnabled(boolean enabled)
    {
        DEFAULT_CONFIG.setSimpleStrategyEnabled(enabled);
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
    public boolean getDropKeyspaceEnabled()
    {
        return DEFAULT_CONFIG.getDropKeyspaceEnabled();
    }

    @Override
    public void setDropKeyspaceEnabled(boolean enabled)
    {
        DEFAULT_CONFIG.setDropKeyspaceEnabled(enabled);
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
    public String getPartitionSizeWarnThreshold()
    {
        return sizeToString(DEFAULT_CONFIG.getPartitionSizeWarnThreshold());
    }

    @Override
    @Nullable
    public String getPartitionSizeFailThreshold()
    {
        return sizeToString(DEFAULT_CONFIG.getPartitionSizeFailThreshold());
    }

    @Override
    public void setPartitionSizeThreshold(@Nullable String warnSize, @Nullable String failSize)
    {
        DEFAULT_CONFIG.setPartitionSizeThreshold(sizeFromString(warnSize), sizeFromString(failSize));
    }

    @Override
    public long getPartitionTombstonesWarnThreshold()
    {
        return DEFAULT_CONFIG.getPartitionTombstonesWarnThreshold();
    }

    @Override
    public long getPartitionTombstonesFailThreshold()
    {
        return DEFAULT_CONFIG.getPartitionTombstonesFailThreshold();
    }

    @Override
    public void setPartitionTombstonesThreshold(long warn, long fail)
    {
        DEFAULT_CONFIG.setPartitionTombstonesThreshold(warn, fail);
    }

    @Override
    @Nullable
    public String getColumnValueSizeWarnThreshold()
    {
        return sizeToString(DEFAULT_CONFIG.getColumnValueSizeWarnThreshold());
    }

    @Override
    @Nullable
    public String getColumnValueSizeFailThreshold()
    {
        return sizeToString(DEFAULT_CONFIG.getColumnValueSizeFailThreshold());
    }

    @Override
    public void setColumnValueSizeThreshold(@Nullable String warnSize, @Nullable String failSize)
    {
        DEFAULT_CONFIG.setColumnValueSizeThreshold(sizeFromString(warnSize), sizeFromString(failSize));
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
    public int getVectorDimensionsWarnThreshold()
    {
        return DEFAULT_CONFIG.getVectorDimensionsWarnThreshold();
    }

    @Override
    public int getVectorDimensionsFailThreshold()
    {
        return DEFAULT_CONFIG.getVectorDimensionsFailThreshold();
    }

    @Override
    public void setVectorDimensionsThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setVectorDimensionsThreshold(warn, fail);
    }

    @Override
    public int getMaximumReplicationFactorWarnThreshold()
    {
        return DEFAULT_CONFIG.getMaximumReplicationFactorWarnThreshold();
    }

    @Override
    public int getMaximumReplicationFactorFailThreshold()
    {
        return DEFAULT_CONFIG.getMaximumReplicationFactorFailThreshold();
    }

    @Override
    public void setMaximumReplicationFactorThreshold (int warn, int fail)
    {
        DEFAULT_CONFIG.setMaximumReplicationFactorThreshold(warn, fail);
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

    @Override
    public boolean getZeroTTLOnTWCSEnabled()
    {
        return DEFAULT_CONFIG.getZeroTTLOnTWCSEnabled();
    }

    @Override
    public void setZeroTTLOnTWCSEnabled(boolean value)
    {
        DEFAULT_CONFIG.setZeroTTLOnTWCSEnabled(value);
    }

    @Override
    public boolean getZeroTTLOnTWCSWarned()
    {
        return DEFAULT_CONFIG.getZeroTTLOnTWCSWarned();
    }

    @Override
    public void setZeroTTLOnTWCSWarned(boolean value)
    {
        DEFAULT_CONFIG.setZeroTTLOnTWCSWarned(value);
    }

    @Override
    public String getMaximumTimestampWarnThreshold()
    {
        return durationToString(DEFAULT_CONFIG.getMaximumTimestampWarnThreshold());
    }

    @Override
    public String getMaximumTimestampFailThreshold()
    {
        return durationToString(DEFAULT_CONFIG.getMaximumTimestampFailThreshold());
    }

    @Override
    public void setMaximumTimestampThreshold(String warnSeconds, String failSeconds)
    {
        DEFAULT_CONFIG.setMaximumTimestampThreshold(durationFromString(warnSeconds), durationFromString(failSeconds));
    }

    @Override
    public String getMinimumTimestampWarnThreshold()
    {
        return durationToString(DEFAULT_CONFIG.getMinimumTimestampWarnThreshold());
    }

    @Override
    public String getMinimumTimestampFailThreshold()
    {
        return durationToString(DEFAULT_CONFIG.getMinimumTimestampFailThreshold());
    }

    @Override
    public void setMinimumTimestampThreshold(String warnSeconds, String failSeconds)
    {
        DEFAULT_CONFIG.setMinimumTimestampThreshold(durationFromString(warnSeconds), durationFromString(failSeconds));
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

    private static String durationToString(@Nullable DurationSpec duration)
    {
        return duration == null ? null : duration.toString();
    }

    private static DurationSpec.LongMicrosecondsBound durationFromString(@Nullable String duration)
    {
        return StringUtils.isEmpty(duration) ? null : new DurationSpec.LongMicrosecondsBound(duration);
    }

    private static long maximumTimestampAsRelativeMicros(@Nullable DurationSpec.LongMicrosecondsBound duration)
    {
        return duration == null
               ? Long.MAX_VALUE
               : (ClientState.getLastTimestampMicros() + duration.toMicroseconds());
    }

    private static long minimumTimestampAsRelativeMicros(@Nullable DurationSpec.LongMicrosecondsBound duration)
    {
        return duration == null
               ? Long.MIN_VALUE
               : (ClientState.getLastTimestampMicros() - duration.toMicroseconds());
    }
}
