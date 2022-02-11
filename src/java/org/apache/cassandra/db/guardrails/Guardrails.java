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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.GuardrailsOptions;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.MBeanWrapper;

import static java.lang.String.format;

/**
 * Entry point for Guardrails, storing the defined guardrails and providing a few global methods over them.
 */
public final class Guardrails implements GuardrailsMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=Guardrails";

    private static final GuardrailsConfigProvider CONFIG_PROVIDER = GuardrailsConfigProvider.instance;
    private static final GuardrailsOptions DEFAULT_CONFIG = DatabaseDescriptor.getGuardrailsConfig();

    @VisibleForTesting
    static final Guardrails instance = new Guardrails();

    /**
     * Guardrail on the total number of user keyspaces.
     */
    public static final Threshold keyspaces =
    new Threshold(state -> CONFIG_PROVIDER.getOrCreate(state).getKeyspacesWarnThreshold(),
                  state -> CONFIG_PROVIDER.getOrCreate(state).getKeyspacesFailThreshold(),
                  (isWarning, what, value, threshold) ->
                  isWarning ? format("Creating keyspace %s, current number of keyspaces %s exceeds warning threshold of %s.",
                                     what, value, threshold)
                            : format("Cannot have more than %s keyspaces, aborting the creation of keyspace %s",
                                     threshold, what));

    /**
     * Guardrail on the total number of tables on user keyspaces.
     */
    public static final Threshold tables =
    new Threshold(state -> CONFIG_PROVIDER.getOrCreate(state).getTablesWarnThreshold(),
                  state -> CONFIG_PROVIDER.getOrCreate(state).getTablesFailThreshold(),
                  (isWarning, what, value, threshold) ->
                  isWarning ? format("Creating table %s, current number of tables %s exceeds warning threshold of %s.",
                                     what, value, threshold)
                            : format("Cannot have more than %s tables, aborting the creation of table %s",
                                     threshold, what));

    /**
     * Guardrail on the number of columns per table.
     */
    public static final Threshold columnsPerTable =
    new Threshold(state -> CONFIG_PROVIDER.getOrCreate(state).getColumnsPerTableWarnThreshold(),
                  state -> CONFIG_PROVIDER.getOrCreate(state).getColumnsPerTableFailThreshold(),
                  (isWarning, what, value, threshold) ->
                  isWarning ? format("The table %s has %s columns, this exceeds the warning threshold of %s.",
                                     what, value, threshold)
                            : format("Tables cannot have more than %s columns, but %s provided for table %s",
                                     threshold, value, what));

    public static final Threshold secondaryIndexesPerTable =
    new Threshold(state -> CONFIG_PROVIDER.getOrCreate(state).getSecondaryIndexesPerTableWarnThreshold(),
                  state -> CONFIG_PROVIDER.getOrCreate(state).getSecondaryIndexesPerTableFailThreshold(),
                  (isWarning, what, value, threshold) ->
                  isWarning ? format("Creating secondary index %s, current number of indexes %s exceeds warning threshold of %s.",
                                     what, value, threshold)
                            : format("Tables cannot have more than %s secondary indexes, aborting the creation of secondary index %s",
                                     threshold, what));

    /**
     * Guardrail on the number of materialized views per table.
     */
    public static final Threshold materializedViewsPerTable =
    new Threshold(state -> CONFIG_PROVIDER.getOrCreate(state).getMaterializedViewsPerTableWarnThreshold(),
                  state -> CONFIG_PROVIDER.getOrCreate(state).getMaterializedViewsPerTableFailThreshold(),
                  (isWarning, what, value, threshold) ->
                  isWarning ? format("Creating materialized view %s, current number of views %s exceeds warning threshold of %s.",
                                     what, value, threshold)
                            : format("Tables cannot have more than %s materialized views, aborting the creation of materialized view %s",
                                     threshold, what));

    /**
     * Guardrail ignoring/disallowing the usage of certain table properties.
     */
    public static final Values<String> tableProperties =
    new Values<>(state -> CONFIG_PROVIDER.getOrCreate(state).getTablePropertiesIgnored(),
                 state -> CONFIG_PROVIDER.getOrCreate(state).getTablePropertiesDisallowed(),
                 "Table Properties");

    /**
     * Guardrail disabling user-provided timestamps.
     */
    public static final DisableFlag userTimestampsEnabled =
    new DisableFlag(state -> !CONFIG_PROVIDER.getOrCreate(state).getUserTimestampsEnabled(),
                    "User provided timestamps (USING TIMESTAMP)");

    /**
     * Guardrail on the number of elements returned within page.
     */
    public static final Threshold pageSize =
    new Threshold(state -> CONFIG_PROVIDER.getOrCreate(state).getPageSizeWarnThreshold(),
                  state -> CONFIG_PROVIDER.getOrCreate(state).getPageSizeFailThreshold(),
                  (isWarning, what, value, threshold) ->
                  isWarning ? format("Query for table %s with page size %s exceeds warning threshold of %s.",
                                     what, value, threshold)
                            : format("Aborting query for table %s, page size %s exceeds fail threshold of %s.",
                                     what, value, threshold));

    /**
     * Guardrail on the number of partition keys in the IN clause.
     */
    public static final Threshold partitionKeysInSelect =
    new Threshold(state -> CONFIG_PROVIDER.getOrCreate(state).getPartitionKeysInSelectWarnThreshold(),
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
    new DisableFlag(state -> !CONFIG_PROVIDER.getOrCreate(state).getReadBeforeWriteListOperationsEnabled(),
                    "List operation requiring read before write");

    private Guardrails()
    {
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    /**
     * Whether guardrails are enabled.
     *
     * @return {@code true} if guardrails are enabled and daemon is initialized,
     * {@code false} otherwise (in which case no guardrail will trigger).
     */
    public static boolean enabled(ClientState state)
    {
        return CONFIG_PROVIDER.getOrCreate(state).getEnabled() && DatabaseDescriptor.isDaemonInitialized();
    }

    @Override
    public boolean getEnabled()
    {
        return DEFAULT_CONFIG.getEnabled();
    }

    @Override
    public void setEnabled(boolean enabled)
    {
        DEFAULT_CONFIG.setEnabled(enabled);
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
    public void setPartitionKeysInSelectThreshold(int warn, int fail)
    {
        DEFAULT_CONFIG.setPartitionKeysInSelectThreshold(warn, fail);
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

    private static String toCSV(Set<String> values)
    {
        return values == null ? "" : String.join(",", values);
    }

    private static Set<String> fromCSV(String csv)
    {
        return csv == null ? null : ImmutableSet.copyOf(csv.split(","));
    }
}
