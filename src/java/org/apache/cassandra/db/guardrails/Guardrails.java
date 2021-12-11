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
     * Guardrail on the total number of tables on user keyspaces.
     */
    public static final Threshold tablesLimit =
    new Threshold(state -> CONFIG_PROVIDER.getOrCreate(state).getTables(),
                  (isWarning, what, value, threshold) ->
                  isWarning ? format("Creating table %s, current number of tables %s exceeds warning threshold of %s.",
                                     what, value, threshold)
                            : format("Cannot have more than %s tables, aborting the creation of table %s",
                                     threshold, what));

    /**
     * Guardrail on the number of columns per table.
     */
    public static final Threshold columnsPerTable =
    new Threshold(state -> CONFIG_PROVIDER.getOrCreate(state).getColumnsPerTable(),
                  (isWarning, what, value, threshold) ->
                  isWarning ? format("The table %s has %s columns, this exceeds the warning threshold of %s.",
                                     what, value, threshold)
                            : format("Tables cannot have more than %s columns, but %s provided for table %s",
                                     threshold, value, what));

    public static final Threshold secondaryIndexesPerTable =
    new Threshold(state -> CONFIG_PROVIDER.getOrCreate(state).getSecondaryIndexesPerTable(),
                  (isWarning, what, value, threshold) ->
                  isWarning ? format("Creating secondary index %s, current number of indexes %s exceeds warning threshold of %s.",
                                     what, value, threshold)
                            : format("Tables cannot have more than %s secondary indexes, aborting the creation of secondary index %s",
                                     threshold, what));

    /**
     * Guardrail on the number of materialized views per table.
     */
    public static final Threshold materializedViewsPerTable =
    new Threshold(state -> CONFIG_PROVIDER.getOrCreate(state).getMaterializedViewsPerTable(),
                  (isWarning, what, value, threshold) ->
                  isWarning ? format("Creating materialized view %s, current number of views %s exceeds warning threshold of %s.",
                                     what, value, threshold)
                            : format("Tables cannot have more than %s materialized views, aborting the creation of materialized view %s",
                                     threshold, what));

    /**
     * Guardrail ignoring/disallowing the usage of certain table properties.
     */
    public static final Values<String> tableProperties =
    new Values<>(state -> CONFIG_PROVIDER.getOrCreate(state).getTableProperties(),
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
    new Threshold(state -> CONFIG_PROVIDER.getOrCreate(state).getPageSize(),
                  (isWarning, what, value, threshold) ->
                  isWarning ? format("Query for table %s with page size %s exceeds warning threshold of %s.",
                                     what, value, threshold)
                            : format("Aborting query for table %s, page size %s exceeds abort threshold of %s.",
                                     what, value, threshold));


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
    public int getTablesWarnThreshold()
    {
        return (int) DEFAULT_CONFIG.getTables().getWarnThreshold();
    }

    @Override
    public int getTablesAbortThreshold()
    {
        return (int) DEFAULT_CONFIG.getTables().getAbortThreshold();
    }

    @Override
    public void setTablesThreshold(int warn, int abort)
    {
        DEFAULT_CONFIG.getTables().setThresholds(warn, abort);
    }

    @Override
    public int getColumnsPerTableWarnThreshold()
    {
        return (int) DEFAULT_CONFIG.getColumnsPerTable().getWarnThreshold();
    }

    @Override
    public int getColumnsPerTableAbortThreshold()
    {
        return (int) DEFAULT_CONFIG.getColumnsPerTable().getAbortThreshold();
    }

    @Override
    public void setColumnsPerTableThreshold(int warn, int abort)
    {
        DEFAULT_CONFIG.getColumnsPerTable().setThresholds(warn, abort);
    }

    @Override
    public int getSecondaryIndexesPerTableWarnThreshold()
    {
        return (int) DEFAULT_CONFIG.getSecondaryIndexesPerTable().getWarnThreshold();
    }

    @Override
    public int getSecondaryIndexesPerTableAbortThreshold()
    {
        return (int) DEFAULT_CONFIG.getSecondaryIndexesPerTable().getAbortThreshold();
    }

    @Override
    public void setSecondaryIndexesPerTableThreshold(int warn, int abort)
    {
        DEFAULT_CONFIG.getSecondaryIndexesPerTable().setThresholds(warn, abort);
    }

    @Override
    public int getMaterializedViewsPerTableWarnThreshold()
    {
        return (int) DEFAULT_CONFIG.getMaterializedViewsPerTable().getWarnThreshold();
    }

    @Override
    public int getMaterializedViewsPerTableAbortThreshold()
    {
        return (int) DEFAULT_CONFIG.getMaterializedViewsPerTable().getAbortThreshold();
    }

    @Override
    public void setMaterializedViewsPerTableThreshold(int warn, int abort)
    {
        DEFAULT_CONFIG.getMaterializedViewsPerTable().setThresholds(warn, abort);
    }

    @Override
    public Set<String> getTablePropertiesDisallowed()
    {
        return DEFAULT_CONFIG.getTableProperties().getDisallowed();
    }

    @Override
    public String getTablePropertiesDisallowedCSV()
    {
        return toCSV(DEFAULT_CONFIG.getTableProperties().getDisallowed());
    }

    public void setTablePropertiesDisallowed(String... properties)
    {
        setTablePropertiesDisallowed(ImmutableSet.copyOf(properties));
    }

    @Override
    public void setTablePropertiesDisallowed(Set<String> properties)
    {
        DEFAULT_CONFIG.getTableProperties().setDisallowedValues(properties);
    }

    @Override
    public void setTablePropertiesDisallowedCSV(String properties)
    {
        setTablePropertiesDisallowed(fromCSV(properties));
    }

    @Override
    public Set<String> getTablePropertiesIgnored()
    {
        return DEFAULT_CONFIG.getTableProperties().getIgnored();
    }

    @Override
    public String getTablePropertiesIgnoredCSV()
    {
        return toCSV(DEFAULT_CONFIG.getTableProperties().getIgnored());
    }

    public void setTablePropertiesIgnored(String... properties)
    {
        setTablePropertiesIgnored(ImmutableSet.copyOf(properties));
    }

    @Override
    public void setTablePropertiesIgnored(Set<String> properties)
    {
        DEFAULT_CONFIG.getTableProperties().setIgnoredValues(properties);
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
        return (int) DEFAULT_CONFIG.getPageSize().getWarnThreshold();
    }

    @Override
    public int getPageSizeAbortThreshold()
    {
        return (int) DEFAULT_CONFIG.getPageSize().getAbortThreshold();
    }

    @Override
    public void setPageSizeThreshold(int warn, int abort)
    {
        DEFAULT_CONFIG.getPageSize().setThresholds(warn, abort);
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
