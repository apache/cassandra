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

package org.apache.cassandra.repair;

import java.io.Serializable;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;

public class AutoRepairConfig implements Serializable
{
    // enable/disable auto repair globally, overrides all other settings. Cannot be modified dynamically.
    public final Boolean enabled;
    // the interval in seconds between checks for eligible repair operations. Cannot be modified dynamically.
    public final Integer repair_check_interval_in_sec = 300; // 5 minutes
    // configures how long repair history is kept for a replaced node
    public volatile Integer history_clear_delete_hosts_buffer_in_sec = 60 * 60 * 2;  // two hours
    // global_settings overides Options.defaultOptions for all repair types
    public volatile Options global_settings;

    public enum RepairType
    {full, incremental}

    // repair_type_overrides overrides the global_settings for a specific repair type
    public volatile Map<RepairType, Options> repair_type_overrides = new EnumMap<>(RepairType.class);

    public AutoRepairConfig()
    {
        this(false);
    }

    public AutoRepairConfig(boolean enabled)
    {
        this.enabled = enabled;
        global_settings = Options.getDefaultOptions();
        for (RepairType type : RepairType.values())
        {
            repair_type_overrides.put(type, new Options());
        }
    }

    public int getRepairCheckIntervalInSec()
    {
        return repair_check_interval_in_sec;
    }

    public boolean isAutoRepairSchedulingEnabled()
    {
        return enabled;
    }

    public int getAutoRepairHistoryClearDeleteHostsBufferInSec()
    {
        return history_clear_delete_hosts_buffer_in_sec;
    }

    public void setAutoRepairHistoryClearDeleteHostsBufferInSec(int seconds)
    {
        history_clear_delete_hosts_buffer_in_sec = seconds;
    }

    public boolean isAutoRepairEnabled(RepairType repairType)
    {
        return enabled && applyOverrides(repairType, opt -> opt.enabled);
    }

    public void setAutoRepairEnabled(RepairType repairType, boolean enabled)
    {
        if (enabled && repairType == RepairType.incremental &&
            (DatabaseDescriptor.getMaterializedViewsEnabled() || DatabaseDescriptor.isCDCEnabled()))
            throw new ConfigurationException("Cannot enable incremental repair with materialized views or CDC enabled");

        repair_type_overrides.get(repairType).enabled = enabled;
    }

    public void setRepairByKeyspace(RepairType repairType, boolean repairByKeyspace)
    {
        repair_type_overrides.get(repairType).repair_by_keyspace = repairByKeyspace;
    }

    public boolean getRepairByKeyspace(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.repair_by_keyspace);
    }

    public int getRepairThreads(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.number_of_repair_threads);
    }

    public void setRepairThreads(RepairType repairType, int repairThreads)
    {
        repair_type_overrides.get(repairType).number_of_repair_threads = repairThreads;
    }

    public int getRepairSubRangeNum(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.number_of_subranges);
    }

    public void setRepairSubRangeNum(RepairType repairType, int repairSubRanges)
    {
        repair_type_overrides.get(repairType).number_of_subranges = repairSubRanges;
    }

    public int getRepairMinIntervalInHours(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.min_repair_interval_in_hours);
    }

    public void setRepairMinIntervalInHours(RepairType repairType, int repairMinFrequencyInHours)
    {
        repair_type_overrides.get(repairType).min_repair_interval_in_hours = repairMinFrequencyInHours;
    }

    public int getRepairSSTableCountHigherThreshold(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.sstable_upper_threshold);
    }

    public void setRepairSSTableCountHigherThreshold(RepairType repairType, int sstableHigherThreshold)
    {
        repair_type_overrides.get(repairType).sstable_upper_threshold = sstableHigherThreshold;
    }

    public String getRepairIgnoreKeyspaces(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.ignore_keyspaces);
    }

    public void setRepairIgnoreKeyspaces(RepairType repairType, String ignoreKeyspace)
    {
        repair_type_overrides.get(repairType).ignore_keyspaces = ignoreKeyspace;
    }

    public String getRepairOnlyKeyspaces(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.repair_only_keyspaces);
    }

    public void setRepairOnlyKeyspaces(RepairType repairType, String repairOnlyKeyspaces)
    {
        repair_type_overrides.get(repairType).repair_only_keyspaces = repairOnlyKeyspaces;
    }

    public long getAutoRepairTableMaxRepairTimeInSec(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.table_max_repair_time_in_sec);
    }

    public void setAutoRepairTableMaxRepairTimeInSec(RepairType repairType, long autoRepairTableMaxRepairTimeInSec)
    {
        repair_type_overrides.get(repairType).table_max_repair_time_in_sec = autoRepairTableMaxRepairTimeInSec;
    }

    public Set<String> getIgnoreDCs(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.ignore_dcs);
    }

    public void setIgnoreDCs(RepairType repairType, Set<String> ignoreDCs)
    {
        repair_type_overrides.get(repairType).ignore_dcs = ignoreDCs;
    }

    public void setDCGroups(RepairType repairType, Set<String> dcGroups)
    {
        repair_type_overrides.get(repairType).repair_dc_groups = dcGroups;
    }

    public Set<String> getDCGroups(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.repair_dc_groups);
    }

    public boolean getRepairPrimaryTokenRangeOnly(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.repair_primary_token_range_only);
    }

    public void setRepairPrimaryTokenRangeOnly(RepairType repairType, boolean primaryTokenRangeOnly)
    {
        repair_type_overrides.get(repairType).repair_primary_token_range_only = primaryTokenRangeOnly;
    }

    public int getParallelRepairPercentageInGroup(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.parallel_repair_percentage_in_group);
    }

    public void setParallelRepairPercentageInGroup(RepairType repairType, int percentageInGroup)
    {
        repair_type_overrides.get(repairType).parallel_repair_percentage_in_group = percentageInGroup;
    }

    public int getParallelRepairCountInGroup(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.parallel_repair_count_in_group);
    }

    public void setParallelRepairCountInGroup(RepairType repairType, int countInGroup)
    {
        repair_type_overrides.get(repairType).parallel_repair_count_in_group = countInGroup;
    }

    public boolean getMVRepairEnabled(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.mv_repair_enabled);
    }

    public void setMVRepairEnabled(RepairType repairType, boolean enabled)
    {
        repair_type_overrides.get(repairType).mv_repair_enabled = enabled;
    }

    public void setForceRepairNewNode(RepairType repairType, boolean forceRepairNewNode)
    {
        repair_type_overrides.get(repairType).force_repair_new_node = forceRepairNewNode;
    }

    public boolean getForceRepairNewNode(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.force_repair_new_node);
    }

    // Options configures auto-repair behavior for a given repair type.
    // The function of each of these fields is described here: https://docs.google.com/document/d/1Z1d27moU9yPT-r_1JhpcO_3ws-aQkUww5JA0abwK9dE/edit#heading=h.izpcb3d6hq4n
    // All fields can be modified dynamically.
    public static class Options implements Serializable
    {
        // The separator separating different DCs in repair_dc_groups
        public static final String DC_GROUP_SEPARATOR = "\\|";

        // defaultOptions defines the default auto-repair behavior when no overrides are defined
        @VisibleForTesting
        protected static final Options defaultOptions = getDefaultOptions();

        public Options()
        {
        }

        @VisibleForTesting
        protected static Options getDefaultOptions()
        {
            Options opts = new Options();

            opts.enabled = false;
            opts.repair_by_keyspace = false;
            opts.number_of_subranges = 1;
            opts.number_of_repair_threads = 1;
            opts.parallel_repair_count_in_group = 1;
            opts.parallel_repair_percentage_in_group = 0;
            opts.sstable_upper_threshold = 10000;
            opts.ignore_keyspaces = "\\b(?!system_auth\\b)system\\w+|.*staging.*|.*test.*|health|pingless";
            opts.repair_only_keyspaces = "";
            opts.min_repair_interval_in_hours = 24;
            opts.ignore_dcs = new HashSet<>();
            opts.repair_primary_token_range_only = true;
            opts.repair_dc_groups = new HashSet<>();
            opts.force_repair_new_node = false;
            opts.table_max_repair_time_in_sec = 6 * 60 * 60L; // six hours
            opts.mv_repair_enabled = true;

            return opts;
        }

        // enable/disable auto repair for the given repair type
        public volatile Boolean enabled;
        // auto repair is default repair table by table, if this is enabled, we will repair keyspace by keyspace
        public volatile Boolean repair_by_keyspace;
        // the number of subranges to split each to-be-repaired token range into,
        // the higher this number, the smaller the repair sessions will be
        public volatile Integer number_of_subranges;
        // the number of repair threads to run for the given repair type
        public volatile Integer number_of_repair_threads;
        // the number of repair sessions that can run in parallel in a single group
        public volatile Integer parallel_repair_count_in_group;
        // the number of repair sessions that can run in parallel in a single groupas a percentage
        // of the total number of nodes in the group [0,100]
        public volatile Integer parallel_repair_percentage_in_group;
        // the upper threshold of SSTables allowed to participate in a single repair session
        public volatile Integer sstable_upper_threshold;
        // specifies a denylist of keyspaces to repair
        public volatile String ignore_keyspaces;
        // specifies an allowlist of keyspaces to repair
        public volatile String repair_only_keyspaces;
        // the minimum time in hours between repairs of the same token range
        public volatile Integer min_repair_interval_in_hours;
        // specifies a denylist of datacenters to repair
        public volatile Set<String> ignore_dcs;
        // Set this 'true' if AutoRepair should repair only the primary ranges owned by this node; else, 'false'
        public volatile Boolean repair_primary_token_range_only;
        // by default, the value is empty, it means the all nodes are in one ring and the repair in that ring should be done
        // node by node. Adding this config for special case(cstar-peloton) that certain keyspace data is only stored in
        // certain cluster. We can run multiple node repair at the same time without worrying about the conflict. For example,
        // if the value is "dca1,phx2|dca11|phx3|phx4,dca5,dca6", there will be 4 groups {dca1, phx2}, {dca11}, {phx3} and
        // {phx4, dca5, dca6}. This means we can run repair parallely on 4 nodes, each in one group.
        public volatile Set<String> repair_dc_groups;
        // configures whether to force immediate repair on new nodes
        public volatile Boolean force_repair_new_node;
        // the maximum time in seconds that a repair session can run for a single table
        public volatile Long table_max_repair_time_in_sec;
        /**
         * MVs are mutated at LOCAL_ONE consistency level. By default, historically, we have not been running full repair on MV tables.
         * Due to that, on the server side, MV replicas are out of sync, which leads to inconsistencies when reading from MV itself.
         * <p>
         * This flag determines whether we need to run anti-entropy, a.k.a, repair on the MV table or not.
         */
        public volatile Boolean mv_repair_enabled;
    }

    @VisibleForTesting
    protected <T> T applyOverrides(RepairType repairType, Function<Options, T> optionSupplier)
    {
        return Stream.of(repair_type_overrides.get(repairType), global_settings, Options.defaultOptions)
                     .map(opt -> Optional.ofNullable(opt).map(optionSupplier).orElse(null))
                     .filter(Objects::nonNull)
                     .findFirst()
                     .orElse(null);
    }
}
