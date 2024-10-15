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

package org.apache.cassandra.repair.autorepair;

import java.io.Serializable;
import java.util.ArrayList;
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

        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).enabled = enabled;
    }

    public void setRepairByKeyspace(RepairType repairType, boolean repairByKeyspace)
    {
        ensureOverrides(repairType);
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
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).number_of_repair_threads = repairThreads;
    }

    public int getRepairSubRangeNum(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.number_of_subranges);
    }

    public void setRepairSubRangeNum(RepairType repairType, int repairSubRanges)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).number_of_subranges = repairSubRanges;
    }

    public int getRepairMinIntervalInHours(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.min_repair_interval_in_hours);
    }

    public void setRepairMinIntervalInHours(RepairType repairType, int repairMinFrequencyInHours)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).min_repair_interval_in_hours = repairMinFrequencyInHours;
    }

    public int getRepairSSTableCountHigherThreshold(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.sstable_upper_threshold);
    }

    public void setRepairSSTableCountHigherThreshold(RepairType repairType, int sstableHigherThreshold)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).sstable_upper_threshold = sstableHigherThreshold;
    }

    public long getAutoRepairTableMaxRepairTimeInSec(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.table_max_repair_time_in_sec);
    }

    public void setAutoRepairTableMaxRepairTimeInSec(RepairType repairType, long autoRepairTableMaxRepairTimeInSec)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).table_max_repair_time_in_sec = autoRepairTableMaxRepairTimeInSec;
    }

    public Set<String> getIgnoreDCs(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.ignore_dcs);
    }

    public void setIgnoreDCs(RepairType repairType, Set<String> ignoreDCs)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).ignore_dcs = ignoreDCs;
    }

    public void setDCGroups(RepairType repairType, Set<String> dcGroups)
    {
        ensureOverrides(repairType);
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
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).repair_primary_token_range_only = primaryTokenRangeOnly;
    }

    public int getParallelRepairPercentageInGroup(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.parallel_repair_percentage_in_group);
    }

    public void setParallelRepairPercentageInGroup(RepairType repairType, int percentageInGroup)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).parallel_repair_percentage_in_group = percentageInGroup;
    }

    public int getParallelRepairCountInGroup(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.parallel_repair_count_in_group);
    }

    public void setParallelRepairCountInGroup(RepairType repairType, int countInGroup)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).parallel_repair_count_in_group = countInGroup;
    }

    public boolean getMVRepairEnabled(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.mv_repair_enabled);
    }

    public void setMVRepairEnabled(RepairType repairType, boolean enabled)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).mv_repair_enabled = enabled;
    }

    public void setForceRepairNewNode(RepairType repairType, boolean forceRepairNewNode)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).force_repair_new_node = forceRepairNewNode;
    }

    public boolean getForceRepairNewNode(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.force_repair_new_node);
    }

    public String getTokenRangeSplitter(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.token_range_splitter);
    }

    // Options configures auto-repair behavior for a given repair type.
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
            opts.min_repair_interval_in_hours = 24;
            opts.ignore_dcs = new HashSet<>();
            opts.repair_primary_token_range_only = true;
            opts.repair_dc_groups = new HashSet<>();
            opts.force_repair_new_node = false;
            opts.table_max_repair_time_in_sec = 6 * 60 * 60L; // six hours
            opts.mv_repair_enabled = true;
            opts.token_range_splitter = DefaultAutoRepairTokenSplitter.class.getName();

            return opts;
        }

        // enable/disable auto repair for the given repair type
        public volatile Boolean enabled;
        // auto repair is default repair table by table, if this is enabled, the framework will repair all the tables in a keyspace in one go.
        public volatile Boolean repair_by_keyspace;
        // the number of subranges to split each to-be-repaired token range into,
        // the higher this number, the smaller the repair sessions will be
        // How many subranges to divide one range into? The default is 1.
        // If you are using v-node, say 256, then the repair will always go one v-node range at a time, this parameter, additionally, will let us further subdivide a given v-node range into sub-ranges.
        // With the value “1” and v-nodes of 256, a given table on a node will undergo the repair 256 times. But with a value “2,” the same table on a node will undergo a repair 512 times because every v-node range will be further divided by two.
        // If you do not use v-nodes or the number of v-nodes is pretty small, say 8, setting this value to a higher number, say 16, will be useful to repair on a smaller range, and the chance of succeeding is higher.
        public volatile Integer number_of_subranges;
        // the number of repair threads to run for a given invoked Repair Job.
        // Once the scheduler schedules one Job, then howmany threads to use inside that job will be controlled through this parameter.
        // This is similar to -j for repair options for the nodetool repair command.
        public volatile Integer number_of_repair_threads;
        // the number of repair sessions that can run in parallel in a single group
        // The number of nodes running repair parallelly. If parallelrepaircount is set, it will choose the larger value of the two. The default is 3.
        // This configuration controls how many nodes would run repair in parallel.
        // The value “3” means, at any given point in time, at most 3 nodes would be running repair in parallel.
        // If one or more node(s) finish repair, then the framework automatically picks up the next candidate and ensures the maximum number of nodes running repair do not exceed “3”.
        public volatile Integer parallel_repair_count_in_group;
        // the number of repair sessions that can run in parallel in a single groupas a percentage
        // of the total number of nodes in the group [0,100]
        // The percentage of nodes in the cluster that run repair parallelly. If parallelrepaircount is set, it will choose the larger value of the two.
        //The problem with a fixed number of nodes (the above property) is that in a large-scale environment,
        // the nodes keep getting added/removed due to elasticity, so if we have a fixed number, then manual interventions would increase because, on a continuous basis,operators would have to adjust to meet the SLA.
        //The default is 3%, which means that 3% of the nodes in the Cassandra cluster would be repaired in parallel.
        // So now, if a fleet, an operator won't have to worry about changing the repair frequency, etc., as overall repair time will continue to remain the same even if nodes are added or removed due to elasticity.
        // Extremely fewer manual interventions as it will rarely violate the repair SLA for customers
        public volatile Integer parallel_repair_percentage_in_group;
        // the upper threshold of SSTables allowed to participate in a single repair session
        // Threshold to skip a table if it has too many sstables. The default is 10000. This means, if a table on a node has 10000 or more SSTables, then that table will be skipped.
        // This is to avoid penalizing good neighbors with an outlier.
        public volatile Integer sstable_upper_threshold;
        // the minimum time in hours between repairs of the same token range
        // The minimum number of hours to run one repair cycle is 24 hours. The default is 24 hours.
        // This means that if auto repair finishes one round on one cluster within 24 hours, it won’t start a new round.
        // This is applicable for extremely tiny clusters, say 3 nodes.
        public volatile Integer min_repair_interval_in_hours;
        // specifies a denylist of datacenters to repair
        // This is useful if you want to completely avoid running repairs in one or more data centers. By default, it is empty, i.e., the framework will repair nodes in all the datacenters.
        public volatile Set<String> ignore_dcs;
        // Set this 'true' if AutoRepair should repair only the primary ranges owned by this node; else, 'false'
        // It is the same as -pr in nodetool repair options.
        public volatile Boolean repair_primary_token_range_only;
        // By default, the value is empty, which means all nodes are in one ring and the repair in that ring should be done node by node.
        // Adding this config for special cases where certain keyspace data is only stored in
        // certain data centers. We can run multiple node repairs at the same time.
        // For example, if the value is “dc1,dc2|dc3|dc4|dc5,dc6,dc7”, there will be 4 groups {dc1, dc2}, {dc3}, {dc4} and {dc5, dc6, dc7}.
        // This means we can run repair parallely on 4 nodes, each in one group.
        public volatile Set<String> repair_dc_groups;
        // configures whether to force immediate repair on new nodes
        public volatile Boolean force_repair_new_node;
        // the maximum time in seconds that a repair session can run for a single table
        // Max time for repairing one table, if exceeded, skip the table. The default is 6 * 60 * 60, which is 6 hours.
        // Let's say there is a Cassandra cluster in that there are 10 tables belonging to 10 different customers.
        // Out of these 10 tables, 1 table is humongous. Repairing this 1 table, say, takes 5 days, in the worst case, but others could finish in just 1 hour.
        // Then we would penalize 9 customers just because of one bad actor, and those 9 customers would ping an operator telling them they are violating SLA even if I am a neighbor, and it would require a lot of back-and-forth manual interventions, etc.
        // So, the idea here is to penalize the outliers instead of good candidates. This can easily be configured with a higher value if we want to disable the functionality.
        // Please note the repair will still run in parallel on other nodes, this is to address outliers on a given node.
        public volatile Long table_max_repair_time_in_sec;
        // the default is 'true'. MVs are mutated at LOCAL_ONE consistency level in Cassandra.
        // This flag determines whether the auto-repair framework needs to run anti-entropy, a.k.a, repair on the MV table or not.
        public volatile Boolean mv_repair_enabled;
        // the default is DefaultAutoRepairTokenSplitter.class.getName(). The class should implement IAutoRepairTokenRangeSplitter.
        // The default implementation splits the tokens based on the token ranges owned by this node divided by the number of 'number_of_subranges'
        public volatile String token_range_splitter;
    }

    @VisibleForTesting
    protected <T> T applyOverrides(RepairType repairType, Function<Options, T> optionSupplier)
    {
        ArrayList<Options> optsProviders = new ArrayList<>();
        if (repair_type_overrides != null)
        {
            optsProviders.add(repair_type_overrides.get(repairType));
        }
        optsProviders.add(global_settings);
        optsProviders.add(Options.defaultOptions);

        return optsProviders.stream()
                            .map(opt -> Optional.ofNullable(opt).map(optionSupplier).orElse(null))
                            .filter(Objects::nonNull)
                            .findFirst()
                            .orElse(null);
    }

    protected void ensureOverrides(RepairType repairType)
    {
        if (repair_type_overrides == null)
        {
            repair_type_overrides = new EnumMap<>(RepairType.class);
        }

        if (repair_type_overrides.get(repairType) == null)
        {
            repair_type_overrides.put(repairType, new Options());
        }
    }
}
