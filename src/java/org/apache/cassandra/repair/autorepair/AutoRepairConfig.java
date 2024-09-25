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
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.ParameterizedClass;

public class AutoRepairConfig implements Serializable
{
    // enable/disable auto repair globally, overrides all other settings. Cannot be modified dynamically.
    // if it is set to false, then no repair will be scheduled, including full and incremental repairs by this framework.
    // if it is set to true, then this repair scheduler will consult another config available for each RepairType, and based on that config, it will schedule repairs.
    public final Boolean enabled;
    // the interval between successive checks for repair scheduler to check if either the ongoing repair is completed or if
    // none is going, then check if it's time to schedule or wait
    public final DurationSpec.IntSecondsBound repair_check_interval = new DurationSpec.IntSecondsBound("5m");
    // when any nodes leave the ring then the repair schedule needs to adjust the order, etc.
    // the repair scheduler keeps the deleted hosts information in its persisted metadata for the defined interval in this config.
    // This information is useful so the scheduler is absolutely sure that the node is indeed removed from the ring, and then it can adjust the repair schedule accordingly.
    // So, the duration in this config determinses for how long deleted host's information is kept in the scheduler's metadata.
    public volatile DurationSpec.IntSecondsBound history_clear_delete_hosts_buffer_interval = new DurationSpec.IntSecondsBound("2h");
    // the maximum number of retries for a repair session.
    public volatile Integer repair_max_retries = 3;
    // the backoff time in seconds for retrying a repair session.
    public volatile DurationSpec.LongSecondsBound repair_retry_backoff = new DurationSpec.LongSecondsBound("30s");

    // global_settings overides Options.defaultOptions for all repair types
    public volatile Options global_settings;

    public enum RepairType implements Serializable
    {
        full,
        incremental;

        public static AutoRepairState getAutoRepairState(RepairType repairType)
        {
            switch (repairType)
            {
                case full:
                    return new FullRepairState();
                case incremental:
                    return new IncrementalRepairState();
            }

            throw new IllegalArgumentException("Invalid repair type: " + repairType);
        }
    }

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

    public DurationSpec.IntSecondsBound getRepairCheckInterval()
    {
        return repair_check_interval;
    }

    public boolean isAutoRepairSchedulingEnabled()
    {
        return enabled;
    }

    public DurationSpec.IntSecondsBound getAutoRepairHistoryClearDeleteHostsBufferInterval()
    {
        return history_clear_delete_hosts_buffer_interval;
    }

    public void setAutoRepairHistoryClearDeleteHostsBufferInterval(String duration)
    {
        history_clear_delete_hosts_buffer_interval = new DurationSpec.IntSecondsBound(duration);
    }

    public int getRepairMaxRetries()
    {
        return repair_max_retries;
    }

    public void setRepairMaxRetries(int maxRetries)
    {
        repair_max_retries = maxRetries;
    }

    public DurationSpec.LongSecondsBound getRepairRetryBackoff()
    {
        return repair_retry_backoff;
    }

    public void setRepairRetryBackoff(String interval)
    {
        repair_retry_backoff = new DurationSpec.LongSecondsBound(interval);
    }

    public boolean isAutoRepairEnabled(RepairType repairType)
    {
        return enabled && applyOverrides(repairType, opt -> opt.enabled);
    }

    public void setAutoRepairEnabled(RepairType repairType, boolean enabled)
    {
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

    public DurationSpec.IntSecondsBound getRepairMinInterval(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.min_repair_interval);
    }

    public void setRepairMinInterval(RepairType repairType, String minRepairInterval)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).min_repair_interval = new DurationSpec.IntSecondsBound(minRepairInterval);
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

    public DurationSpec.IntSecondsBound getAutoRepairTableMaxRepairTime(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.table_max_repair_time);
    }

    public void setAutoRepairTableMaxRepairTime(RepairType repairType, String autoRepairTableMaxRepairTime)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).table_max_repair_time = new DurationSpec.IntSecondsBound(autoRepairTableMaxRepairTime);
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

    public boolean getRepairPrimaryTokenRangeOnly(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.repair_primary_token_range_only);
    }

    public void setRepairPrimaryTokenRangeOnly(RepairType repairType, boolean primaryTokenRangeOnly)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).repair_primary_token_range_only = primaryTokenRangeOnly;
    }

    public int getParallelRepairPercentage(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.parallel_repair_percentage);
    }

    public void setParallelRepairPercentage(RepairType repairType, int percentage)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).parallel_repair_percentage = percentage;
    }

    public int getParallelRepairCount(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.parallel_repair_count);
    }

    public void setParallelRepairCount(RepairType repairType, int count)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).parallel_repair_count = count;
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

    public ParameterizedClass getTokenRangeSplitter(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.token_range_splitter);
    }

    public void setInitialSchedulerDelay(RepairType repairType, String initialSchedulerDelay)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).initial_scheduler_delay = new DurationSpec.IntSecondsBound(initialSchedulerDelay);
    }

    public DurationSpec.IntSecondsBound getInitialSchedulerDelay(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.initial_scheduler_delay);
    }

    public DurationSpec.IntSecondsBound getRepairSessionTimeout(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.repair_session_timeout);
    }

    public void setRepairSessionTimeout(RepairType repairType, String repairSessionTimeout)
    {
        ensureOverrides(repairType);
        repair_type_overrides.get(repairType).repair_session_timeout = new DurationSpec.IntSecondsBound(repairSessionTimeout);
    }

    // Options configures auto-repair behavior for a given repair type.
    // All fields can be modified dynamically.
    public static class Options implements Serializable
    {
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
            opts.number_of_subranges = 16;
            opts.number_of_repair_threads = 1;
            opts.parallel_repair_count = 3;
            opts.parallel_repair_percentage = 3;
            opts.sstable_upper_threshold = 10000;
            opts.min_repair_interval = new DurationSpec.IntSecondsBound("24h");
            opts.ignore_dcs = new HashSet<>();
            opts.repair_primary_token_range_only = true;
            opts.force_repair_new_node = false;
            opts.table_max_repair_time = new DurationSpec.IntSecondsBound("6h");
            opts.mv_repair_enabled = false;
            opts.token_range_splitter = new ParameterizedClass(DefaultAutoRepairTokenSplitter.class.getName(), Collections.emptyMap());
            opts.initial_scheduler_delay = new DurationSpec.IntSecondsBound("5m"); // 5 minutes
            opts.repair_session_timeout = new DurationSpec.IntSecondsBound("3h"); // 3 hours

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
        // Once the scheduler schedules one repair session, then howmany threads to use inside that job will be controlled through this parameter.
        // This is similar to -j for repair options for the nodetool repair command.
        public volatile Integer number_of_repair_threads;
        // The number of nodes running repair parallelly. If parallel_repair_count is set, it will choose the larger value of the two. The default is 3.
        // This configuration controls how many nodes would run repair in parallel.
        // The value “3” means, at any given point in time, at most 3 nodes would be running repair in parallel. These selected nodes can be from any datacenters.
        // If one or more node(s) finish repair, then the framework automatically picks up the next candidate and ensures the maximum number of nodes running repair do not exceed “3”.
        public volatile Integer parallel_repair_count;
        // the number of repair nodes that can run in parallel
        // of the total number of nodes in the group [0,100]
        // The percentage of nodes in the cluster that run repair parallelly. If parallelrepaircount is set, it will choose the larger value of the two.
        // The problem with a fixed number of nodes (the above property) is that in a large-scale environment,
        // the nodes keep getting added/removed due to elasticity, so if we have a fixed number, then manual interventions would increase because, on a continuous basis,operators would have to adjust to meet the SLA.
        // The default is 3%, which means that 3% of the nodes in the Cassandra cluster would be repaired in parallel.
        // So now, if a fleet, an operator won't have to worry about changing the repair frequency, etc., as overall repair time will continue to remain the same even if nodes are added or removed due to elasticity.
        // Extremely fewer manual interventions as it will rarely violate the repair SLA for customers
        public volatile Integer parallel_repair_percentage;
        // the upper threshold of SSTables allowed to participate in a single repair session
        // Threshold to skip a table if it has too many sstables. The default is 10000. This means, if a table on a node has 10000 or more SSTables, then that table will be skipped.
        // This is to avoid penalizing good tables (neighbors) with an outlier.
        public volatile Integer sstable_upper_threshold;
        // the minimum time in hours between repairing the same node again. This is useful for extremely tiny clusters, say 5 nodes, which finishes
        // repair quicly.
        // The default is 24 hours. This means that if the scheduler finishes one round on all the nodes in < 24 hours. On a given node it won’t start a new repair round
        // until the last repair conducted on a given node is < 24 hours.
        public volatile DurationSpec.IntSecondsBound min_repair_interval;
        // specifies a denylist of datacenters to repair
        // This is useful if you want to completely avoid running repairs in one or more data centers. By default, it is empty, i.e., the framework will repair nodes in all the datacenters.
        public volatile Set<String> ignore_dcs;
        // Set this 'true' if AutoRepair should repair only the primary ranges owned by this node; else, 'false'
        // It is the same as -pr in nodetool repair options.
        public volatile Boolean repair_primary_token_range_only;
        // configures whether to force immediate repair on new nodes
        // default it is set to 'false'; this is useful if you want to repair new nodes immediately after they join the ring.
        public volatile Boolean force_repair_new_node;
        // the maximum time that a repair session can run for a single table
        // Max time for repairing one table on a given node, if exceeded, skip the table. The default is 6 hours.
        // Let's say there is a Cassandra cluster in that there are 10 tables belonging to 10 different customers.
        // Out of these 10 tables, 1 table is humongous. Repairing this 1 table, say, takes 5 days, in the worst case, but others could finish in just 1 hour.
        // Then we would penalize 9 customers just because of one bad actor, and those 9 customers would ping an operator and would require a lot of back-and-forth manual interventions, etc.
        // So, the idea here is to penalize the outliers instead of good candidates. This can easily be configured with a higher value if we want to disable the functionality.
        public volatile DurationSpec.IntSecondsBound table_max_repair_time;
        // the default is 'true'.
        // This flag determines whether the auto-repair framework needs to run anti-entropy, a.k.a, repair on the MV table or not.
        public volatile Boolean mv_repair_enabled;
        // the default is DefaultAutoRepairTokenSplitter. The class should implement IAutoRepairTokenRangeSplitter.
        // The default implementation splits the tokens based on the token ranges owned by this node divided by the number of 'number_of_subranges'
        public volatile ParameterizedClass token_range_splitter;
        // the minimum delay after a node starts before the scheduler starts running repair
        public volatile DurationSpec.IntSecondsBound initial_scheduler_delay;
        // repair session timeout - this is applicable for each repair session
        // the major issue with Repair is a session sometimes hangs; so this timeout is useful to unblock such problems
        public volatile DurationSpec.IntSecondsBound repair_session_timeout;

        public String toString()
        {
            return "Options{" +
                   "enabled=" + enabled +
                   ", repair_by_keyspace=" + repair_by_keyspace +
                   ", number_of_subranges=" + number_of_subranges +
                   ", number_of_repair_threads=" + number_of_repair_threads +
                   ", parallel_repair_count=" + parallel_repair_count +
                   ", parallel_repair_percentage=" + parallel_repair_percentage +
                   ", sstable_upper_threshold=" + sstable_upper_threshold +
                   ", min_repair_interval=" + min_repair_interval +
                   ", ignore_dcs=" + ignore_dcs +
                   ", repair_primary_token_range_only=" + repair_primary_token_range_only +
                   ", force_repair_new_node=" + force_repair_new_node +
                   ", table_max_repair_time=" + table_max_repair_time +
                   ", mv_repair_enabled=" + mv_repair_enabled +
                   ", token_range_splitter=" + token_range_splitter +
                   ", intial_scheduler_delay=" + initial_scheduler_delay +
                   ", repair_session_timeout=" + repair_session_timeout +
                   '}';
        }
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

        repair_type_overrides.computeIfAbsent(repairType, k -> new Options());
    }
}
