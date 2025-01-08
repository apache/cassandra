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
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.utils.LocalizeString;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

public class AutoRepairConfig implements Serializable
{
    // Enable/Disable auto repair scheduler. If it is set to false, then the scheduler thread will not be spun up.
    // If it is set to true, then the repair scheduler thread will be created. The thread will
    // look for secondary config available for each repair type (full, incremental, and preview_repaired),
    // and based on that, it will schedule repairs.
    public volatile Boolean enabled;
    // Time interval between successive checks for repair scheduler to check if either the ongoing repair is completed or if
    // none is going, then check if it's time to schedule or wait.
    public final DurationSpec.IntSecondsBound repair_check_interval = new DurationSpec.IntSecondsBound("5m");
    // When any nodes leave the ring then the repair schedule needs to adjust the order, etc.
    // The AutoRepair scheduler keeps the deleted hosts information in its persisted metadata for the defined interval in this config.
    // This information is useful so the scheduler is absolutely sure that the node is indeed removed from the ring, and then it can adjust the repair schedule accordingly.
    // So, the duration in this config determinses for how long deleted host's information is kept in the scheduler's metadata.
    public volatile DurationSpec.IntSecondsBound history_clear_delete_hosts_buffer_interval = new DurationSpec.IntSecondsBound("2h");
    // Maximum number of retries for a repair session.
    public volatile Integer repair_max_retries = 3;
    // Backoff time in seconds for retrying a repair session.
    public volatile DurationSpec.LongSecondsBound repair_retry_backoff = new DurationSpec.LongSecondsBound("30s");
    // Minimum duration for the execution of a single repair task (i.e.: RepairRunnable).
    // This helps prevent the auto-repair scheduler from overwhelming the node by scheduling a large number of
    // repair tasks in a short period of time.
    public volatile DurationSpec.LongSecondsBound repair_task_min_duration = new DurationSpec.LongSecondsBound("5s");

    // global_settings overides Options.defaultOptions for all repair types
    public volatile Options global_settings;

    public static final Class<? extends IAutoRepairTokenRangeSplitter> DEFAULT_SPLITTER = RepairTokenRangeSplitter.class;

    // make transient so gets consturcted in the implementation.
    private final transient Map<RepairType, IAutoRepairTokenRangeSplitter> tokenRangeSplitters = new EnumMap<>(RepairType.class);

    public enum RepairType implements Serializable
    {
        FULL,
        INCREMENTAL,
        PREVIEW_REPAIRED;

        private final String configName;

        RepairType()
        {
            this.configName = LocalizeString.toLowerCaseLocalized(name());
        }

        /**
         * @return Format of the repair type as it should be represented in configuration.
         * Canonically this is the enum name in lowerCase.
         */
        public String getConfigName()
        {
            return configName;
        }

        public static AutoRepairState getAutoRepairState(RepairType repairType)
        {
            switch (repairType)
            {
                case FULL:
                    return new FullRepairState();
                case INCREMENTAL:
                    return new IncrementalRepairState();
                case PREVIEW_REPAIRED:
                    return new PreviewRepairedState();
            }

            throw new IllegalArgumentException("Invalid repair type: " + repairType);
        }
    }

    // repair_type_overrides overrides the global_settings for a specific repair type.  String used as key instead
    // of enum to allow lower case key in yaml.
    public volatile ConcurrentMap<String, Options> repair_type_overrides = Maps.newConcurrentMap();

    public AutoRepairConfig()
    {
        this(false);
    }

    public AutoRepairConfig(boolean enabled)
    {
        this.enabled = enabled;
        global_settings = Options.getDefaultOptions();
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

    public void startScheduler()
    {
        enabled = true;
        AutoRepair.instance.setup();
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

    public DurationSpec.LongSecondsBound getRepairTaskMinDuration()
    {
        return repair_task_min_duration;
    }

    public void setRepairTaskMinDuration(String duration)
    {
        repair_task_min_duration = new DurationSpec.LongSecondsBound(duration);
    }

    public boolean isAutoRepairEnabled(RepairType repairType)
    {
        return enabled && applyOverrides(repairType, opt -> opt.enabled);
    }

    public void setAutoRepairEnabled(RepairType repairType, boolean enabled)
    {
        getOptions(repairType).enabled = enabled;
    }

    public void setRepairByKeyspace(RepairType repairType, boolean repairByKeyspace)
    {
        getOptions(repairType).repair_by_keyspace = repairByKeyspace;
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
        getOptions(repairType).number_of_repair_threads = repairThreads;
    }

    public DurationSpec.IntSecondsBound getRepairMinInterval(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.min_repair_interval);
    }

    public void setRepairMinInterval(RepairType repairType, String minRepairInterval)
    {
        getOptions(repairType).min_repair_interval = new DurationSpec.IntSecondsBound(minRepairInterval);
    }

    public int getRepairSSTableCountHigherThreshold(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.sstable_upper_threshold);
    }

    public void setRepairSSTableCountHigherThreshold(RepairType repairType, int sstableHigherThreshold)
    {
        getOptions(repairType).sstable_upper_threshold = sstableHigherThreshold;
    }

    public DurationSpec.IntSecondsBound getAutoRepairTableMaxRepairTime(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.table_max_repair_time);
    }

    public void setAutoRepairTableMaxRepairTime(RepairType repairType, String autoRepairTableMaxRepairTime)
    {
        getOptions(repairType).table_max_repair_time = new DurationSpec.IntSecondsBound(autoRepairTableMaxRepairTime);
    }

    public Set<String> getIgnoreDCs(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.ignore_dcs);
    }

    public void setIgnoreDCs(RepairType repairType, Set<String> ignoreDCs)
    {
        getOptions(repairType).ignore_dcs = ignoreDCs;
    }

    public boolean getRepairPrimaryTokenRangeOnly(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.repair_primary_token_range_only);
    }

    public void setRepairPrimaryTokenRangeOnly(RepairType repairType, boolean primaryTokenRangeOnly)
    {
        getOptions(repairType).repair_primary_token_range_only = primaryTokenRangeOnly;
    }

    public int getParallelRepairPercentage(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.parallel_repair_percentage);
    }

    public void setParallelRepairPercentage(RepairType repairType, int percentage)
    {
        getOptions(repairType).parallel_repair_percentage = percentage;
    }

    public int getParallelRepairCount(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.parallel_repair_count);
    }

    public void setParallelRepairCount(RepairType repairType, int count)
    {
        getOptions(repairType).parallel_repair_count = count;
    }

    public boolean getMVRepairEnabled(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.mv_repair_enabled);
    }

    public void setMVRepairEnabled(RepairType repairType, boolean enabled)
    {
        getOptions(repairType).mv_repair_enabled = enabled;
    }

    public void setForceRepairNewNode(RepairType repairType, boolean forceRepairNewNode)
    {
        getOptions(repairType).force_repair_new_node = forceRepairNewNode;
    }

    public boolean getForceRepairNewNode(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.force_repair_new_node);
    }

    public ParameterizedClass getTokenRangeSplitter(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.token_range_splitter);
    }

    /**
     * Set a new token range splitter, this is not meant to be used other than for testing.
     */
    @VisibleForTesting
    void setTokenRangeSplitter(RepairType repairType, ParameterizedClass tokenRangeSplitter)
    {
        getOptions(repairType).token_range_splitter = tokenRangeSplitter;
        tokenRangeSplitters.remove(repairType);
    }

    public IAutoRepairTokenRangeSplitter getTokenRangeSplitterInstance(RepairType repairType)
    {
        return tokenRangeSplitters.computeIfAbsent(repairType,
                                                   key -> newAutoRepairTokenRangeSplitter(key, getTokenRangeSplitter(key)));
    }

    public void setInitialSchedulerDelay(RepairType repairType, String initialSchedulerDelay)
    {
        getOptions(repairType).initial_scheduler_delay = new DurationSpec.IntSecondsBound(initialSchedulerDelay);
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
        getOptions(repairType).repair_session_timeout = new DurationSpec.IntSecondsBound(repairSessionTimeout);
    }

    @VisibleForTesting
    static IAutoRepairTokenRangeSplitter newAutoRepairTokenRangeSplitter(RepairType repairType, ParameterizedClass parameterizedClass) throws ConfigurationException
    {
        try
        {
            Class<? extends IAutoRepairTokenRangeSplitter> tokenRangeSplitterClass;
            final String className;
            if (parameterizedClass.class_name != null && !parameterizedClass.class_name.isEmpty())
            {
                className = parameterizedClass.class_name.contains(".") ?
                            parameterizedClass.class_name :
                            "org.apache.cassandra.repair.autorepair." + parameterizedClass.class_name;
                tokenRangeSplitterClass = FBUtilities.classForName(className, "token_range_splitter");
            }
            else
            {
                // If token_range_splitter.class_name is not defined, just use default, this is for convenience.
                tokenRangeSplitterClass = AutoRepairConfig.DEFAULT_SPLITTER;
            }
            try
            {
                Map<String, String> parameters = parameterizedClass.parameters != null ? parameterizedClass.parameters : Collections.emptyMap();
                // first attempt to initialize with RepairType and Map arguments.
                return tokenRangeSplitterClass.getConstructor(RepairType.class, Map.class).newInstance(repairType, parameters);
            }
            catch (NoSuchMethodException nsme)
            {
                // fall back on no argument constructor.
                return tokenRangeSplitterClass.getConstructor().newInstance();
            }
        }
        catch (Exception ex)
        {
            throw new ConfigurationException("Unable to create instance of IAutoRepairTokenRangeSplitter", ex);
        }
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
            opts.token_range_splitter = new ParameterizedClass(DEFAULT_SPLITTER.getName(), Collections.emptyMap());
            opts.initial_scheduler_delay = new DurationSpec.IntSecondsBound("5m"); // 5 minutes
            opts.repair_session_timeout = new DurationSpec.IntSecondsBound("3h"); // 3 hours

            return opts;
        }

        // Enable/Disable full auto repair
        public volatile Boolean enabled;
        //  Repair table by table if this is set to 'false'. If it is 'true', then repair all the tables in a keyspace in one go.
        public volatile Boolean repair_by_keyspace;
        //  Number of repair threads to run for a given invoked Repair Job.
        //  Once the scheduler schedules one repair session, then howmany threads to use inside that job will be controlled through this parameter.
        //  This is similar to -j for repair options for the nodetool repair command.
        public volatile Integer number_of_repair_threads;
        //  The number of nodes running repair parallelly. If parallel_repair_percentage is set, it will choose the larger value of the two. The default is 3.
        //  This configuration controls how many nodes would run repair in parallel.
        //  The value “3” means, at any given point in time, at most 3 nodes would be running repair in parallel. These selected nodes can be from any datacenters.
        //  If one or more node(s) finish repair, then the framework automatically picks up the next candidate(s) based on the least repair time.
        //  It will ensure the maximum number of nodes running repair do not exceed “3”.
        public volatile Integer parallel_repair_count;
        //  The percentage of nodes in the cluster that run repair parallelly. If parallel_repair_count is set, it will choose the larger value of the two.
        //  The problem with a fixed number of nodes (parallel_repair_count property) is that in a large-scale environment,
        //  the nodes keep getting added/removed due to elasticity, so if we have a fixed number, then manual interventions would increase because, on a continuous basis,operators would have to adjust to meet the SLA.
        //  The default is 3%, which means that 3% of the nodes in the Cassandra cluster would be repaired in parallel.
        //  So now, if a fleet, an operator won't have to worry about changing the repair frequency, etc., as overall repair time will continue to remain the same even if nodes are added or removed due to elasticity.
        //  Extremely fewer manual interventions as it will rarely violate the repair SLA for customers
        public volatile Integer parallel_repair_percentage;
        //  Threshold to skip a table if it has too many sstables. The default is 10000. This means, if a table on a node has 10000 or more SSTables, then that table will be skipped.
        //  This is to avoid penalizing good tables (neighbors) with an outlier.
        public volatile Integer sstable_upper_threshold;
        //  Minimum time in hours between repairing the same node again. This is useful for extremely tiny clusters, say 5 nodes, which finishes
        //  repair quicly. The default is 24 hours. This means that if the scheduler finishes one round on all the nodes in < 24 hours. On a given node it won’t start a new repair round
        //  until the last repair conducted on a given node is < 24 hours.
        public volatile DurationSpec.IntSecondsBound min_repair_interval;
        //  This is useful if you want to completely avoid running repairs in one or more data centers. By default, it is empty, i.e., the framework will repair nodes in all the datacenters.
        //  If you want to avoid repairing nodes in one or more data centers, then you can specify the data centers in this list.
        public volatile Set<String> ignore_dcs;
        //  Set this 'true' if AutoRepair should repair only the primary ranges owned by this node; else, 'false'
        //  It is the same as -pr in nodetool repair options.
        public volatile Boolean repair_primary_token_range_only;
        //  Whether to force immediate repair on new nodes. This is useful if you want to repair newly bootstrapped nodes immediately after they join the ring.
        public volatile Boolean force_repair_new_node;
        //  Max time for repairing one table on a given node, if exceeded, skip the table. The default is 6 hours.
        //  Let's say there is a Cassandra cluster in that there are 10 tables belonging to 10 different customers.
        //  Out of these 10 tables, 1 table is humongous. Repairing this 1 table, say, takes 5 days, in the worst case, but others could finish in just 1 hour.
        //  Then we would penalize 9 customers just because of one bad actor, and those 9 customers would ping an operator and would require a lot of back-and-forth manual interventions, etc.
        //  So, the idea here is to penalize the outliers instead of good candidates. This can easily be configured with a higher value if we want to disable the functionality.
        public volatile DurationSpec.IntSecondsBound table_max_repair_time;
        //  If the scheduler should repair MV table or not.
        public volatile Boolean mv_repair_enabled;
        /**
         * Splitter implementation to use for generating repair assignments.
         * <p>
         * The default is {@link RepairTokenRangeSplitter}. The class should implement {@link IAutoRepairTokenRangeSplitter}
         * and have a constructor accepting ({@link RepairType}, {@link java.util.Map})
         */
        public volatile ParameterizedClass token_range_splitter;
        //  After a node restart, wait for this much delay before scheduler starts running repair; this is to avoid starting repair immediately after a node restart.
        public volatile DurationSpec.IntSecondsBound initial_scheduler_delay;
        //  The major issue with Repair is sometimes repair session hangs; so this timeout is useful to resume such stuck repair sessions.
        public volatile DurationSpec.IntSecondsBound repair_session_timeout;

        public String toString()
        {
            return "Options{" +
                   "enabled=" + enabled +
                   ", repair_by_keyspace=" + repair_by_keyspace +
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

    @Nonnull
    protected Options getOptions(RepairType repairType)
    {
        return repair_type_overrides.computeIfAbsent(repairType.getConfigName(), k -> new Options());
    }

    private static <T> T getOverride(Options options, Function<Options, T> optionSupplier)
    {
        return options != null ? optionSupplier.apply(options) : null;
    }

    @VisibleForTesting
    protected <T> T applyOverrides(RepairType repairType, Function<Options, T> optionSupplier)
    {
        // Check option by repair type first
        Options repairTypeOverrides = getOptions(repairType);
        T val = optionSupplier.apply(repairTypeOverrides);

        if (val != null)
            return val;

        // Check option in global settings
        if (global_settings != null)
        {
            val = getOverride(global_settings, optionSupplier);

            if (val != null)
                return val;
        }

        // Otherwise check defaults
        return getOverride(Options.defaultOptions, optionSupplier);
    }
}
