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
import java.util.Objects;
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

/**
 * Defines configurations for AutoRepair.
 */
public class AutoRepairConfig implements Serializable
{
    // Enable/Disable the auto-repair scheduler.
    // If set to false, the scheduler thread will not be started.
    // If set to true, the repair scheduler thread will be created. The thread will
    // check for secondary configuration available for each repair type (full, incremental,
    // and preview_repaired), and based on that, it will schedule repairs.
    public volatile Boolean enabled;
    // Time interval between successive checks to see if ongoing repairs are complete or if it is time to schedule
    // repairs.
    public final DurationSpec.IntSecondsBound repair_check_interval = new DurationSpec.IntSecondsBound("5m");
    // The scheduler needs to adjust its order when nodes leave the ring. Deleted hosts are tracked in metadata
    // for a specified duration to ensure they are indeed removed before adjustments are made to the schedule.
    public volatile DurationSpec.IntSecondsBound history_clear_delete_hosts_buffer_interval = new DurationSpec.IntSecondsBound("2h");
    // Minimum duration for the execution of a single repair task. This prevents the scheduler from overwhelming
    // the node by scheduling too many repair tasks in a short period of time.
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

        /**
         * Case-insensitive parsing of the repair type string into {@link RepairType}
         *
         * @param repairTypeStr the repair type string
         * @return the {@link RepairType} represented by the {@code repairTypeStr} string
         * @throws IllegalArgumentException when the repair type string does not match any repair type
         */
        public static RepairType parse(String repairTypeStr)
        {
            return RepairType.valueOf(LocalizeString.toUpperCaseLocalized(Objects.requireNonNull(repairTypeStr, "repairTypeStr cannot be null")));
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

    @VisibleForTesting
    public void setAutoRepairSchedulingEnabled(boolean enabled)
    {
        this.enabled = enabled;
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

    public boolean getAllowParallelReplicaRepair(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.allow_parallel_replica_repair);
    }

    public void setAllowParallelReplicaRepair(RepairType repairType, boolean enabled)
    {
        getOptions(repairType).allow_parallel_replica_repair = enabled;
    }

    public boolean getAllowParallelReplicaRepairAcrossSchedules(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.allow_parallel_replica_repair_across_schedules);
    }

    public void setAllowParallelReplicaRepairAcrossSchedules(RepairType repairType, boolean enabled)
    {
        getOptions(repairType).allow_parallel_replica_repair_across_schedules = enabled;
    }

    public boolean getMaterializedViewRepairEnabled(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.materialized_view_repair_enabled);
    }

    public void setMaterializedViewRepairEnabled(RepairType repairType, boolean enabled)
    {
        getOptions(repairType).materialized_view_repair_enabled = enabled;
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

    public int getRepairMaxRetries(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.repair_max_retries);
    }

    public void setRepairMaxRetries(RepairType repairType, int maxRetries)
    {
        getOptions(repairType).repair_max_retries = maxRetries;
    }

    public DurationSpec.LongSecondsBound getRepairRetryBackoff(RepairType repairType)
    {
        return applyOverrides(repairType, opt -> opt.repair_retry_backoff);
    }

    public void setRepairRetryBackoff(RepairType repairType, String interval)
    {
        getOptions(repairType).repair_retry_backoff = new DurationSpec.LongSecondsBound(interval);
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
        private static Map<AutoRepairConfig.RepairType, Options> defaultOptions;

        private static Map<AutoRepairConfig.RepairType, Options> initializeDefaultOptions()
        {
            Map<AutoRepairConfig.RepairType, Options> options = new EnumMap<>(AutoRepairConfig.RepairType.class);
            options.put(AutoRepairConfig.RepairType.FULL, getDefaultOptions());
            options.put(RepairType.INCREMENTAL, getDefaultOptions());
            options.put(RepairType.PREVIEW_REPAIRED, getDefaultOptions());

            return options;
        }

        public static Map<AutoRepairConfig.RepairType, Options> getDefaultOptionsMap()
        {
            if (defaultOptions == null)
            {
                synchronized (AutoRepairConfig.class)
                {
                    if (defaultOptions == null)
                    {
                        defaultOptions = initializeDefaultOptions();
                    }
                }
            }
            return defaultOptions;
        }

        public Options()
        {
        }

        @VisibleForTesting
        protected static Options getDefaultOptions()
        {
            Options opts = new Options();

            opts.enabled = false;
            opts.repair_by_keyspace = true;
            opts.number_of_repair_threads = 1;
            opts.parallel_repair_count = 3;
            opts.parallel_repair_percentage = 3;
            opts.allow_parallel_replica_repair = false;
            opts.allow_parallel_replica_repair_across_schedules = true;
            opts.sstable_upper_threshold = 50000;
            opts.ignore_dcs = new HashSet<>();
            opts.repair_primary_token_range_only = true;
            opts.force_repair_new_node = false;
            opts.table_max_repair_time = new DurationSpec.IntSecondsBound("6h");
            opts.materialized_view_repair_enabled = false;
            opts.token_range_splitter = new ParameterizedClass(DEFAULT_SPLITTER.getName(), Collections.emptyMap());
            opts.initial_scheduler_delay = new DurationSpec.IntSecondsBound("5m");
            opts.repair_session_timeout = new DurationSpec.IntSecondsBound("3h");
            opts.min_repair_interval = new DurationSpec.IntSecondsBound("24h");

            return opts;
        }

        // Enable/Disable full or incremental or previewed_repair auto repair
        public volatile Boolean enabled;
        // If true, attempts to group tables in the same keyspace into one repair; otherwise, each table is repaired
        // individually.
        public volatile Boolean repair_by_keyspace;
        // Number of threads to use for each repair job scheduled by the scheduler. Similar to the -j option in nodetool
        // repair.
        public volatile Integer number_of_repair_threads;
        // Number of nodes running repair in parallel. If parallel_repair_percentage is set, the larger value is used.
        public volatile Integer parallel_repair_count;
        // Percentage of nodes in the cluster running repair in parallel. If parallel_repair_count is set, the larger value
        // is used. Recommendation is that the repair cycle on the cluster should finish within gc_grace_seconds.
        public volatile Integer parallel_repair_percentage;
        // Whether to allow a node to take its turn running repair while one or more of its replicas are running repair.
        // Defaults to false, as running repairs concurrently on replicas can increase load and also cause
        // anticompaction conflicts while running incremental repair.
        public volatile Boolean allow_parallel_replica_repair;
        // An addition to allow_parallel_replica_repair that also blocks repairs when replicas (including this node itself)
        // are repairing in any schedule. For example, if a replica is executing full repairs, a value of false will
        // prevent starting incremental repairs for this node. Defaults to true and is only evaluated when
        // allow_parallel_replica_repair is false.
        public volatile Boolean allow_parallel_replica_repair_across_schedules;
        // Threshold to skip repairing tables with too many SSTables. Defaults to 10,000 SSTables to avoid penalizing good
        // tables.
        public volatile Integer sstable_upper_threshold;
        // Minimum duration between repairing the same node again. This is useful for tiny clusters, such as
        // clusters with 5 nodes that finish repairs quickly. The default is 24 hours. This means that if the scheduler
        // completes one round on all nodes in less than 24 hours, it will not start a new repair round on a given node
        // until 24 hours have passed since the last repair.
        public volatile DurationSpec.IntSecondsBound min_repair_interval;
        // Avoid running repairs in specific data centers. By default, repairs run in all data centers. Specify data
        // centers to exclude in this list. Note that repair sessions will still consider all replicas from excluded
        // data centers. Useful if you have keyspaces that are not replicated in certain data centers, and you want to
        // not run repair schedule in certain data centers.
        public volatile Set<String> ignore_dcs;
        // Repair only the primary ranges owned by a node. Equivalent to the -pr option in nodetool repair. Defaults
        // to true. General advice is to keep this true.
        public volatile Boolean repair_primary_token_range_only;
        // Force immediate repair on new nodes after they join the ring.
        public volatile Boolean force_repair_new_node;
        // Maximum time allowed for repairing one table on a given node. If exceeded, the repair proceeds to the
        // next table.
        public volatile DurationSpec.IntSecondsBound table_max_repair_time;
        // Repairs materialized views if true.
        public volatile Boolean materialized_view_repair_enabled;
        /**
         * Splitter implementation to use for generating repair assignments.
         * <p>
         * The default is {@link RepairTokenRangeSplitter}. The class should implement {@link IAutoRepairTokenRangeSplitter}
         * and have a constructor accepting ({@link RepairType}, {@link java.util.Map})
         */
        public volatile ParameterizedClass token_range_splitter;
        // After a node restart, wait for this much delay before scheduler starts running repair; this is to avoid starting repair immediately after a node restart.
        public volatile DurationSpec.IntSecondsBound initial_scheduler_delay;
        // Timeout for retrying stuck repair sessions.
        public volatile DurationSpec.IntSecondsBound repair_session_timeout;
        // Maximum number of retries for a repair session.
        public volatile Integer repair_max_retries = 3;
        // Backoff time before retrying a repair session.
        public volatile DurationSpec.LongSecondsBound repair_retry_backoff = new DurationSpec.LongSecondsBound("30s");

        public String toString()
        {
            return "Options{" +
                   "enabled=" + enabled +
                   ", repair_by_keyspace=" + repair_by_keyspace +
                   ", number_of_repair_threads=" + number_of_repair_threads +
                   ", parallel_repair_count=" + parallel_repair_count +
                   ", parallel_repair_percentage=" + parallel_repair_percentage +
                   ", allow_parallel_replica_repair=" + allow_parallel_replica_repair +
                   ", allow_parallel_replica_repair_across_schedules=" + allow_parallel_replica_repair_across_schedules +
                   ", sstable_upper_threshold=" + sstable_upper_threshold +
                   ", min_repair_interval=" + min_repair_interval +
                   ", ignore_dcs=" + ignore_dcs +
                   ", repair_primary_token_range_only=" + repair_primary_token_range_only +
                   ", force_repair_new_node=" + force_repair_new_node +
                   ", table_max_repair_time=" + table_max_repair_time +
                   ", materialized_view_repair_enabled=" + materialized_view_repair_enabled +
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
        return getOverride(Options.getDefaultOptionsMap().get(repairType), optionSupplier);
    }

    public String toString()
    {
        return "AutoRepairConfig{" +
               "enabled=" + enabled +
               ", repair_check_interval=" + repair_check_interval +
               ", history_clear_delete_hosts_buffer_interval=" + history_clear_delete_hosts_buffer_interval +
               ", repair_task_min_duration=" + repair_task_min_duration +
               ", global_settings=" + global_settings +
               ", repair_type_overrides=" + repair_type_overrides +
               "}";
    }
}
