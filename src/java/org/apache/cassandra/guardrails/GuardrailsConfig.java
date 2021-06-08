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

package org.apache.cassandra.guardrails;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.units.SizeUnit;

import static java.lang.String.format;

/**
 * Configuration settings for guardrails (populated from the Yaml file).
 *
 * <p>Note that the settings here must only be used by the {@link Guardrails} class and not directly by the code
 * checking each guarded constraint (which, again, should use the higher level abstractions defined in
 * {@link Guardrails}).
 *
 * <p>We have 2 variants of guardrails, soft (warn) and hard (fail) limits, each guardrail having either one of the
 * variant or both (note in particular that hard limits only make sense for guardrails triggering during query
 * execution. For other guardrails, say one triggering during compaction, failing does not make sense).
 *
 * <p>Additionally, each individual setting should have a specific value (typically -1 for numeric settings),
 * that allows to disable the corresponding guardrail.
 *
 * <p>The default values for each guardrail settings should reflect what is mandated for DCaaS.
 *
 * <p>For consistency, guardrails based on a simple numeric threshold should use the naming scheme
 * {@code <what_is_guarded>_warn_threshold} for soft limits and {@code <what_is_guarded>_failure_threshold} for hard
 * ones, and if the value has a unit, that unit should be added at the end (for instance,
 * {@code <what_is_guarded>_failure_threshold_in_kb}). For "boolean" guardrails that disable a feature, use
 * {@code <what_is_guarded_enabled}. Other type of guardrails can use appropriate suffixes but should start with
 * {@code <what is guarded>}.
 */
public class GuardrailsConfig
{
    public static final String INDEX_GUARDRAILS_TABLE_FAILURE_THRESHOLD = Config.PROPERTY_PREFIX + "index.guardrails.table_failure_threshold";
    public static final String INDEX_GUARDRAILS_TOTAL_FAILURE_THRESHOLD = Config.PROPERTY_PREFIX + "index.guardrails.total_failure_threshold";

    public static final int NO_LIMIT = -1;
    public static final int UNSET = -2;
    public static final int DEFAULT_INDEXES_PER_TABLE_THRESHOLD = 10;
    public static final int DEFAULT_INDEXES_TOTAL_THRESHOLD = 100;

    public volatile Long column_value_size_failure_threshold_in_kb;
    public volatile Long columns_per_table_failure_threshold;
    public volatile Long fields_per_udt_failure_threshold;
    public volatile Long collection_size_warn_threshold_in_kb;
    public volatile Long items_per_collection_warn_threshold;
    public volatile Boolean read_before_write_list_operations_enabled;

    // Legacy 2i guardrail
    public volatile Integer secondary_index_per_table_failure_threshold;
    public volatile Integer sasi_indexes_per_table_failure_threshold;
    // SAI indexes guardrail
    public volatile Integer sai_indexes_per_table_failure_threshold;
    public volatile Integer sai_indexes_total_failure_threshold;
    public volatile Integer materialized_view_per_table_failure_threshold;

    public volatile Long tables_warn_threshold;
    public volatile Long tables_failure_threshold;
    // N.B. Not safe for concurrent modification
    public volatile Set<String> table_properties_disallowed;
    public volatile Set<String> table_properties_ignored;

    public volatile Boolean user_timestamps_enabled;

    public volatile Boolean logged_batch_enabled;

    public volatile Boolean truncate_table_enabled;

    public volatile Boolean counter_enabled;

    public volatile Set<String> write_consistency_levels_disallowed;

    // For paging by bytes having a page bigger than this threshold will result in a failure
    // For paging by rows the result will be silently cut short if it is bigger than the threshold
    public volatile Integer page_size_failure_threshold_in_kb;

    // Limit number of terms and their cartesian product in IN query
    public volatile Integer in_select_cartesian_product_failure_threshold;
    public volatile Integer partition_keys_in_select_failure_threshold;

    // represent percentage of disk space, -1 means disabled
    public volatile Integer disk_usage_percentage_warn_threshold;
    public volatile Integer disk_usage_percentage_failure_threshold;
    public volatile Long disk_usage_max_disk_size_in_gb;

    // When executing a scan, within or across a partition, we need to keep the
    // tombstones seen in memory so we can return them to the coordinator, which
    // will use them to make sure other replicas also know about the deleted rows.
    // With workloads that generate a lot of tombstones, this can cause performance
    // problems and even exaust the server heap.
    // (http://www.datastax.com/dev/blog/cassandra-anti-patterns-queues-and-queue-like-datasets)
    // Adjust the thresholds here if you understand the dangers and want to
    // scan more tombstones anyway. These thresholds may also be adjusted at runtime
    // using the StorageService mbean.
    public volatile Integer tombstone_warn_threshold;
    public volatile Integer tombstone_failure_threshold;

    // Log WARN on any multiple-partition batch size that exceeds this value. 5kb per batch by default.
    // Use caution when increasing the size of this threshold as it can lead to node instability.
    public volatile Integer batch_size_warn_threshold_in_kb;
    // Fail any multiple-partition batch that exceeds this value. The calculated default is 50kb (10x warn threshold).
    public volatile Integer batch_size_fail_threshold_in_kb;
    // Log WARN on any batches not of type LOGGED than span across more partitions than this limit.
    public volatile Integer unlogged_batch_across_partitions_warn_threshold;

    public volatile Integer partition_size_warn_threshold_in_mb;

    /**
     * If {@link DatabaseDescriptor#isEmulateDbaasDefaults()} is true, apply cloud defaults to guardrails settings that
     * are not specified in yaml; otherwise, apply on-prem defaults to guardrails settings that are not specified in yaml;
     */
    @VisibleForTesting
    public void applyConfig()
    {
        // for read requests
        enforceDefault(page_size_failure_threshold_in_kb, v -> page_size_failure_threshold_in_kb = v, NO_LIMIT, 512);

        enforceDefault(in_select_cartesian_product_failure_threshold, v -> in_select_cartesian_product_failure_threshold = v, NO_LIMIT, 25);
        enforceDefault(partition_keys_in_select_failure_threshold, v -> partition_keys_in_select_failure_threshold = v, NO_LIMIT, 20);

        enforceDefault(tombstone_warn_threshold, v -> tombstone_warn_threshold = v, 1000, 1000);
        enforceDefault(tombstone_failure_threshold, v -> tombstone_failure_threshold = v, 100000, 100000);

        // for write requests
        enforceDefault(logged_batch_enabled, v -> logged_batch_enabled = v, true, true);
        enforceDefault(batch_size_warn_threshold_in_kb, v -> batch_size_warn_threshold_in_kb = v, 64, 64);
        enforceDefault(batch_size_fail_threshold_in_kb, v -> batch_size_fail_threshold_in_kb = v, 640, 640);
        enforceDefault(unlogged_batch_across_partitions_warn_threshold, v -> unlogged_batch_across_partitions_warn_threshold = v, 10, 10);

        enforceDefault(truncate_table_enabled, v -> truncate_table_enabled = v, true, true);

        enforceDefault(user_timestamps_enabled, v -> user_timestamps_enabled = v, true, true);

        enforceDefault(column_value_size_failure_threshold_in_kb, v -> column_value_size_failure_threshold_in_kb = v, -1L, 5 * 1024L);

        enforceDefault(read_before_write_list_operations_enabled, v -> read_before_write_list_operations_enabled = v, true, false);

        // We use a LinkedHashSet just for the sake of preserving the ordering in error messages
        enforceDefault(write_consistency_levels_disallowed,
                       v -> write_consistency_levels_disallowed = ImmutableSet.copyOf(v),
                       Collections.<String>emptySet(),
                       new LinkedHashSet<>(Arrays.asList("ANY", "ONE", "LOCAL_ONE")));

        // for schema
        enforceDefault(counter_enabled, v -> counter_enabled = v, true, true);

        enforceDefault(fields_per_udt_failure_threshold, v -> fields_per_udt_failure_threshold = v, -1L, 10L);
        enforceDefault(collection_size_warn_threshold_in_kb, v -> collection_size_warn_threshold_in_kb = v, -1L, 5 * 1024L);
        enforceDefault(items_per_collection_warn_threshold, v -> items_per_collection_warn_threshold = v, -1L, 20L);

        enforceDefault(columns_per_table_failure_threshold, v -> columns_per_table_failure_threshold = v, -1L, 50L);
        enforceDefault(secondary_index_per_table_failure_threshold, v -> secondary_index_per_table_failure_threshold = v, NO_LIMIT, 1);
        enforceDefault(sasi_indexes_per_table_failure_threshold, v -> sasi_indexes_per_table_failure_threshold = v, NO_LIMIT, 0);
        enforceDefault(materialized_view_per_table_failure_threshold, v -> materialized_view_per_table_failure_threshold = v, NO_LIMIT, 2);
        enforceDefault(tables_warn_threshold, v -> tables_warn_threshold = v, -1L, 100L);
        enforceDefault(tables_failure_threshold, v -> tables_failure_threshold = v, -1L, 200L);

        enforceDefault(table_properties_disallowed,
                       v -> table_properties_disallowed = ImmutableSet.copyOf(v),
                       Collections.<String>emptySet(),
                       Collections.<String>emptySet());

        enforceDefault(table_properties_ignored,
                       v -> table_properties_ignored = ImmutableSet.copyOf(v),
                       Collections.<String>emptySet(),
                       new LinkedHashSet<>(TableAttributes.allKeywords().stream()
                                                          .sorted()
                                                          .filter(p -> !p.equals("default_time_to_live"))
                                                          .collect(Collectors.toList())));

        // for node status
        enforceDefault(disk_usage_percentage_warn_threshold, v -> disk_usage_percentage_warn_threshold = v, NO_LIMIT, 70);
        enforceDefault(disk_usage_percentage_failure_threshold, v -> disk_usage_percentage_failure_threshold = v, NO_LIMIT, 80);
        enforceDefault(disk_usage_max_disk_size_in_gb, v -> disk_usage_max_disk_size_in_gb = v, (long) NO_LIMIT, (long) NO_LIMIT);

        enforceDefault(partition_size_warn_threshold_in_mb, v -> partition_size_warn_threshold_in_mb = v, 100, 100);

        // SAI Table Failure threshold (maye be overridden via system property)
        Integer overrideTableFailureThreshold = Integer.getInteger(INDEX_GUARDRAILS_TABLE_FAILURE_THRESHOLD, UNSET);
        if (overrideTableFailureThreshold != UNSET)
            sai_indexes_per_table_failure_threshold = overrideTableFailureThreshold;
        enforceDefault(sai_indexes_per_table_failure_threshold, v -> sai_indexes_per_table_failure_threshold = v, DEFAULT_INDEXES_PER_TABLE_THRESHOLD, DEFAULT_INDEXES_PER_TABLE_THRESHOLD);

        // SAI Table Failure threshold (maye be overridden via system property)
        Integer overrideTotalFailureThreshold = Integer.getInteger(INDEX_GUARDRAILS_TOTAL_FAILURE_THRESHOLD, UNSET);
        if (overrideTotalFailureThreshold != UNSET)
            sai_indexes_total_failure_threshold = overrideTotalFailureThreshold;
        enforceDefault(sai_indexes_total_failure_threshold, v -> sai_indexes_total_failure_threshold = v, DEFAULT_INDEXES_TOTAL_THRESHOLD, DEFAULT_INDEXES_TOTAL_THRESHOLD);
    }

    /**
     * Validate that the value provided for each guardrail setting is valid.
     *
     * @throws ConfigurationException if any of the settings has an invalid setting.
     */
    public void validate()
    {
        validateStrictlyPositiveInteger(column_value_size_failure_threshold_in_kb,
                                        "column_value_size_failure_threshold_in_kb");

        validateStrictlyPositiveInteger(columns_per_table_failure_threshold,
                                        "columns_per_table_failure_threshold");

        validateStrictlyPositiveInteger(fields_per_udt_failure_threshold,
                                        "fields_per_udt_failure_threshold");

        validateStrictlyPositiveInteger(collection_size_warn_threshold_in_kb,
                                        "collection_size_warn_threshold_in_kb");

        validateStrictlyPositiveInteger(items_per_collection_warn_threshold,
                                        "items_per_collection_warn_threshold");

        validateStrictlyPositiveInteger(tables_warn_threshold, "tables_warn_threshold");
        validateStrictlyPositiveInteger(tables_failure_threshold, "tables_failure_threshold");
        validateWarnLowerThanFail(tables_warn_threshold, tables_failure_threshold, "tables");

        validateDisallowedTableProperties();
        validateIgnoredTableProperties();

        validateStrictlyPositiveInteger(page_size_failure_threshold_in_kb, "page_size_failure_threshold_in_kb");

        validateStrictlyPositiveInteger(partition_size_warn_threshold_in_mb, "partition_size_warn_threshold_in_mb");

        validateStrictlyPositiveInteger(partition_keys_in_select_failure_threshold, "partition_keys_in_select_failure_threshold");

        validateStrictlyPositiveInteger(in_select_cartesian_product_failure_threshold, "in_select_cartesian_product_failure_threshold");

        validateDiskUsageThreshold();

        validateTombstoneThreshold(tombstone_warn_threshold, tombstone_failure_threshold);

        validateBatchSizeThreshold(batch_size_warn_threshold_in_kb, batch_size_fail_threshold_in_kb);
        validateStrictlyPositiveInteger(unlogged_batch_across_partitions_warn_threshold, "unlogged_batch_across_partitions_warn_threshold");

        for (String rawCL : write_consistency_levels_disallowed)
        {
            try
            {
                ConsistencyLevel.fromString(rawCL);
            }
            catch (Exception e)
            {
                throw new ConfigurationException(format("Invalid value for write_consistency_levels_disallowed guardrail: "
                                                        + "'%s' does not parse as a Consistency Level", rawCL));
            }
        }
    }

    /**
     * This validation method should only be called after {@link DatabaseDescriptor#createAllDirectories()} has been called.
     */
    public void validateAfterDataDirectoriesExist()
    {
        validateDiskUsageMaxSize();
    }

    @VisibleForTesting
    public void validateDiskUsageMaxSize()
    {
        long totalDiskSizeInGb = 0L;
        for (Directories.DataDirectory directory : Directories.dataDirectories.getAllDirectories())
        {
            totalDiskSizeInGb += SizeUnit.BYTES.toGigaBytes(directory.getTotalSpace());
        }

        if (totalDiskSizeInGb == 0L)
        {
            totalDiskSizeInGb = Long.MAX_VALUE;
        }
        validatePositiveNumeric(disk_usage_max_disk_size_in_gb, totalDiskSizeInGb, false, "disk_usage_max_disk_size_in_gb");
    }

    /**
     * Enforce default value based on {@link DatabaseDescriptor#isEmulateDbaasDefaults()} if
     * it's not specified in yaml
     *
     * @param current current config value defined in yaml
     * @param optionSetter setter to updated given config
     * @param onPremDefault default value for on-prem
     * @param dbaasDefault default value for constellation DB-as-a-service
     * @param <T>
     */
    private static <T> void enforceDefault(T current, Consumer<T> optionSetter, T onPremDefault, T dbaasDefault)
    {
        if (current != null)
            return;

        optionSetter.accept(DatabaseDescriptor.isEmulateDbaasDefaults() ? dbaasDefault : onPremDefault);
    }

    /**
     * @return true if given disk usage threshold disables disk usage guardrail
     */
    public static boolean diskUsageGuardrailDisabled(double value)
    {
        return value < 0;
    }

    /**
     * Validate that the values provided for disk usage are valid.
     *
     * @throws ConfigurationException if any of the settings has an invalid setting.
     */
    @VisibleForTesting
    public void validateDiskUsageThreshold()
    {
        validatePositiveNumeric(disk_usage_percentage_warn_threshold, 100, false, "disk_usage_percentage_warn_threshold");
        validatePositiveNumeric(disk_usage_percentage_failure_threshold, 100, false, "disk_usage_percentage_failure_threshold");
        validateWarnLowerThanFail(disk_usage_percentage_warn_threshold, disk_usage_percentage_failure_threshold, "disk_usage_percentage");
    }

    public void validateTombstoneThreshold(long warnThreshold, long failureThreshold)
    {
        validateStrictlyPositiveInteger(warnThreshold, "tombstone_warn_threshold");
        validateStrictlyPositiveInteger(failureThreshold, "tombstone_failure_threshold");
        validateWarnLowerThanFail(warnThreshold, failureThreshold, "tombstone_threshold");
    }

    private void validateDisallowedTableProperties()
    {
        Set<String> diff = Sets.difference(table_properties_disallowed.stream().map(String::toLowerCase).collect(Collectors.toSet()),
                                           TableAttributes.allKeywords());

        if (!diff.isEmpty())
            throw new ConfigurationException(format("Invalid value for table_properties_disallowed guardrail: "
                                                    + "'%s' do not parse as valid table properties", diff.toString()));
    }

    private void validateIgnoredTableProperties()
    {
        Set<String> diff = Sets.difference(table_properties_ignored.stream().map(String::toLowerCase).collect(Collectors.toSet()),
                                           TableAttributes.allKeywords());

        if (!diff.isEmpty())
            throw new ConfigurationException(format("Invalid value for table_properties_ignored guardrail: "
                                                    + "'%s' do not parse as valid table properties", diff.toString()));
    }

    private void validateStrictlyPositiveInteger(long value, String name)
    {
        // We use 'long' for generality, but most numeric guardrail cannot effectively be more than a 'int' for various
        // internal reasons. Not that any should ever come close in practice ...
        // Also, in most cases, zero does not make sense (allowing 0 tables or columns is not exactly useful).
        validatePositiveNumeric(value, Integer.MAX_VALUE, false, name);
    }

    private void validatePositiveNumeric(long value, long maxValue, boolean allowZero, String name)
    {
        if (value > maxValue)
            throw new ConfigurationException(format("Invalid value %d for guardrail %s: maximum allowed value is %d",
                                                    value, name, maxValue));

        if (value == 0 && !allowZero)
            throw new ConfigurationException(format("Invalid value for guardrail %s: 0 is not allowed", name));

        // We allow -1 as a general "disabling" flag. But reject anything lower to avoid mistakes.
        if (value < -1L)
            throw new ConfigurationException(format("Invalid value %d for guardrail %s: negative values are not "
                                                    + "allowed, outside of -1 which disables the guardrail",
                                                    value, name));
    }

    private void validateWarnLowerThanFail(long warnValue, long failValue, String guardName)
    {
        if (warnValue == -1 || failValue == -1)
            return;

        if (failValue < warnValue)
            throw new ConfigurationException(format("The warn threshold %d for the %s guardrail should be lower "
                                                    + "than the failure threshold %d",
                                                    warnValue, guardName, failValue));
    }

    public void setTombstoneFailureThreshold(int threshold)
    {
        validateTombstoneThreshold(tombstone_warn_threshold, threshold);
        tombstone_failure_threshold = threshold;
    }

    public void setTombstoneWarnThreshold(int threshold)
    {
        validateTombstoneThreshold(threshold, tombstone_failure_threshold);
        tombstone_warn_threshold = threshold;
    }


    public void validateBatchSizeThreshold(long warnThreshold, long failureThreshold)
    {
        validateStrictlyPositiveInteger(warnThreshold, "batch_size_warn_threshold_in_kb");
        validateStrictlyPositiveInteger(failureThreshold, "batch_size_fail_threshold_in_kb");
        validateWarnLowerThanFail(warnThreshold, failureThreshold, "batch_size_threshold");
    }

    public int getBatchSizeWarnThreshold()
    {
        return batch_size_warn_threshold_in_kb * 1024;
    }

    public int getBatchSizeFailThreshold()
    {
        return batch_size_fail_threshold_in_kb * 1024;
    }

    public void setBatchSizeWarnThresholdInKB(int threshold)
    {
        validateBatchSizeThreshold(threshold, batch_size_fail_threshold_in_kb);
        batch_size_warn_threshold_in_kb = threshold;
    }

    public void setBatchSizeFailThresholdInKB(int threshold)
    {
        validateBatchSizeThreshold(batch_size_warn_threshold_in_kb, threshold);
        batch_size_fail_threshold_in_kb = threshold;
    }
}
