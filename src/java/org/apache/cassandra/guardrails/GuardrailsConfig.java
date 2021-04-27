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
import com.google.common.collect.Sets;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;

/**
 * Configuration settings for guardrails (populated from the Yaml file).
 *
 * <p>Note that the settings here must only be used by the {@link Guardrails} class and not directly by the code
 * checking each guarded constraint (which, again, should use the higher level abstractions defined in
 * {@link Guardrails}).
 *
 * <p>This contains a main setting, {@code enabled}, controlling if guardrails are globally active or not, and
 * individual setting to control each guardrail. We have 2 variants of guardrails, soft (warn) and hard (fail) limits,
 * each guardrail having either one of the variant or both (note in particular that hard limits only make sense for
 * guardrails triggering during query execution. For other guardrails, say one triggering during compaction, failing
 * does not make sense).
 *
 * <p>If {@code enabled == false}, no limits should be enforced, be it soft or hard. Additionally, each individual
 * setting should have a specific value (typically -1 for numeric settings), that allows to disable the corresponding
 * guardrail.
 *
 * <p>The default values for each guardrail settings should reflect what is mandated for C* aaS environment.
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
    public static final Long NO_LIMIT = -1L;

    public Boolean enabled = false;

    public Long column_value_size_failure_threshold_in_kb;
    public Long columns_per_table_failure_threshold;

    public Long tables_warn_threshold;
    public Long tables_failure_threshold;
    public Set<String> table_properties_disallowed;

    public Boolean user_timestamps_enabled;

    public Long secondary_index_per_table_failure_threshold;
    public Long materialized_view_per_table_failure_threshold;

    public Set<String> write_consistency_levels_disallowed;

    public Integer partition_size_warn_threshold_in_mb;
    public Integer partition_keys_in_select_failure_threshold;

    // Limit number of terms and their cartesian product in IN query
    public Integer in_select_cartesian_product_failure_threshold;

    public Long fields_per_udt_failure_threshold;
    public Long collection_size_warn_threshold_in_kb;
    public Long items_per_collection_warn_threshold;

    public Integer disk_usage_percentage_warn_threshold;
    public Integer disk_usage_percentage_failure_threshold;

    public Boolean read_before_write_list_operations_enabled;

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

        validateStrictlyPositiveInteger(tables_warn_threshold, "tables_warn_threshold");
        validateStrictlyPositiveInteger(tables_failure_threshold, "tables_failure_threshold");
        validateWarnLowerThanFail(tables_warn_threshold, tables_failure_threshold, "tables");
        validateStrictlyPositiveInteger(partition_size_warn_threshold_in_mb, "partition_size_warn_threshold_in_mb");
        validateStrictlyPositiveInteger(partition_keys_in_select_failure_threshold, "partition_keys_in_select_failure_threshold");

        validateStrictlyPositiveInteger(fields_per_udt_failure_threshold, "fields_per_udt_failure_threshold");
        validateStrictlyPositiveInteger(collection_size_warn_threshold_in_kb, "collection_size_warn_threshold_in_kb");
        validateStrictlyPositiveInteger(items_per_collection_warn_threshold, "items_per_collection_warn_threshold");

        validateStrictlyPositiveInteger(in_select_cartesian_product_failure_threshold, "in_select_cartesian_product_failure_threshold");

        validateDisallowedTableProperties();

        validateDiskUsageThreshold();

        for (String rawCL : write_consistency_levels_disallowed)
        {
            try
            {
                ConsistencyLevel.fromString(rawCL);
            }
            catch (Exception e)
            {
                throw new ConfigurationException(format("Invalid value for write_consistency_level_disallowed guardrail: "
                                                        + "'%s' does not parse as a Consistency Level", rawCL));
            }
        }
    }

    /**
     * If {@link DatabaseDescriptor#isApplyDbaasDefaults()} is true, apply cloud defaults to guardrails settings that
     * are not specified in yaml; otherwise, apply on-prem defaults to guardrails settings that are not specified in yaml;
     */
    public void applyConfig()
    {
        enforceDefault(user_timestamps_enabled, v -> user_timestamps_enabled = v, true, true);

        enforceDefault(column_value_size_failure_threshold_in_kb, v -> column_value_size_failure_threshold_in_kb = v, NO_LIMIT, 5 * 1024L);

        enforceDefault(columns_per_table_failure_threshold, v -> columns_per_table_failure_threshold = v, NO_LIMIT, 20L);
        enforceDefault(secondary_index_per_table_failure_threshold, v -> secondary_index_per_table_failure_threshold = v, NO_LIMIT, 1L);
        enforceDefault(materialized_view_per_table_failure_threshold, v -> materialized_view_per_table_failure_threshold = v, NO_LIMIT, 2L);
        enforceDefault(tables_warn_threshold, v -> tables_warn_threshold = v, NO_LIMIT, 100L);
        enforceDefault(tables_failure_threshold, v -> tables_failure_threshold = v, NO_LIMIT, 200L);

        // We use a LinkedHashSet just for the sake of preserving the ordering in error messages
        enforceDefault(write_consistency_levels_disallowed,
                       v -> write_consistency_levels_disallowed = v,
                       Collections.<String>emptySet(),
                       new LinkedHashSet<>(Arrays.asList("ANY", "ONE", "LOCAL_ONE")));

        enforceDefault(table_properties_disallowed,
                       v -> table_properties_disallowed = v,
                       Collections.<String>emptySet(),
                       new LinkedHashSet<>(TableAttributes.validKeywords.stream().sorted().filter(p -> !p.equals("default_time_to_live")).collect(Collectors.toList())));

        enforceDefault(partition_size_warn_threshold_in_mb, v -> partition_size_warn_threshold_in_mb = v, 100, 100);
        enforceDefault(partition_keys_in_select_failure_threshold, v -> partition_keys_in_select_failure_threshold = v, NO_LIMIT.intValue(), 20);

        enforceDefault(fields_per_udt_failure_threshold, v -> fields_per_udt_failure_threshold = v, NO_LIMIT, 10L);
        enforceDefault(collection_size_warn_threshold_in_kb, v -> collection_size_warn_threshold_in_kb = v, NO_LIMIT, 5 * 1024L);
        enforceDefault(items_per_collection_warn_threshold, v -> items_per_collection_warn_threshold = v, NO_LIMIT, 20L);

        // for node status
        enforceDefault(disk_usage_percentage_warn_threshold, v -> disk_usage_percentage_warn_threshold = v, NO_LIMIT.intValue(), 70);
        enforceDefault(disk_usage_percentage_failure_threshold, v -> disk_usage_percentage_failure_threshold = v, NO_LIMIT.intValue(), 80);

        enforceDefault(in_select_cartesian_product_failure_threshold, v -> in_select_cartesian_product_failure_threshold = v, NO_LIMIT.intValue(), 25);
        enforceDefault(read_before_write_list_operations_enabled, v -> read_before_write_list_operations_enabled = v, true, false);
    }

    private void validateDisallowedTableProperties()
    {
        Set<String> diff = Sets.difference(table_properties_disallowed.stream().map(String::toLowerCase).collect(Collectors.toSet()),
                                           TableAttributes.validKeywords);

        if (!diff.isEmpty())
            throw new ConfigurationException(format("Invalid value for table_properties_disallowed guardrail: "
                                                    + "'%s' do not parse as valid table properties", diff.toString()));
    }

    private void validateStrictlyPositiveInteger(long value, String name)
    {
        // We use 'long' for generality, but most numeric guardrails cannot effectively be more than an 'int' for various
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

    /**
     * Enforce default value based on {@link DatabaseDescriptor#isApplyDbaasDefaults()} if
     * it's not specified in yaml
     *
     * @param current       current config value defined in yaml
     * @param optionSetter  setter to updated given config
     * @param onPremDefault default value for on-prem
     * @param dbaasDefault  default value for constellation DB-as-a-service
     * @param <T>
     */
    private static <T> void enforceDefault(T current, Consumer<T> optionSetter, T onPremDefault, T dbaasDefault)
    {
        if (current != null)
            return;

        optionSetter.accept(DatabaseDescriptor.isApplyDbaasDefaults() ? dbaasDefault : onPremDefault);
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
}
