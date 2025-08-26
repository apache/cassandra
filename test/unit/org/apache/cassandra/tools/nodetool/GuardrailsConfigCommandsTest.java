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

package org.apache.cassandra.tools.nodetool;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.guardrails.GuardrailsMBean;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.tools.nodetool.GuardrailsConfigCommand.GetGuardrailsConfig;

import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GuardrailsConfigCommandsTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testGuardrailsConfigCommands()
    {
        ToolResult getResult = invokeNodetool("getguardrailsconfig");
        getResult.asserts().success();
        assertEquals(removeMultipleSpaces(ALL_GUARDRAILS_GETTER_OUTPUT), getOutput(getResult));

        ToolResult getResultVerbose = invokeNodetool("getguardrailsconfig", "--expand");
        getResultVerbose.asserts().success();
        assertEquals(removeMultipleSpaces(ALL_GUARDRAILS_GETTER_VERBOSE_OUTPUT), getOutput(getResultVerbose));

        ToolResult getFlagsResult = invokeNodetool("getguardrailsconfig", "-c", "flags");
        getFlagsResult.asserts().success();
        assertEquals(removeMultipleSpaces(ALL_FLAGS_GETTER_OUTPUT), getOutput(getFlagsResult));

        ToolResult getValuesResult = invokeNodetool("getguardrailsconfig", "-c", "values");
        getValuesResult.asserts().success();
        assertEquals(removeMultipleSpaces(ALL_VALUES_GETTER_OUTPUT), getOutput(getValuesResult));

        ToolResult getThresholdsResult = invokeNodetool("getguardrailsconfig", "-c", "thresholds");
        getThresholdsResult.asserts().success();
        assertEquals(removeMultipleSpaces(ALL_THRESHOLDS_GETTER_OUTPUT), getOutput(getThresholdsResult));

        ToolResult wrongCategory = invokeNodetool("getguardrailsconfig", "-c", "nonsense");
        wrongCategory.asserts().failure();
        assertTrue(getOutput(wrongCategory).contains("Invalid value for option '--category': expected one of [values, thresholds, flags, others] (case-sensitive) but was 'nonsense'"));

        // individual guardrail
        ToolResult individualResult = invokeNodetool("getguardrailsconfig", "group_by_enabled");
        individualResult.asserts().success();
        assertEquals("true\n", getOutput(individualResult));

        // more than one guardrail
        ToolResult multipleResult = invokeNodetool("getguardrailsconfig", "group_by_enabled", "keyspaces_fail_threshold");
        multipleResult.asserts().failure();
        assertTrue(getOutput(multipleResult).contains("Unmatched argument at index 6: 'keyspaces_fail_threshold'"));

        // category with individual
        ToolResult categoryWithIndividualResult = invokeNodetool("getguardrailsconfig", "-c", "values", "group_by_enabled");
        categoryWithIndividualResult.asserts().failure();
        assertTrue(categoryWithIndividualResult.getStdout().contains("Do not specify additional arguments when --category/-c is set."));

        // set

        ToolResult setResultNoArgs = invokeNodetool("setguardrailsconfig");
        setResultNoArgs.asserts().failure();
        assertTrue(getOutput(setResultNoArgs).contains("No arguments."));

        // it would be quite cumbersome to test all guardrails are settable, so we will
        // set one from each category to prove the point

        // flag
        setFlag("allow_filtering_enabled", false);
        assertFalse(getFlag("allow_filtering_enabled"));
        setFlag("allow_filtering_enabled", true);
        assertTrue(getFlag("allow_filtering_enabled"));

        // value
        setValues("table_properties_warned", "comment", "cdc");
        assertArrayEquals(new String[]{ "comment", "cdc" }, getValues("table_properties_warned"));
        setValues("table_properties_warned", "null");
        assertArrayEquals(new String[0], getValues("table_properties_warned"));
        setValues("table_properties_warned", "comment", "cdc");
        assertArrayEquals(new String[]{ "comment", "cdc" }, getValues("table_properties_warned"));
        setValues("table_properties_warned", "[]");
        assertArrayEquals(new String[0], getValues("table_properties_warned"));

        // threshold
        setThresholds("keyspaces_threshold", "20", "10");
        assertEquals("20", getThreshold("keyspaces_fail_threshold"));
        assertEquals("10", getThreshold("keyspaces_warn_threshold"));
        setThresholds("keyspaces_threshold", "-1", "-1");
        assertEquals("-1", getThreshold("keyspaces_fail_threshold"));
        assertEquals("-1", getThreshold("keyspaces_warn_threshold"));

        // test incorrect number of parameters or invalid values
        ToolResult invalidNumberOfArgsForThreshold = invokeNodetool("setguardrailsconfig", "keyspaces_threshold", "10", "20", "30");
        invalidNumberOfArgsForThreshold.asserts().failure();
        assertTrue(invalidNumberOfArgsForThreshold.getStdout().contains("keyspaces_threshold is expecting 2 argument values. Getting 3 instead."));

        // separated by comma
        ToolResult argumentsForValuesSeparatedByComma = invokeNodetool("setguardrailsconfig", "table_properties_warned", "comment,cdc");
        argumentsForValuesSeparatedByComma.asserts().success();

        // enumerated
        ToolResult argumentsForValuesEnumerated = invokeNodetool("setguardrailsconfig", "table_properties_warned", "comment", "cdc");
        argumentsForValuesEnumerated.asserts().success();

        // invalid boolean
        ToolResult invalidBooleanForFlags = invokeNodetool("setguardrailsconfig", "allow_filtering_enabled", "nonsense");
        invalidBooleanForFlags.asserts().failure();
        assertTrue(invalidBooleanForFlags.getStdout().contains("Use 'true' or 'false' values for booleans"));

        // test propagation of errors from guardrail when values are wrong
        ToolResult nonsenseSetterArgs = invokeNodetool("setguardrailsconfig", "keyspaces_threshold", "-10", "-20");
        nonsenseSetterArgs.asserts().failure();
        assertTrue(nonsenseSetterArgs.getStdout().contains("Error occured when setting the config for setter keyspaces_threshold with arguments [-10, -20]: " +
                                                           "Invalid value -20 for keyspaces_warn_threshold: negative values are not allowed, outside of -1 which disables the guardrail"));

        // invalid set name
        ToolResult setInvalidName = invokeNodetool("setguardrailsconfig", "non_sense", "10");
        setInvalidName.asserts().failure();
        assertTrue(setInvalidName.getStdout().contains("Guardrail non_sense not found."));

        // invalid get name
        ToolResult getInvalidName = invokeNodetool("getguardrailsconfig", "non_sense");
        getInvalidName.asserts().failure();
        assertTrue(getInvalidName.getStdout().contains("Guardrail non_sense not found."));
    }

    @Test
    public void testParsedGuardrailNamesFromMBeanExistInCassandraYaml()
    {
        Set<String> configFieldNames = getConfigFieldNames();
        Map<String, List<Method>> snakeCaseGuardrailsMap = GetGuardrailsConfig.parseGuardrailNames(GuardrailsMBean.class.getDeclaredMethods(), null);

        for (Map.Entry<String, List<Method>> entry : snakeCaseGuardrailsMap.entrySet())
        {
            for (Method method : entry.getValue())
            {
                String guardrailName = GuardrailsConfigCommand.toSnakeCase(method.getName().substring(3));
                if (entry.getValue().size() == 1)
                    assertEquals(entry.getKey(), guardrailName);
                // else it is threshold, so it does not match the key

                // assert converted snake-case guardrail name is actually in Config / cassandra.yaml
                assertTrue(configFieldNames.contains(guardrailName));
            }
        }
    }

    private Set<String> getConfigFieldNames()
    {
        Set<String> variableNames = new HashSet<>();
        for (Field field : Config.class.getFields())
        {
            // ignore the constants
            if (Modifier.isFinal(field.getModifiers()))
                continue;
            variableNames.add(field.getName());
        }

        return variableNames;
    }


    private static final String ALL_FLAGS_GETTER_OUTPUT =
    "allow_filtering_enabled                         true\n" +
    "alter_table_enabled                             true\n" +
    "bulk_load_enabled                               true\n" +
    "compact_tables_enabled                          true\n" +
    "drop_keyspace_enabled                           true\n" +
    "drop_truncate_table_enabled                     true\n" +
    "group_by_enabled                                true\n" +
    "intersect_filtering_query_enabled               true\n" +
    "intersect_filtering_query_warned                true\n" +
    "non_partition_restricted_index_query_enabled    true\n" +
    "read_before_write_list_operations_enabled       true\n" +
    "secondary_indexes_enabled                       true\n" +
    "simplestrategy_enabled                          true\n" +
    "uncompressed_tables_enabled                     true\n" +
    "user_timestamps_enabled                         true\n" +
    "vector_type_enabled                             true\n" +
    "zero_ttl_on_twcs_enabled                        true\n" +
    "zero_ttl_on_twcs_warned                         true\n";

    private static final String ALL_THRESHOLDS_GETTER_OUTPUT =
    "collection_list_size_threshold               [null, null]  \n" +
    "collection_map_size_threshold                [null, null]  \n" +
    "collection_set_size_threshold                [null, null]  \n" +
    "collection_size_threshold                    [null, null]  \n" +
    "column_ascii_value_size_threshold            [null, null]  \n" +
    "column_blob_value_size_threshold             [null, null]  \n" +
    "column_text_and_varchar_value_size_threshold [null, null]  \n" +
    "column_value_size_threshold                  [null, null]  \n" +
    "columns_per_table_threshold                  [-1, -1]      \n" +
    "data_disk_usage_percentage_threshold         [-1, -1]      \n" +
    "fields_per_udt_threshold                     [-1, -1]      \n" +
    "in_select_cartesian_product_threshold        [-1, -1]      \n" +
    "items_per_collection_threshold               [-1, -1]      \n" +
    "keyspaces_threshold                          [-1, -1]      \n" +
    "materialized_views_per_table_threshold       [-1, -1]      \n" +
    "maximum_replication_factor_threshold         [-1, -1]      \n" +
    "maximum_timestamp_threshold                  [null, null]  \n" +
    "minimum_replication_factor_threshold         [-1, -1]      \n" +
    "minimum_timestamp_threshold                  [null, null]  \n" +
    "page_size_threshold                          [-1, -1]      \n" +
    "partition_keys_in_select_threshold           [-1, -1]      \n" +
    "partition_size_threshold                     [null, null]  \n" +
    "partition_tombstones_threshold               [-1, -1]      \n" +
    "sai_frozen_term_size_threshold               [8KiB, 1KiB]  \n" +
    "sai_sstable_indexes_per_query_threshold      [-1, 32]      \n" +
    "sai_string_term_size_threshold               [8KiB, 1KiB]  \n" +
    "sai_vector_term_size_threshold               [32KiB, 16KiB]\n" +
    "secondary_indexes_per_table_threshold        [-1, -1]      \n" +
    "tables_threshold                             [-1, -1]      \n" +
    "vector_dimensions_threshold                  [-1, -1]      \n";

    private static final String ALL_THRESHOLDS_GETTER_VERBOSE_OUTPUT =
    "collection_list_size_fail_threshold               null \n" +
    "collection_list_size_warn_threshold               null \n" +
    "collection_map_size_fail_threshold                null \n" +
    "collection_map_size_warn_threshold                null \n" +
    "collection_set_size_fail_threshold                null \n" +
    "collection_set_size_warn_threshold                null \n" +
    "collection_size_fail_threshold                    null \n" +
    "collection_size_warn_threshold                    null \n" +
    "column_ascii_value_size_fail_threshold            null \n" +
    "column_ascii_value_size_warn_threshold            null \n" +
    "column_blob_value_size_fail_threshold             null \n" +
    "column_blob_value_size_warn_threshold             null \n" +
    "column_text_and_varchar_value_size_fail_threshold null \n" +
    "column_text_and_varchar_value_size_warn_threshold null \n" +
    "column_value_size_fail_threshold                  null \n" +
    "column_value_size_warn_threshold                  null \n" +
    "columns_per_table_fail_threshold                  -1   \n" +
    "columns_per_table_warn_threshold                  -1   \n" +
    "data_disk_usage_percentage_fail_threshold         -1   \n" +
    "data_disk_usage_percentage_warn_threshold         -1   \n" +
    "fields_per_udt_fail_threshold                     -1   \n" +
    "fields_per_udt_warn_threshold                     -1   \n" +
    "in_select_cartesian_product_fail_threshold        -1   \n" +
    "in_select_cartesian_product_warn_threshold        -1   \n" +
    "items_per_collection_fail_threshold               -1   \n" +
    "items_per_collection_warn_threshold               -1   \n" +
    "keyspaces_fail_threshold                          -1   \n" +
    "keyspaces_warn_threshold                          -1   \n" +
    "materialized_views_per_table_fail_threshold       -1   \n" +
    "materialized_views_per_table_warn_threshold       -1   \n" +
    "maximum_replication_factor_fail_threshold         -1   \n" +
    "maximum_replication_factor_warn_threshold         -1   \n" +
    "maximum_timestamp_fail_threshold                  null \n" +
    "maximum_timestamp_warn_threshold                  null \n" +
    "minimum_replication_factor_fail_threshold         -1   \n" +
    "minimum_replication_factor_warn_threshold         -1   \n" +
    "minimum_timestamp_fail_threshold                  null \n" +
    "minimum_timestamp_warn_threshold                  null \n" +
    "page_size_fail_threshold                          -1   \n" +
    "page_size_warn_threshold                          -1   \n" +
    "partition_keys_in_select_fail_threshold           -1   \n" +
    "partition_keys_in_select_warn_threshold           -1   \n" +
    "partition_size_fail_threshold                     null \n" +
    "partition_size_warn_threshold                     null \n" +
    "partition_tombstones_fail_threshold               -1   \n" +
    "partition_tombstones_warn_threshold               -1   \n" +
    "sai_frozen_term_size_fail_threshold               8KiB \n" +
    "sai_frozen_term_size_warn_threshold               1KiB \n" +
    "sai_sstable_indexes_per_query_fail_threshold      -1   \n" +
    "sai_sstable_indexes_per_query_warn_threshold      32   \n" +
    "sai_string_term_size_fail_threshold               8KiB \n" +
    "sai_string_term_size_warn_threshold               1KiB \n" +
    "sai_vector_term_size_fail_threshold               32KiB\n" +
    "sai_vector_term_size_warn_threshold               16KiB\n" +
    "secondary_indexes_per_table_fail_threshold        -1   \n" +
    "secondary_indexes_per_table_warn_threshold        -1   \n" +
    "tables_fail_threshold                             -1   \n" +
    "tables_warn_threshold                             -1   \n" +
    "vector_dimensions_fail_threshold                  -1   \n" +
    "vector_dimensions_warn_threshold                  -1   \n";

    private static final String ALL_VALUES_GETTER_OUTPUT =
    "read_consistency_levels_disallowed           []   \n" +
    "read_consistency_levels_warned               []   \n" +
    "table_properties_disallowed                  []   \n" +
    "table_properties_ignored                     []   \n" +
    "table_properties_warned                      []   \n" +
    "write_consistency_levels_disallowed          []   \n" +
    "write_consistency_levels_warned              []   \n";

    private static final String ALL_OTHER_GETTER_OUTPUT =
    "data_disk_usage_max_disk_size                null \n";


    private static final String ALL_GUARDRAILS_GETTER_OUTPUT = removeMultipleSpaces(ALL_FLAGS_GETTER_OUTPUT +
                                                                                    ALL_THRESHOLDS_GETTER_OUTPUT +
                                                                                    ALL_VALUES_GETTER_OUTPUT +
                                                                                    ALL_OTHER_GETTER_OUTPUT);

    private static final String ALL_GUARDRAILS_GETTER_VERBOSE_OUTPUT = removeMultipleSpaces(ALL_FLAGS_GETTER_OUTPUT +
                                                                                            ALL_THRESHOLDS_GETTER_VERBOSE_OUTPUT +
                                                                                            ALL_VALUES_GETTER_OUTPUT +
                                                                                            ALL_OTHER_GETTER_OUTPUT);

    private static String removeMultipleSpaces(String input)
    {
        return input.replaceAll(" +", " ").replaceAll(" \n", "\n");
    }

    private String getOutput(ToolResult toolResult)
    {
        return removeMultipleSpaces(toolResult.getStdout());
    }

    private ToolResult setFlag(String name, Boolean flag)
    {
        return invokeNodetool("setguardrailsconfig", name, flag.toString());
    }

    private ToolResult setThresholds(String name, String fail, String warn)
    {
        return invokeNodetool("setguardrailsconfig", name, fail, warn);
    }

    private ToolResult setValues(String name, String... values)
    {
        return invokeNodetool("setguardrailsconfig", name, String.join(",", Arrays.asList(values)));
    }

    private boolean getFlag(String name)
    {
        return Boolean.parseBoolean(invokeNodetool("getguardrailsconfig", name).getStdout().replaceAll("\n", ""));
    }

    private String getThreshold(String name)
    {
        return invokeNodetool("getguardrailsconfig", name).getStdout().replaceAll("\n", "");
    }

    private String[] getValues(String name)
    {
        String[] split = invokeNodetool("getguardrailsconfig", name).getStdout()
                                                                    .replace("\n", "")
                                                                    .replace("[", "")
                                                                    .replace("]", "")
                                                                    .replace(" ", "")
                                                                    .split(",");

        if (split.length == 1 && split[0].isEmpty())
            return new String[0];
        else
            return split;
    }
}
