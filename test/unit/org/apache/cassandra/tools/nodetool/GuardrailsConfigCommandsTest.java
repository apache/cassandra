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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.tools.nodetool.GuardrailsConfigCommand.GuardrailCategory;

import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

        ToolResult getFlagsResult = invokeNodetool("getguardrailsconfig", "-c", "flags");
        getFlagsResult.asserts().success();
        assertEquals(removeMultipleSpaces(ALL_FLAGS_GETTER_OUTPUT), getOutput(getFlagsResult));

        ToolResult getValuesResult = invokeNodetool("getguardrailsconfig", "-c", "values");
        getValuesResult.asserts().success();
        assertEquals(removeMultipleSpaces(ALL_VALUES_GETTER_OUTPUT), getOutput(getValuesResult));

        ToolResult getThresholdsResult = invokeNodetool("getguardrailsconfig", "-c", "thresholds");
        getValuesResult.asserts().success();
        assertEquals(removeMultipleSpaces(ALL_THRESHOLDS_GETTER_OUTPUT), getOutput(getThresholdsResult));

        ToolResult wrongCategory = invokeNodetool("getguardrailsconfig", "-c", "nonsense");
        wrongCategory.asserts().failure();
        assertTrue(getOutput(wrongCategory).contains("Error: Illegal value for -c/--category used: 'nonsense'. Supported values are values,thresholds,flags,others."));

        // individual guardrail
        ToolResult individualResult = invokeNodetool("getguardrailsconfig", "group_by_enabled");
        getResult.asserts().success();
        assertEquals("true\n", getOutput(individualResult));

        // more than one result
        ToolResult multipleResult = invokeNodetool("getguardrailsconfig", "group_by_enabled", "keyspaces_fail_threshold");
        getResult.asserts().success();
        assertEquals("group_by_enabled true\nkeyspaces_fail_threshold -1\n", getOutput(multipleResult));

        // category with individual
        ToolResult categoryWithIndividualResult = invokeNodetool("getguardrailsconfig", "-c", "values", "group_by_enabled");
        categoryWithIndividualResult.asserts().failure();
        assertTrue(categoryWithIndividualResult.getStdout().contains("Do not specify additional arguments when --category/-c is set."));

        // get config of all guardrails enumerated on command line
        String[] allLines = ALL_GUARDRAILS_GETTER_OUTPUT.split("\n");

        List<String> argsForAllGuardrails = new ArrayList<>();
        argsForAllGuardrails.add("getguardrailsconfig");

        for (String line : allLines)
            argsForAllGuardrails.add(line.split(" ")[0]);

        ToolResult allGuardrails = invokeNodetool(argsForAllGuardrails);
        allGuardrails.asserts().success();
        assertEquals(ALL_GUARDRAILS_GETTER_OUTPUT, removeMultipleSpaces(allGuardrails.getStdout()));

        // set

        ToolResult setResultNoArgs = invokeNodetool("setguardrailsconfig");
        setResultNoArgs.asserts().failure();
        assertTrue(getOutput(setResultNoArgs).contains("No arguments."));

        ToolResult setResultList = invokeNodetool("setguardrailsconfig", "--list");
        setResultList.asserts().success();
        setResultList = invokeNodetool("setguardrailsconfig", "-l");
        setResultList.asserts().success();
        assertEquals(removeMultipleSpaces(ALL_GUARDRAILS_SETTER_OUTPUT), getOutput(setResultList));

        ToolResult categoryList = invokeNodetool("setguardrailsconfig", "--list", "--category", "values");
        categoryList.asserts().success();

        for (GuardrailCategory category : GuardrailCategory.values())
        {
            categoryList = invokeNodetool("setguardrailsconfig", "-l", "-c", category.name());
            categoryList.asserts().success();

            String expectedOutput = null;
            switch (category)
            {
                case flags:
                    expectedOutput = ALL_FLAGS_SETTER_OUTPUT;
                    break;
                case values:
                    expectedOutput = ALL_VALUES_SETTER_OUTPUT;
                    break;
                case thresholds:
                    expectedOutput = ALL_THRESHOLDS_SETTER_OUTPUT;
                    break;
                case others:
                    expectedOutput = ALL_OTHER_SETTER_OUTPUT;
                    break;
                default:
                    fail("Untested category " + category);
            }
            assertEquals(removeMultipleSpaces(expectedOutput), getOutput(categoryList));
        }

        // test -c without -l does not make sense
        ToolResult emptyCategoryListing = invokeNodetool("setguardrailsconfig", "-l", "-c");
        emptyCategoryListing.asserts().failure();
        assertTrue(getOutput(emptyCategoryListing).contains("Required values for option 'guardrailCategory' not provided"));

        // test -c alone does not make sense
        ToolResult onlyCategory = invokeNodetool("setguardrailsconfig", "-c", "values");
        onlyCategory.asserts().failure();
        assertTrue(getOutput(onlyCategory).contains("--category/-c can be specified only together with --list/-l"));

        // it would be quite cumbersome to test all guardrails are settable so we will
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

        // threshold
        setThresholds("keyspaces_threshold", "10", "20");
        assertEquals("20", getThreshold("keyspaces_fail_threshold"));
        assertEquals("10", getThreshold("keyspaces_warn_threshold"));
        setThresholds("keyspaces_threshold", "-1", "-1");
        assertEquals("-1", getThreshold("keyspaces_fail_threshold"));
        assertEquals("-1", getThreshold("keyspaces_warn_threshold"));

        // test incorrect number of parameters or invalid values
        ToolResult invalidNumberOfArgsForThreshold = invokeNodetool("setguardrailsconfig", "keyspaces_threshold", "10", "20", "30");
        invalidNumberOfArgsForThreshold.asserts().failure();
        assertTrue(invalidNumberOfArgsForThreshold.getStdout().contains("keyspaces_threshold is expecting 2 argument values. Getting 3 instead."));

        // not separated by comma
        ToolResult invalidNumberOfArgsForValues = invokeNodetool("setguardrailsconfig", "table_properties_warned", "comment", "cdc");
        invalidNumberOfArgsForValues.asserts().failure();
        assertTrue(invalidNumberOfArgsForValues.getStdout().contains("table_properties_warned is expecting 1 argument values. Getting 2 instead."));

        // invalid boolean
        ToolResult invalidBooleanForFlags = invokeNodetool("setguardrailsconfig", "allow_filtering_enabled", "nonsense");
        invalidBooleanForFlags.asserts().failure();
        assertTrue(invalidBooleanForFlags.getStdout().contains("Use 'true' or 'false' values for booleans"));

        // test propagation of errors from guardrail when values are wrong
        ToolResult nonsenseSetterArgs = invokeNodetool("setguardrailsconfig", "keyspaces_threshold", "-10", "-20");
        nonsenseSetterArgs.asserts().failure();
        assertTrue(nonsenseSetterArgs.getStdout().contains("Error occured when setting the config for setter keyspaces_threshold with arguments [-10, -20]: " +
                                                           "Invalid value -10 for keyspaces_warn_threshold: negative values are not allowed, outside of -1 which disables the guardrail"));
    }

    private static final String ALL_FLAGS_GETTER_OUTPUT =
    "allow_filtering_enabled                      true \n" +
    "alter_table_enabled                          true \n" +
    "compact_tables_enabled                       true \n" +
    "drop_keyspace_enabled                        true \n" +
    "drop_truncate_table_enabled                  true \n" +
    "group_by_enabled                             true \n" +
    "intersect_filtering_query_enabled            true \n" +
    "intersect_filtering_query_warned             true \n" +
    "non_partition_restricted_query_enabled       true \n" +
    "read_before_write_list_operations_enabled    true \n" +
    "secondary_indexes_enabled                    true \n" +
    "simple_strategy_enabled                      true \n" +
    "uncompressed_tables_enabled                  true \n" +
    "user_timestamps_enabled                      true \n" +
    "vector_type_enabled                          true \n" +
    "zero_ttl_on_twcs_enabled                     true \n" +
    "zero_ttl_on_twcs_warned                      true \n";

    private static final String ALL_THRESHOLDS_GETTER_OUTPUT =
    "collection_size_fail_threshold               null \n" +
    "collection_size_warn_threshold               null \n" +
    "column_value_size_fail_threshold             null \n" +
    "column_value_size_warn_threshold             null \n" +
    "columns_per_table_fail_threshold             -1   \n" +
    "columns_per_table_warn_threshold             -1   \n" +
    "data_disk_usage_percentage_fail_threshold    -1   \n" +
    "data_disk_usage_percentage_warn_threshold    -1   \n" +
    "fields_per_udt_fail_threshold                -1   \n" +
    "fields_per_udt_warn_threshold                -1   \n" +
    "in_select_cartesian_product_fail_threshold   -1   \n" +
    "in_select_cartesian_product_warn_threshold   -1   \n" +
    "items_per_collection_fail_threshold          -1   \n" +
    "items_per_collection_warn_threshold          -1   \n" +
    "keyspaces_fail_threshold                     -1   \n" +
    "keyspaces_warn_threshold                     -1   \n" +
    "materialized_views_per_table_fail_threshold  -1   \n" +
    "materialized_views_per_table_warn_threshold  -1   \n" +
    "maximum_replication_factor_fail_threshold    -1   \n" +
    "maximum_replication_factor_warn_threshold    -1   \n" +
    "maximum_timestamp_fail_threshold             null \n" +
    "maximum_timestamp_warn_threshold             null \n" +
    "minimum_replication_factor_fail_threshold    -1   \n" +
    "minimum_replication_factor_warn_threshold    -1   \n" +
    "minimum_timestamp_fail_threshold             null \n" +
    "minimum_timestamp_warn_threshold             null \n" +
    "page_size_fail_threshold                     -1   \n" +
    "page_size_warn_threshold                     -1   \n" +
    "partition_keys_in_select_fail_threshold      -1   \n" +
    "partition_keys_in_select_warn_threshold      -1   \n" +
    "partition_size_fail_threshold                null \n" +
    "partition_size_warn_threshold                null \n" +
    "partition_tombstones_fail_threshold          -1   \n" +
    "partition_tombstones_warn_threshold          -1   \n" +
    "sai_frozen_term_size_fail_threshold          8KiB \n" +
    "sai_frozen_term_size_warn_threshold          1KiB \n" +
    "sai_sstable_indexes_per_query_fail_threshold -1   \n" +
    "sai_sstable_indexes_per_query_warn_threshold 32   \n" +
    "sai_string_term_size_fail_threshold          8KiB \n" +
    "sai_string_term_size_warn_threshold          1KiB \n" +
    "sai_vector_term_size_fail_threshold          32KiB\n" +
    "sai_vector_term_size_warn_threshold          16KiB\n" +
    "secondary_indexes_per_table_fail_threshold   -1   \n" +
    "secondary_indexes_per_table_warn_threshold   -1   \n" +
    "tables_fail_threshold                        -1   \n" +
    "tables_warn_threshold                        -1   \n" +
    "vector_dimensions_fail_threshold             -1   \n" +
    "vector_dimensions_warn_threshold             -1   \n";

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

    private static final String ALL_FLAGS_SETTER_OUTPUT =
    "allow_filtering_enabled                   boolean                             \n" +
    "alter_table_enabled                       boolean                             \n" +
    "compact_tables_enabled                    boolean                             \n" +
    "drop_keyspace_enabled                     boolean                             \n" +
    "drop_truncate_table_enabled               boolean                             \n" +
    "group_by_enabled                          boolean                             \n" +
    "intersect_filtering_query_enabled         boolean                             \n" +
    "intersect_filtering_query_warned          boolean                             \n" +
    "non_partition_restricted_query_enabled    boolean                             \n" +
    "read_before_write_list_operations_enabled boolean                             \n" +
    "secondary_indexes_enabled                 boolean                             \n" +
    "simple_strategy_enabled                   boolean                             \n" +
    "uncompressed_tables_enabled               boolean                             \n" +
    "user_timestamps_enabled                   boolean                             \n" +
    "vector_type_enabled                       boolean                             \n" +
    "zero_ttl_on_twcs_enabled                  boolean                             \n" +
    "zero_ttl_on_twcs_warned                   boolean                             \n";

    private static final String ALL_THRESHOLDS_SETTER_OUTPUT =
    "collection_size_threshold                 [java.lang.String, java.lang.String]\n" +
    "column_value_size_threshold               [java.lang.String, java.lang.String]\n" +
    "columns_per_table_threshold               [int, int]                          \n" +
    "data_disk_usage_percentage_threshold      [int, int]                          \n" +
    "fields_per_udt_threshold                  [int, int]                          \n" +
    "in_select_cartesian_product_threshold     [int, int]                          \n" +
    "items_per_collection_threshold            [int, int]                          \n" +
    "keyspaces_threshold                       [int, int]                          \n" +
    "materialized_views_per_table_threshold    [int, int]                          \n" +
    "maximum_replication_factor_threshold      [int, int]                          \n" +
    "maximum_timestamp_threshold               [java.lang.String, java.lang.String]\n" +
    "minimum_replication_factor_threshold      [int, int]                          \n" +
    "minimum_timestamp_threshold               [java.lang.String, java.lang.String]\n" +
    "page_size_threshold                       [int, int]                          \n" +
    "partition_keys_in_select_threshold        [int, int]                          \n" +
    "partition_size_threshold                  [java.lang.String, java.lang.String]\n" +
    "partition_tombstones_threshold            [long, long]                        \n" +
    "sai_frozen_term_size_threshold            [java.lang.String, java.lang.String]\n" +
    "sai_sstable_indexes_per_query_threshold   [int, int]                          \n" +
    "sai_string_term_size_threshold            [java.lang.String, java.lang.String]\n" +
    "sai_vector_term_size_threshold            [java.lang.String, java.lang.String]\n" +
    "secondary_indexes_per_table_threshold     [int, int]                          \n" +
    "tables_threshold                          [int, int]                          \n" +
    "vector_dimensions_threshold               [int, int]                          \n";

    private static final String ALL_VALUES_SETTER_OUTPUT =
    "read_consistency_levels_disallowed        java.util.Set                       \n" +
    "read_consistency_levels_warned            java.util.Set                       \n" +
    "table_properties_disallowed               java.util.Set                       \n" +
    "table_properties_ignored                  java.util.Set                       \n" +
    "table_properties_warned                   java.util.Set                       \n" +
    "write_consistency_levels_disallowed       java.util.Set                       \n" +
    "write_consistency_levels_warned           java.util.Set                       \n";

    private static final String ALL_OTHER_SETTER_OUTPUT =
    "data_disk_usage_max_disk_size             java.lang.String                    \n";


    private static final String ALL_GUARDRAILS_GETTER_OUTPUT = removeMultipleSpaces(ALL_FLAGS_GETTER_OUTPUT +
                                                                                    ALL_THRESHOLDS_GETTER_OUTPUT +
                                                                                    ALL_VALUES_GETTER_OUTPUT +
                                                                                    ALL_OTHER_GETTER_OUTPUT);

    private static final String ALL_GUARDRAILS_SETTER_OUTPUT = removeMultipleSpaces(ALL_FLAGS_SETTER_OUTPUT +
                                                                                    ALL_THRESHOLDS_SETTER_OUTPUT +
                                                                                    ALL_VALUES_SETTER_OUTPUT +
                                                                                    ALL_OTHER_SETTER_OUTPUT);

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

    private ToolResult setThresholds(String name, String warn, String fail)
    {
        return invokeNodetool("setguardrailsconfig", name, warn, fail);
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
