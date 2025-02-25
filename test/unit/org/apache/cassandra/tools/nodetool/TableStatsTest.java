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
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.JsonUtils;
import org.yaml.snakeyaml.Yaml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * @see TableStats
 */
public class TableStatsTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testTableStats()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_schema");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Table:")).isGreaterThan(1);

        tool = ToolRunner.invokeNodetool("tablestats", "system_distributed");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_distributed");
        assertThat(tool.getStdout()).doesNotContain("Keyspace : system_schema");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Table:")).isGreaterThan(1);
    }

    @Test
    public void testTableIgnoreArg()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-i", "system_schema.aggregates");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_schema");
        assertThat(tool.getStdout()).doesNotContain("Table: system_schema.aggregates");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Table:")).isGreaterThan(1);
    }

    @Test
    public void testHumanReadableArg()
    {
        Arrays.asList("-H", "--human-readable").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg);
            tool.assertOnCleanExit();
            assertThat(tool.getStdout()).contains(" KiB");
        });
    }

    @Test
    public void testSortArg()
    {
        Pattern regExp = Pattern.compile("((?m)Table: .*$)");

        Arrays.asList("-s", "--sort").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg, "table_name");
            Matcher m = regExp.matcher(tool.getStdout());
            ArrayList<String> orig = new ArrayList<>();
            while (m.find())
                orig.add(m.group(1));

            tool = ToolRunner.invokeNodetool("tablestats", arg, "sstable_count");
            tool.assertOnCleanExit();
            m = regExp.matcher(tool.getStdout());
            ArrayList<String> sorted = new ArrayList<>();
            while (m.find())
                sorted.add(m.group(1));

            assertThat(sorted).isNotEqualTo(orig);
            Collections.sort(orig);
            Collections.sort(sorted);
            assertThat(sorted).isEqualTo(orig);
        });

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-s", "wrongSort");
        assertThat(tool.getStdout()).contains("argument for sort must be one of");
        tool.assertCleanStdErr();
        assertThat(tool.getExitCode()).isEqualTo(1);
    }

    @Test
    public void testTopArg()
    {
        Arrays.asList("-t", "--top").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-s", "table_name", arg, "1");
            tool.assertOnCleanExit();
            assertThat(StringUtils.countMatches(tool.getStdout(), "Table:")).isEqualTo(1);
        });

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-s", "table_name", "-t", "-1");
        tool.assertCleanStdErr();
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("argument for top must be a positive integer");
    }

    @Test
    public void testSSTableLocationCheckArg()
    {
        Arrays.asList("-l", "--sstable-location-check").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg, "system.local");
            tool.assertOnCleanExit();
            assertThat(StringUtils.countMatches(tool.getStdout(), "SSTables in correct location: ")).isEqualTo(1);
        });

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "system.local");
        tool.assertCleanStdErr();
        assertThat(tool.getStdout()).doesNotContain("SSTables in correct location: ");
    }

    @Test
    public void testFormatJson()
    {
        Arrays.asList("-F", "--format").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg, "json");
            tool.assertOnCleanExit();
            String json = tool.getStdout();
            assertThatCode(() -> JsonUtils.JSON_OBJECT_MAPPER.readTree(json)).doesNotThrowAnyException();
            assertThat(json).containsPattern("\"sstable_count\"\\s*:\\s*[0-9]+")
                            .containsPattern("\"old_sstable_count\"\\s*:\\s*[0-9]+");
        });
    }

    @Test
    public void testFormatYaml()
    {
        Arrays.asList("-F", "--format").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg, "yaml");
            tool.assertOnCleanExit();
            String yaml = tool.getStdout();
            org.yaml.snakeyaml.LoaderOptions loaderOptions = new org.yaml.snakeyaml.LoaderOptions();
            loaderOptions.setMaxAliasesForCollections(100); // we now have > 50 tables
            assertThatCode(() -> new Yaml(loaderOptions).load(yaml)).doesNotThrowAnyException();
            assertThat(yaml).containsPattern("sstable_count:\\s*[0-9]+")
                            .containsPattern("old_sstable_count:\\s*[0-9]+");
        });
    }
}
