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

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class GetGuardrailsConfigTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();
        startJMXServer();
    }

    @Test
    public void testDefaultConfig()
    {
        // by default, none of the guardrail is enabled
        // guardrails by default is not enabled on superusers
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n\n");
    }

    @Test
    public void testDefaultConfigFullList()
    {
        // by default, none of the guardrail is enabled
        // guardrails by default is not enabled on superusers
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig", "--all");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "AllowFilteringEnabled: true\n" +
                                               "CollectionSizeFailThreshold: null\n" +
                                               "CollectionSizeWarnThreshold: null\n" +
                                               "ColumnsPerTableFailThreshold: -1\n" +
                                               "ColumnsPerTableWarnThreshold: -1\n" +
                                               "CompactTablesEnabled: true\n" +
                                               "DataDiskUsageMaxDiskSize: null\n" +
                                               "DataDiskUsagePercentageFailThreshold: -1\n" +
                                               "DataDiskUsagePercentageWarnThreshold: -1\n" +
                                               "DropTruncateTableEnabled: true\n" +
                                               "FieldsPerUDTFailThreshold: -1\n" +
                                               "FieldsPerUDTWarnThreshold: -1\n" +
                                               "GroupByEnabled: true\n" +
                                               "InSelectCartesianProductFailThreshold: -1\n" +
                                               "InSelectCartesianProductWarnThreshold: -1\n" +
                                               "ItemsPerCollectionFailThreshold: -1\n" +
                                               "ItemsPerCollectionWarnThreshold: -1\n" +
                                               "KeyspacesFailThreshold: -1\n" +
                                               "KeyspacesWarnThreshold: -1\n" +
                                               "MaterializedViewsPerTableFailThreshold: -1\n" +
                                               "MaterializedViewsPerTableWarnThreshold: -1\n" +
                                               "MinimumReplicationFactorFailThreshold: -1\n" +
                                               "MinimumReplicationFactorWarnThreshold: -1\n" +
                                               "PageSizeFailThreshold: -1\n" +
                                               "PageSizeWarnThreshold: -1\n" +
                                               "PartitionKeysInSelectFailThreshold: -1\n" +
                                               "PartitionKeysInSelectWarnThreshold: -1\n" +
                                               "ReadBeforeWriteListOperationsEnabled: true\n" +
                                               "ReadConsistencyLevelsDisallowedCSV: null\n" +
                                               "ReadConsistencyLevelsWarnedCSV: null\n" +
                                               "SecondaryIndexesEnabled: true\n" +
                                               "SecondaryIndexesPerTableFailThreshold: -1\n" +
                                               "SecondaryIndexesPerTableWarnThreshold: -1\n" +
                                               "TablePropertiesDisallowedCSV: null\n" +
                                               "TablePropertiesIgnoredCSV: null\n" +
                                               "TablePropertiesWarnedCSV: null\n" +
                                               "TablesFailThreshold: -1\n" +
                                               "TablesWarnThreshold: -1\n" +
                                               "UncompressedTablesEnabled: true\n" +
                                               "UserTimestampsEnabled: true\n" +
                                               "WriteConsistencyLevelsDisallowedCSV: null\n" +
                                               "WriteConsistencyLevelsWarnedCSV: null\n" +
                                               '\n');
    }

    @Test
    public void testSetMaxThresholdGuardrail()
    {
        Guardrails.instance.setColumnsPerTableThreshold(3, 3);

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "ColumnsPerTableFailThreshold: 3\n" +
                                               "ColumnsPerTableWarnThreshold: 3\n" +
                                               '\n');
        // reset
        Guardrails.instance.setColumnsPerTableThreshold(-1, -1);
        tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               '\n');
    }

    @Test
    public void testSetDisableFlagGuardrail()
    {
        Guardrails.instance.setGroupByEnabled(false);

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "GroupByEnabled: false\n" +
                                               '\n');
        // reset
        Guardrails.instance.setGroupByEnabled(true);
        tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               '\n');
    }

    @Test
    public void testSetValuesGuardrail()
    {
        Guardrails.instance.setWriteConsistencyLevelsDisallowedCSV("ANY,ONE");

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "WriteConsistencyLevelsDisallowedCSV: ANY,ONE\n" +
                                               '\n');
        // reset
        Guardrails.instance.setWriteConsistencyLevelsDisallowedCSV("");
        tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               '\n');
    }
}
