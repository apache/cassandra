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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class GetGuardrailsConfigTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testDefaultConfig()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "SaiSSTableIndexesPerQueryWarnThreshold: 32\n" +
                                               '\n');
    }

    @Test
    public void testDefaultConfigFullList()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig", "--all");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "AllowFilteringEnabled: true\n" +
                                               "AlterTableEnabled: true\n" +
                                               "BulkLoadEnabled: true\n" +
                                               "CollectionSizeFailThreshold: null\n" +
                                               "CollectionSizeWarnThreshold: null\n" +
                                               "ColumnValueSizeFailThreshold: null\n" +
                                               "ColumnValueSizeWarnThreshold: null\n" +
                                               "ColumnsPerTableFailThreshold: -1\n" +
                                               "ColumnsPerTableWarnThreshold: -1\n" +
                                               "CompactTablesEnabled: true\n" +
                                               "DataDiskUsageMaxDiskSize: null\n" +
                                               "DataDiskUsagePercentageFailThreshold: -1\n" +
                                               "DataDiskUsagePercentageWarnThreshold: -1\n" +
                                               "DropKeyspaceEnabled: true\n" +
                                               "DropTruncateTableEnabled: true\n" +
                                               "FieldsPerUDTFailThreshold: -1\n" +
                                               "FieldsPerUDTWarnThreshold: -1\n" +
                                               "GroupByEnabled: true\n" +
                                               "InSelectCartesianProductFailThreshold: -1\n" +
                                               "InSelectCartesianProductWarnThreshold: -1\n" +
                                               "IntersectFilteringQueryEnabled: true\n" +
                                               "IntersectFilteringQueryWarned: true\n" +
                                               "ItemsPerCollectionFailThreshold: -1\n" +
                                               "ItemsPerCollectionWarnThreshold: -1\n" +
                                               "KeyspacesFailThreshold: -1\n" +
                                               "KeyspacesWarnThreshold: -1\n" +
                                               "MaterializedViewsPerTableFailThreshold: -1\n" +
                                               "MaterializedViewsPerTableWarnThreshold: -1\n" +
                                               "MaximumReplicationFactorFailThreshold: -1\n" +
                                               "MaximumReplicationFactorWarnThreshold: -1\n" +
                                               "MaximumTimestampFailThreshold: null\n" +
                                               "MaximumTimestampWarnThreshold: null\n" +
                                               "MinimumReplicationFactorFailThreshold: -1\n" +
                                               "MinimumReplicationFactorWarnThreshold: -1\n" +
                                               "MinimumTimestampFailThreshold: null\n" +
                                               "MinimumTimestampWarnThreshold: null\n" +
                                               "NonPartitionRestrictedQueryEnabled: true\n" +
                                               "PageSizeFailThreshold: -1\n" +
                                               "PageSizeWarnThreshold: -1\n" +
                                               "PartitionKeysInSelectFailThreshold: -1\n" +
                                               "PartitionKeysInSelectWarnThreshold: -1\n" +
                                               "PartitionSizeFailThreshold: null\n" +
                                               "PartitionSizeWarnThreshold: null\n" +
                                               "PartitionTombstonesFailThreshold: -1\n" +
                                               "PartitionTombstonesWarnThreshold: -1\n" +
                                               "ReadBeforeWriteListOperationsEnabled: true\n" +
                                               "ReadConsistencyLevelsDisallowedCSV: null\n" +
                                               "ReadConsistencyLevelsWarnedCSV: null\n" +
                                               "SaiSSTableIndexesPerQueryFailThreshold: -1\n" +
                                               "SaiSSTableIndexesPerQueryWarnThreshold: 32\n" +
                                               "SecondaryIndexesEnabled: true\n" +
                                               "SecondaryIndexesPerTableFailThreshold: -1\n" +
                                               "SecondaryIndexesPerTableWarnThreshold: -1\n" +
                                               "SimpleStrategyEnabled: true\n" +
                                               "TablePropertiesDisallowedCSV: null\n" +
                                               "TablePropertiesIgnoredCSV: null\n" +
                                               "TablePropertiesWarnedCSV: null\n" +
                                               "TablesFailThreshold: -1\n" +
                                               "TablesWarnThreshold: -1\n" +
                                               "UncompressedTablesEnabled: true\n" +
                                               "UserTimestampsEnabled: true\n" +
                                               "VectorDimensionsFailThreshold: -1\n" +
                                               "VectorDimensionsWarnThreshold: -1\n" +
                                               "WriteConsistencyLevelsDisallowedCSV: null\n" +
                                               "WriteConsistencyLevelsWarnedCSV: null\n" +
                                               "ZeroTTLOnTWCSEnabled: true\n" +
                                               "ZeroTTLOnTWCSWarned: true\n" +
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
                                               "SaiSSTableIndexesPerQueryWarnThreshold: 32\n" +
                                               '\n');
        // reset
        Guardrails.instance.setColumnsPerTableThreshold(-1, -1);
        tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "SaiSSTableIndexesPerQueryWarnThreshold: 32\n" +
                                               '\n');
    }

    @Test
    public void testSetEnableFlagGuardrail()
    {
        Guardrails.instance.setGroupByEnabled(false);

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "GroupByEnabled: false\n" +
                                               "SaiSSTableIndexesPerQueryWarnThreshold: 32\n" +
                                               '\n');
        // reset
        Guardrails.instance.setGroupByEnabled(true);
        tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "SaiSSTableIndexesPerQueryWarnThreshold: 32\n" +
                                               '\n');
    }

    @Test
    public void testSetValuesGuardrail()
    {
        Guardrails.instance.setWriteConsistencyLevelsDisallowedCSV("ANY,ONE");

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "SaiSSTableIndexesPerQueryWarnThreshold: 32\n" +
                                               "WriteConsistencyLevelsDisallowedCSV: ANY,ONE\n" +
                                               '\n');
        // reset
        Guardrails.instance.setWriteConsistencyLevelsDisallowedCSV("");
        tool = ToolRunner.invokeNodetool("getguardrailsconfig");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("Guardrails Configuration:\n" +
                                               "SaiSSTableIndexesPerQueryWarnThreshold: 32\n" +
                                               '\n');
    }
}
