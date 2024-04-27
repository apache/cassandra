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
import org.apache.cassandra.db.guardrails.GuardrailsMBean;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class SetGuardrailsConfigTest extends CQLTester
{
    private final GuardrailsMBean mbean = Guardrails.instance;

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testListAllSetters()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setguardrailsconfig", "--list");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("setAllowFilteringEnabled\tboolean\n" +
                                               "setAlterTableEnabled\tboolean\n" +
                                               "setBulkLoadEnabled\tboolean\n" +
                                               "setCollectionSizeThreshold\t[java.lang.String, java.lang.String]\n" +
                                               "setColumnValueSizeThreshold\t[java.lang.String, java.lang.String]\n" +
                                               "setColumnsPerTableThreshold\t[int, int]\n" +
                                               "setCompactTablesEnabled\tboolean\n" +
                                               "setDataDiskUsageMaxDiskSize\tjava.lang.String\n" +
                                               "setDataDiskUsagePercentageThreshold\t[int, int]\n" +
                                               "setDropKeyspaceEnabled\tboolean\n" +
                                               "setDropTruncateTableEnabled\tboolean\n" +
                                               "setFieldsPerUDTThreshold\t[int, int]\n" +
                                               "setGroupByEnabled\tboolean\n" +
                                               "setInSelectCartesianProductThreshold\t[int, int]\n" +
                                               "setIntersectFilteringQueryEnabled\tboolean\n" +
                                               "setIntersectFilteringQueryWarned\tboolean\n" +
                                               "setItemsPerCollectionThreshold\t[int, int]\n" +
                                               "setKeyspacesThreshold\t[int, int]\n" +
                                               "setMaterializedViewsPerTableThreshold\t[int, int]\n" +
                                               "setMaximumReplicationFactorThreshold\t[int, int]\n" +
                                               "setMaximumTimestampThreshold\t[java.lang.String, java.lang.String]\n" +
                                               "setMinimumReplicationFactorThreshold\t[int, int]\n" +
                                               "setMinimumTimestampThreshold\t[java.lang.String, java.lang.String]\n" +
                                               "setNonPartitionRestrictedQueryEnabled\tboolean\n" +
                                               "setPageSizeThreshold\t[int, int]\n" +
                                               "setPartitionKeysInSelectThreshold\t[int, int]\n" +
                                               "setPartitionSizeThreshold\t[java.lang.String, java.lang.String]\n" +
                                               "setPartitionTombstonesThreshold\t[long, long]\n" +
                                               "setReadBeforeWriteListOperationsEnabled\tboolean\n" +
                                               "setReadConsistencyLevelsDisallowed\tjava.util.Set\n" +
                                               "setReadConsistencyLevelsDisallowedCSV\tjava.lang.String\n" +
                                               "setReadConsistencyLevelsWarned\tjava.util.Set\n" +
                                               "setReadConsistencyLevelsWarnedCSV\tjava.lang.String\n" +
                                               "setSaiSSTableIndexesPerQueryThreshold\t[int, int]\n" +
                                               "setSecondaryIndexesEnabled\tboolean\n" +
                                               "setSecondaryIndexesPerTableThreshold\t[int, int]\n" +
                                               "setSimpleStrategyEnabled\tboolean\n" +
                                               "setTablePropertiesDisallowed\tjava.util.Set\n" +
                                               "setTablePropertiesDisallowedCSV\tjava.lang.String\n" +
                                               "setTablePropertiesIgnored\tjava.util.Set\n" +
                                               "setTablePropertiesIgnoredCSV\tjava.lang.String\n" +
                                               "setTablePropertiesWarned\tjava.util.Set\n" +
                                               "setTablePropertiesWarnedCSV\tjava.lang.String\n" +
                                               "setTablesThreshold\t[int, int]\n" +
                                               "setUncompressedTablesEnabled\tboolean\n" +
                                               "setUserTimestampsEnabled\tboolean\n" +
                                               "setVectorDimensionsThreshold\t[int, int]\n" +
                                               "setWriteConsistencyLevelsDisallowed\tjava.util.Set\n" +
                                               "setWriteConsistencyLevelsDisallowedCSV\tjava.lang.String\n" +
                                               "setWriteConsistencyLevelsWarned\tjava.util.Set\n" +
                                               "setWriteConsistencyLevelsWarnedCSV\tjava.lang.String\n" +
                                               "setZeroTTLOnTWCSEnabled\tboolean\n" +
                                               "setZeroTTLOnTWCSWarned\tboolean\n" +
                                               '\n');
    }

    @Test
    public void testIncorrectSetterName()
    {
        ToolRunner.invokeNodetool("setguardrailsconfig", "setWhatever", "value")
                  .asserts()
                  .failure()
                  .errorContains("Setter method setWhatever not found");
    }

    @Test
    public void testIncorrectArgsCountForExistingSetter()
    {
        ToolRunner.invokeNodetool("setguardrailsconfig", "setKeyspacesThreshold", "1")
                  .asserts()
                  .failure()
                  .errorContains("setKeyspacesThreshold is expecting 2 args");
    }

    @Test
    public void testUnhandledParameterType()
    {
        ToolRunner.invokeNodetool("setguardrailsconfig", "setWriteConsistencyLevelsDisallowed", "ANY,ONE")
                  .asserts()
                  .failure()
                  .errorContains("unsupported type");
    }

    @Test
    public void testSetWithIntegerParams()
    {
        assertThat(mbean.getKeyspacesWarnThreshold()).isEqualTo(-1);
        assertThat(mbean.getKeyspacesFailThreshold()).isEqualTo(-1);
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setguardrailsconfig", "setKeyspacesThreshold", "1", "2");
        tool.assertOnCleanExit();
        // check if the config is changed
        assertThat(mbean.getKeyspacesWarnThreshold()).isEqualTo(1);
        assertThat(mbean.getKeyspacesFailThreshold()).isEqualTo(2);
        // reset with "null"
        tool = ToolRunner.invokeNodetool("setguardrailsconfig", "setKeyspacesThreshold", "null", "null");
        tool.assertOnCleanExit();
        assertThat(mbean.getKeyspacesWarnThreshold()).isEqualTo(-1);
        assertThat(mbean.getKeyspacesFailThreshold()).isEqualTo(-1);
        tool = ToolRunner.invokeNodetool("setguardrailsconfig", "setKeyspacesThreshold", "1", "2");
        tool.assertOnCleanExit();
        // check if the config is changed
        assertThat(mbean.getKeyspacesWarnThreshold()).isEqualTo(1);
        assertThat(mbean.getKeyspacesFailThreshold()).isEqualTo(2);
        // reset with -1
        tool = ToolRunner.invokeNodetool("setguardrailsconfig", "setKeyspacesThreshold", "-1", "-1");
        tool.assertOnCleanExit();
        assertThat(mbean.getKeyspacesWarnThreshold()).isEqualTo(-1);
        assertThat(mbean.getKeyspacesFailThreshold()).isEqualTo(-1);
    }

    @Test
    public void testSetWithLongParams()
    {
        assertThat(mbean.getPartitionTombstonesWarnThreshold()).isEqualTo(-1);
        assertThat(mbean.getPartitionTombstonesFailThreshold()).isEqualTo(-1);
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setguardrailsconfig", "setPartitionTombstonesThreshold", "1", "2");
        tool.assertOnCleanExit();
        // check if the config is changed
        assertThat(mbean.getPartitionTombstonesWarnThreshold()).isEqualTo(1);
        assertThat(mbean.getPartitionTombstonesFailThreshold()).isEqualTo(2);
        // reset with "null"
        tool = ToolRunner.invokeNodetool("setguardrailsconfig", "setPartitionTombstonesThreshold", "null", "null");
        tool.assertOnCleanExit();
        assertThat(mbean.getPartitionTombstonesWarnThreshold()).isEqualTo(-1);
        assertThat(mbean.getPartitionTombstonesFailThreshold()).isEqualTo(-1);
        tool = ToolRunner.invokeNodetool("setguardrailsconfig", "setPartitionTombstonesThreshold", "1", "2");
        tool.assertOnCleanExit();
        // check if the config is changed
        assertThat(mbean.getPartitionTombstonesWarnThreshold()).isEqualTo(1);
        assertThat(mbean.getPartitionTombstonesFailThreshold()).isEqualTo(2);
        // reset with -1
        tool = ToolRunner.invokeNodetool("setguardrailsconfig", "setPartitionTombstonesThreshold", "-1", "-1");
        tool.assertOnCleanExit();
        assertThat(mbean.getPartitionTombstonesWarnThreshold()).isEqualTo(-1);
        assertThat(mbean.getPartitionTombstonesFailThreshold()).isEqualTo(-1);
    }

    @Test
    public void testSetWithCSVParams()
    {
        assertThat(mbean.getWriteConsistencyLevelsDisallowedCSV()).isEqualTo("");
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setguardrailsconfig", "setWriteConsistencyLevelsDisallowedCSV", "ANY,ONE");
        tool.assertOnCleanExit();
        // check if the config is changed
        assertThat(mbean.getWriteConsistencyLevelsDisallowedCSV()).isEqualTo("ANY,ONE");
        assertThat(mbean.getWriteConsistencyLevelsDisallowed()).containsExactlyInAnyOrder("ANY", "ONE");
        // reset
        tool = ToolRunner.invokeNodetool("setguardrailsconfig", "setWriteConsistencyLevelsDisallowedCSV", "null");
        tool.assertOnCleanExit();
        assertThat(mbean.getWriteConsistencyLevelsDisallowedCSV()).isEqualTo("");
    }

    @Test
    public void testSetWithNullableStringParams()
    {
        assertThat(mbean.getDataDiskUsageMaxDiskSize()).isEqualTo(null);
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("setguardrailsconfig", "setDataDiskUsageMaxDiskSize", "30MiB");
        tool.assertOnCleanExit();
        // check if the config is changed
        assertThat(mbean.getDataDiskUsageMaxDiskSize()).isEqualTo("30MiB");
        // reset
        tool = ToolRunner.invokeNodetool("setguardrailsconfig", "setDataDiskUsageMaxDiskSize", "null");
        tool.assertOnCleanExit();
        assertThat(mbean.getDataDiskUsageMaxDiskSize()).isEqualTo(null);
    }
}
