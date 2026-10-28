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
package org.apache.cassandra.tools.nodetool.mock;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.tools.ToolRunner;

import static org.mockito.Mockito.when;

public class CompactMockTest extends AbstractNodetoolMock
{
    // Deprecated compact command tests (backward compatibility)

    @Test
    public void testCompactForceKeyspaceCompactionForPartitionKey() throws Throwable
    {
        long key = 43;
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "--partition", Long.toString(key), keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompactionForPartitionKey(keyspace(), Long.toString(key), table);
    }

    @Test
    public void testCompactForceKeyspaceCompactionForTokenRange() throws Throwable
    {
        long key = 34;
        String startToken = Long.toString(key - 1);
        String endToken = Long.toString(key + 1);
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "--start-token", startToken, "--end-token", endToken, keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompactionForTokenRange(keyspace(), startToken, endToken, table);
    }

    @Test
    public void testCompactForceKeyspaceCompaction() throws Throwable
    {
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "--split-output", keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompaction(true, keyspace(), table);
    }

    @Test
    public void testCompactForceUserDefinedCompaction() throws Throwable
    {
        String[] ssTables = new String[] { "ssTable1", "ssTable2" };
        CompactionManagerMBean mock = getMock(COMPACTION_MANAGER_MBEAN);
        invokeNodetool("compact", "--user-defined", ssTables[0], ssTables[1]).assertOnCleanExit();
        Mockito.verify(mock).forceUserDefinedCompaction(String.join(",", ssTables));
    }

    @Test
    public void testDeprecatedCompactPrintsWarning() throws Throwable
    {
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        ToolRunner.ToolResult result = invokeNodetool("compact", keyspace(), table);
        result.assertOnCleanExit();
        Assertions.assertThat(result.getStdout()).contains("WARNING: nodetool compact is deprecated");
    }

    // compact keyspace subcommand tests

    @Test
    public void testCompactKeyspaceSubcommandForceKeyspaceCompaction() throws Throwable
    {
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "keyspace", keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompaction(false, keyspace(), table);
    }

    @Test
    public void testCompactKeyspaceSubcommandWithSplitOutput() throws Throwable
    {
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "keyspace", "--split-output", keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompaction(true, keyspace(), table);
    }

    @Test
    public void testCompactKeyspaceSubcommandForPartitionKey() throws Throwable
    {
        long key = 43;
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "keyspace", "--partition", Long.toString(key), keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompactionForPartitionKey(keyspace(), Long.toString(key), table);
    }

    @Test
    public void testCompactKeyspaceSubcommandSplitOutputAndPartitionKeyFails() throws Throwable
    {
        long key = 43;
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "keyspace", "--split-output", "--partition", Long.toString(key), keyspace(), table)
        .asserts()
        .failure()
        .errorContains("Invalid option combination: Can not use split-output with --partition");
    }

    @Test
    public void testCompactKeyspaceSubcommandWithJobs() throws Throwable
    {
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "keyspace", "--jobs", "2", keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompaction(false, 2, keyspace(), table);
    }

    // compact sstables subcommand tests

    @Test
    public void testCompactSSTablesSubcommandForceUserDefinedCompaction() throws Throwable
    {
        String[] ssTables = new String[] { "ssTable1", "ssTable2" };
        CompactionManagerMBean mock = getMock(COMPACTION_MANAGER_MBEAN);
        invokeNodetool("compact", "sstables", ssTables[0], ssTables[1]).assertOnCleanExit();
        Mockito.verify(mock).forceUserDefinedCompaction(String.join(",", ssTables));
    }

    @Test
    public void testCompactSSTablesSubcommandSingleFile() throws Throwable
    {
        String ssTable = "ssTable1";
        CompactionManagerMBean mock = getMock(COMPACTION_MANAGER_MBEAN);
        invokeNodetool("compact", "sstables", ssTable).assertOnCleanExit();
        Mockito.verify(mock).forceUserDefinedCompaction(ssTable);
    }

    @Test
    public void testCompactSSTablesSubcommandRequiresAtLeastOneFile() throws Throwable
    {
        invokeNodetool("compact", "sstables")
        .asserts()
        .failure();
    }

    // compact range subcommand tests

    @Test
    public void testCompactRangeSubcommandWithBothTokens() throws Throwable
    {
        long key = 34;
        String startToken = Long.toString(key - 1);
        String endToken = Long.toString(key + 1);
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "range", "--start-token", startToken, "--end-token", endToken, keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompactionForTokenRange(keyspace(), startToken, endToken, table);
    }

    @Test
    public void testCompactRangeSubcommandWithStartTokenOnly() throws Throwable
    {
        long key = 34;
        String startToken = Long.toString(key - 1);
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "range", "--start-token", startToken, keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompactionForTokenRange(keyspace(), startToken, "", table);
    }

    @Test
    public void testCompactRangeSubcommandWithEndTokenOnly() throws Throwable
    {
        long key = 34;
        String endToken = Long.toString(key + 1);
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "range", "--end-token", endToken, keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompactionForTokenRange(keyspace(), "", endToken, table);
    }

    @Test
    public void testCompactRangeSubcommandRequiresAtLeastOneToken() throws Throwable
    {
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "range", keyspace(), table)
        .asserts()
        .failure();
    }
}
