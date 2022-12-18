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

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.tools.ToolRunner;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.junit.Assert.assertTrue;

public class CompactionHistoryTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testSystemCompactionHistoryTable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text, value text, PRIMARY KEY ((id)))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        cfs.disableAutoCompaction();
        // write SSTables for the specific key
        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (id, value) VALUES (?, ?)", "key" + i, "value" + i);
            flush(keyspace());
        }
        
        Assertions.assertThat(cfs.getTracker().getView().liveSSTables()).hasSize(10);
        ToolRunner.ToolResult toolCompact = invokeNodetool("compact", keyspace(), currentTable());
        toolCompact.assertOnCleanExit();
        String cql = "select keyspace_name,columnfamily_name,compaction_type  from system." + SystemKeyspace.COMPACTION_HISTORY +
                     " where keyspace_name = '" + keyspace() + "' AND columnfamily_name = '" + currentTable() + "' ALLOW FILTERING";
        
        Object[] row = row(keyspace(), currentTable(), OperationType.MAJOR_COMPACTION.type);    
        assertRows(execute(cql), row);
    }
    
    @Test
    public void testMajorCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (id text, value text, PRIMARY KEY ((id)))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        cfs.disableAutoCompaction();
        // write SSTables for the specific key
        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (id, value) VALUES (?, ?)", "key" + i, "value" + i);
            flush(keyspace());
        }
        
        Assertions.assertThat(cfs.getTracker().getView().liveSSTables()).hasSize(10);
        String[] cmds = { "compact", keyspace(), currentTable() };
        compactionHistoryResultVerify(keyspace(), currentTable(), OperationType.MAJOR_COMPACTION.type, cmds);
    }

    @Test
    public void testGarbageCollect() throws Throwable
    {
        createTable("CREATE TABLE %s (id text, value text, PRIMARY KEY ((id)))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        cfs.disableAutoCompaction();
        // write SSTables for the specific key
        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (id, value) VALUES (?, ?)", "key" + i, "value" + i);
            flush(keyspace());
            execute("DELETE FROM %s WHERE id = ? ", "key" + i);
            flush(keyspace());
        }
        Assertions.assertThat(cfs.getTracker().getView().liveSSTables()).hasSize(20);
        String[] cmds = { "garbagecollect", keyspace(), currentTable() };
        compactionHistoryResultVerify(keyspace(), currentTable(), OperationType.GARBAGE_COLLECT.type, cmds);
    }
    
    @Test
    public void testUpgradeSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text, value text, PRIMARY KEY ((id)))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        cfs.disableAutoCompaction();
        // write SSTables for the specific key
        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (id, value) VALUES (?, ?)", "key" + i, "value" + i);
            flush(keyspace());
        }
        Assertions.assertThat(cfs.getTracker().getView().liveSSTables()).hasSize(10);
        String[] cmds = { "upgradesstables", " -a", keyspace(), currentTable() };
        compactionHistoryResultVerify(keyspace(), currentTable(), OperationType.UPGRADE_SSTABLES.type, cmds);
    }
    
    
    private void compactionHistoryResultVerify( String keyspace, String table, String operationType, String[] cmds)
    {
        ToolRunner.ToolResult toolCompact = invokeNodetool(cmds);
        toolCompact.assertOnCleanExit();

        ToolRunner.ToolResult toolHistory = invokeNodetool("compactionhistory");
        toolHistory.assertOnCleanExit();
        String stdout = toolHistory.getStdout();
        String[] resultArray = stdout.split("\n");
        assertTrue(Arrays.stream(resultArray)
            .anyMatch(result -> result.contains(operationType)
                && result.contains(keyspace)
                && result.contains(table)));
    }
    
}
