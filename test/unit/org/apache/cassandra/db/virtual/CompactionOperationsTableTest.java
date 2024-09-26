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

package org.apache.cassandra.db.virtual;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.reads.range.TokenUpdater;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for the compaction_operations_history virtual table, which is a view over the compaction operations history.
 * @see org.apache.cassandra.db.CleanupTest
 */
public class CompactionOperationsTableTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(SchemaConstants.VIRTUAL_VIEWS,
                                                                      SystemViewsKeyspace.instance.tables()));
    }

    @Test
    public void testCleanup()
    {
        CompactionManager.instance.disableAutoCompaction();

        createTable("CREATE TABLE %s (pk text, ck1 int, ck2 int, v int, PRIMARY KEY (pk, ck1, ck2))");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (?, ?, ?, ?)", "key", i, i, i);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        invokeNodetool("cleanup", "--jobs", "2", cfs.keyspace.getName(), cfs.getTableName())
            .assertOnCleanExit();

        assertRows(execute(String.format("SELECT operation_type, keyspaces, tables, operation_result, " +
                                         "operation_result_by_table, processed_by_keyspace " +
                                         "FROM %s.%s", SchemaConstants.VIRTUAL_VIEWS,
                                         SystemViewsKeyspace.COMPACTION_OPERATIONS_STATUS)),
                   row(OperationType.CLEANUP.toString(),
                       KEYSPACE,
                       String.format("[%s.%s]", cfs.getKeyspaceName(), cfs.getTableName()),
                       CompactionManager.AllSSTableOpStatus.SUCCESSFUL.toString(),
                       "[cql_test_keyspace.table_testcleanup_00: SUCCESSFUL]",
                       "[cql_test_keyspace: 0]"));

        executeFormattedQuery(String.format("TRUNCATE %s.%s", SchemaConstants.VIRTUAL_VIEWS, SystemViewsKeyspace.COMPACTION_OPERATIONS_STATUS));
        assertRowCount(executeFormattedQuery(String.format("SELECT * FROM %s.%s", SchemaConstants.VIRTUAL_VIEWS,
                                                           SystemViewsKeyspace.COMPACTION_OPERATIONS_STATUS)),
                       0);
    }

    /** Verify that the results are ordered by operation_id and the newest operation is first. */
    @Test
    public void testResultsOrder()
    {
        CompactionManager.instance.disableAutoCompaction();

        createTable("CREATE TABLE %s (pk text, ck1 int, ck2 int, v int, PRIMARY KEY (pk, ck1, ck2))");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (?, ?, ?, ?)", "key", i, i, i);
        flush();
        
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        invokeNodetool("cleanup", "--jobs", "2", cfs.keyspace.getName(), cfs.getTableName()).assertOnCleanExit();

        UntypedResultSet first = execute(String.format("SELECT operation_id FROM %s.%s", SchemaConstants.VIRTUAL_VIEWS,
                                               SystemViewsKeyspace.COMPACTION_OPERATIONS_STATUS));
        assertEquals(1, first.size());
        UntypedResultSet.Row row = first.one();
        TimeUUID firstOperationId = row.getTimeUUID("operation_id");

        invokeNodetool("cleanup", "--jobs", "2", cfs.keyspace.getName(), cfs.getTableName())
            .assertOnCleanExit();

        UntypedResultSet second = execute(String.format("SELECT operation_id FROM %s.%s", SchemaConstants.VIRTUAL_VIEWS,
                                               SystemViewsKeyspace.COMPACTION_OPERATIONS_STATUS));
        assertEquals(2, second.size());
        List<TimeUUID> operationIds = second.stream()
                                            .map(rw -> rw.getTimeUUID("operation_id"))
                                            .collect(Collectors.toList());
        assertEquals(operationIds.get(1).asUUID(), firstOperationId.asUUID());
        assertTrue(operationIds.get(0).compareTo(operationIds.get(1)) > 0);
    }

    @Test
    public void testOperationsLinkedTask() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();
        createTable("CREATE TABLE %s (key text, ck1 text, family int, PRIMARY KEY ((key), ck1))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        ByteBuffer column = ByteBufferUtil.bytes("family");
        ByteBuffer value = ByteBuffer.allocate(4);

        for (int i = 0; i < 200; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), ByteBufferUtil.bytes(String.valueOf(i)))
                .clustering(column)
                .add("family", value)
                .build()
                .applyUnsafe();
        }
        flush();

        new TokenUpdater().withTokens(InetAddressAndPort.getByName("127.0.0.1"), new Murmur3Partitioner.LongToken(2))
                          .withTokens(InetAddressAndPort.getByName("127.0.0.2"), new Murmur3Partitioner.LongToken(1))
                          .update();

        invokeNodetool("cleanup", "--jobs", "2", cfs.keyspace.getName(), cfs.getTableName()).assertOnCleanExit();

        // Data should be removed
        assertEquals(0, Util.getAll(Util.cmd(cfs).build()).size());

        assertRows(execute(String.format("SELECT operation_type, keyspace_name, tables, operation_result, " +
                                         "operation_result_by_table, sstables_effectively_processed " +
                                         "FROM %s.%s", SchemaConstants.VIRTUAL_VIEWS, SystemViewsKeyspace.COMPACTION_OPERATIONS_STATUS)),
                   new Object[]{OperationType.CLEANUP.toString(),
                                KEYSPACE,
                                String.format("[%s]", cfs.getTableName()),
                                CompactionManager.AllSSTableOpStatus.SUCCESSFUL.toString(),
                                String.format("[%s: SUCCESSFUL]", cfs.getTableName()),
                                "1"});

        UntypedResultSet rs0 = execute(String.format("SELECT operation_type, operation_id, keyspace_name FROM %s.%s",
                                                     SchemaConstants.VIRTUAL_VIEWS,
                                                     SystemViewsKeyspace.COMPACTION_OPERATIONS_STATUS));
        String operationType = rs0.one().getString("operation_type");
        UUID operationId = rs0.one().getUUID("operation_id");
        String keyspaceName = rs0.one().getString("keyspace_name");

        assertRows(execute(String.format("SELECT operation_type, operation_id, keyspace_name, column_family, " +
                                         "completed, total, sstables FROM %s.%s",
                                         SchemaConstants.VIRTUAL_VIEWS,
                                         SystemViewsKeyspace.COMPACTION_OPERATIONS_LINKED_TASKS)),
                   new Object[] { operationType, operationId, keyspaceName, cfs.getTableName(), "0", "4690", "1" });
    }
}
