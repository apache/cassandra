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
package org.apache.cassandra.triggers;

import java.util.Collection;
import java.util.Collections;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.ByteBufferUtil.toInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TriggersTest
{
    private static boolean triggerCreated = false;

    private static String ksName = "triggers_test_ks";
    private static String cfName = "test_table";
    private static String otherCf = "other_table";

    @BeforeClass
    public static void beforeTest() throws ConfigurationException
    {
        SchemaLoader.loadSchema();
    }

    @Before
    public void setup() throws Exception
    {
        StorageService.instance.initServer(0);

        String cql = String.format("CREATE KEYSPACE IF NOT EXISTS %s " +
                                   "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                                   ksName);
        QueryProcessor.process(cql, ConsistencyLevel.ONE);

        cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int, v1 int, v2 int, PRIMARY KEY (k))", ksName, cfName);
        QueryProcessor.process(cql, ConsistencyLevel.ONE);

        cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int, v1 int, v2 int, PRIMARY KEY (k))", ksName, otherCf);
        QueryProcessor.process(cql, ConsistencyLevel.ONE);

        // no conditional execution of create trigger stmt yet
        if (! triggerCreated)
        {
            cql = String.format("CREATE TRIGGER trigger_1 ON %s.%s USING '%s'",
                                ksName, cfName, TestTrigger.class.getName());
            QueryProcessor.process(cql, ConsistencyLevel.ONE);
            triggerCreated = true;
        }
    }

    @Test
    public void executeTriggerOnCqlInsert() throws Exception
    {
        String cql = String.format("INSERT INTO %s.%s (k, v1) VALUES (0, 0)", ksName, cfName);
        QueryProcessor.process(cql, ConsistencyLevel.ONE);
        assertUpdateIsAugmented(0, "v1", 0);
    }

    @Test
    public void executeTriggerOnCqlBatchInsert() throws Exception
    {
        String cql = String.format("BEGIN BATCH " +
                                   "    INSERT INTO %s.%s (k, v1) VALUES (1, 1); " +
                                   "APPLY BATCH",
                                   ksName, cfName);
        QueryProcessor.process(cql, ConsistencyLevel.ONE);
        assertUpdateIsAugmented(1, "v1", 1);
    }

    @Test
    public void executeTriggerOnCqlInsertWithConditions() throws Exception
    {
        String cql = String.format("INSERT INTO %s.%s (k, v1) VALUES (4, 4) IF NOT EXISTS", ksName, cfName);
        QueryProcessor.process(cql, ConsistencyLevel.ONE);
        assertUpdateIsAugmented(4, "v1", 4);
    }

    @Test
    public void executeTriggerOnCqlBatchWithConditions() throws Exception
    {
        String cql = String.format("BEGIN BATCH " +
                                   "  INSERT INTO %1$s.%2$s (k, v1) VALUES (5, 5) IF NOT EXISTS; " +
                                   "  INSERT INTO %1$s.%2$s (k, v1) VALUES (5, 5); " +
                                   "APPLY BATCH",
                                    ksName, cfName);
        QueryProcessor.process(cql, ConsistencyLevel.ONE);
        assertUpdateIsAugmented(5, "v1", 5);
    }

    @Test(expected=org.apache.cassandra.exceptions.InvalidRequestException.class)
    public void onCqlUpdateWithConditionsRejectGeneratedUpdatesForDifferentPartition() throws Exception
    {
        String cf = "cf" + System.nanoTime();
        try
        {
            setupTableWithTrigger(cf, CrossPartitionTrigger.class);
            String cql = String.format("INSERT INTO %s.%s (k, v1) VALUES (7, 7) IF NOT EXISTS", ksName, cf);
            QueryProcessor.process(cql, ConsistencyLevel.ONE);
        }
        finally
        {
            assertUpdateNotExecuted(cf, 7);
        }
    }

    @Test(expected=org.apache.cassandra.exceptions.InvalidRequestException.class)
    public void onCqlUpdateWithConditionsRejectGeneratedUpdatesForDifferentTable() throws Exception
    {
        String cf = "cf" + System.nanoTime();
        try
        {
            setupTableWithTrigger(cf, CrossTableTrigger.class);
            String cql = String.format("INSERT INTO %s.%s (k, v1) VALUES (8, 8) IF NOT EXISTS", ksName, cf);
            QueryProcessor.process(cql, ConsistencyLevel.ONE);
        }
        finally
        {
            assertUpdateNotExecuted(cf, 7);
        }
    }

    @Test(expected=org.apache.cassandra.exceptions.InvalidRequestException.class)
    public void ifTriggerThrowsErrorNoMutationsAreApplied() throws Exception
    {
        String cf = "cf" + System.nanoTime();
        try
        {
            setupTableWithTrigger(cf, ErrorTrigger.class);
            String cql = String.format("INSERT INTO %s.%s (k, v1) VALUES (11, 11)", ksName, cf);
            QueryProcessor.process(cql, ConsistencyLevel.ONE);
        }
        catch (Exception e)
        {
            assertTrue(e.getMessage().equals(ErrorTrigger.MESSAGE));
            throw e;
        }
        finally
        {
            assertUpdateNotExecuted(cf, 11);
        }
    }

    private void setupTableWithTrigger(String cf, Class<? extends ITrigger> triggerImpl)
    throws RequestExecutionException
    {
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int, v1 int, v2 int, PRIMARY KEY (k))", ksName, cf);
        QueryProcessor.process(cql, ConsistencyLevel.ONE);

        // no conditional execution of create trigger stmt yet
        cql = String.format("CREATE TRIGGER trigger_1 ON %s.%s USING '%s'",
                            ksName, cf, triggerImpl.getName());
        QueryProcessor.process(cql, ConsistencyLevel.ONE);
    }

    private void assertUpdateIsAugmented(int key, String originColumnName, Object originColumnValue)
    {
        UntypedResultSet rs = QueryProcessor.process(String.format("SELECT * FROM %s.%s WHERE k=%s", ksName, cfName, key), ConsistencyLevel.ONE);
        assertRowValue(rs.one(), key, "v2", 999); // from trigger
        assertRowValue(rs.one(), key, originColumnName, originColumnValue); // from original update
    }
    
    private void assertRowValue(UntypedResultSet.Row row, int key, String columnName, Object columnValue)
    {
        assertTrue(String.format("Expected value (%s) for augmented cell %s was not found", key, columnName),
                   row.has(columnName));
        assertEquals(columnValue, row.getInt(columnName));
    }

    private void assertUpdateNotExecuted(String cf, int key)
    {
        UntypedResultSet rs = QueryProcessor.executeInternal(
                String.format("SELECT * FROM %s.%s WHERE k=%s", ksName, cf, key));
        assertTrue(rs.isEmpty());
    }

    public static class TestTrigger implements ITrigger
    {
        public Collection<Mutation> augment(Partition partition)
        {
            RowUpdateBuilder update = new RowUpdateBuilder(partition.metadata(), FBUtilities.timestampMicros(), partition.partitionKey().getKey());
            update.add("v2", 999);

            return Collections.singletonList(update.build());
        }
    }

    public static class CrossPartitionTrigger implements ITrigger
    {
        public Collection<Mutation> augment(Partition partition)
        {
            RowUpdateBuilder update = new RowUpdateBuilder(partition.metadata(), FBUtilities.timestampMicros(), toInt(partition.partitionKey().getKey()) + 1000);
            update.add("v2", 999);

            return Collections.singletonList(update.build());
        }
    }

    public static class CrossTableTrigger implements ITrigger
    {
        public Collection<Mutation> augment(Partition partition)
        {

            RowUpdateBuilder update = new RowUpdateBuilder(Schema.instance.getTableMetadata(ksName, otherCf), FBUtilities.timestampMicros(), partition.partitionKey().getKey());
            update.add("v2", 999);

            return Collections.singletonList(update.build());
        }
    }

    public static class ErrorTrigger implements ITrigger
    {
        public static final String MESSAGE = "Thrown by ErrorTrigger";
        public Collection<Mutation> augment(Partition partition)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException(MESSAGE);
        }
    }
}
