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

package org.apache.cassandra.repair.autorepair;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_DEFAULT_RF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Unit tests to cover AutoRepair functionality inside {@link org.apache.cassandra.service.StorageService}
 */
public class SSTableRepairedAtTest extends CQLTester
{
    public static final String TEST_KEYSPACE = "test_keyspace";
    public static ColumnFamilyStore table1;
    public static ColumnFamilyStore table2;

    @BeforeClass
    public static void setUp() throws ConfigurationException, UnknownHostException
    {
        requireNetwork();
        AutoRepairUtils.setup();
        StorageService.instance.doAutoRepairSetup();
        DatabaseDescriptor.setCDCEnabled(false);
    }

    @Before
    public void clearData()
    {
        SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
        QueryProcessor.executeInternal(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", TEST_KEYSPACE));
        QueryProcessor.executeInternal(String.format("CREATE TABLE %s.%s (key text, val text, primary key(key))", TEST_KEYSPACE, "table1"));
        QueryProcessor.executeInternal(String.format("CREATE TABLE %s.%s (key text, val text, primary key(key))", TEST_KEYSPACE, "table2"));

        Keyspace.open(TEST_KEYSPACE).getColumnFamilyStore("table1").truncateBlocking();
        Keyspace.open(TEST_KEYSPACE).getColumnFamilyStore("table2").truncateBlocking();

                table1 = Keyspace.open(TEST_KEYSPACE).getColumnFamilyStore("table1");
        assert table1 != null;
        table2 = Keyspace.open(TEST_KEYSPACE).getColumnFamilyStore("table2");
        assert table2 != null;
    }

    @Test
    public void testGetTablesForKeyspace()
    {
        List<String> result = StorageService.instance.getTablesForKeyspace(TEST_KEYSPACE);

        assertEquals(Arrays.asList(table1.name, table2.name), result.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    public void testGetTablesForKeyspaceNotFound()
    {
        String missingKeyspace = "MISSING_KEYSPACE";
        try
        {
            StorageService.instance.getTablesForKeyspace(missingKeyspace);
            fail("Expected an AssertionError to be thrown");
        }
        catch (AssertionError e)
        {
            assertEquals("Unknown keyspace " + missingKeyspace, e.getMessage());
        }
    }

    @Test
    public void testMutateSSTableRepairedStateTableNotFound()
    {
        try
        {
            StorageService.instance.mutateSSTableRepairedState(true, false, TEST_KEYSPACE, List.of("MISSING_TABLE"));
            fail("Expected an InvalidRequestException to be thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals("Table MISSING_TABLE does not exist in keyspace " + TEST_KEYSPACE, e.getMessage());
            // Test passed
        }
    }

    @Test
    public void testMutateSSTableRepairedStateTablePreview()
    {
        SchemaLoader.insertData(TEST_KEYSPACE, table1.name, 0, 1);
        table1.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertEquals(1, table1.getLiveSSTables().size());

        List<String> result = StorageService.instance.mutateSSTableRepairedState(true, true, TEST_KEYSPACE, Arrays.asList(table1.name));

        assertEquals(1, result.size());
        table1.getLiveSSTables().forEach(sstable -> {
            assertFalse(sstable.isRepaired());
            assertTrue(result.contains(sstable.descriptor.baseFile().name()));
        });
    }

    @Test
    public void testMutateSSTableRepairedStateTableRepaired()
    {
        SchemaLoader.insertData(TEST_KEYSPACE, table1.name, 0, 1);
        table1.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        SchemaLoader.insertData(TEST_KEYSPACE, table1.name, 0, 1);
        table1.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertEquals(2, table1.getLiveSSTables().size());
        table1.getLiveSSTables().forEach(sstable -> {
            assertFalse(sstable.isRepaired());
        });

        List<String> result = StorageService.instance.mutateSSTableRepairedState(true, false, TEST_KEYSPACE, Arrays.asList(table1.name));

        assertEquals(2, result.size());
        table1.getLiveSSTables().forEach(sstable -> {
            assertTrue(sstable.isRepaired());
            assertTrue(result.contains(sstable.descriptor.baseFile().name()));
        });
    }

    @Test
    public void testMutateSSTableRepairedStateTableUnrepaired() throws Exception
    {
        SchemaLoader.insertData(TEST_KEYSPACE, table1.name, 0, 1);
        table1.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        SchemaLoader.insertData(TEST_KEYSPACE, table1.name, 0, 1);
        table1.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        table1.getCompactionStrategyManager().mutateRepaired(table1.getLiveSSTables(), 1, null, false);
        assertEquals(2, table1.getLiveSSTables().stream().filter(SSTableReader::isRepaired).count());

        List<String> result = StorageService.instance.mutateSSTableRepairedState(false, false, TEST_KEYSPACE, Arrays.asList(table1.name));

        assertEquals(2, result.size());
        table1.getLiveSSTables().forEach(sstable -> {
            assertFalse(sstable.isRepaired());
            assertTrue(result.contains(sstable.descriptor.baseFile().name()));
        });
    }
}
