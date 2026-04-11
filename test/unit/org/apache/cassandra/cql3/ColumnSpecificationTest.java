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

package org.apache.cassandra.cql3;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.CompactionParams;

/**
 * Test various "extensions" to a column spec when altering / creating a table
 */
public class ColumnSpecificationTest extends CQLTester
{
    @Before
    public void before()
    {
        DatabaseDescriptor.setDynamicDataMaskingEnabled(true);
    }

    @Test
    public void testCreateTableWithColumnHavingMaskBeforeCheck()
    {
        createTable("CREATE TABLE %s (pk text primary key, name text MASKED WITH system.mask_default() CHECK NOT NULL AND LENGTH() > 1);");
        verifyColumnSpec("name text MASKED WITH system.mask_default() CHECK NOT NULL AND LENGTH() > 1");
    }

    @Test
    public void testAlterTableAlterColumnWithMaskAndCheckStandalone()
    {
        createTable("CREATE TABLE %s (pk text, name text, primary key (pk));");
        execute("ALTER TABLE %s ALTER name MASKED WITH system.mask_default()");
        execute("ALTER TABLE %s ALTER name CHECK NOT NULL AND LENGTH() > 1;");
        verifyColumnSpec("name text MASKED WITH system.mask_default() CHECK NOT NULL AND LENGTH() > 1");
    }

    @Test
    public void testAlterTableAlterColumnWithMask()
    {
        createTable("CREATE TABLE %s (pk text, name text, primary key (pk));");
        execute("ALTER TABLE %s ALTER name MASKED WITH system.mask_default()");
        verifyColumnSpec("name text MASKED WITH system.mask_default()");
    }

    @Test
    public void testAlterTableAlterColumnWithCheck()
    {
        createTable("CREATE TABLE %s (pk text, name text, primary key (pk));");
        execute("ALTER TABLE %s ALTER name CHECK NOT NULL AND LENGTH() > 1;");
        verifyColumnSpec("name text CHECK NOT NULL AND LENGTH() > 1");
    }

    @Test
    public void testAddingCheckToColumnWithMask()
    {
        createTable("CREATE TABLE %s (pk text primary key, name text MASKED WITH system.mask_default());");
        execute("ALTER TABLE %s ALTER name CHECK NOT NULL AND LENGTH() > 1");
        verifyColumnSpec("name text MASKED WITH system.mask_default() CHECK NOT NULL AND LENGTH() > 1");
    }

    @Test
    public void testAddingMaskToColumnWithCheck()
    {
        createTable("CREATE TABLE %s (pk text primary key, name text CHECK NOT NULL AND LENGTH() > 1);");
        execute("ALTER TABLE %s ALTER name MASKED WITH system.mask_default()");
        verifyColumnSpec("name text MASKED WITH system.mask_default() CHECK NOT NULL AND LENGTH() > 1");
    }

    @Test
    public void testDroppingCheckKeepsMask()
    {
        createTable("CREATE TABLE %s (pk text primary key, name text MASKED WITH system.mask_default() CHECK NOT NULL AND LENGTH() > 1);");
        execute("ALTER TABLE %s ALTER name DROP CHECK");
        verifyColumnSpec("name text MASKED WITH system.mask_default()");
    }

    @Test
    public void droppingMaskKeepsCheck()
    {
        createTable("CREATE TABLE %s (pk text primary key, name text MASKED WITH system.mask_default() CHECK NOT NULL AND LENGTH() > 1);");
        execute("ALTER TABLE %s ALTER name DROP MASKED");
        verifyColumnSpec("name text CHECK NOT NULL AND LENGTH() > 1");
    }

    @Test
    public void testAlterTableAddColumnWithCheck()
    {
        createTable("CREATE TABLE %s (pk text primary key);");
        execute("ALTER TABLE %s ADD name text CHECK NOT NULL AND LENGTH() > 1");
        verifyColumnSpec("name text CHECK NOT NULL AND LENGTH() > 1");
    }

    @Test
    public void testAlterTableAddColumnWithMask()
    {
        createTable("CREATE TABLE %s (pk text primary key);");
        execute("ALTER TABLE %s ADD name text MASKED WITH system.mask_default()");
        verifyColumnSpec("name text MASKED WITH system.mask_default()");
    }

    @Test
    public void testAlterTableAddColumnWithMaskAndCheck()
    {
        createTable("CREATE TABLE %s (pk text primary key);");
        execute("ALTER TABLE %s ADD name text MASKED WITH system.mask_default() CHECK NOT NULL");
        verifyColumnSpec("name text MASKED WITH system.mask_default() CHECK NOT NULL");
    }

    @Test
    public void testAlterTableAddColumnWithMaskAndMultipleChecks()
    {
        createTable("CREATE TABLE %s (pk text primary key);");
        execute("ALTER TABLE %s ADD name text MASKED WITH system.mask_default() CHECK NOT NULL AND LENGTH() > 1");
        verifyColumnSpec("name text MASKED WITH system.mask_default() CHECK NOT NULL AND LENGTH() > 1");
    }

    /**
     * TODO - investigate if it is possible to specify checks before mask when creating a table
     */
    @Test(expected = RuntimeException.class)
    public void testFailingCreateTableWithColumnHavingMaskAfterCheck()
    {
        createTable("CREATE TABLE %s (pk text primary key, name text CHECK NOT NULL AND LENGTH() > 1 MASKED WITH system.mask_default());");
    }

    /**
     * TODO - investigate if it is possible to specify both check and mask, check being first
     */
    @Test(expected = RuntimeException.class)
    public void testFailingAlterTableAlterColumnWithCheckAndMask()
    {
        createTable("CREATE TABLE %s (pk text, name text, primary key (pk));");
        execute("ALTER TABLE %s ALTER name CHECK NOT NULL AND LENGTH() > 1 MASKED WITH system.mask_default();");
        verifyColumnSpec("name text MASKED WITH system.mask_default() CHECK NOT NULL AND LENGTH() > 1");
    }

    /**
     * TODO - investigate if it is possible to specify both check and mask, mask being first
     */
    @Test(expected = RuntimeException.class)
    public void testFailingAlterTableAlterColumnWithMaskAndCheck()
    {
        createTable("CREATE TABLE %s (pk text, name text, primary key (pk));");
        execute("ALTER TABLE %s ALTER name MASKED WITH system.mask_default() CHECK NOT NULL AND LENGTH() > 1");
        verifyColumnSpec("name text MASKED WITH system.mask_default() CHECK NOT NULL AND LENGTH() > 1");
    }

    private void verifyColumnSpec(String modifiedColumn)
    {
        assertRowsContains(executeNetWithoutPaging("DESCRIBE TABLE " + KEYSPACE + '.' + currentTable()),
                           row(KEYSPACE,
                               "table",
                               currentTable(),
                               "CREATE TABLE " + KEYSPACE + '.' + currentTable() + " (\n" +
                               "    pk text PRIMARY KEY,\n" +
                               "    " + modifiedColumn + '\n' +
                               ") WITH " + tableParametersCql()));
    }

    static String tableParametersCql()
    {
        return "additional_write_policy = '99p'\n" +
               "    AND allow_auto_snapshot = true\n" +
               "    AND bloom_filter_fp_chance = 0.01\n" +
               "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
               "    AND cdc = false\n" +
               "    AND comment = ''\n" +
               "    AND compaction = " + cqlQuoted(CompactionParams.DEFAULT.asMap()) + '\n' +
               "    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
               "    AND memtable = 'default'\n" +
               "    AND crc_check_chance = 1.0\n" +
               "    AND fast_path = 'keyspace'\n" +
               "    AND default_time_to_live = 0\n" +
               "    AND extensions = {}\n" +
               "    AND gc_grace_seconds = 864000\n" +
               "    AND incremental_backups = true\n" +
               "    AND max_index_interval = 2048\n" +
               "    AND memtable_flush_period_in_ms = 0\n" +
               "    AND min_index_interval = 128\n" +
               "    AND read_repair = 'BLOCKING'\n" +
               "    AND transactional_mode = 'off'\n" +
               "    AND transactional_migration_from = 'none'\n" +
               "    AND speculative_retry = '99p';";
    }

    private static String cqlQuoted(Map<String, String> map)
    {
        return new CqlBuilder().append(map).toString();
    }
}
