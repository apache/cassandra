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

package org.apache.cassandra.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;

public class AlterTableValidationTest extends CQLTester
{
    @Before
    public void setup()
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int)");
    }

    @After
    public void close()
    {
        dropTable("Drop TABLE %s");
    }

    @Test
    public void testUsingAllSSTableFormatVariables()
    {
        alterTable("ALTER TABLE %s  WITH bloom_filter_fp_chance = 0.1 and crc_check_chance = 0.5 and min_index_interval = 256 and max_index_interval = 1024");
    }

    @Test
    public void testUsingSSTableFormatParam()
    {
        alterTable("ALTER TABLE %s WITH sstable_format = 'bti-fast' ");
        // Undeclared option will use default values
        alterTable("ALTER TABLE %s  WITH sstable_format = { 'type' : 'bti-fast', 'bloom_filter_fp_chance' : 0.001}; ");
    }

    @Test
    public void testUsingSSTableFormatParamWithException() throws Throwable
    {
        // not default value and not declared in yaml
        assertInvalidThrowMessage("SSTableFormat configuration \"bti-fastest\" not found", ConfigurationException.class, "ALTER TABLE %s WITH sstable_format = 'bti-fastest' ");
        assertInvalidThrowMessage("SSTableFormat configuration \"bti-fastest\" not found", ConfigurationException.class, "ALTER TABLE %s WITH sstable_format = { 'type' : 'bti-fastest', 'bloom_filter_fp_chance' : 0.001}; ");
    }

    @Test
    public void testBothSSTableFormatParamAndVariables() throws Throwable
    {
        // it's ok to declared both of them with same  value
        alterTable("ALTER TABLE %s WITH sstable_format = 'bti-fast' ");

        // it's not ok to declared both of them with different value
        assertInvalidThrowMessage("Cannot define bloom_filter_fp_chance AND sstable_format' bloom_filter_fp_chance at the same time with different value",
                                  ConfigurationException.class,
                                  "ALTER TABLE %s WITH sstable_format = { 'type' : 'bti-fast', 'bloom_filter_fp_chance' : 0.001} AND bloom_filter_fp_chance = 0.1 ; ");
        assertInvalidThrowMessage("Cannot define crc_check_chance AND sstable_format' crc_check_chance at the same time with different value",
                                  ConfigurationException.class,
                                  "ALTER TABLE %s WITH sstable_format = { 'type' : 'bti-fast', 'crc_check_chance' : 0.001} AND crc_check_chance = 0.1 ; ");
        assertInvalidThrowMessage("Cannot define min_index_interval AND sstable_format' min_index_interval at the same time with different value",
                                  ConfigurationException.class, "ALTER TABLE %s WITH sstable_format = { 'type' : 'bti-fast', 'min_index_interval' : 128} AND min_index_interval = 256 ; ");
        assertInvalidThrowMessage("Cannot define max_index_interval AND sstable_format' max_index_interval at the same time with different value",
                                  ConfigurationException.class, "ALTER TABLE %s WITH sstable_format = { 'type' : 'bti-fast', 'max_index_interval' : 1024} AND max_index_interval = 1025 ; ");
        assertInvalidThrowMessage("Cannot define bloom_filter_fp_chance AND sstable_format' bloom_filter_fp_chance at the same time with different value",
                                  ConfigurationException.class, "ALTER TABLE %s WITH sstable_format = { 'type' : 'bti-fast', 'bloom_filter_fp_chance' : 0.001, 'crc_check_chance' : 0.2, 'min_index_interval' : 129, 'max_index_interval' : 255} " +
                                                                                     "AND bloom_filter_fp_chance = 0.1 " +
                                                                                     "AND crc_check_chance = 0.02 " +
                                                                                     "AND min_index_interval = 128 " +
                                                                                     "AND max_index_interval = 256; ");
    }
}
