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

package org.apache.cassandra.repair.state;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;

public class IncrementalRepairStateTest extends CQLTester
{
    private static final String BASIC_TABLE = "basic_table";
    private static final String MV_BASE_TABLE = "mv_table";
    private static final String CDC_TABLE = "cdc_table";

    @BeforeClass
    public static void setupClass() throws Exception
    {
        DatabaseDescriptor.setCDCEnabled(true);
        DatabaseDescriptor.setMaterializedViewsEnabled(true);
        requireNetwork();
    }

    @Before
    public void setUp()
    {
        createTable(String.format("CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", KEYSPACE, BASIC_TABLE));
        createTable(String.format("CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", KEYSPACE, MV_BASE_TABLE));
        createTable(String.format("CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv " +
                                  "AS SELECT * FROM %s.%s WHERE pk IS NOT NULL AND v IS NOT NULL PRIMARY KEY (pk, v)",
                                  KEYSPACE, KEYSPACE, MV_BASE_TABLE));
        createTable(String.format("CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int) WITH cdc = true",
                                  KEYSPACE, CDC_TABLE));
    }

    @Test
    public void testIncrementalGetRepairRunnableSkipsMVAndCDCTables()
    {
        IncrementalRepairState state = new IncrementalRepairState();

        List<String> safeTables = state.filterOutUnsafeTables(KEYSPACE,
                                                              ImmutableList.of(BASIC_TABLE, MV_BASE_TABLE, CDC_TABLE));

        assertEquals(ImmutableList.of(BASIC_TABLE), safeTables);
    }
}
