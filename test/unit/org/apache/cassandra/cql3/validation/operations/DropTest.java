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

package org.apache.cassandra.cql3.validation.operations;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

public class DropTest extends CQLTester
{
    @Test
    public void testDropTableWithNameCapitalPAndColumnDuration() throws Throwable
    {
        // CASSANDRA-17919
        createTable(KEYSPACE, "CREATE TABLE %s (a INT PRIMARY KEY, b DURATION);", "P");
        execute("DROP TABLE %s");
        assertRowsIgnoringOrder(execute(String.format("SELECT * FROM system_schema.dropped_columns WHERE keyspace_name = '%s' AND table_name = 'P'", keyspace())));
    }

    @Test
    public void testNonExistingOnes() throws Throwable
    {
        assertInvalidMessage(String.format("Table '%s.table_does_not_exist' doesn't exist", KEYSPACE),  "DROP TABLE " + KEYSPACE + ".table_does_not_exist");
        assertInvalidMessage("Table 'keyspace_does_not_exist.table_does_not_exist' doesn't exist", "DROP TABLE keyspace_does_not_exist.table_does_not_exist");

        execute("DROP TABLE IF EXISTS " + KEYSPACE + ".table_does_not_exist");
        execute("DROP TABLE IF EXISTS keyspace_does_not_exist.table_does_not_exist");
    }

    @Test
    public void testDropTableWithDroppedColumns() throws Throwable
    {
        // CASSANDRA-13730: entry should be removed from dropped_columns table when table is dropped
        String cf = createTable("CREATE TABLE %s (k1 int, c1 int , v1 int, v2 int, PRIMARY KEY (k1, c1))");

        execute("ALTER TABLE %s DROP v2");
        execute("DROP TABLE %s");

        assertRowsIgnoringOrder(execute("select * from system_schema.dropped_columns where keyspace_name = '"
                + keyspace()
                + "' and table_name = '" + cf + "'"));
    }
}
