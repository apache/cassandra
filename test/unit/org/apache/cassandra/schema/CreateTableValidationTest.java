/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CreateTableValidationTest extends CQLTester
{
    private static final String KEYSPACE1 = "CreateTableValidationTest";

    @Test
    public void testInvalidBloomFilterFPRatio() throws Throwable
    {
        try
        {
            createTableMayThrow("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.0000001");
            fail("Expected an fp chance of 0.0000001 to be rejected");
        }
        catch (ConfigurationException exc) { }

        try
        {
            createTableMayThrow("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 1.1");
            fail("Expected an fp chance of 1.1 to be rejected");
        }
        catch (ConfigurationException exc) { }

        // sanity check
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1");
    }

    @Test
    public void testCreateTableErrorOnNonClusterKey()
    {
        try
        {
            createTableMayThrow("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC, v ASC);");
        }
        catch (Throwable ex)
        {
            assertEquals(ex.getMessage(), "Only clustering key columns can be defined in CLUSTERING ORDER directive");
        }

        try
        {
            createTableMayThrow("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (v ASC);");
        }
        catch (Throwable ex)
        {
            assertEquals(ex.getMessage(), "Only clustering key columns can be defined in CLUSTERING ORDER directive");
        }

        try
        {
            createTableMayThrow("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk ASC);");
        }
        catch (Throwable ex)
        {
            assertEquals(ex.getMessage(), "Only clustering key columns can be defined in CLUSTERING ORDER directive");
        }

        try
        {
            createTableMayThrow("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk ASC, ck1 DESC);");
        }
        catch (Throwable ex)
        {
            assertEquals(ex.getMessage(), "Only clustering key columns can be defined in CLUSTERING ORDER directive");
        }

        try
        {
            createTableMayThrow("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC, pk DESC);");
        }
        catch (Throwable ex)
        {
            assertEquals(ex.getMessage(), "Only clustering key columns can be defined in CLUSTERING ORDER directive");
        }

        try
        {
            createTableMayThrow("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk DESC, v DESC);");
        }
        catch (Throwable ex)
        {
            assertEquals(ex.getMessage(), "Only clustering key columns can be defined in CLUSTERING ORDER directive");
        }

        try
        {
            createTableMayThrow("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk DESC, v DESC, ck1 DESC);");
        }
        catch (Throwable ex)
        {
            assertEquals(ex.getMessage(), "Only clustering key columns can be defined in CLUSTERING ORDER directive");
        }

        try
        {
            createTableMayThrow("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, v ASC);");
        }
        catch (Throwable ex)
        {
            assertEquals(ex.getMessage(), "Only clustering key columns can be defined in CLUSTERING ORDER directive");
        }

        try
        {
            createTableMayThrow("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (v ASC, ck1 DESC);");
        }
        catch (Throwable ex)
        {
            assertEquals(ex.getMessage(), "Only clustering key columns can be defined in CLUSTERING ORDER directive");
        }
    }
}
