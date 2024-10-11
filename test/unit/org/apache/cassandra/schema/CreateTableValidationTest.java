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
import org.apache.cassandra.utils.BloomCalculations;

import org.junit.Test;

public class CreateTableValidationTest extends CQLTester
{
    @Test
    public void testInvalidBloomFilterFPRatio()
    {
        expectedFailure(ConfigurationException.class, "CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.0000001",
                        "bloom_filter_fp_chance must be larger than " + BloomCalculations.minSupportedBloomFilterFpChance() + " and less than or equal to 1.0 (got 1.0E-7)");
        expectedFailure(ConfigurationException.class, "CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 1.1",
                        "bloom_filter_fp_chance must be larger than " + BloomCalculations.minSupportedBloomFilterFpChance() + " and less than or equal to 1.0 (got 1.1");
        // sanity check
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1");
    }

    @Test
    public void testCreateTableOnSelectedClusteringColumn()
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");
    }

    @Test
    public void testCreateTableOnAllClusteringColumns()
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC);");
    }

    @Test
    public void testCreateTableErrorOnNonClusteringKey()
    {
        String expectedMessage = "Only clustering key columns can be defined in CLUSTERING ORDER directive";
        expectedFailure(InvalidRequestException.class, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC, v ASC);",
                        expectedMessage+": [v]");
        expectedFailure(InvalidRequestException.class, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (v ASC);",
                        expectedMessage+": [v]");
        expectedFailure(InvalidRequestException.class, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk ASC);",
                        expectedMessage+": [pk]");
        expectedFailure(InvalidRequestException.class, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk ASC, ck1 DESC);",
                        expectedMessage+": [pk]");
        expectedFailure(InvalidRequestException.class, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC, pk DESC);",
                        expectedMessage+": [pk]");
        expectedFailure(InvalidRequestException.class, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk DESC, v DESC);",
                        expectedMessage+": [pk, v]");
        expectedFailure(InvalidRequestException.class, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk DESC, v DESC, ck1 DESC);",
                        expectedMessage+": [pk, v]");
        expectedFailure(InvalidRequestException.class, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, v ASC);",
                        expectedMessage+": [v]");
        expectedFailure(InvalidRequestException.class, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (v ASC, ck1 DESC);",
                        expectedMessage+": [v]");
    }

    @Test
    public void testCreateTableInWrongOrder()
    {
        expectedFailure(InvalidRequestException.class, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck2 ASC, ck1 DESC);",
                        "The order of columns in the CLUSTERING ORDER directive must match that of the clustering columns");
    }

    @Test
    public void testCreateTableWithMissingClusteringColumn()
    {
        expectedFailure(InvalidRequestException.class, "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck2 ASC);",
                        "Missing CLUSTERING ORDER for column ck1");
    }

}
