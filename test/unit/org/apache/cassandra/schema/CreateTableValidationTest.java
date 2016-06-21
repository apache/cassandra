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
import org.junit.Test;

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
}
