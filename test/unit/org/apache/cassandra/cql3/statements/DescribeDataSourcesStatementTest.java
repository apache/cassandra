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
package org.apache.cassandra.cql3.statements;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.transport.ProtocolVersion;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for DESCRIBE DATA_SOURCES statement
 */
public class DescribeDataSourcesStatementTest extends CQLTester
{
    private static final String TEST_SOURCE = "test_cdc_source";
    private static final String TEST_KEYSPACE = "test_ks";
    private static final String TEST_TABLE = "test_table";
    private static final String TEST_SINK = "test_sink";

    private void createTestSource(String sourceName, String keyspace, String table, String sink) throws Throwable
    {
        String insertQuery = String.format("INSERT INTO %s.service_configs (type, service, config) VALUES (?, ?, ?)",
                                          SchemaConstants.DISTRIBUTED_KEYSPACE_NAME);

        Map<String, String> config = new HashMap<>();
        config.put("keyspace", keyspace);
        config.put("table", table);
        config.put("sink", sink);

        org.apache.cassandra.cql3.QueryProcessor.execute(insertQuery,
                                                         ConsistencyLevel.ONE,
                                                         "DATA_SOURCE",
                                                         sourceName,
                                                         config);
    }

    private void cleanupTestSource(String sourceName) throws Throwable
    {
        String deleteQuery = String.format("DELETE FROM %s.service_configs WHERE service = ? AND type = ?",
                                          SchemaConstants.DISTRIBUTED_KEYSPACE_NAME);

        org.apache.cassandra.cql3.QueryProcessor.execute(deleteQuery,
                                                         ConsistencyLevel.ONE,
                                                         sourceName,
                                                         "DATA_SOURCE");
    }

    @Test
    public void testDescribeDataSources() throws Throwable
    {
        try
        {
            // Create test data source
            createTestSource(TEST_SOURCE, TEST_KEYSPACE, TEST_TABLE, TEST_SINK);

            // Test DESCRIBE, DESC keywords
            for (String describeKeyword : new String[]{ "DESCRIBE", "DESC" })
            {
                ResultSet resultSources = executeDescribeNet(describeKeyword + " DATA_SOURCES");

                // Verify column metadata
                assertEquals("Should have keyspace_name column", "keyspace_name",
                             resultSources.getColumnDefinitions().asList().get(0).getName());
                assertEquals("Should have type column", "type",
                             resultSources.getColumnDefinitions().asList().get(1).getName());
                assertEquals("Should have name column", "name",
                             resultSources.getColumnDefinitions().asList().get(2).getName());
                assertEquals("Should have create_statement column", "create_statement",
                             resultSources.getColumnDefinitions().asList().get(3).getName());

                // Verify source appears in results
                boolean foundSource = false;
                for (Row row : resultSources.all())
                {
                    if (TEST_SOURCE.equals(row.getString("name")))
                    {
                        foundSource = true;

                        // Verify metadata columns
                        assertEquals(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, row.getString("keyspace_name"));
                        assertNotNull("Type column should not be null", row.getString("type"));
                        assertNotNull("Create statement should not be null", row.getString("create_statement"));

                        String createStmt = row.getString("create_statement");
                        assertTrue("Create statement should contain source name",
                                  createStmt.contains(TEST_SOURCE));

                        // The format is {service: [type, config]} based on DescribeStatement implementation
                        assertTrue("Create statement should be in expected format",
                                  createStmt.startsWith("{") && createStmt.endsWith("}"));
                        break;
                    }
                }
                assertTrue("Should find created source with keyword: " + describeKeyword, foundSource);
            }
        }
        finally
        {
            cleanupTestSource(TEST_SOURCE);
        }
    }

    private ResultSet executeDescribeNet(String cql) throws Throwable
    {
        return executeNet(ProtocolVersion.CURRENT, cql);
    }
}