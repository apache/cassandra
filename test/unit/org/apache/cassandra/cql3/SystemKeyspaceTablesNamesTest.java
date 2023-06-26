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

import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.tracing.TraceKeyspace;

import static java.lang.String.format;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SystemKeyspaceTablesNamesTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        // Needed for distributed keyspaces
        requireNetwork();
    }

    @Test
    public void testSystemKeyspaceTableNames()
    {
        assertExpectedTablesInKeyspace(SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                       "SystemKeyspace.TABLE_NAMES",
                                       SystemKeyspace.TABLE_NAMES);
    }

    @Test
    public void testSystemSchemaKeyspaceTableNames()
    {
        assertExpectedTablesInKeyspace(SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                       "SchemaKeyspaceTables.ALL",
                                       ImmutableSet.copyOf(SchemaKeyspaceTables.ALL));
    }

    @Test
    public void testSystemTraceKeyspaceTableNames()
    {
        assertExpectedTablesInKeyspace(SchemaConstants.TRACE_KEYSPACE_NAME,
                                       "TraceKeyspace.TABLE_NAMES",
                                       TraceKeyspace.TABLE_NAMES);
    }

    @Test
    public void testSystemAuthKeyspaceTableNames()
    {
        assertExpectedTablesInKeyspace(SchemaConstants.AUTH_KEYSPACE_NAME,
                                       "AuthKeyspace.TABLE_NAMES",
                                       AuthKeyspace.TABLE_NAMES);
    }

    @Test
    public void testSystemDistributedKeyspaceTableNames()
    {
        assertExpectedTablesInKeyspace(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                       "SystemDistributedKeyspace.TABLE_NAMES",
                                       SystemDistributedKeyspace.TABLE_NAMES);
    }
    
    private static void assertExpectedTablesInKeyspace(String keyspaceName, String expectedTableSource, Set<String> expectedTables)
    {
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
        assertNotNull(keyspace);
        Set<String> actualKeyspaceTables = keyspace.tables.stream().map(t -> t.name).collect(Collectors.toSet());

        Sets.SetView<String> diff = Sets.difference(actualKeyspaceTables, expectedTables);
        assertTrue(format("The following tables are missing from %s: %s", expectedTableSource, diff), diff.isEmpty());

        diff = Sets.difference(expectedTables, actualKeyspaceTables);
        assertTrue(format("The following tables are in %s but should not be: %s", expectedTableSource,  diff), diff.isEmpty());
    }
}
