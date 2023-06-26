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
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.tracing.TraceKeyspace;

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
        Set<String> tables = Schema.instance.getKeyspaceMetadata(SchemaConstants.SYSTEM_KEYSPACE_NAME).tables
                             .stream().map(t -> t.name).collect(Collectors.toSet());

        Sets.SetView<String> diff = Sets.difference(tables, SystemKeyspace.TABLE_NAMES);
        assertTrue("The following tables are missing from SystemKeyspace.TABLE_NAMES: " + diff, diff.isEmpty());

        diff = Sets.difference(SystemKeyspace.TABLE_NAMES, tables);
        assertTrue("The following tables are in SystemKeyspace.TABLE_NAMES but should not be: " + diff, diff.isEmpty());
    }

    @Test
    public void testSystemSchemaKeyspaceTableNames()
    {
        Set<String> tables = Schema.instance.getKeyspaceMetadata(SchemaConstants.SCHEMA_KEYSPACE_NAME).tables
                             .stream().map(t -> t.name).collect(Collectors.toSet());

        Sets.SetView<String> diff = Sets.difference(tables, ImmutableSet.copyOf(SchemaKeyspaceTables.ALL));
        assertTrue("The following tables are missing from SchemaKeyspaceTables.ALL: " + diff, diff.isEmpty());

        diff = Sets.difference(ImmutableSet.copyOf(SchemaKeyspaceTables.ALL), tables);
        assertTrue("The following tables are in SchemaKeyspaceTables.ALL but should not be: " + diff, diff.isEmpty());
    }

    @Test
    public void testSystemTraceKeyspaceTableNames()
    {
        Set<String> keyspaceTableNames = Schema.instance.getKeyspaceMetadata(SchemaConstants.TRACE_KEYSPACE_NAME).tables
                                         .stream().map(t -> t.name).collect(Collectors.toSet());

        Sets.SetView<String> diff = Sets.difference(keyspaceTableNames, TraceKeyspace.TABLE_NAMES);
        assertTrue("The following tables are missing from TraceKeyspace.TABLE_NAMES: " + diff, diff.isEmpty());

        diff = Sets.difference(TraceKeyspace.TABLE_NAMES, keyspaceTableNames);
        assertTrue("The following tables are in TraceKeyspace.TABLE_NAMES but should not be: " + diff, diff.isEmpty());
    }

    @Test
    public void testSystemAuthKeyspaceTableNames()
    {
        Set<String> keyspaceTableNames = Schema.instance.getKeyspaceMetadata(SchemaConstants.AUTH_KEYSPACE_NAME).tables
                                         .stream().map(t -> t.name).collect(Collectors.toSet());

        Sets.SetView<String> diff = Sets.difference(keyspaceTableNames, AuthKeyspace.TABLE_NAMES);
        assertTrue("The following tables are missing from AuthKeyspace.TABLE_NAMES: " + diff, diff.isEmpty());

        diff = Sets.difference(AuthKeyspace.TABLE_NAMES, keyspaceTableNames);
        assertTrue("The following tables are in AuthKeyspace.TABLE_NAMES but should not be: " + diff, diff.isEmpty());
    }

    @Test
    public void testSystemDistributedKeyspaceTableNames()
    {
        Set<String> keyspaceTableNames = Schema.instance.getKeyspaceMetadata(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME).tables
                                         .stream().map(t -> t.name).collect(Collectors.toSet());

        Sets.SetView<String> diff = Sets.difference(keyspaceTableNames, SystemDistributedKeyspace.TABLE_NAMES);
        assertTrue("The following tables are missing from SystemDistributedKeyspace.TABLE_NAMES: " + diff, diff.isEmpty());

        diff = Sets.difference(SystemDistributedKeyspace.TABLE_NAMES, keyspaceTableNames);
        assertTrue("The following tables are in SystemDistributedKeyspace.TABLE_NAMES but should not be: " + diff, diff.isEmpty());
    }
}
