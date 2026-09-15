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

import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.tracing.TraceKeyspace;

import static org.junit.Assert.assertEquals;

/**
 * Verifies that the table name constant sets registered in {@link SchemaConstants}
 * match the actual keyspace table definitions (CASSANDRA-21156).
 */
public class SystemTableNamesConsistencyTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testSystemTableNamesConsistency()
    {
        Set<String> allTables = new HashSet<>();
        allTables.addAll(assertTableNames(SystemKeyspace.metadata(), SchemaConstants.SYSTEM_KEYSPACE_TABLE_NAMES));
        allTables.addAll(assertTableNames(SchemaKeyspace.metadata(), new HashSet<>(SchemaKeyspaceTables.ALL)));
        allTables.addAll(assertTableNames(TraceKeyspace.metadata(), SchemaConstants.TRACE_KEYSPACE_TABLE_NAMES));
        allTables.addAll(assertTableNames(AuthKeyspace.metadata(), SchemaConstants.AUTH_KEYSPACE_TABLE_NAMES));
        allTables.addAll(assertTableNames(SystemDistributedKeyspace.metadata(), SchemaConstants.DISTRIBUTED_KEYSPACE_TABLE_NAMES));
        allTables.addAll(assertTableNames(AccordKeyspace.metadata(), SchemaConstants.ACCORD_KEYSPACE_TABLE_NAMES));
        assertEquals(allTables, SchemaConstants.getLocalAndReplicatedSystemTableNames());
    }

    private static Set<String> assertTableNames(KeyspaceMetadata keyspace, Set<String> expected)
    {
        Set<String> actual = new HashSet<>();
        for (TableMetadata table : keyspace.tables)
            actual.add(table.name);
        assertEquals(keyspace.name, expected, actual);
        return actual;
    }
}
