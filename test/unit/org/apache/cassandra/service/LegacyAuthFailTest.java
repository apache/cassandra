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

package org.apache.cassandra.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.base.Joiner;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.SchemaConstants;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LegacyAuthFailTest extends CQLTester
{
    @Test
    public void testStartupChecks() throws Throwable
    {
        createKeyspace();

        List<String> legacyTables = new ArrayList<>(SchemaConstants.LEGACY_AUTH_TABLES);

        // test reporting for individual tables
        for (String legacyTable : legacyTables)
        {
            createLegacyTable(legacyTable);

            Optional<String> errMsg = StartupChecks.checkLegacyAuthTablesMessage();
            assertEquals(format("Legacy auth tables %s in keyspace %s still exist and have not been properly migrated.",
                                legacyTable,
                                SchemaConstants.AUTH_KEYSPACE_NAME), errMsg.get());
            dropLegacyTable(legacyTable);
        }

        // test reporting of multiple existing tables
        for (String legacyTable : legacyTables)
            createLegacyTable(legacyTable);

        while (!legacyTables.isEmpty())
        {
            Optional<String> errMsg = StartupChecks.checkLegacyAuthTablesMessage();
            assertEquals(format("Legacy auth tables %s in keyspace %s still exist and have not been properly migrated.",
                                Joiner.on(", ").join(legacyTables),
                                SchemaConstants.AUTH_KEYSPACE_NAME), errMsg.get());

            dropLegacyTable(legacyTables.remove(0));
        }

        // no legacy tables found
        Optional<String> errMsg = StartupChecks.checkLegacyAuthTablesMessage();
        assertFalse(errMsg.isPresent());
    }

    private void dropLegacyTable(String legacyTable) throws Throwable
    {
        execute(format("DROP TABLE %s.%s", SchemaConstants.AUTH_KEYSPACE_NAME, legacyTable));
    }

    private void createLegacyTable(String legacyTable) throws Throwable
    {
        execute(format("CREATE TABLE %s.%s (id int PRIMARY KEY, val text)", SchemaConstants.AUTH_KEYSPACE_NAME, legacyTable));
    }

    private void createKeyspace() throws Throwable
    {
        execute(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", SchemaConstants.AUTH_KEYSPACE_NAME));
    }
}
