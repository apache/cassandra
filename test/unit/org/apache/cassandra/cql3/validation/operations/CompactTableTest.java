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

import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompactTableTest extends CQLTester
{
    @Test
    public void dropCompactStorageTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk)) WITH COMPACT STORAGE;");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 1)");
        alterTable("ALTER TABLE %s DROP COMPACT STORAGE");
        assertRows(execute( "SELECT * FROM %s"),
                   row(1, null, 1, null));

        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE;");
        execute("INSERT INTO %s (pk, ck) VALUES (1, 1)");
        alterTable("ALTER TABLE %s DROP COMPACT STORAGE");
        assertRows(execute( "SELECT * FROM %s"),
                   row(1, 1, null));

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE;");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        alterTable("ALTER TABLE %s DROP COMPACT STORAGE");
        assertRows(execute( "SELECT * FROM %s"),
                   row(1, 1, 1));
    }

    @Test
    public void dropCompactStorageShouldInvalidatePreparedStatements() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE;");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        String templateSelect = "SELECT * FROM %s WHERE pk = 1";
        assertRows(execute(templateSelect), row(1, 1, 1));

        // Verify that the prepared statement has been added to the cache after our first query.
        String formattedQuery = formatQuery(templateSelect);
        ConcurrentMap<String, QueryHandler.Prepared> original = QueryProcessor.getInternalStatements();
        assertTrue(original.containsKey(formattedQuery));

        // Verify that schema change listeners are told statements are affected with DROP COMPACT STORAGE.
        SchemaChangeListener listener = new SchemaChangeListener()
        {
            @Override
            public void onAlterTable(TableMetadata before, TableMetadata after, boolean affectsStatements)
            {
                assertTrue(affectsStatements);
            }
        };

        Schema.instance.registerListener(listener);

        try
        {
            alterTable("ALTER TABLE %s DROP COMPACT STORAGE");
            ConcurrentMap<String, QueryHandler.Prepared> postDrop = QueryProcessor.getInternalStatements();

            // Verify that the prepared statement has been removed the cache after DROP COMPACT STORAGE.
            assertFalse(postDrop.containsKey(formattedQuery));

            // Verify that the prepared statement has been added back to the cache after our second query.
            assertRows(execute(templateSelect), row(1, 1, 1));
            ConcurrentMap<String, QueryHandler.Prepared> postQuery = QueryProcessor.getInternalStatements();
            assertTrue(postQuery.containsKey(formattedQuery));
        }
        finally
        {
            // Clean up the listener so this doesn't fail other tests.
            Schema.instance.unregisterListener(listener);
        }
    }

    @Test
    public void compactStorageSemanticsTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", 1, 1);
        execute("DELETE FROM %s WHERE pk = ? AND ck = ?", 1, 1);
        assertEmpty(execute("SELECT * FROM %s WHERE pk = ?", 1));

        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY (pk, ck1, ck2)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (pk, ck1, v) VALUES (?, ?, ?)", 2, 2, 2);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?",2),
                   row(2, 2, null, 2));
    }

    @Test
    public void testColumnDeletionWithCompactTableWithMultipleColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (pk, v1, v2) VALUES (1, 1, 1) USING TIMESTAMP 1000");
        flush();
        execute("INSERT INTO %s (pk, v1) VALUES (1, 2) USING TIMESTAMP 2000");
        flush();
        execute("DELETE v1 FROM %s USING TIMESTAMP 3000 WHERE pk = 1");
        flush();

        assertRows(execute("SELECT * FROM %s WHERE pk=1"), row(1, null, 1));
        assertRows(execute("SELECT v1, v2 FROM %s WHERE pk=1"), row(null, 1));
        assertRows(execute("SELECT v1 FROM %s WHERE pk=1"), row((Integer) null));
    }
}
