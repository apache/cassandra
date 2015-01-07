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

import java.util.Locale;

import org.apache.commons.lang.StringUtils;

import org.junit.Test;

public class CreateIndexStatementTest extends CQLTester
{
    @Test
    public void testCreateAndDropIndex() throws Throwable
    {
        testCreateAndDropIndex("test", false);
        testCreateAndDropIndex("test2", true);
    }

    @Test
    public void testCreateAndDropIndexWithQuotedIdentifier() throws Throwable
    {
        testCreateAndDropIndex("\"quoted_ident\"", false);
        testCreateAndDropIndex("\"quoted_ident2\"", true);
    }

    @Test
    public void testCreateAndDropIndexWithCamelCaseIdentifier() throws Throwable
    {
        testCreateAndDropIndex("CamelCase", false);
        testCreateAndDropIndex("CamelCase2", true);
    }

    /**
     * Test creating and dropping an index with the specified name.
     *
     * @param indexName the index name
     * @param addKeyspaceOnDrop add the keyspace name in the drop statement
     * @throws Throwable if an error occurs
     */
    private void testCreateAndDropIndex(String indexName, boolean addKeyspaceOnDrop) throws Throwable
    {
        execute("USE system");
        assertInvalidMessage("Index '" + removeQuotes(indexName.toLowerCase(Locale.US)) + "' could not be found", "DROP INDEX " + indexName + ";");

        createTable("CREATE TABLE %s (a int primary key, b int);");
        createIndex("CREATE INDEX " + indexName + " ON %s(b);");
        createIndex("CREATE INDEX IF NOT EXISTS " + indexName + " ON %s(b);");

        assertInvalidMessage("Index already exists", "CREATE INDEX " + indexName + " ON %s(b)");

        execute("INSERT INTO %s (a, b) values (?, ?);", 0, 0);
        execute("INSERT INTO %s (a, b) values (?, ?);", 1, 1);
        execute("INSERT INTO %s (a, b) values (?, ?);", 2, 2);
        execute("INSERT INTO %s (a, b) values (?, ?);", 3, 1);

        assertRows(execute("SELECT * FROM %s where b = ?", 1), row(1, 1), row(3, 1));
        assertInvalidMessage("Index '" + removeQuotes(indexName.toLowerCase(Locale.US)) + "' could not be found in any of the tables of keyspace 'system'", "DROP INDEX " + indexName);

        if (addKeyspaceOnDrop)
        {
            dropIndex("DROP INDEX " + KEYSPACE + "." + indexName);
        }
        else
        {
            execute("USE " + KEYSPACE);
            dropIndex("DROP INDEX " + indexName);
        }

        assertInvalidMessage("No secondary indexes on the restricted columns support the provided operators",
                             "SELECT * FROM %s where b = ?", 1);
        dropIndex("DROP INDEX IF EXISTS " + indexName);
        assertInvalidMessage("Index '" + removeQuotes(indexName.toLowerCase(Locale.US)) + "' could not be found", "DROP INDEX " + indexName);
    }

    /**
     * Removes the quotes from the specified index name.
     *
     * @param indexName the index name from which the quotes must be removed.
     * @return the unquoted index name.
     */
    private static String removeQuotes(String indexName)
    {
        return StringUtils.remove(indexName, '\"');
    }
}
