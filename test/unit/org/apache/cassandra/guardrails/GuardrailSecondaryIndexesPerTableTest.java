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

package org.apache.cassandra.guardrails;


import com.google.common.base.Strings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class GuardrailSecondaryIndexesPerTableTest extends GuardrailTester
{
    private Long defaultSIPerTableFailureThreshold;

    @Before
    public void before()
    {
        defaultSIPerTableFailureThreshold = config().secondary_index_per_table_failure_threshold;
        config().secondary_index_per_table_failure_threshold = 1L;
    }

    @After
    public void after()
    {
        config().secondary_index_per_table_failure_threshold = defaultSIPerTableFailureThreshold;
    }

    @Test
    public void testCreateIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");
        String indexName = createIndex("CREATE INDEX ON %s(v1)");
        assertNumIndexes(1);

        assertFails("", "v2", 1);
        assertFails("custom_index_name", "v2", 1);
        assertNumIndexes(1);

        // 2i guardrail won't affect custom index
        assertValid("CREATE CUSTOM INDEX ON %s (v2) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
        assertNumIndexes(2);

        // drop the first index, we should be able to create new index again
        dropIndex(format("DROP INDEX %s.%s", keyspace(), indexName));
        assertNumIndexes(1);

        execute("CREATE INDEX ON %s(v2)");
        assertNumIndexes(2);

        // previous guardrail should not apply to another base table
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");
        assertValid("CREATE INDEX ON %s(v1)");
        assertNumIndexes(1);

        assertFails("custom_index_name2", "v2", 1);
        assertNumIndexes(1);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");
        testExcludedUsers("CREATE INDEX excluded_1 ON %s(v1)",
                          "CREATE INDEX excluded_2 ON %s(v2)",
                          "DROP INDEX excluded_1",
                          "DROP INDEX excluded_2");
    }

    private void assertNumIndexes(int count)
    {
        assertEquals(count, getCurrentColumnFamilyStore().indexManager.listIndexes().size());
    }

    private void assertFails(String indexName, String column, int indexes) throws Throwable
    {
        String expectedMessage = String.format("failed to create secondary index %son table %s",
                                               Strings.isNullOrEmpty(indexName) ? "" : indexName + " ", currentTable());
        assertFails(expectedMessage, format("CREATE INDEX %s ON %%s(%s)", indexName, column));
    }
}
