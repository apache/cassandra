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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;

import static org.apache.cassandra.index.sai.cql.VectorTypeTest.assertContainsInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class VectorAndLuceneTest extends VectorTester
{

    @Test
    public void basicVectorLuceneSelectTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'" +
                    " WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        // The str_val is set up to make sure that tokens are properly analyzed and lowercased.
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'One Duplicate phrase', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'This duplicate PHRASE', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'A different Phrase', [3.0, 4.0, 5.0])");

        // 'phrase' is in all rows, so expect all rows to be returned.
        UntypedResultSet result = execute("SELECT * FROM %s WHERE str_val : 'phrase' ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);

        // 'missing' is in no rows, so expect no rows to be returned.
        result = execute("SELECT * FROM %s WHERE str_val : 'missing' ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(0);

        // Use limit to 1 to be the limiting condition. 'phrase' matches all three str_val. Matches row 1 becuase the vector is closer.
        result = execute("SELECT * FROM %s WHERE str_val : 'phrase' ORDER BY val ann of [2.1, 3.1, 4.1] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        // 'one' only matches for row 0. The vector is the same as the one in the 2nd row.
        result = execute("SELECT * FROM %s WHERE str_val : 'one' ORDER BY val ann of [3.0, 4.0, 5.0] LIMIT 3");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 0);

        // 'duplicate' matches rows 0 and 1. The vector matches row 2, but that doesn't match the WHERE clause.
        result = execute("SELECT * FROM %s WHERE str_val : 'duplicate' ORDER BY val ann of [3.0, 4.0, 5.0] LIMIT 3");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 0);
        assertContainsInt(result, "pk", 1);

    }

    // partition delete won't trigger UpdateTransaction#onUpdated
    @Test
    public void partitionDeleteVectorInMemoryTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'" +
                    " WITH OPTIONS = { 'index_analyzer': 'standard' }");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'this test has tokens', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'not so plural token', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'another test too', [3.0, 4.0, 5.0])");

        UntypedResultSet result = execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 3");
        assertThat(result).hasSize(3);

        execute("UPDATE %s SET val = null WHERE pk = 0");

        assertEquals(0, execute("SELECT * FROM %s WHERE str_val : 'tokens' ORDER BY val ann of [1.1, 2.1, 3.1] LIMIT 2").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE str_val : 'token'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE str_val : 'test'").size());

        result = execute("SELECT * FROM %s ORDER BY val ann of [1.1, 2.1, 3.1] LIMIT 1"); // closer to row 0
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s ORDER BY val ann of [2.1, 3.1, 4.1] LIMIT 1"); // closer to row 1
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 2);

        flush();

        result = execute("SELECT * FROM %s ORDER BY val ann of [2.1, 3.1, 4.1] LIMIT 1");  // closer to row 1
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 2);
    }
}
