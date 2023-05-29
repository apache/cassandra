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
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

import static org.apache.cassandra.index.sai.cql.VectorTypeTest.assertContainsInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VectorTombstoneTest extends SAITester
{
    @Test
    public void deleteVectorInMemoryTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(2);

        execute("UPDATE %s SET val = null WHERE pk = 0");

        result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).isEmpty();
    }

    @Test
    public void deleteVectorPostFlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(2);
        flush();

        execute("UPDATE %s SET val = null WHERE pk = 0");
        result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);

        execute("DELETE from %s WHERE pk = 1");
        result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).isEmpty();
        flush();

        result = execute("SELECT * FROM %s WHERE val ann of [2.5, 3.5, 4.5] LIMIT 2");
        assertThat(result).isEmpty();
    }

    @Test
    public void upsertTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");

        UntypedResultSet result = execute("SELECT * FROM %s WHERE val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 0);
        assertContainsInt(result, "pk", 1);
        flush();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        result = execute("SELECT * FROM %s WHERE val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 0);
        assertContainsInt(result, "pk", 1);
        flush();

        result = execute("SELECT * FROM %s WHERE val ann of [0.5, 1.5, 2.5] LIMIT 2");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 0);
        assertContainsInt(result, "pk", 1);
    }
}
