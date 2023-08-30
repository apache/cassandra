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

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VectorInvalidQueryTest extends SAITester
{
    @Test
    public void cannotCreateEmptyVectorColumn()
    {
        assertThatThrownBy(() -> execute(String.format("CREATE TABLE %s.%s (pk int, str_val text, val vector<float, 0>, PRIMARY KEY(pk))",
                                                       KEYSPACE, createTableName())))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("vectors may only have positive dimensions; given 0");
    }

    @Test
    public void cannotQueryEmptyVectorColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        assertThatThrownBy(() -> execute("SELECT similarity_cosine((vector<float, 0>) [], []) FROM %s"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("vectors may only have positive dimensions; given 0");
    }

    @Test
    public void cannotInsertWrongNumberOfDimensions()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");

        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0])"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Invalid vector literal for val of type vector<float, 3>; expected 3 elements, but given 2");
    }

    @Test
    public void cannotQueryWrongNumberOfDimensions() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])");

        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5] LIMIT 5"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Invalid vector literal for val of type vector<float, 3>; expected 3 elements, but given 2");
    }

    @Test
    public void testMultiVectorOrderingsNotAllowed() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val1 vector<float, 3>, val2 vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val2) USING 'StorageAttachedIndex'");

        assertInvalidMessage("Cannot specify more than one ANN ordering",
                             "SELECT * FROM %s ORDER BY val1 ann of [2.5, 3.5, 4.5], val2 ann of [2.1, 3.2, 4.0] LIMIT 2");
    }

    @Test
    public void testDescendingVectorOrderingIsNotAllowed() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        assertInvalidMessage("Descending ANN ordering is not supported",
                             "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] DESC LIMIT 2");
    }

    @Test
    public void testVectorOrderingIsNotAllowedWithClusteringOrdering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, val vector<float, 3>, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        assertInvalidMessage("ANN ordering does not support secondary ordering",
                             "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5], ck ASC LIMIT 2");
    }

    @Test
    public void testVectorOrderingIsNotAllowedWithoutIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");

        assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEX_MESSAGE,
                             "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 5");

        assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEX_MESSAGE,
                             "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 5 ALLOW FILTERING");
    }

    @Test
    public void annOrderingMustHaveLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, val vector<float, 3>, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        assertInvalidMessage("Use of ANN OF in an ORDER BY clause requires a LIMIT that is not greater than 1000. LIMIT was NO LIMIT",
                             "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5]");

    }

    @Test
    public void testInvalidColumnNameWithAnn() throws Throwable
    {
        String table = createTable(KEYSPACE, "CREATE TABLE %s (k int, c int, v int, primary key (k, c))");
        assertInvalidMessage(String.format("Undefined column name bad_col in table %s", KEYSPACE + "." + table),
                             "SELECT k from %s ORDER BY bad_col ANN OF [1.0] LIMIT 1");
    }

    @Test
    public void disallowZeroVectorsWithCosineSimilarity() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'cosine'}");

        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, [0.0, 0.0])")).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(0.0f, 0.0f))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1.0f, Float.NaN))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1.0f, Float.POSITIVE_INFINITY))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(Float.NEGATIVE_INFINITY, 1.0f))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY value ann of [0.0, 0.0] LIMIT 2")).isInstanceOf(InvalidRequestException.class);
    }
}
