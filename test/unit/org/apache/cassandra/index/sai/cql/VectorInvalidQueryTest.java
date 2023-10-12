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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

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
    public void cannotQueryEmptyVectorColumn()
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
    public void cannotQueryWrongNumberOfDimensions()
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
    public void testInvalidColumnNameWithAnn() throws Throwable
    {
        String table = createTable(KEYSPACE, "CREATE TABLE %s (k int, c int, v int, primary key (k, c))");
        assertInvalidMessage(String.format("Undefined column name bad_col in table %s", KEYSPACE + '.' + table),
                             "SELECT k from %s ORDER BY bad_col ANN OF [1.0] LIMIT 1");
    }

    @Test
    public void cannotPerformNonANNQueryOnVectorIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, val vector<float, 3>, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        assertInvalidMessage(StatementRestrictions.VECTOR_INDEXES_ANN_ONLY_MESSAGE,
                             "SELECT * FROM %s WHERE val = [1.0, 2.0, 3.0]");
    }

    @Test
    public void cannotOrderWithAnnOnNonVectorColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, primary key(k))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        assertInvalidMessage(StatementRestrictions.ANN_ONLY_SUPPORTED_ON_VECTOR_MESSAGE,
                             "SELECT * FROM %s ORDER BY v ANN OF 1 LIMIT 1");
    }

    @Test
    public void disallowZeroVectorsWithCosineSimilarity()
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

    @Test
    public void mustHaveLimitSpecifiedAndWithinMaxAllowed()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 1>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        assertThatThrownBy(() -> executeNet("SELECT * FROM %s WHERE k = 1 ORDER BY v ANN OF [0]"))
        .isInstanceOf(InvalidQueryException.class).hasMessage(SelectStatement.TOPK_LIMIT_ERROR);

        assertThatThrownBy(() -> executeNet("SELECT * FROM %s WHERE k = 1 ORDER BY v ANN OF [0] LIMIT 1001"))
        .isInstanceOf(InvalidQueryException.class).hasMessage(String.format(StorageAttachedIndex.ANN_LIMIT_ERROR, IndexWriterConfig.MAX_TOP_K, 1001));
    }

    @Test
    public void mustHaveLimitWithinPageSize()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, v) VALUES (1, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (k, v) VALUES (2, [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (k, v) VALUES (3, [3.0, 4.0, 5.0])");

        assertThatThrownBy(() -> executeNetWithPaging("SELECT * FROM %s WHERE k = 1 ORDER BY v ANN OF [2.0, 3.0, 4.0] LIMIT 10", 9))
        .isInstanceOf(InvalidQueryException.class).hasMessageContaining(SelectStatement.TOPK_LIMIT_ERROR);

        ResultSet resultSet = executeNetWithPaging("SELECT * FROM %s WHERE k = 1 ORDER BY v ANN OF [2.0, 3.0, 4.0] LIMIT 10", 11);

        assertEquals(1, resultSet.all().size());
    }

    @Test
    public void cannotHaveAggregationOnANNQuery()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 1>, c int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, v, c) VALUES (1, [4], 1)");
        execute("INSERT INTO %s (k, v, c) VALUES (2, [3], 10)");
        execute("INSERT INTO %s (k, v, c) VALUES (3, [2], 100)");
        execute("INSERT INTO %s (k, v, c) VALUES (4, [1], 1000)");

        assertThatThrownBy(() -> executeNet("SELECT sum(c) FROM %s WHERE k = 1 ORDER BY v ANN OF [0] LIMIT 4"))
        .isInstanceOf(InvalidQueryException.class).hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);
    }

    @Test
    public void multipleVectorColumnsInQueryFailCorrectlyTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 vector<float, 1>, v2 vector<float, 1>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, v1, v2) VALUES (1, [1], [2])");

        assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEX_MESSAGE,
                             "SELECT * FROM %s WHERE v1 = [1] ORDER BY v2 ANN OF [2] ALLOW FILTERING");
    }

    @Test
    public void annOrderingIsNotAllowedWithoutIndexWhereIndexedColumnExistsInQueryTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 1>, c int)");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, v, c) VALUES (1, [4], 1)");
        execute("INSERT INTO %s (k, v, c) VALUES (2, [3], 10)");
        execute("INSERT INTO %s (k, v, c) VALUES (3, [2], 100)");
        execute("INSERT INTO %s (k, v, c) VALUES (4, [1], 1000)");

        assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEX_MESSAGE,
                             "SELECT * FROM %s WHERE c >= 100 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");
    }

    @Test
    public void cannotPostFilterOnNonIndexedColumnWithAnnOrdering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, ck1 int, ck2 int, v vector<float, 1>, c int, primary key ((pk1, pk2), ck1, ck2))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, v, c) VALUES (1, 1, 1, 1, [4], 1)");
        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, v, c) VALUES (2, 2, 1, 1, [3], 10)");
        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, v, c) VALUES (3, 3, 1, 1, [2], 100)");
        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, v, c) VALUES (4, 4, 1, 1, [1], 1000)");

        assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c >= 100 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");

        assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE ck1 >= 0 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");

        assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE ck2 = 1 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");

        assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE pk1 = 1 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");

        assertInvalidMessage(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE pk2 = 1 ORDER BY v ANN OF [1] LIMIT 4 ALLOW FILTERING");
    }

    @Test
    public void cannotCreateIndexOnNonFloatVector()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<int, 1>)");

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'"))
            .isInstanceOf(InvalidRequestException.class).hasRootCauseMessage(StorageAttachedIndex.VECTOR_NON_FLOAT_ERROR);
    }
}
