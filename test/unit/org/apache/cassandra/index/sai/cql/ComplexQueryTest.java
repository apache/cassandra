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

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class ComplexQueryTest extends SAITester
{
    @Test
    public void basicOrTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, a int, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 2, 2);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 3, 3);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE a = 1 or a = 3");

        assertRowsIgnoringOrder(resultSet, row(1), row(3) );
    }

    @Test
    public void basicInTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, a int, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 2, 2);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 3, 3);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 4, 4);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 5, 5);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE a in (1, 3, 5)");

        assertRowsIgnoringOrder(resultSet, row(1), row(3), row(5));
    }

    @Test
    public void complexQueryTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, a int, b int, c int, d int, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(d) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 1, 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 2, 2, 1, 1, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 3, 3, 2, 1, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 4, 4, 2, 2, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 5, 5, 3, 2, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 6, 6, 3, 2, 2);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 7, 7, 4, 3, 2);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 8, 8, 4, 3, 3);


        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE (a = 1 AND c = 1) OR (b IN (3, 4) AND d = 2)");

        assertRowsIgnoringOrder(resultSet, row(1), row(6), row(7) );
    }

    @Test
    public void disjunctionWithIndexOnClusteringKey() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 2, 2, 2);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE a = 1 or ck = 2");

        assertRowsIgnoringOrder(resultSet, row(1), row(2));
    }

    @Test
    public void complexQueryWithMultipleClusterings() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck0 int, ck1 int, a int, b int, c int, d int, e int, PRIMARY KEY(pk, ck0, ck1))");
        createIndex("CREATE CUSTOM INDEX ON %s(ck0) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ck1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(d) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(e) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 1, 1, 1, 1, 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 2, 2, 2, 2, 2, 2, 2, 2);
        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 3, 3, 3, 3, 3, 3, 3, 3);
        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 4, 4, 4, 4, 4, 4, 4, 4);
        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 5, 5, 5, 5, 5, 5, 5, 5);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE b = 6 AND d = 6 OR (a = 6 OR (c = 3 OR ck0 = 5))");

        assertRowsIgnoringOrder(resultSet, row(3), row(5));

        resultSet = execute("SELECT pk FROM %s WHERE ck0 = 1 AND (b = 6 AND c = 6 OR (d = 6 OR e = 6))");

        assertEquals(0 , resultSet.size());

        resultSet = execute("SELECT pk FROM %s WHERE b = 4 OR a = 3 OR c = 5");

        assertRowsIgnoringOrder(resultSet, row(3), row(4), row(5));

    }

    @Test
    public void complexQueryWithPartitionKeyRestriction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");

        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 1, 1, 5);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 2, 2, 6);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 2, 1, 3, 7);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 2, 2, 4, 8);

        UntypedResultSet resultSet = execute("SELECT pk, ck FROM %s WHERE pk = 1 AND (a = 2 OR b = 7)");

        assertRowsIgnoringOrder(resultSet, row(1, 2));

        assertThatThrownBy(() -> execute("SELECT pk, ck FROM %s WHERE pk = 1 OR a = 2 OR b = 7"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);

        resultSet = execute("SELECT pk, ck FROM %s WHERE pk = 1 OR (a = 2 OR b = 7)");

        assertRowsIgnoringOrder(resultSet, row(1, 1), row(1, 2), row(2, 1));
    }

    @Test
    public void complexQueryWithPartitionKeyRestrictionAndIndexes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 1, 1, 5);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 2, 2, 6);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 2, 1, 3, 7);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 2, 2, 4, 8);

        UntypedResultSet resultSet = execute("SELECT pk, ck FROM %s WHERE pk = 1 AND (a = 2 OR b = 7)");

        assertRowsIgnoringOrder(resultSet, row(1, 2));

        assertThatThrownBy(() -> execute("SELECT pk, ck FROM %s WHERE pk = 1 OR a = 2 OR b = 7"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);

        resultSet = execute("SELECT pk, ck FROM %s WHERE pk = 1 OR (a = 2 OR b = 7)");

        assertRowsIgnoringOrder(resultSet, row(1, 1), row(1, 2), row(2, 1));
    }

    @Test
    public void indexNotSupportingDisjunctionTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, a int, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'org.apache.cassandra.index.sasi.SASIIndex'");

        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 2, 2);

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE a = 1 or a = 2")).isInstanceOf(InvalidRequestException.class)
                                                                                   .hasMessage(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_DISJUNCTION);

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE a = 1 or a = 2 ALLOW FILTERING")).isInstanceOf(InvalidRequestException.class)
                                                                                                   .hasMessage(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_DISJUNCTION);
    }
}
