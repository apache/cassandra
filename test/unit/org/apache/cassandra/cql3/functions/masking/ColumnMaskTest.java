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

package org.apache.cassandra.cql3.functions.masking;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.Arguments;
import org.apache.cassandra.cql3.functions.FunctionFactory;
import org.apache.cassandra.cql3.functions.FunctionParameter;
import org.apache.cassandra.cql3.functions.NativeFunction;
import org.apache.cassandra.cql3.functions.NativeFunctions;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.apache.cassandra.cql3.functions.masking.ColumnMask.DISABLED_ERROR_MESSAGE;

/**
 * Tests schema altering queries ({@code CREATE TABLE}, {@code ALTER TABLE}, etc.) that attach/dettach dynamic data
 * masking functions to column definitions.
 */
public class ColumnMaskTest extends ColumnMaskTester
{
    @Test
    public void testCollections() throws Throwable
    {
        // Create table with masks
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, " +
                                   "s set<int> MASKED WITH DEFAULT, " +
                                   "l list<int> MASKED WITH DEFAULT, " +
                                   "m map<int, int> MASKED WITH DEFAULT, " +
                                   "fs frozen<set<int>> MASKED WITH DEFAULT, " +
                                   "fl frozen<list<int>> MASKED WITH DEFAULT, " +
                                   "fm frozen<map<int, int>> MASKED WITH DEFAULT)");
        assertColumnIsMasked(table, "s", "mask_default", emptyList(), emptyList());
        assertColumnIsMasked(table, "l", "mask_default", emptyList(), emptyList());
        assertColumnIsMasked(table, "m", "mask_default", emptyList(), emptyList());
        assertColumnIsMasked(table, "fs", "mask_default", emptyList(), emptyList());
        assertColumnIsMasked(table, "fl", "mask_default", emptyList(), emptyList());
        assertColumnIsMasked(table, "fm", "mask_default", emptyList(), emptyList());

        // Alter column masks
        alterTable("ALTER TABLE %s ALTER s MASKED WITH mask_null()");
        alterTable("ALTER TABLE %s ALTER l MASKED WITH mask_null()");
        alterTable("ALTER TABLE %s ALTER m MASKED WITH mask_null()");
        alterTable("ALTER TABLE %s ALTER fs MASKED WITH mask_null()");
        alterTable("ALTER TABLE %s ALTER fl MASKED WITH mask_null()");
        alterTable("ALTER TABLE %s ALTER fm MASKED WITH mask_null()");
        assertColumnIsMasked(table, "s", "mask_null", emptyList(), emptyList());
        assertColumnIsMasked(table, "l", "mask_null", emptyList(), emptyList());
        assertColumnIsMasked(table, "m", "mask_null", emptyList(), emptyList());
        assertColumnIsMasked(table, "fs", "mask_null", emptyList(), emptyList());
        assertColumnIsMasked(table, "fl", "mask_null", emptyList(), emptyList());
        assertColumnIsMasked(table, "fm", "mask_null", emptyList(), emptyList());

        // Drop masks
        alterTable("ALTER TABLE %s ALTER s DROP MASKED");
        alterTable("ALTER TABLE %s ALTER l DROP MASKED");
        alterTable("ALTER TABLE %s ALTER m DROP MASKED");
        alterTable("ALTER TABLE %s ALTER fs DROP MASKED");
        alterTable("ALTER TABLE %s ALTER fl DROP MASKED");
        alterTable("ALTER TABLE %s ALTER fm DROP MASKED");
        assertTableColumnsAreNotMasked("s", "l", "m", "fs", "fl", "fm");
    }

    @Test
    public void testUDTs() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a1 varint, a2 varint, a3 varint);");

        // Create table with mask
        String table = createTable(format("CREATE TABLE %%s (k int PRIMARY KEY, v %s MASKED WITH DEFAULT)", type));
        assertColumnIsMasked(table, "v", "mask_default", emptyList(), emptyList());

        // Alter column mask
        alterTable("ALTER TABLE %s ALTER v MASKED WITH mask_null()");
        assertColumnIsMasked(table, "v", "mask_null", emptyList(), emptyList());

        // Drop mask
        alterTable("ALTER TABLE %s ALTER v DROP MASKED");
        assertTableColumnsAreNotMasked("v");
    }

    @Test
    public void testVectors() throws Throwable
    {
        // Create table with mask
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<int, 3> MASKED WITH DEFAULT)");
        assertColumnIsMasked(table, "v", "mask_default", emptyList(), emptyList());

        // Alter column mask
        alterTable("ALTER TABLE %s ALTER v MASKED WITH mask_null()");
        assertColumnIsMasked(table, "v", "mask_null", emptyList(), emptyList());

        // Drop mask
        alterTable("ALTER TABLE %s ALTER v DROP MASKED");
        assertTableColumnsAreNotMasked("v");
    }

    @Test
    public void testAlterTableAddMaskingToNonExistingColumn() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        execute("ALTER TABLE %s ALTER IF EXISTS unknown MASKED WITH DEFAULT");
        assertInvalidMessage(format("Column with name 'unknown' doesn't exist on table '%s'", table),
                             formatQuery("ALTER TABLE %s ALTER unknown MASKED WITH DEFAULT"));
    }

    @Test
    public void testAlterTableRemoveMaskingFromNonExistingColumn() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        execute("ALTER TABLE %s ALTER IF EXISTS unknown DROP MASKED");
        assertInvalidMessage(format("Column with name 'unknown' doesn't exist on table '%s'", table),
                             formatQuery("ALTER TABLE %s ALTER unknown DROP MASKED"));
    }

    @Test
    public void testAlterTableRemoveMaskFromUnmaskedColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        execute("ALTER TABLE %s ALTER v DROP MASKED");
        assertTableColumnsAreNotMasked("v");
    }

    @Test
    public void testInvalidMaskingFunctionName() throws Throwable
    {
        // create table
        createTableName();
        assertInvalidMessage("Unable to find masking function for v, no declared function matches the signature mask_missing()",
                             formatQuery("CREATE TABLE %s (k int PRIMARY KEY, v int MASKED WITH mask_missing())"));

        // alter table
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidMessage("Unable to find masking function for v, no declared function matches the signature mask_missing()",
                             "ALTER TABLE %s ALTER v MASKED WITH mask_missing()");

        assertTableColumnsAreNotMasked("k", "v");
    }

    @Test
    public void testInvalidMaskingFunctionArguments() throws Throwable
    {
        // create table
        createTableName();
        assertInvalidMessage("Invalid number of arguments for function system.mask_default(any)",
                             formatQuery("CREATE TABLE %s (k int PRIMARY KEY, v int MASKED WITH mask_default(1))"));

        // alter table
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidMessage("Invalid number of arguments for function system.mask_default(any)",
                             "ALTER TABLE %s ALTER v MASKED WITH mask_default(1)");

        assertTableColumnsAreNotMasked("k", "v");
    }

    @Test
    public void testInvalidMaskingFunctionArgumentTypes() throws Throwable
    {
        // create table
        createTableName();
        assertInvalidMessage("Function system.mask_inner requires an argument of type int, but found argument 'a' of type ascii",
                             formatQuery("CREATE TABLE %s (k int PRIMARY KEY, v text MASKED WITH mask_inner('a', 'b'))"));

        // alter table
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        assertInvalidMessage("Function system.mask_inner requires an argument of type int, but found argument 'a' of type ascii",
                             "ALTER TABLE %s ALTER v MASKED WITH mask_inner('a', 'b')");
        assertTableColumnsAreNotMasked("k", "v");
    }

    @Test
    public void testColumnMaskingWithNotMaskingFunction() throws Throwable
    {
        // create table
        createTableName();
        assertInvalidMessage("Not-masking function tojson() cannot be used for masking table columns",
                             formatQuery("CREATE TABLE %s (k int PRIMARY KEY, v text MASKED WITH tojson())"));

        // alter table
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        assertInvalidMessage("Not-masking function tojson() cannot be used for masking table columns",
                             "ALTER TABLE %s ALTER v MASKED WITH tojson()");
        assertTableColumnsAreNotMasked("k", "v");
    }

    @Test
    public void testPreparedStatement() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text MASKED WITH DEFAULT)");
        execute("INSERT INTO %s (k, v) VALUES (0, 'sensitive')");

        Session session = sessionNet();
        PreparedStatement prepared = session.prepare(formatQuery("SELECT v FROM %s WHERE k = ?"));
        BoundStatement bound = prepared.bind(0);
        assertRowsNet(session.execute(bound), row("****"));

        alterTable("ALTER TABLE %s ALTER v DROP MASKED");
        assertRowsNet(session.execute(bound), row("sensitive"));

        alterTable("ALTER TABLE %s ALTER v MASKED WITH mask_replace('redacted')");
        assertRowsNet(session.execute(bound), row("redacted"));
    }

    @Test
    public void testViews() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text MASKED WITH mask_replace('redacted'), PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 'sensitive')");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                 "WHERE k IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL " +
                                 "PRIMARY KEY (v, k, c)");
        waitForViewMutations();
        assertRowsNet(executeNet(format("SELECT v FROM %s.%s", KEYSPACE, view)), row("redacted"));
        assertRowsNet(executeNet(format("SELECT v FROM %s.%s WHERE v='sensitive'", KEYSPACE, view)), row("redacted"));
        assertRowsNet(executeNet(format("SELECT v FROM %s.%s WHERE v='redacted'", KEYSPACE, view)));

        alterTable("ALTER TABLE %s ALTER v DROP MASKED");
        assertRowsNet(executeNet(format("SELECT v FROM %s.%s", KEYSPACE, view)), row("sensitive"));
        assertRowsNet(executeNet(format("SELECT v FROM %s.%s WHERE v='sensitive'", KEYSPACE, view)), row("sensitive"));
        assertRowsNet(executeNet(format("SELECT v FROM %s.%s WHERE v='redacted'", KEYSPACE, view)));
    }

    @Test
    public void testPreparedStatementOnView() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text MASKED WITH DEFAULT, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 'sensitive')");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                 "WHERE k IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL " +
                                 "PRIMARY KEY (v, k, c)");
        waitForViewMutations();

        Session session = sessionNet();
        PreparedStatement prepared = session.prepare(format("SELECT v FROM %s.%s WHERE v=?", KEYSPACE, view));
        BoundStatement bound = prepared.bind("sensitive");
        assertRowsNet(session.execute(bound), row("****"));

        alterTable("ALTER TABLE %s ALTER v DROP MASKED");
        assertRowsNet(session.execute(bound), row("sensitive"));

        alterTable("ALTER TABLE %s ALTER v MASKED WITH mask_replace('redacted')");
        assertRowsNet(session.execute(bound), row("redacted"));
    }

    @Test
    public void testGroupBy() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 'sensitive')");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 'sensitive')");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 0, 'sensitive')");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 1, 'sensitive')");

        // without masks
        String query = "SELECT * FROM %s GROUP BY k";
        assertRowsNet(executeNet(query), row(1, 0, "sensitive"), row(0, 0, "sensitive"));

        // with masked regular column
        alterTable("ALTER TABLE %s ALTER v MASKED WITH mask_replace('redacted')");
        assertRowsNet(executeNet(query), row(1, 0, "redacted"), row(0, 0, "redacted"));

        // with masked clustering key
        alterTable("ALTER TABLE %s ALTER c MASKED WITH mask_replace(-1)");
        assertRowsNet(executeNet(query), row(1, -1, "redacted"), row(0, -1, "redacted"));

        // with masked partition key
        alterTable("ALTER TABLE %s ALTER k MASKED WITH mask_replace(-1)");
        assertRowsNet(executeNet(query), row(-1, -1, "redacted"), row(-1, -1, "redacted"));

        // again without masks
        alterTable("ALTER TABLE %s ALTER k DROP MASKED");
        alterTable("ALTER TABLE %s ALTER c DROP MASKED");
        alterTable("ALTER TABLE %s ALTER v DROP MASKED");
        assertRowsNet(executeNet(query), row(1, 0, "sensitive"), row(0, 0, "sensitive"));
    }

    @Test
    public void testPaging() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 'sensitive')");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 'sensitive')");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 0, 'sensitive')");

        // without masks
        assertRowsWithPaging("SELECT * FROM %s", row(1, 0, "sensitive"), row(0, 0, "sensitive"), row(0, 1, "sensitive"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 1", row(1, 0, "sensitive"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 0", row(0, 0, "sensitive"), row(0, 1, "sensitive"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 0 AND c = 1", row(0, 1, "sensitive"));

        // with masked regular column
        alterTable("ALTER TABLE %s ALTER v MASKED WITH mask_replace('redacted')");
        assertRowsWithPaging("SELECT * FROM %s", row(1, 0, "redacted"), row(0, 0, "redacted"), row(0, 1, "redacted"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 1", row(1, 0, "redacted"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 0", row(0, 0, "redacted"), row(0, 1, "redacted"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 0 AND c = 1", row(0, 1, "redacted"));

        // with masked clustering key
        alterTable("ALTER TABLE %s ALTER c MASKED WITH mask_replace(-1)");
        assertRowsWithPaging("SELECT * FROM %s", row(1, -1, "redacted"), row(0, -1, "redacted"), row(0, -1, "redacted"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 1", row(1, -1, "redacted"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 0", row(0, -1, "redacted"), row(0, -1, "redacted"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 0 AND c = 1", row(0, -1, "redacted"));

        // with masked partition key
        alterTable("ALTER TABLE %s ALTER k MASKED WITH mask_replace(-1)");
        assertRowsWithPaging("SELECT * FROM %s", row(-1, -1, "redacted"), row(-1, -1, "redacted"), row(-1, -1, "redacted"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 1", row(-1, -1, "redacted"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 0", row(-1, -1, "redacted"), row(-1, -1, "redacted"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 0 AND c = 1", row(-1, -1, "redacted"));

        // again without masks
        alterTable("ALTER TABLE %s ALTER k DROP MASKED");
        alterTable("ALTER TABLE %s ALTER c DROP MASKED");
        alterTable("ALTER TABLE %s ALTER v DROP MASKED");
        assertRowsWithPaging("SELECT * FROM %s", row(1, 0, "sensitive"), row(0, 0, "sensitive"), row(0, 1, "sensitive"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 1", row(1, 0, "sensitive"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 0", row(0, 0, "sensitive"), row(0, 1, "sensitive"));
        assertRowsWithPaging("SELECT * FROM %s WHERE k = 0 AND c = 1", row(0, 1, "sensitive"));
    }

    /**
     * Tests that rows are always ordered according to the clear values of the columns, even for the post-ordering done
     * for queries with {@code IN} restrictions and {@code ORDER BY} clauses.
     */
    @Test
    public void testPostOrdering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 0, 3)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 1, 4)");
        execute("INSERT INTO %s (k, c, v) VALUES (2, 0, 6)");
        execute("INSERT INTO %s (k, c, v) VALUES (2, 1, 7)");
        NativeFunctions.instance.add(NEGATIVE);

        // Test ordering without masking, just for reference
        assertRowsNet(executeNet("SELECT * FROM %s WHERE k IN (0, 1, 2)"),
                      row(0, 0, 0),
                      row(0, 1, 1),
                      row(1, 0, 3),
                      row(1, 1, 4),
                      row(2, 0, 6),
                      row(2, 1, 7));
        assertRowsNet(executeNetWithoutPaging("SELECT * FROM %s WHERE k IN (0, 1, 2) ORDER BY c ASC"),
                      row(0, 0, 0),
                      row(1, 0, 3),
                      row(2, 0, 6),
                      row(0, 1, 1),
                      row(1, 1, 4),
                      row(2, 1, 7));
        assertRowsNet(executeNetWithoutPaging("SELECT * FROM %s WHERE k IN (0, 1, 2) ORDER BY c DESC"),
                      row(0, 1, 1),
                      row(1, 1, 4),
                      row(2, 1, 7),
                      row(0, 0, 0),
                      row(1, 0, 3),
                      row(2, 0, 6));

        // Test ordering with manually applied masking function, just for reference
        assertRowsNet(executeNet("SELECT k, mask_negative(c), v FROM %s WHERE k IN (0, 1, 2)"),
                      row(0, -0, 0), // (0, 0, 0)
                      row(0, -1, 1), // (0, 1, 1)
                      row(1, -0, 3), // (1, 0, 3)
                      row(1, -1, 4), // (1, 1, 4)
                      row(2, -0, 6), // (2, 0, 6)
                      row(2, -1, 7)); // (2, 1, 7)
        assertRowsNet(executeNetWithoutPaging("SELECT k, mask_negative(c), v FROM %s WHERE k IN (0, 1, 2) ORDER BY c ASC"),
                      row(0, -0, 0), // (0, 0, 0)
                      row(1, -0, 3), // (1, 0, 3)
                      row(2, -0, 6), // (2, 0, 6)
                      row(0, -1, 1), // (0, 1, 1)
                      row(1, -1, 4), // (1, 1, 4)
                      row(2, -1, 7)); // (2, 1, 7)
        assertRowsNet(executeNetWithoutPaging("SELECT k, mask_negative(c), v FROM %s WHERE k IN (0, 1, 2) ORDER BY c DESC"),
                      row(0, -1, 1), // (0, 1, 1)
                      row(1, -1, 4), // (1, 1, 4)
                      row(2, -1, 7), // (2, 1, 7)
                      row(0, -0, 0), // (0, 0, 0)
                      row(1, -0, 3), // (1, 0, 3)
                      row(2, -0, 6)); // (2, 0, 6)

        alterTable("ALTER TABLE %s ALTER c MASKED WITH mask_negative()");

        // Test ordering of wildcard queries with masked column
        assertRowsNet(executeNet("SELECT * FROM %s WHERE k IN (0, 1, 2)"),
                      row(0, -0, 0), // (0, 0, 0)
                      row(0, -1, 1), // (0, 1, 1)
                      row(1, -0, 3), // (1, 0, 3)
                      row(1, -1, 4), // (1, 1, 4)
                      row(2, -0, 6), // (2, 0, 6)
                      row(2, -1, 7)); // (2, 1, 7)
        assertRowsNet(executeNetWithoutPaging("SELECT * FROM %s WHERE k IN (0, 1, 2) ORDER BY c ASC"),
                      row(0, -0, 0), // (0, 0, 0)
                      row(1, -0, 3), // (1, 0, 3)
                      row(2, -0, 6), // (2, 0, 6)
                      row(0, -1, 1), // (0, 1, 1)
                      row(1, -1, 4), // (1, 1, 4)
                      row(2, -1, 7)); // (2, 1, 7)
        assertRowsNet(executeNetWithoutPaging("SELECT * FROM %s WHERE k IN (0, 1, 2) ORDER BY c DESC"),
                      row(0, -1, 1), // (0, 1, 1)
                      row(1, -1, 4), // (1, 1, 4)
                      row(2, -1, 7), // (2, 1, 7)
                      row(0, -0, 0), // (0, 0, 0)
                      row(1, -0, 3), // (1, 0, 3)
                      row(2, -0, 6)); // (2, 0, 6)

        // Test ordering of column selection queries with masked column
        assertRowsNet(executeNet("SELECT k, c, v FROM %s WHERE k IN (0, 1, 2)"),
                      row(0, -0, 0), // (0, 0, 0)
                      row(0, -1, 1), // (0, 1, 1)
                      row(1, -0, 3), // (1, 0, 3)
                      row(1, -1, 4), // (1, 1, 4)
                      row(2, -0, 6), // (2, 0, 6)
                      row(2, -1, 7)); // (2, 1, 7)
        assertRowsNet(executeNetWithoutPaging("SELECT k, c, v FROM %s WHERE k IN (0, 1, 2) ORDER BY c ASC"),
                      row(0, -0, 0), // (0, 0, 0)
                      row(1, -0, 3), // (1, 0, 3)
                      row(2, -0, 6), // (2, 0, 6)
                      row(0, -1, 1), // (0, 1, 1)
                      row(1, -1, 4), // (1, 1, 4)
                      row(2, -1, 7)); // (2, 1, 7)
        assertRowsNet(executeNetWithoutPaging("SELECT k, c, v FROM %s WHERE k IN (0, 1, 2) ORDER BY c DESC"),
                      row(0, -1, 1), // (0, 1, 1)
                      row(1, -1, 4), // (1, 1, 4)
                      row(2, -1, 7), // (2, 1, 7)
                      row(0, -0, 0), // (0, 0, 0)
                      row(1, -0, 3), // (1, 0, 3)
                      row(2, -0, 6)); // (2, 0, 6)

        // Test ordering of column selection queries with masked column, without selecting the ordered column
        assertRowsNet(executeNet("SELECT k, v FROM %s WHERE k IN (0, 1, 2)"),
                      row(0, 0), // (0, 0, 0)
                      row(0, 1), // (0, 1, 1)
                      row(1, 3), // (1, 0, 3)
                      row(1, 4), // (1, 1, 4)
                      row(2, 6), // (2, 0, 6)
                      row(2, 7)); // (2, 1, 7)
        assertRowsNet(executeNetWithoutPaging("SELECT k, v FROM %s WHERE k IN (0, 1, 2) ORDER BY c ASC"),
                      row(0, 0), // (0, 0, 0)
                      row(1, 3), // (1, 0, 3)
                      row(2, 6), // (2, 0, 6)
                      row(0, 1), // (0, 1, 1)
                      row(1, 4), // (1, 1, 4)
                      row(2, 7)); // (2, 1, 7)
        assertRowsNet(executeNetWithoutPaging("SELECT k, v FROM %s WHERE k IN (0, 1, 2) ORDER BY c DESC"),
                      row(0, 1), // (0, 1, 1)
                      row(1, 4), // (1, 1, 4)
                      row(2, 7), // (2, 1, 7)
                      row(0, 0), // (0, 0, 0)
                      row(1, 3), // (1, 0, 3)
                      row(2, 6)); // (2, 0, 6)

        // Test ordering of wildcard JSON queries with masked column
        assertRowsNet(executeNet("SELECT JSON * FROM %s WHERE k IN (0, 1, 2)"),
                      row("{\"k\": 0, \"c\": 0, \"v\": 0}"), // (0, 0, 0)
                      row("{\"k\": 0, \"c\": -1, \"v\": 1}"), // (0, 1, 1)
                      row("{\"k\": 1, \"c\": 0, \"v\": 3}"), // (1, 0, 3)
                      row("{\"k\": 1, \"c\": -1, \"v\": 4}"), // (1, 1, 4)
                      row("{\"k\": 2, \"c\": 0, \"v\": 6}"), // (2, 0, 6)
                      row("{\"k\": 2, \"c\": -1, \"v\": 7}")); // (2, 1, 7)
        assertRowsNet(executeNetWithoutPaging("SELECT JSON * FROM %s WHERE k IN (0, 1, 2) ORDER BY c ASC"),
                      row("{\"k\": 0, \"c\": 0, \"v\": 0}"), // (0, 0, 0)
                      row("{\"k\": 1, \"c\": 0, \"v\": 3}"), // (1, 0, 3)
                      row("{\"k\": 2, \"c\": 0, \"v\": 6}"), // (2, 0, 6)
                      row("{\"k\": 0, \"c\": -1, \"v\": 1}"), // (0, 1, 1)
                      row("{\"k\": 1, \"c\": -1, \"v\": 4}"), // (1, 1, 4)
                      row("{\"k\": 2, \"c\": -1, \"v\": 7}")); // (2, 1, 7)
        assertRowsNet(executeNetWithoutPaging("SELECT JSON * FROM %s WHERE k IN (0, 1, 2) ORDER BY c DESC"),
                      row("{\"k\": 0, \"c\": -1, \"v\": 1}"), // (0, 1, 1)
                      row("{\"k\": 1, \"c\": -1, \"v\": 4}"), // (1, 1, 4)
                      row("{\"k\": 2, \"c\": -1, \"v\": 7}"), // (2, 1, 7)
                      row("{\"k\": 0, \"c\": 0, \"v\": 0}"), // (0, 0, 0)
                      row("{\"k\": 1, \"c\": 0, \"v\": 3}"), // (1, 0, 3)
                      row("{\"k\": 2, \"c\": 0, \"v\": 6}")); // (2, 0, 6)
    }

    @Test
    public void testEnableFlag() throws Throwable
    {
        // verify that we cannot create tables with masked columns if DDM is disabled
        DatabaseDescriptor.setDynamicDataMaskingEnabled(false);
        assertInvalidThrowMessage(DISABLED_ERROR_MESSAGE,
                                  InvalidRequestException.class,
                                  "CREATE TABLE t (k int PRIMARY KEY, v text MASKED WITH DEFAULT)");
        assertInvalidThrowMessage(DISABLED_ERROR_MESSAGE,
                                  InvalidRequestException.class,
                                  "CREATE TABLE t (k int MASKED WITH DEFAULT PRIMARY KEY, v text)");
        assertInvalidThrowMessage(DISABLED_ERROR_MESSAGE,
                                  InvalidRequestException.class,
                                  "CREATE TABLE t (k int, c int MASKED WITH DEFAULT, v text, PRIMARY KEY(k, c))");

        // verify that we cannot mask an existing column if DDM is disabled
        createTable("CREATE TABLE %s (k int, c int, s int static, r int, PRIMARY KEY(k, c))");
        assertInvalidThrowMessage(DISABLED_ERROR_MESSAGE,
                                  InvalidRequestException.class,
                                  "ALTER TABLE %s ALTER k MASKED WITH DEFAULT");
        assertInvalidThrowMessage(DISABLED_ERROR_MESSAGE,
                                  InvalidRequestException.class,
                                  "ALTER TABLE %s ALTER c MASKED WITH DEFAULT");
        assertInvalidThrowMessage(DISABLED_ERROR_MESSAGE,
                                  InvalidRequestException.class,
                                  "ALTER TABLE %s ALTER s MASKED WITH DEFAULT");
        assertInvalidThrowMessage(DISABLED_ERROR_MESSAGE,
                                  InvalidRequestException.class,
                                  "ALTER TABLE %s ALTER r MASKED WITH DEFAULT");

        // enable DDM and add some masked data
        DatabaseDescriptor.setDynamicDataMaskingEnabled(true);
        alterTable("ALTER TABLE %s ALTER k MASKED WITH DEFAULT");
        alterTable("ALTER TABLE %s ALTER c MASKED WITH DEFAULT");
        alterTable("ALTER TABLE %s ALTER s MASKED WITH DEFAULT");
        alterTable("ALTER TABLE %s ALTER r MASKED WITH DEFAULT");
        execute("INSERT INTO %s (k, c, s, r) VALUES (1, 2, 3, 4)");
        assertRowsNet(executeNet("SELECT * FROM %s"), row(0, 0, 0, 0));

        // verify that column masks are not applied if DDM is disabled
        DatabaseDescriptor.setDynamicDataMaskingEnabled(false);
        assertRowsNet(executeNet("SELECT * FROM %s"), row(1, 2, 3, 4));

        // verify that we can drop column masks even if DDM is disabled
        alterTable("ALTER TABLE %s ALTER k DROP MASKED");
        alterTable("ALTER TABLE %s ALTER c DROP MASKED");
        alterTable("ALTER TABLE %s ALTER s DROP MASKED");
        alterTable("ALTER TABLE %s ALTER r DROP MASKED");
        DatabaseDescriptor.setDynamicDataMaskingEnabled(true);
        assertRowsNet(executeNet("SELECT * FROM %s"), row(1, 2, 3, 4));
    }

    private void assertRowsWithPaging(String query, Object[]... rows)
    {
        for (int pageSize : Arrays.asList(1, 2, 3, 4, 5, 100))
        {
            assertRowsNet(executeNetWithPaging(query, pageSize), rows);

            for (int limit : Arrays.asList(1, 2, 3, 4, 5, 100))
            {
                assertRowsNet(executeNetWithPaging(query + " LIMIT " + limit, pageSize),
                              Arrays.copyOfRange(rows, 0, Math.min(limit, rows.length)));
            }
        }
    }

    private static final FunctionFactory NEGATIVE = new FunctionFactory("mask_negative", FunctionParameter.fixed(CQL3Type.Native.INT))
    {
        @Override
        protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
        {
            return new MaskingFunction(name, argTypes.get(0), argTypes.get(0))
            {
                @Override
                public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
                {
                    Integer value = arguments.getAsInt(0) * -1;
                    return Int32Type.instance.decompose(value);
                }
            };
        }
    };
}
