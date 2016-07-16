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

package org.apache.cassandra.cql3.selection;

import java.math.BigDecimal;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.junit.Assert.assertEquals;

public class TermSelectionTest extends CQLTester
{
    // Helper method for testSelectLiteral()
    private void assertConstantResult(UntypedResultSet result, Object constant)
    {
        assertRows(result,
                   row(1, "one", constant),
                   row(2, "two", constant),
                   row(3, "three", constant));
    }

    @Test
    public void testSelectLiteral() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, t text, PRIMARY KEY (pk, ck) )");
        execute("INSERT INTO %s (pk, ck, t) VALUES (1, 1, 'one')");
        execute("INSERT INTO %s (pk, ck, t) VALUES (1, 2, 'two')");
        execute("INSERT INTO %s (pk, ck, t) VALUES (1, 3, 'three')");

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, 'a const' FROM %s");
        assertConstantResult(execute("SELECT ck, t, (text)'a const' FROM %s"), "a const");

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, 42 FROM %s");
        assertConstantResult(execute("SELECT ck, t, (int)42 FROM %s"), 42);

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, (1, 'foo') FROM %s");
        assertConstantResult(execute("SELECT ck, t, (tuple<int, text>)(1, 'foo') FROM %s"), tuple(1, "foo"));

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, [1, 2, 3] FROM %s");
        assertConstantResult(execute("SELECT ck, t, (list<int>)[1, 2, 3] FROM %s"), list(1, 2, 3));

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, {1, 2, 3} FROM %s");
        assertConstantResult(execute("SELECT ck, t, (set<int>){1, 2, 3} FROM %s"), set(1, 2, 3));

        assertInvalidMessage("Cannot infer type for term", "SELECT ck, t, {1: 'foo', 2: 'bar', 3: 'baz'} FROM %s");
        assertConstantResult(execute("SELECT ck, t, (map<int, text>){1: 'foo', 2: 'bar', 3: 'baz'} FROM %s"), map(1, "foo", 2, "bar", 3, "baz"));

        assertColumnNames(execute("SELECT ck, t, (int)42, (int)43 FROM %s"), "ck", "t", "(int)42", "(int)43");
        assertRows(execute("SELECT ck, t, (int) 42, (int) 43 FROM %s"),
                   row(1, "one", 42, 43),
                   row(2, "two", 42, 43),
                   row(3, "three", 42, 43));
    }

    @Test
    public void testSelectUDTLiteral() throws Throwable
    {
        String type = createType("CREATE TYPE %s(a int, b text)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + type + ")");

        execute("INSERT INTO %s(k, v) VALUES (?, ?)", 0, userType("a", 3, "b", "foo"));

        assertInvalidMessage("Cannot infer type for term", "SELECT k, v, { a: 4, b: 'bar'} FROM %s");

        assertRows(execute("SELECT k, v, (" + type + "){ a: 4, b: 'bar'} FROM %s"),
            row(0, userType("a", 3, "b", "foo"), userType("a", 4, "b", "bar"))
        );
    }

    @Test
    public void testInvalidSelect() throws Throwable
    {
        // Creates a table just so we can reference it in the (invalid) SELECT below
        createTable("CREATE TABLE %s (k int PRIMARY KEY)");

        assertInvalidMessage("Cannot infer type for term", "SELECT ? FROM %s");
        assertInvalidMessage("Cannot infer type for term", "SELECT k, ? FROM %s");

        assertInvalidMessage("Cannot infer type for term", "SELECT k, null FROM %s");
    }

    private void assertColumnSpec(ColumnSpecification spec, String expectedName, AbstractType<?> expectedType)
    {
        assertEquals(expectedName, spec.name.toString());
        assertEquals(expectedType, spec.type);
    }

    @Test
    public void testSelectPrepared() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, t text, PRIMARY KEY (pk, ck) )");
        execute("INSERT INTO %s (pk, ck, t) VALUES (1, 1, 'one')");
        execute("INSERT INTO %s (pk, ck, t) VALUES (1, 2, 'two')");
        execute("INSERT INTO %s (pk, ck, t) VALUES (1, 3, 'three')");

        String query = "SELECT (int)?, (decimal):adecimal, (text)?, (tuple<int,text>):atuple, pk, ck, t FROM %s WHERE pk = ?";
        ResultMessage.Prepared prepared = prepare(query);

        List<ColumnSpecification> boundNames = prepared.metadata.names;

        // 5 bound variables
        assertEquals(5, boundNames.size());
        assertColumnSpec(boundNames.get(0), "[selection]", Int32Type.instance);
        assertColumnSpec(boundNames.get(1), "adecimal", DecimalType.instance);
        assertColumnSpec(boundNames.get(2), "[selection]", UTF8Type.instance);
        assertColumnSpec(boundNames.get(3), "atuple", TypeParser.parse("TupleType(Int32Type,UTF8Type)"));
        assertColumnSpec(boundNames.get(4), "pk", Int32Type.instance);


        List<ColumnSpecification> resultNames = prepared.resultMetadata.names;

        // 7 result "columns"
        assertEquals(7, resultNames.size());
        assertColumnSpec(resultNames.get(0), "(int)?", Int32Type.instance);
        assertColumnSpec(resultNames.get(1), "(decimal)?", DecimalType.instance);
        assertColumnSpec(resultNames.get(2), "(text)?", UTF8Type.instance);
        assertColumnSpec(resultNames.get(3), "(tuple<int, text>)?", TypeParser.parse("TupleType(Int32Type,UTF8Type)"));
        assertColumnSpec(resultNames.get(4), "pk", Int32Type.instance);
        assertColumnSpec(resultNames.get(5), "ck", Int32Type.instance);
        assertColumnSpec(resultNames.get(6), "t", UTF8Type.instance);

        assertRows(execute(query, 88, BigDecimal.TEN, "foo bar baz", tuple(42, "ursus"), 1),
                   row(88, BigDecimal.TEN, "foo bar baz", tuple(42, "ursus"),
                       1, 1, "one"),
                   row(88, BigDecimal.TEN, "foo bar baz", tuple(42, "ursus"),
                       1, 2, "two"),
                   row(88, BigDecimal.TEN, "foo bar baz", tuple(42, "ursus"),
                       1, 3, "three"));
    }

    @Test
    public void testConstantFunctionArgs() throws Throwable
    {
        String fInt = createFunction(KEYSPACE,
                                     "int,int",
                                     "CREATE FUNCTION %s (val1 int, val2 int) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS int " +
                                     "LANGUAGE java\n" +
                                     "AS 'return Math.max(val1, val2);';");
        String fFloat = createFunction(KEYSPACE,
                                       "float,float",
                                       "CREATE FUNCTION %s (val1 float, val2 float) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS float " +
                                       "LANGUAGE java\n" +
                                       "AS 'return Math.max(val1, val2);';");
        String fText = createFunction(KEYSPACE,
                                      "text,text",
                                      "CREATE FUNCTION %s (val1 text, val2 text) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE java\n" +
                                      "AS 'return val2;';");
        String fAscii = createFunction(KEYSPACE,
                                       "ascii,ascii",
                                       "CREATE FUNCTION %s (val1 ascii, val2 ascii) " +
                                       "CALLED ON NULL INPUT " +
                                       "RETURNS ascii " +
                                       "LANGUAGE java\n" +
                                       "AS 'return val2;';");
        String fTimeuuid = createFunction(KEYSPACE,
                                          "timeuuid,timeuuid",
                                          "CREATE FUNCTION %s (val1 timeuuid, val2 timeuuid) " +
                                          "CALLED ON NULL INPUT " +
                                          "RETURNS timeuuid " +
                                          "LANGUAGE java\n" +
                                          "AS 'return val2;';");

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, valInt int, valFloat float, valText text, valAscii ascii, valTimeuuid timeuuid)");
        execute("INSERT INTO %s (pk, valInt, valFloat, valText, valAscii, valTimeuuid) " +
                "VALUES (1, 10, 10.0, '100', '100', 2deb23e0-96b5-11e5-b26d-a939dd1405a3)");

        assertRows(execute("SELECT pk, " + fInt + "(valInt, 100) FROM %s"),
                   row(1, 100));
        assertRows(execute("SELECT pk, " + fInt + "(valInt, (int)100) FROM %s"),
                   row(1, 100));
        assertInvalidMessage("Type error: (bigint)100 cannot be passed as argument 1 of function",
                             "SELECT pk, " + fInt + "(valInt, (bigint)100) FROM %s");
        assertRows(execute("SELECT pk, " + fFloat + "(valFloat, (float)100.00) FROM %s"),
                   row(1, 100f));
        assertRows(execute("SELECT pk, " + fText + "(valText, 'foo') FROM %s"),
                   row(1, "foo"));
        assertRows(execute("SELECT pk, " + fAscii + "(valAscii, (ascii)'foo') FROM %s"),
                   row(1, "foo"));
        assertRows(execute("SELECT pk, " + fTimeuuid + "(valTimeuuid, (timeuuid)34617f80-96b5-11e5-b26d-a939dd1405a3) FROM %s"),
                   row(1, UUID.fromString("34617f80-96b5-11e5-b26d-a939dd1405a3")));

        // ambiguous

        String fAmbiguousFunc1 = createFunction(KEYSPACE,
                                                "int,bigint",
                                                "CREATE FUNCTION %s (val1 int, val2 bigint) " +
                                                "CALLED ON NULL INPUT " +
                                                "RETURNS bigint " +
                                                "LANGUAGE java\n" +
                                                "AS 'return Math.max((long)val1, val2);';");
        assertRows(execute("SELECT pk, " + fAmbiguousFunc1 + "(valInt, 100) FROM %s"),
                   row(1, 100L));
        createFunctionOverload(fAmbiguousFunc1, "int,int",
                                                "CREATE FUNCTION %s (val1 int, val2 int) " +
                                                "CALLED ON NULL INPUT " +
                                                "RETURNS bigint " +
                                                "LANGUAGE java\n" +
                                                "AS 'return (long)Math.max(val1, val2);';");
        assertInvalidMessage("Ambiguous call to function cql_test_keyspace.function_",
                             "SELECT pk, " + fAmbiguousFunc1 + "(valInt, 100) FROM %s");
    }

    @Test
    public void testPreparedFunctionArgs() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, t text, i int, PRIMARY KEY (pk, ck) )");
        execute("INSERT INTO %s (pk, ck, t, i) VALUES (1, 1, 'one', 50)");
        execute("INSERT INTO %s (pk, ck, t, i) VALUES (1, 2, 'two', 100)");
        execute("INSERT INTO %s (pk, ck, t, i) VALUES (1, 3, 'three', 150)");

        String fIntMax = createFunction(KEYSPACE,
                                        "int,int",
                                        "CREATE FUNCTION %s (val1 int, val2 int) " +
                                        "CALLED ON NULL INPUT " +
                                        "RETURNS int " +
                                        "LANGUAGE java\n" +
                                        "AS 'return Math.max(val1, val2);';");

        // weak typing

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s", 0),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s", 100),
                   row(1, 1, 100),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s", 200),
                   row(1, 1, 200),
                   row(1, 2, 200),
                   row(1, 3, 200));

        // explicit typing

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s", 0),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s", 100),
                   row(1, 1, 100),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s", 200),
                   row(1, 1, 200),
                   row(1, 2, 200),
                   row(1, 3, 200));

        // weak typing

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s WHERE pk = " + fIntMax + "(1,1)", 0),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s WHERE pk = " + fIntMax + "(2,1)", 0));

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s WHERE pk = " + fIntMax + "(?,1)", 0, 1),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, ?) FROM %s WHERE pk = " + fIntMax + "(?,1)", 0, 2));

        // explicit typing

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)1,(int)1)", 0),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)2,(int)1)", 0));

        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)?,(int)1)", 0, 1),
                   row(1, 1, 50),
                   row(1, 2, 100),
                   row(1, 3, 150));
        assertRows(execute("SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)?,(int)1)", 0, 2));

        assertInvalidMessage("Invalid unset value for argument", "SELECT pk, ck, " + fIntMax + "(i, (int)?) FROM %s WHERE pk = " + fIntMax + "((int)1,(int)1)", unset());
    }

    @Test
    public void testInsertUpdateDelete() throws Throwable
    {
        String fIntMax = createFunction(KEYSPACE,
                                        "int,int",
                                        "CREATE FUNCTION %s (val1 int, val2 int) " +
                                        "CALLED ON NULL INPUT " +
                                        "RETURNS int " +
                                        "LANGUAGE java\n" +
                                        "AS 'return Math.max(val1, val2);';");

        createTable("CREATE TABLE %s (pk int, ck int, t text, i int, PRIMARY KEY (pk, ck) )");

        execute("UPDATE %s SET i = " + fIntMax + "(100, 200) WHERE pk = 1 AND ck = 1");
        assertRows(execute("SELECT i FROM %s WHERE pk = 1 AND ck = 1"),
                   row(200));

        execute("UPDATE %s SET i = " + fIntMax + "(100, 300) WHERE pk = 1 AND ck = " + fIntMax + "(1,2)");
        assertRows(execute("SELECT i FROM %s WHERE pk = 1 AND ck = 2"),
                   row(300));

        execute("DELETE FROM %s WHERE pk = 1 AND ck = " + fIntMax + "(1,2)");
        assertRows(execute("SELECT i FROM %s WHERE pk = 1 AND ck = 2"));

        execute("INSERT INTO %s (pk, ck, i) VALUES (1, " + fIntMax + "(1,2), " + fIntMax + "(100, 300))");
        assertRows(execute("SELECT i FROM %s WHERE pk = 1 AND ck = 2"),
                   row(300));
    }
}
