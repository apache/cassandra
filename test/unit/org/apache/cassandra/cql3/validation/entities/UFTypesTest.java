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

package org.apache.cassandra.cql3.validation.entities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.UUIDGen;

public class UFTypesTest extends CQLTester
{

    @Test
    public void testComplexNullValues() throws Throwable
    {
        String type = KEYSPACE + '.' + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, lst list<double>, st set<text>, mp map<int, boolean>," +
                    "tup frozen<tuple<double, text, int, boolean>>, udt frozen<" + type + ">)");

        String fList = createFunction(KEYSPACE, "list<double>",
                                      "CREATE FUNCTION %s( coll list<double> ) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS list<double> " +
                                      "LANGUAGE java\n" +
                                      "AS $$return coll;$$;");
        String fSet = createFunction(KEYSPACE, "set<text>",
                                     "CREATE FUNCTION %s( coll set<text> ) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS set<text> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return coll;$$;");
        String fMap = createFunction(KEYSPACE, "map<int, boolean>",
                                     "CREATE FUNCTION %s( coll map<int, boolean> ) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS map<int, boolean> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return coll;$$;");
        String fTup = createFunction(KEYSPACE, "tuple<double, text, int, boolean>",
                                     "CREATE FUNCTION %s( val tuple<double, text, int, boolean> ) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS tuple<double, text, int, boolean> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return val;$$;");
        String fUdt = createFunction(KEYSPACE, type,
                                     "CREATE FUNCTION %s( val " + type + " ) " +
                                     "CALLED ON NULL INPUT " +
                                     "RETURNS " + type + " " +
                                     "LANGUAGE java\n" +
                                     "AS $$return val;$$;");
        List<Double> list = Arrays.asList(1d, 2d, 3d);
        Set<String> set = new TreeSet<>(Arrays.asList("one", "three", "two"));
        Map<Integer, Boolean> map = new TreeMap<>();
        map.put(1, true);
        map.put(2, false);
        map.put(3, true);
        Object t = tuple(1d, "one", 42, false);

        execute("INSERT INTO %s (key, lst, st, mp, tup, udt) VALUES (1, ?, ?, ?, ?, {txt: 'one', i:1})", list, set, map, t);
        execute("INSERT INTO %s (key, lst, st, mp, tup, udt) VALUES (2, ?, ?, ?, ?, null)", null, null, null, null);

        execute("SELECT " +
                fList + "(lst), " +
                fSet + "(st), " +
                fMap + "(mp), " +
                fTup + "(tup), " +
                fUdt + "(udt) FROM %s WHERE key = 1");
        UntypedResultSet.Row row = execute("SELECT " +
                                           fList + "(lst) as l, " +
                                           fSet + "(st) as s, " +
                                           fMap + "(mp) as m, " +
                                           fTup + "(tup) as t, " +
                                           fUdt + "(udt) as u " +
                                           "FROM %s WHERE key = 1").one();
        Assert.assertNotNull(row.getBytes("l"));
        Assert.assertNotNull(row.getBytes("s"));
        Assert.assertNotNull(row.getBytes("m"));
        Assert.assertNotNull(row.getBytes("t"));
        Assert.assertNotNull(row.getBytes("u"));
        row = execute("SELECT " +
                      fList + "(lst) as l, " +
                      fSet + "(st) as s, " +
                      fMap + "(mp) as m, " +
                      fTup + "(tup) as t, " +
                      fUdt + "(udt) as u " +
                      "FROM %s WHERE key = 2").one();
        Assert.assertNull(row.getBytes("l"));
        Assert.assertNull(row.getBytes("s"));
        Assert.assertNull(row.getBytes("m"));
        Assert.assertNull(row.getBytes("t"));
        Assert.assertNull(row.getBytes("u"));

        for (ProtocolVersion version : PROTOCOL_VERSIONS)
        {
            Row r = executeNet(version, "SELECT " +
                                        fList + "(lst) as l, " +
                                        fSet + "(st) as s, " +
                                        fMap + "(mp) as m, " +
                                        fTup + "(tup) as t, " +
                                        fUdt + "(udt) as u " +
                                        "FROM %s WHERE key = 1").one();
            Assert.assertNotNull(r.getBytesUnsafe("l"));
            Assert.assertNotNull(r.getBytesUnsafe("s"));
            Assert.assertNotNull(r.getBytesUnsafe("m"));
            Assert.assertNotNull(r.getBytesUnsafe("t"));
            Assert.assertNotNull(r.getBytesUnsafe("u"));
            r = executeNet(version, "SELECT " +
                                    fList + "(lst) as l, " +
                                    fSet + "(st) as s, " +
                                    fMap + "(mp) as m, " +
                                    fTup + "(tup) as t, " +
                                    fUdt + "(udt) as u " +
                                    "FROM %s WHERE key = 2").one();
            Assert.assertNull(r.getBytesUnsafe("l"));
            Assert.assertNull(r.getBytesUnsafe("s"));
            Assert.assertNull(r.getBytesUnsafe("m"));
            Assert.assertNull(r.getBytesUnsafe("t"));
            Assert.assertNull(r.getBytesUnsafe("u"));
        }
    }

    private static class TypesTestDef
    {
        final String udfType;
        final String tableType;
        final String columnName;
        final Object referenceValue;

        String fCheckArgAndReturn;

        String fCalledOnNull;
        String fReturnsNullOnNull;

        TypesTestDef(String udfType, String tableType, String columnName, Object referenceValue)
        {
            this.udfType = udfType;
            this.tableType = tableType;
            this.columnName = columnName;
            this.referenceValue = referenceValue;
        }
    }

    @Test
    public void testTypesWithAndWithoutNulls() throws Throwable
    {
        // test various combinations of types against UDFs with CALLED ON NULL or RETURNS NULL ON NULL

        String type = createType("CREATE TYPE %s (txt text, i int)");

        TypesTestDef[] typeDefs =
        {
        //                udf type,            table type,                 column, reference value
        new TypesTestDef("timestamp", "timestamp", "ts", new Date()),
        new TypesTestDef("date", "date", "dt", 12345),
        new TypesTestDef("time", "time", "tim", 12345L),
        new TypesTestDef("uuid", "uuid", "uu", UUID.randomUUID()),
        new TypesTestDef("timeuuid", "timeuuid", "tu", UUIDGen.getTimeUUID()),
        new TypesTestDef("tinyint", "tinyint", "ti", (byte) 42),
        new TypesTestDef("smallint", "smallint", "si", (short) 43),
        new TypesTestDef("int", "int", "i", 44),
        new TypesTestDef("bigint", "bigint", "b", 45L),
        new TypesTestDef("float", "float", "f", 46f),
        new TypesTestDef("double", "double", "d", 47d),
        new TypesTestDef("boolean", "boolean", "x", true),
        new TypesTestDef("ascii", "ascii", "a", "tqbfjutld"),
        new TypesTestDef("text", "text", "t", "k\u00f6lsche jung"),
        //new TypesTestDef(type,                 "frozen<" + type + '>',     "u",    null),
        new TypesTestDef("tuple<int, text>", "frozen<tuple<int, text>>", "tup", tuple(1, "foo"))
        };

        String createTableDDL = "CREATE TABLE %s (key int PRIMARY KEY";
        String insertDML = "INSERT INTO %s (key";
        List<Object> values = new ArrayList<>();
        for (TypesTestDef typeDef : typeDefs)
        {
            createTableDDL += ", " + typeDef.columnName + ' ' + typeDef.tableType;
            insertDML += ", " + typeDef.columnName;
            String typeName = typeDef.udfType;
            typeDef.fCheckArgAndReturn = createFunction(KEYSPACE,
                                                        typeName,
                                                        "CREATE OR REPLACE FUNCTION %s(val " + typeName + ") " +
                                                        "CALLED ON NULL INPUT " +
                                                        "RETURNS " + typeName + ' ' +
                                                        "LANGUAGE java\n" +
                                                        "AS 'return val;';");
            typeDef.fCalledOnNull = createFunction(KEYSPACE,
                                                   typeName,
                                                   "CREATE OR REPLACE FUNCTION %s(val " + typeName + ") " +
                                                   "CALLED ON NULL INPUT " +
                                                   "RETURNS text " +
                                                   "LANGUAGE java\n" +
                                                   "AS 'return \"called\";';");
            typeDef.fReturnsNullOnNull = createFunction(KEYSPACE,
                                                        typeName,
                                                        "CREATE OR REPLACE FUNCTION %s(val " + typeName + ") " +
                                                        "RETURNS NULL ON NULL INPUT " +
                                                        "RETURNS text " +
                                                        "LANGUAGE java\n" +
                                                        "AS 'return \"called\";';");
            values.add(typeDef.referenceValue);
        }

        createTableDDL += ')';
        createTable(createTableDDL);

        insertDML += ") VALUES (1";
        for (TypesTestDef ignored : typeDefs)
            insertDML += ", ?";
        insertDML += ')';

        execute(insertDML, values.toArray());

        // second row with null values
        for (int i = 0; i < values.size(); i++)
            values.set(i, null);
        execute(insertDML.replace('1', '2'), values.toArray());

        // check argument input + return
        for (TypesTestDef typeDef : typeDefs)
        {
            assertRows(execute("SELECT " + typeDef.fCheckArgAndReturn + '(' + typeDef.columnName + ") FROM %s WHERE key = 1"),
                       row(new Object[]{ typeDef.referenceValue }));
        }

        // check for CALLED ON NULL INPUT with non-null arguments
        for (TypesTestDef typeDef : typeDefs)
        {
            assertRows(execute("SELECT " + typeDef.fCalledOnNull + '(' + typeDef.columnName + ") FROM %s WHERE key = 1"),
                       row(new Object[]{ "called" }));
        }

        // check for CALLED ON NULL INPUT with null arguments
        for (TypesTestDef typeDef : typeDefs)
        {
            assertRows(execute("SELECT " + typeDef.fCalledOnNull + '(' + typeDef.columnName + ") FROM %s WHERE key = 2"),
                       row(new Object[]{ "called" }));
        }

        // check for RETURNS NULL ON NULL INPUT with non-null arguments
        for (TypesTestDef typeDef : typeDefs)
        {
            assertRows(execute("SELECT " + typeDef.fReturnsNullOnNull + '(' + typeDef.columnName + ") FROM %s WHERE key = 1"),
                       row(new Object[]{ "called" }));
        }

        // check for RETURNS NULL ON NULL INPUT with null arguments
        for (TypesTestDef typeDef : typeDefs)
        {
            assertRows(execute("SELECT " + typeDef.fReturnsNullOnNull + '(' + typeDef.columnName + ") FROM %s WHERE key = 2"),
                       row(new Object[]{ null }));
        }

    }

    @Test
    public void testFunctionWithFrozenSetType() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<set<int>>)");
        createIndex("CREATE INDEX ON %s (FULL(b))");

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 0, set());
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, set(1, 2, 3));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, set(4, 5, 6));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 3, set(7, 8, 9));

        assertInvalidMessage("The function arguments should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".frozenSetArg(values frozen<set<int>>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS int " +
                             "LANGUAGE java\n" +
                             "AS 'int sum = 0; for (Object value : values) {sum += value;} return sum;';");

        assertInvalidMessage("The function return type should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".frozenReturnType(values set<int>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS frozen<set<int>> " +
                             "LANGUAGE java\n" +
                             "AS 'return values;';");

        String functionName = createFunction(KEYSPACE,
                                             "set<int>",
                                             "CREATE FUNCTION %s (values set<int>) " +
                                             "CALLED ON NULL INPUT " +
                                             "RETURNS int " +
                                             "LANGUAGE java\n" +
                                             "AS 'int sum = 0; for (Object value : values) {sum += ((Integer) value);} return sum;';");

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 0"), row(0, 0));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 1"), row(1, 6));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 2"), row(2, 15));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 3"), row(3, 24));

        functionName = createFunction(KEYSPACE,
                                      "set<int>",
                                      "CREATE FUNCTION %s (values set<int>) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS set<int> " +
                                      "LANGUAGE java\n" +
                                      "AS 'return values;';");

        assertRows(execute("SELECT a FROM %s WHERE b = " + functionName + "(?)", set(1, 2, 3)),
                   row(1));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP FUNCTION " + functionName + "(frozen<set<int>>);");
    }

    @Test
    public void testFunctionWithFrozenListType() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<list<int>>)");
        createIndex("CREATE INDEX ON %s (FULL(b))");

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 0, list());
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, list(1, 2, 3));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, list(4, 5, 6));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 3, list(7, 8, 9));

        assertInvalidMessage("The function arguments should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".withFrozenArg(values frozen<list<int>>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS int " +
                             "LANGUAGE java\n" +
                             "AS 'int sum = 0; for (Object value : values) {sum += value;} return sum;';");

        assertInvalidMessage("The function return type should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".frozenReturnType(values list<int>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS frozen<list<int>> " +
                             "LANGUAGE java\n" +
                             "AS 'return values;';");

        String functionName = createFunction(KEYSPACE,
                                             "list<int>",
                                             "CREATE FUNCTION %s (values list<int>) " +
                                             "CALLED ON NULL INPUT " +
                                             "RETURNS int " +
                                             "LANGUAGE java\n" +
                                             "AS 'int sum = 0; for (Object value : values) {sum += ((Integer) value);} return sum;';");

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 0"), row(0, 0));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 1"), row(1, 6));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 2"), row(2, 15));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 3"), row(3, 24));

        functionName = createFunction(KEYSPACE,
                                      "list<int>",
                                      "CREATE FUNCTION %s (values list<int>) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS list<int> " +
                                      "LANGUAGE java\n" +
                                      "AS 'return values;';");

        assertRows(execute("SELECT a FROM %s WHERE b = " + functionName + "(?)", set(1, 2, 3)),
                   row(1));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP FUNCTION " + functionName + "(frozen<list<int>>);");
    }

    @Test
    public void testFunctionWithFrozenMapType() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<map<int, int>>)");
        createIndex("CREATE INDEX ON %s (FULL(b))");

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 0, map());
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, map(1, 1, 2, 2, 3, 3));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, map(4, 4, 5, 5, 6, 6));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 3, map(7, 7, 8, 8, 9, 9));

        assertInvalidMessage("The function arguments should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".withFrozenArg(values frozen<map<int, int>>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS int " +
                             "LANGUAGE java\n" +
                             "AS 'int sum = 0; for (Object value : values.values()) {sum += value;} return sum;';");

        assertInvalidMessage("The function return type should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".frozenReturnType(values map<int, int>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS frozen<map<int, int>> " +
                             "LANGUAGE java\n" +
                             "AS 'return values;';");

        String functionName = createFunction(KEYSPACE,
                                             "map<int, int>",
                                             "CREATE FUNCTION %s (values map<int, int>) " +
                                             "CALLED ON NULL INPUT " +
                                             "RETURNS int " +
                                             "LANGUAGE java\n" +
                                             "AS 'int sum = 0; for (Object value : values.values()) {sum += ((Integer) value);} return sum;';");

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 0"), row(0, 0));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 1"), row(1, 6));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 2"), row(2, 15));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 3"), row(3, 24));

        functionName = createFunction(KEYSPACE,
                                      "map<int, int>",
                                      "CREATE FUNCTION %s (values map<int, int>) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS map<int, int> " +
                                      "LANGUAGE java\n" +
                                      "AS 'return values;';");

        assertRows(execute("SELECT a FROM %s WHERE b = " + functionName + "(?)", map(1, 1, 2, 2, 3, 3)),
                   row(1));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP FUNCTION " + functionName + "(frozen<map<int, int>>);");
    }

    @Test
    public void testFunctionWithFrozenTupleType() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<tuple<int, int>>)");
        createIndex("CREATE INDEX ON %s (b)");

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 0, tuple());
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, tuple(1, 2));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, tuple(4, 5));
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 3, tuple(7, 8));

        assertInvalidMessage("The function arguments should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".withFrozenArg(values frozen<tuple<int, int>>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS text " +
                             "LANGUAGE java\n" +
                             "AS 'return values.toString();';");

        assertInvalidMessage("The function return type should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".frozenReturnType(values tuple<int, int>) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS frozen<tuple<int, int>> " +
                             "LANGUAGE java\n" +
                             "AS 'return values;';");

        String functionName = createFunction(KEYSPACE,
                                             "tuple<int, int>",
                                             "CREATE FUNCTION %s (values tuple<int, int>) " +
                                             "CALLED ON NULL INPUT " +
                                             "RETURNS text " +
                                             "LANGUAGE java\n" +
                                             "AS 'return values.toString();';");

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 0"), row(0, "(NULL,NULL)"));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 1"), row(1, "(1,2)"));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 2"), row(2, "(4,5)"));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 3"), row(3, "(7,8)"));

        functionName = createFunction(KEYSPACE,
                                      "tuple<int, int>",
                                      "CREATE FUNCTION %s (values tuple<int, int>) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS tuple<int, int> " +
                                      "LANGUAGE java\n" +
                                      "AS 'return values;';");

        assertRows(execute("SELECT a FROM %s WHERE b = " + functionName + "(?)", tuple(1, 2)),
                   row(1));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP FUNCTION " + functionName + "(frozen<tuple<int, int>>);");
    }

    @Test
    public void testFunctionWithFrozenUDType() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (f int)");
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<" + myType + ">)");
        createIndex("CREATE INDEX ON %s (b)");

        execute("INSERT INTO %s (a, b) VALUES (?, {f : ?})", 0, 0);
        execute("INSERT INTO %s (a, b) VALUES (?, {f : ?})", 1, 1);
        execute("INSERT INTO %s (a, b) VALUES (?, {f : ?})", 2, 4);
        execute("INSERT INTO %s (a, b) VALUES (?, {f : ?})", 3, 7);

        assertInvalidMessage("The function arguments should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".withFrozenArg(values frozen<" + myType + ">) " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS text " +
                             "LANGUAGE java\n" +
                             "AS 'return values.toString();';");

        assertInvalidMessage("The function return type should not be frozen",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".frozenReturnType(values " + myType + ") " +
                             "CALLED ON NULL INPUT " +
                             "RETURNS frozen<" + myType + "> " +
                             "LANGUAGE java\n" +
                             "AS 'return values;';");

        String functionName = createFunction(KEYSPACE,
                                             myType,
                                             "CREATE FUNCTION %s (values " + myType + ") " +
                                             "CALLED ON NULL INPUT " +
                                             "RETURNS text " +
                                             "LANGUAGE java\n" +
                                             "AS 'return values.toString();';");

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 0"), row(0, "{f:0}"));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 1"), row(1, "{f:1}"));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 2"), row(2, "{f:4}"));
        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s WHERE a = 3"), row(3, "{f:7}"));

        functionName = createFunction(KEYSPACE,
                                      myType,
                                      "CREATE FUNCTION %s (values " + myType + ") " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS " + myType + " " +
                                      "LANGUAGE java\n" +
                                      "AS 'return values;';");

        assertRows(execute("SELECT a FROM %s WHERE b = " + functionName + "({f: ?})", 1),
                   row(1));

        assertInvalidMessage("The function arguments should not be frozen",
                             "DROP FUNCTION " + functionName + "(frozen<" + myType + ">);");
    }
}
