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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;

public class UFJavaTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setPartitioner(ByteOrderedPartitioner.instance);
    }


    @Test
    public void testJavaFunctionNoParameters() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = "\n  return 1L;\n";

        String fName = createFunction(KEYSPACE, "",
                                      "CREATE OR REPLACE FUNCTION %s() " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS bigint " +
                                      "LANGUAGE JAVA\n" +
                                      "AS '" +functionBody + "';");

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE keyspace_name=? AND function_name=?",
                           KEYSPACE, parseFunctionName(fName).name),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, " + fName + "() FROM %s"),
                   row(1, 1d, 1L),
                   row(2, 2d, 1L),
                   row(3, 3d, 1L)
        );
    }

    @Test
    public void testJavaFunctionInvalidBodies() throws Throwable
    {
        try
        {
            execute("CREATE OR REPLACE FUNCTION " + KEYSPACE + ".jfinv() " +
                    "RETURNS NULL ON NULL INPUT " +
                    "RETURNS bigint " +
                    "LANGUAGE JAVA\n" +
                    "AS '\n" +
                    "foobarbaz" +
                    "\n';");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Java source compilation failed"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("insert \";\" to complete BlockStatements"));
        }

        try
        {
            execute("CREATE OR REPLACE FUNCTION " + KEYSPACE + ".jfinv() " +
                    "RETURNS NULL ON NULL INPUT " +
                    "RETURNS bigint " +
                    "LANGUAGE JAVA\n" +
                    "AS '\n" +
                    "foobarbaz;" +
                    "\n';");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Java source compilation failed"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("foobarbaz cannot be resolved to a type"));
        }
    }

    @Test
    public void testJavaFunctionInvalidReturn() throws Throwable
    {
        assertInvalidMessage("system keyspace is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION jfir(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE JAVA\n" +
                             "AS 'return 1L;';");
    }

    @Test
    public void testJavaFunctionArgumentTypeMismatch() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val bigint)");

        String fName = createFunction(KEYSPACE, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double)" +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE JAVA " +
                                      "AS 'return Double.valueOf(val);';");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1L);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2L);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3L);
        assertInvalidMessage("val cannot be passed as argument 0 of function",
                             "SELECT key, val, " + fName + "(val) FROM %s");
    }

    @Test
    public void testJavaFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = '\n' +
                              "  // parameter val is of type java.lang.Double\n" +
                              "  /* return type is of type java.lang.Double */\n" +
                              "  if (val == null) {\n" +
                              "    return null;\n" +
                              "  }\n" +
                              "  return Math.sin(val);\n";

        String fName = createFunction(KEYSPACE, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE JAVA " +
                                      "AS '" + functionBody + "';");

        FunctionName fNameName = parseFunctionName(fName);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE keyspace_name=? AND function_name=?",
                           fNameName.keyspace, fNameName.name),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );
    }

    @Test
    public void testJavaFunctionCounter() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val counter)");

        String fName = createFunction(KEYSPACE, "counter",
                                      "CREATE OR REPLACE FUNCTION %s(val counter) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS bigint " +
                                      "LANGUAGE JAVA " +
                                      "AS 'return val + 1;';");

        execute("UPDATE %s SET val = val + 1 WHERE key = 1");
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                   row(1, 1L, 2L));
        execute("UPDATE %s SET val = val + 1 WHERE key = 1");
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                   row(1, 2L, 3L));
        execute("UPDATE %s SET val = val + 2 WHERE key = 1");
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                   row(1, 4L, 5L));
        execute("UPDATE %s SET val = val - 2 WHERE key = 1");
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                   row(1, 2L, 3L));
    }

    @Test
    public void testJavaKeyspaceFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = '\n' +
                              "  // parameter val is of type java.lang.Double\n" +
                              "  /* return type is of type java.lang.Double */\n" +
                              "  if (val == null) {\n" +
                              "    return null;\n" +
                              "  }\n" +
                              "  return Math.sin( val );\n";

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE JAVA " +
                                      "AS '" + functionBody + "';");

        FunctionName fNameName = parseFunctionName(fName);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE keyspace_name=? AND function_name=?",
                           fNameName.keyspace, fNameName.name),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );
    }

    @Test
    public void testJavaRuntimeException() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = '\n' +
                              "  throw new RuntimeException(\"oh no!\");\n";

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE JAVA\n" +
                                      "AS '" + functionBody + "';");

        FunctionName fNameName = parseFunctionName(fName);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE keyspace_name=? AND function_name=?",
                           fNameName.keyspace, fNameName.name),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);

        // function throws a RuntimeException which is wrapped by FunctionExecutionException
        assertInvalidThrowMessage("java.lang.RuntimeException: oh no", FunctionExecutionException.class,
                                  "SELECT key, val, " + fName + "(val) FROM %s");
    }

    @Test
    public void testJavaDollarQuotedFunction() throws Throwable
    {
        String functionBody = '\n' +
                              "  // parameter val is of type java.lang.Double\n" +
                              "  /* return type is of type java.lang.Double */\n" +
                              "  if (input == null) {\n" +
                              "    return null;\n" +
                              "  }\n" +
                              "  return \"'\"+Math.sin(input)+'\\\'';\n";

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE FUNCTION %s( input double ) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE java\n" +
                                      "AS $$" + functionBody + "$$;");

        FunctionName fNameName = parseFunctionName(fName);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE keyspace_name=? AND function_name=?",
                           fNameName.keyspace, fNameName.name),
                   row("java", functionBody));
    }

    @Test
    public void testJavaSimpleCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, lst list<double>, st set<text>, mp map<int, boolean>)");

        String fList = createFunction(KEYSPACE_PER_TEST, "list<double>",
                                      "CREATE FUNCTION %s( lst list<double> ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS list<double> " +
                                      "LANGUAGE java\n" +
                                      "AS $$return lst;$$;");
        String fSet = createFunction(KEYSPACE_PER_TEST, "set<text>",
                                     "CREATE FUNCTION %s( st set<text> ) " +
                                     "RETURNS NULL ON NULL INPUT " +
                                     "RETURNS set<text> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return st;$$;");
        String fMap = createFunction(KEYSPACE_PER_TEST, "map<int, boolean>",
                                     "CREATE FUNCTION %s( mp map<int, boolean> ) " +
                                     "RETURNS NULL ON NULL INPUT " +
                                     "RETURNS map<int, boolean> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return mp;$$;");

        List<Double> list = Arrays.asList(1d, 2d, 3d);
        Set<String> set = new TreeSet<>(Arrays.asList("one", "three", "two"));
        Map<Integer, Boolean> map = new TreeMap<>();
        map.put(1, true);
        map.put(2, false);
        map.put(3, true);

        execute("INSERT INTO %s (key, lst, st, mp) VALUES (1, ?, ?, ?)", list, set, map);

        assertRows(execute("SELECT " + fList + "(lst), " + fSet + "(st), " + fMap + "(mp) FROM %s WHERE key = 1"),
                   row(list, set, map));

        // same test - but via native protocol
        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fList + "(lst), " + fSet + "(st), " + fMap + "(mp) FROM %s WHERE key = 1"),
                          row(list, set, map));
    }

    @Test
    public void testJavaTupleType() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, tup frozen<tuple<double, text, int, boolean>>)");

        String fName = createFunction(KEYSPACE, "tuple<double, text, int, boolean>",
                                      "CREATE FUNCTION %s( tup tuple<double, text, int, boolean> ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS tuple<double, text, int, boolean> " +
                                      "LANGUAGE java\n" +
                                      "AS $$return tup;$$;");

        Object t = tuple(1d, "foo", 2, true);

        execute("INSERT INTO %s (key, tup) VALUES (1, ?)", t);

        assertRows(execute("SELECT tup FROM %s WHERE key = 1"),
                   row(t));

        assertRows(execute("SELECT " + fName + "(tup) FROM %s WHERE key = 1"),
                   row(t));
    }

    @Test
    public void testJavaTupleTypeCollection() throws Throwable
    {
        String tupleTypeDef = "tuple<double, list<double>, set<text>, map<int, boolean>>";

        createTable("CREATE TABLE %s (key int primary key, tup frozen<" + tupleTypeDef + ">)");

        String fTup0 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                                      "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS " + tupleTypeDef + ' ' +
                                      "LANGUAGE java\n" +
                                      "AS $$return " +
                                      "       tup;$$;");
        String fTup1 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                                      "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE java\n" +
                                      "AS $$return " +
                                      "       Double.valueOf(tup.getDouble(0));$$;");
        String fTup2 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                                      "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS list<double> " +
                                      "LANGUAGE java\n" +
                                      "AS $$return " +
                                      "       tup.getList(1, Double.class);$$;");
        String fTup3 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                                      "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS set<text> " +
                                      "LANGUAGE java\n" +
                                      "AS $$return " +
                                      "       tup.getSet(2, String.class);$$;");
        String fTup4 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                                      "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS map<int, boolean> " +
                                      "LANGUAGE java\n" +
                                      "AS $$return " +
                                      "       tup.getMap(3, Integer.class, Boolean.class);$$;");

        List<Double> list = Arrays.asList(1d, 2d, 3d);
        Set<String> set = new TreeSet<>(Arrays.asList("one", "three", "two"));
        Map<Integer, Boolean> map = new TreeMap<>();
        map.put(1, true);
        map.put(2, false);
        map.put(3, true);

        Object t = tuple(1d, list, set, map);

        execute("INSERT INTO %s (key, tup) VALUES (1, ?)", t);

        assertRows(execute("SELECT " + fTup0 + "(tup) FROM %s WHERE key = 1"),
                   row(t));
        assertRows(execute("SELECT " + fTup1 + "(tup) FROM %s WHERE key = 1"),
                   row(1d));
        assertRows(execute("SELECT " + fTup2 + "(tup) FROM %s WHERE key = 1"),
                   row(list));
        assertRows(execute("SELECT " + fTup3 + "(tup) FROM %s WHERE key = 1"),
                   row(set));
        assertRows(execute("SELECT " + fTup4 + "(tup) FROM %s WHERE key = 1"),
                   row(map));

        TupleType tType = TupleType.of(DataType.cdouble(),
                                       DataType.list(DataType.cdouble()),
                                       DataType.set(DataType.text()),
                                       DataType.map(DataType.cint(), DataType.cboolean()));
        TupleValue tup = tType.newValue(1d, list, set, map);
        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
        {
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup0 + "(tup) FROM %s WHERE key = 1"),
                          row(tup));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup1 + "(tup) FROM %s WHERE key = 1"),
                          row(1d));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup2 + "(tup) FROM %s WHERE key = 1"),
                          row(list));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup3 + "(tup) FROM %s WHERE key = 1"),
                          row(set));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup4 + "(tup) FROM %s WHERE key = 1"),
                          row(map));
        }
    }

    @Test
    public void testJavaUserTypeWithUse() throws Throwable
    {
        String type = createType("CREATE TYPE %s (txt text, i int)");
        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + KEYSPACE + '.' + type + ">)");
        execute("INSERT INTO %s (key, udt) VALUES (1, {txt: 'one', i:1})");

        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
        {
            executeNet(version, "USE " + KEYSPACE);

            executeNet(version,
                       "CREATE FUNCTION f_use1( udt " + type + " ) " +
                       "RETURNS NULL ON NULL INPUT " +
                       "RETURNS " + type + " " +
                       "LANGUAGE java " +
                       "AS $$return " +
                       "     udt;$$;");
            try
            {
                List<Row> rowsNet = executeNet(version, "SELECT f_use1(udt) FROM %s WHERE key = 1").all();
                Assert.assertEquals(1, rowsNet.size());
                UDTValue udtVal = rowsNet.get(0).getUDTValue(0);
                Assert.assertEquals("one", udtVal.getString("txt"));
                Assert.assertEquals(1, udtVal.getInt("i"));
            }
            finally
            {
                executeNet(version, "DROP FUNCTION f_use1");
            }
        }
    }

    @Test
    public void testJavaUserType() throws Throwable
    {
        String type = KEYSPACE + '.' + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fUdt0 = createFunction(KEYSPACE, type,
                                      "CREATE FUNCTION %s( udt " + type + " ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS " + type + " " +
                                      "LANGUAGE java " +
                                      "AS $$return " +
                                      "     udt;$$;");
        String fUdt1 = createFunction(KEYSPACE, type,
                                      "CREATE FUNCTION %s( udt " + type + ") " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE java " +
                                      "AS $$return " +
                                      "     udt.getString(\"txt\");$$;");
        String fUdt2 = createFunction(KEYSPACE, type,
                                      "CREATE FUNCTION %s( udt " + type + ") " +
                                      "CALLED ON NULL INPUT " +
                                      "RETURNS int " +
                                      "LANGUAGE java " +
                                      "AS $$return " +
                                      "     Integer.valueOf(udt.getInt(\"i\"));$$;");

        execute("INSERT INTO %s (key, udt) VALUES (1, {txt: 'one', i:1})");

        UntypedResultSet rows = execute("SELECT " + fUdt0 + "(udt) FROM %s WHERE key = 1");
        Assert.assertEquals(1, rows.size());
        assertRows(execute("SELECT " + fUdt1 + "(udt) FROM %s WHERE key = 1"),
                   row("one"));
        assertRows(execute("SELECT " + fUdt2 + "(udt) FROM %s WHERE key = 1"),
                   row(1));

        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
        {
            List<Row> rowsNet = executeNet(version, "SELECT " + fUdt0 + "(udt) FROM %s WHERE key = 1").all();
            Assert.assertEquals(1, rowsNet.size());
            UDTValue udtVal = rowsNet.get(0).getUDTValue(0);
            Assert.assertEquals("one", udtVal.getString("txt"));
            Assert.assertEquals(1, udtVal.getInt("i"));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fUdt1 + "(udt) FROM %s WHERE key = 1"),
                          row("one"));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fUdt2 + "(udt) FROM %s WHERE key = 1"),
                          row(1));
        }
    }

    @Test
    public void testJavaUserTypeRenameField() throws Throwable
    {
        String type = KEYSPACE + '.' + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fName = createFunction(KEYSPACE, type,
                                      "CREATE FUNCTION %s( udt " + type + " ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE java\n" +
                                      "AS $$return udt.getString(\"txt\");$$;");

        execute("INSERT INTO %s (key, udt) VALUES (1, {txt: 'one', i:1})");

        assertRows(execute("SELECT " + fName + "(udt) FROM %s WHERE key = 1"),
                   row("one"));

        execute("ALTER TYPE " + type + " RENAME txt TO str");

        assertInvalidMessage("txt is not a field defined in this UDT",
                             "SELECT " + fName + "(udt) FROM %s WHERE key = 1");

        execute("ALTER TYPE " + type + " RENAME str TO txt");

        assertRows(execute("SELECT " + fName + "(udt) FROM %s WHERE key = 1"),
                   row("one"));
    }

    @Test
    public void testJavaUserTypeAddFieldWithReplace() throws Throwable
    {
        String type = KEYSPACE + '.' + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fName1replace = createFunction(KEYSPACE, type,
                                              "CREATE FUNCTION %s( udt " + type + ") " +
                                              "RETURNS NULL ON NULL INPUT " +
                                              "RETURNS text " +
                                              "LANGUAGE java\n" +
                                              "AS $$return udt.getString(\"txt\");$$;");
        String fName2replace = createFunction(KEYSPACE, type,
                                              "CREATE FUNCTION %s( udt " + type + " ) " +
                                              "CALLED ON NULL INPUT " +
                                              "RETURNS int " +
                                              "LANGUAGE java\n" +
                                              "AS $$return Integer.valueOf(udt.getInt(\"i\"));$$;");
        String fName3replace = createFunction(KEYSPACE, type,
                                              "CREATE FUNCTION %s( udt " + type + " ) " +
                                              "CALLED ON NULL INPUT " +
                                              "RETURNS double " +
                                              "LANGUAGE java\n" +
                                              "AS $$return Double.valueOf(udt.getDouble(\"added\"));$$;");
        String fName4replace = createFunction(KEYSPACE, type,
                                              "CREATE FUNCTION %s( udt " + type + " ) " +
                                              "RETURNS NULL ON NULL INPUT " +
                                              "RETURNS " + type + " " +
                                              "LANGUAGE java\n" +
                                              "AS $$return udt;$$;");

        String fName1noReplace = createFunction(KEYSPACE, type,
                                                "CREATE FUNCTION %s( udt " + type + " ) " +
                                                "RETURNS NULL ON NULL INPUT " +
                                                "RETURNS text " +
                                                "LANGUAGE java\n" +
                                                "AS $$return udt.getString(\"txt\");$$;");
        String fName2noReplace = createFunction(KEYSPACE, type,
                                                "CREATE FUNCTION %s( udt " + type + " ) " +
                                                "CALLED ON NULL INPUT " +
                                                "RETURNS int " +
                                                "LANGUAGE java\n" +
                                                "AS $$return Integer.valueOf(udt.getInt(\"i\"));$$;");
        String fName3noReplace = createFunction(KEYSPACE, type,
                                                "CREATE FUNCTION %s( udt " + type + " ) " +
                                                "CALLED ON NULL INPUT " +
                                                "RETURNS double " +
                                                "LANGUAGE java\n" +
                                                "AS $$return Double.valueOf(udt.getDouble(\"added\"));$$;");
        String fName4noReplace = createFunction(KEYSPACE, type,
                                                "CREATE FUNCTION %s( udt " + type + " ) " +
                                                "RETURNS NULL ON NULL INPUT " +
                                                "RETURNS " + type + " " +
                                                "LANGUAGE java\n" +
                                                "AS $$return udt;$$;");

        execute("INSERT INTO %s (key, udt) VALUES (1, {txt: 'one', i:1})");

        assertRows(execute("SELECT " + fName1replace + "(udt) FROM %s WHERE key = 1"),
                   row("one"));
        assertRows(execute("SELECT " + fName2replace + "(udt) FROM %s WHERE key = 1"),
                   row(1));

        // add field

        execute("ALTER TYPE " + type + " ADD added double");

        execute("INSERT INTO %s (key, udt) VALUES (2, {txt: 'two', i:2, added: 2})");

        // note: type references of functions remain at the state _before_ the type mutation
        // means we need to recreate the functions

        execute(String.format("CREATE OR REPLACE FUNCTION %s( udt %s ) " +
                              "RETURNS NULL ON NULL INPUT " +
                              "RETURNS text " +
                              "LANGUAGE java\n" +
                              "AS $$return " +
                              "     udt.getString(\"txt\");$$;",
                              fName1replace, type));
        Assert.assertEquals(1, Functions.find(parseFunctionName(fName1replace)).size());
        execute(String.format("CREATE OR REPLACE FUNCTION %s( udt %s ) " +
                              "CALLED ON NULL INPUT " +
                              "RETURNS int " +
                              "LANGUAGE java\n" +
                              "AS $$return " +
                              "     Integer.valueOf(udt.getInt(\"i\"));$$;",
                              fName2replace, type));
        Assert.assertEquals(1, Functions.find(parseFunctionName(fName2replace)).size());
        execute(String.format("CREATE OR REPLACE FUNCTION %s( udt %s ) " +
                              "CALLED ON NULL INPUT " +
                              "RETURNS double " +
                              "LANGUAGE java\n" +
                              "AS $$return " +
                              "     Double.valueOf(udt.getDouble(\"added\"));$$;",
                              fName3replace, type));
        Assert.assertEquals(1, Functions.find(parseFunctionName(fName3replace)).size());
        execute(String.format("CREATE OR REPLACE FUNCTION %s( udt %s ) " +
                              "RETURNS NULL ON NULL INPUT " +
                              "RETURNS %s " +
                              "LANGUAGE java\n" +
                              "AS $$return " +
                              "     udt;$$;",
                              fName4replace, type, type));
        Assert.assertEquals(1, Functions.find(parseFunctionName(fName4replace)).size());

        assertRows(execute("SELECT " + fName1replace + "(udt) FROM %s WHERE key = 2"),
                   row("two"));
        assertRows(execute("SELECT " + fName2replace + "(udt) FROM %s WHERE key = 2"),
                   row(2));
        assertRows(execute("SELECT " + fName3replace + "(udt) FROM %s WHERE key = 2"),
                   row(2d));
        assertRows(execute("SELECT " + fName3replace + "(udt) FROM %s WHERE key = 1"),
                   row(0d));

        // un-replaced functions will work since the user type has changed
        // and the UDF has exchanged the user type reference

        assertRows(execute("SELECT " + fName1noReplace + "(udt) FROM %s WHERE key = 2"),
                   row("two"));
        assertRows(execute("SELECT " + fName2noReplace + "(udt) FROM %s WHERE key = 2"),
                   row(2));
        assertRows(execute("SELECT " + fName3noReplace + "(udt) FROM %s WHERE key = 2"),
                   row(2d));
        assertRows(execute("SELECT " + fName3noReplace + "(udt) FROM %s WHERE key = 1"),
                   row(0d));

        execute("DROP FUNCTION " + fName1replace);
        execute("DROP FUNCTION " + fName2replace);
        execute("DROP FUNCTION " + fName3replace);
        execute("DROP FUNCTION " + fName4replace);
        execute("DROP FUNCTION " + fName1noReplace);
        execute("DROP FUNCTION " + fName2noReplace);
        execute("DROP FUNCTION " + fName3noReplace);
        execute("DROP FUNCTION " + fName4noReplace);
    }

    @Test
    public void testJavaUTCollections() throws Throwable
    {
        String type = KEYSPACE + '.' + createType("CREATE TYPE %s (txt text, i int)");

        createTable(String.format("CREATE TABLE %%s " +
                                  "(key int primary key, lst list<frozen<%s>>, st set<frozen<%s>>, mp map<int, frozen<%s>>)",
                                  type, type, type));

        String fName1 = createFunction(KEYSPACE, "list<frozen<" + type + ">>",
                                       "CREATE FUNCTION %s( lst list<frozen<" + type + ">> ) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java\n" +
                                       "AS $$" +
                                       "     com.datastax.driver.core.UDTValue udtVal = (com.datastax.driver.core.UDTValue)lst.get(1);" +
                                       "     return udtVal.getString(\"txt\");$$;");
        String fName2 = createFunction(KEYSPACE, "set<frozen<" + type + ">>",
                                       "CREATE FUNCTION %s( st set<frozen<" + type + ">> ) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java\n" +
                                       "AS $$" +
                                       "     com.datastax.driver.core.UDTValue udtVal = (com.datastax.driver.core.UDTValue)st.iterator().next();" +
                                       "     return udtVal.getString(\"txt\");$$;");
        String fName3 = createFunction(KEYSPACE, "map<int, frozen<" + type + ">>",
                                       "CREATE FUNCTION %s( mp map<int, frozen<" + type + ">> ) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java\n" +
                                       "AS $$" +
                                       "     com.datastax.driver.core.UDTValue udtVal = (com.datastax.driver.core.UDTValue)mp.get(Integer.valueOf(3));" +
                                       "     return udtVal.getString(\"txt\");$$;");

        execute("INSERT INTO %s (key, lst, st, mp) values (1, " +
                "[ {txt: 'one', i:1}, {txt: 'three', i:1}, {txt: 'one', i:1} ] , " +
                "{ {txt: 'one', i:1}, {txt: 'three', i:3}, {txt: 'two', i:2} }, " +
                "{ 1: {txt: 'one', i:1}, 2: {txt: 'one', i:3}, 3: {txt: 'two', i:2} })");

        assertRows(execute("SELECT " + fName1 + "(lst), " + fName2 + "(st), " + fName3 + "(mp) FROM %s WHERE key = 1"),
                   row("three", "one", "two"));

        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fName1 + "(lst), " + fName2 + "(st), " + fName3 + "(mp) FROM %s WHERE key = 1"),
                          row("three", "one", "two"));
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

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s"),
                   row(0, 0),
                   row(1, 6),
                   row(2, 15),
                   row(3, 24));

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

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s"),
                   row(0, 0),
                   row(1, 6),
                   row(2, 15),
                   row(3, 24));

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

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s"),
                   row(0, 0),
                   row(1, 6),
                   row(2, 15),
                   row(3, 24));

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

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s"),
                   row(0, "(null, null)"),
                   row(1, "(1, 2)"),
                   row(2, "(4, 5)"),
                   row(3, "(7, 8)"));

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

        assertRows(execute("SELECT a, " + functionName + "(b) FROM %s"),
                   row(0, "{f:0}"),
                   row(1, "{f:1}"),
                   row(2, "{f:4}"),
                   row(3, "{f:7}"));

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

    @Test
    public void testEmptyString() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, sval text, aval ascii, bval blob, empty_int int)");
        execute("INSERT INTO %s (key, sval, aval, bval, empty_int) VALUES (?, ?, ?, ?, blobAsInt(0x))", 1, "", "", ByteBuffer.allocate(0));

        String fNameSRC = createFunction(KEYSPACE_PER_TEST, "text",
                                         "CREATE OR REPLACE FUNCTION %s(val text) " +
                                         "CALLED ON NULL INPUT " +
                                         "RETURNS text " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return val;'");

        String fNameSCC = createFunction(KEYSPACE_PER_TEST, "text",
                                         "CREATE OR REPLACE FUNCTION %s(val text) " +
                                         "CALLED ON NULL INPUT " +
                                         "RETURNS text " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return \"\";'");

        String fNameSRN = createFunction(KEYSPACE_PER_TEST, "text",
                                         "CREATE OR REPLACE FUNCTION %s(val text) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS text " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return val;'");

        String fNameSCN = createFunction(KEYSPACE_PER_TEST, "text",
                                         "CREATE OR REPLACE FUNCTION %s(val text) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS text " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return \"\";'");

        String fNameBRC = createFunction(KEYSPACE_PER_TEST, "blob",
                                         "CREATE OR REPLACE FUNCTION %s(val blob) " +
                                         "CALLED ON NULL INPUT " +
                                         "RETURNS blob " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return val;'");

        String fNameBCC = createFunction(KEYSPACE_PER_TEST, "blob",
                                         "CREATE OR REPLACE FUNCTION %s(val blob) " +
                                         "CALLED ON NULL INPUT " +
                                         "RETURNS blob " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return ByteBuffer.allocate(0);'");

        String fNameBRN = createFunction(KEYSPACE_PER_TEST, "blob",
                                         "CREATE OR REPLACE FUNCTION %s(val blob) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS blob " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return val;'");

        String fNameBCN = createFunction(KEYSPACE_PER_TEST, "blob",
                                         "CREATE OR REPLACE FUNCTION %s(val blob) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS blob " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return ByteBuffer.allocate(0);'");

        String fNameIRC = createFunction(KEYSPACE_PER_TEST, "int",
                                         "CREATE OR REPLACE FUNCTION %s(val int) " +
                                         "CALLED ON NULL INPUT " +
                                         "RETURNS int " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return val;'");

        String fNameICC = createFunction(KEYSPACE_PER_TEST, "int",
                                         "CREATE OR REPLACE FUNCTION %s(val int) " +
                                         "CALLED ON NULL INPUT " +
                                         "RETURNS int " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return 0;'");

        String fNameIRN = createFunction(KEYSPACE_PER_TEST, "int",
                                         "CREATE OR REPLACE FUNCTION %s(val int) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS int " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return val;'");

        String fNameICN = createFunction(KEYSPACE_PER_TEST, "int",
                                         "CREATE OR REPLACE FUNCTION %s(val int) " +
                                         "RETURNS NULL ON NULL INPUT " +
                                         "RETURNS int " +
                                         "LANGUAGE JAVA\n" +
                                         "AS 'return 0;'");

        assertRows(execute("SELECT " + fNameSRC + "(sval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSRN + "(sval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSCC + "(sval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSCN + "(sval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSRC + "(aval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSRN + "(aval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSCC + "(aval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameSCN + "(aval) FROM %s"), row(""));
        assertRows(execute("SELECT " + fNameBRC + "(bval) FROM %s"), row(ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertRows(execute("SELECT " + fNameBRN + "(bval) FROM %s"), row(ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertRows(execute("SELECT " + fNameBCC + "(bval) FROM %s"), row(ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertRows(execute("SELECT " + fNameBCN + "(bval) FROM %s"), row(ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertRows(execute("SELECT " + fNameIRC + "(empty_int) FROM %s"), row(new Object[]{null}));
        assertRows(execute("SELECT " + fNameIRN + "(empty_int) FROM %s"), row(new Object[]{null}));
        assertRows(execute("SELECT " + fNameICC + "(empty_int) FROM %s"), row(0));
        assertRows(execute("SELECT " + fNameICN + "(empty_int) FROM %s"), row(new Object[]{null}));
    }

    @Test
    public void testAllNativeTypes() throws Throwable
    {
        StringBuilder sig = new StringBuilder();
        StringBuilder args = new StringBuilder();
        for (CQL3Type.Native type : CQL3Type.Native.values())
        {
            if (sig.length() > 0)
                sig.append(',');
            sig.append(type.toString());

            if (args.length() > 0)
                args.append(',');
            args.append("arg").append(type.toString()).append(' ').append(type.toString());
        }
        createFunction(KEYSPACE, sig.toString(),
                       "CREATE OR REPLACE FUNCTION %s(" + args + ") " +
                       "RETURNS NULL ON NULL INPUT " +
                       "RETURNS int " +
                       "LANGUAGE JAVA\n" +
                       "AS 'return 0;'");

        for (CQL3Type.Native type : CQL3Type.Native.values())
        {
            createFunction(KEYSPACE_PER_TEST, type.toString(),
                           "CREATE OR REPLACE FUNCTION %s(val " + type.toString() + ") " +
                           "RETURNS NULL ON NULL INPUT " +
                           "RETURNS int " +
                           "LANGUAGE JAVA\n" +
                           "AS 'return 0;'");
        }
    }
}
