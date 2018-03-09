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

import java.math.BigDecimal;
import java.math.BigInteger;
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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.UUIDGen;

public class UFScriptTest extends CQLTester
{
    // Just JavaScript UDFs to check how UDF - especially security/class-loading/sandboxing stuff -
    // behaves, if no Java UDF has been executed before.

    // Do not add any other test here - especially none using Java UDFs

    @Test
    public void testJavascriptSimpleCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, lst list<double>, st set<text>, mp map<int, boolean>)");

        String fName1 = createFunction(KEYSPACE_PER_TEST, "list<double>",
                                       "CREATE FUNCTION %s( lst list<double> ) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS list<double> " +
                                       "LANGUAGE javascript\n" +
                                       "AS 'lst;';");
        String fName2 = createFunction(KEYSPACE_PER_TEST, "set<text>",
                                       "CREATE FUNCTION %s( st set<text> ) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS set<text> " +
                                       "LANGUAGE javascript\n" +
                                       "AS 'st;';");
        String fName3 = createFunction(KEYSPACE_PER_TEST, "map<int, boolean>",
                                       "CREATE FUNCTION %s( mp map<int, boolean> ) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS map<int, boolean> " +
                                       "LANGUAGE javascript\n" +
                                       "AS 'mp;';");

        List<Double> list = Arrays.asList(1d, 2d, 3d);
        Set<String> set = new TreeSet<>(Arrays.asList("one", "three", "two"));
        Map<Integer, Boolean> map = new TreeMap<>();
        map.put(1, true);
        map.put(2, false);
        map.put(3, true);

        execute("INSERT INTO %s (key, lst, st, mp) VALUES (1, ?, ?, ?)", list, set, map);

        assertRows(execute("SELECT lst, st, mp FROM %s WHERE key = 1"),
                   row(list, set, map));

        assertRows(execute("SELECT " + fName1 + "(lst), " + fName2 + "(st), " + fName3 + "(mp) FROM %s WHERE key = 1"),
                   row(list, set, map));

        for (ProtocolVersion version : PROTOCOL_VERSIONS)
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fName1 + "(lst), " + fName2 + "(st), " + fName3 + "(mp) FROM %s WHERE key = 1"),
                          row(list, set, map));
    }

    @Test
    public void testJavascriptTupleType() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, tup frozen<tuple<double, text, int, boolean>>)");

        String fName = createFunction(KEYSPACE_PER_TEST, "tuple<double, text, int, boolean>",
                                      "CREATE FUNCTION %s( tup tuple<double, text, int, boolean> ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS tuple<double, text, int, boolean> " +
                                      "LANGUAGE javascript\n" +
                                      "AS $$tup;$$;");

        Object t = tuple(1d, "foo", 2, true);

        execute("INSERT INTO %s (key, tup) VALUES (1, ?)", t);

        assertRows(execute("SELECT tup FROM %s WHERE key = 1"),
                   row(t));

        assertRows(execute("SELECT " + fName + "(tup) FROM %s WHERE key = 1"),
                   row(t));
    }

    @Test
    public void testJavascriptUserType() throws Throwable
    {
        String type = createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fUdt1 = createFunction(KEYSPACE, type,
                                      "CREATE FUNCTION %s( udt " + type + " ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS " + type + ' ' +
                                      "LANGUAGE javascript\n" +
                                      "AS $$" +
                                      "     udt;$$;");
        String fUdt2 = createFunction(KEYSPACE, type,
                                      "CREATE FUNCTION %s( udt " + type + " ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE javascript\n" +
                                      "AS $$" +
                                      "     udt.getString(\"txt\");$$;");
        String fUdt3 = createFunction(KEYSPACE, type,
                                      "CREATE FUNCTION %s( udt " + type + " ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS int " +
                                      "LANGUAGE javascript\n" +
                                      "AS $$" +
                                      "     udt.getInt(\"i\");$$;");

        execute("INSERT INTO %s (key, udt) VALUES (1, {txt: 'one', i:1})");

        UntypedResultSet rows = execute("SELECT " + fUdt1 + "(udt) FROM %s WHERE key = 1");
        Assert.assertEquals(1, rows.size());
        assertRows(execute("SELECT " + fUdt2 + "(udt) FROM %s WHERE key = 1"),
                   row("one"));
        assertRows(execute("SELECT " + fUdt3 + "(udt) FROM %s WHERE key = 1"),
                   row(1));
    }

    @Test
    public void testJavascriptUTCollections() throws Throwable
    {
        String type = createType("CREATE TYPE %s (txt text, i int)");

        createTable(String.format("CREATE TABLE %%s " +
                                  "(key int primary key, lst list<frozen<%s>>, st set<frozen<%s>>, mp map<int, frozen<%s>>)",
                                  type, type, type));

        String fName = createFunction(KEYSPACE, "list<frozen<" + type + ">>",
                                      "CREATE FUNCTION %s( lst list<frozen<" + type + ">> ) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS text " +
                                      "LANGUAGE javascript\n" +
                                      "AS $$" +
                                      "        lst.get(1).getString(\"txt\");$$;");
        createFunctionOverload(fName, "set<frozen<" + type + ">>",
                               "CREATE FUNCTION %s( st set<frozen<" + type + ">> ) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               "LANGUAGE javascript\n" +
                               "AS $$" +
                               "        st.iterator().next().getString(\"txt\");$$;");
        createFunctionOverload(fName, "map<int, frozen<" + type + ">>",
                               "CREATE FUNCTION %s( mp map<int, frozen<" + type + ">> ) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               "LANGUAGE javascript\n" +
                               "AS $$" +
                               "        mp.get(java.lang.Integer.valueOf(3)).getString(\"txt\");$$;");

        execute("INSERT INTO %s (key, lst, st, mp) values (1, " +
                // list<frozen<UDT>>
                "[ {txt: 'one', i:1}, {txt: 'three', i:1}, {txt: 'one', i:1} ] , " +
                // set<frozen<UDT>>
                "{ {txt: 'one', i:1}, {txt: 'three', i:3}, {txt: 'two', i:2} }, " +
                // map<int, frozen<UDT>>
                "{ 1: {txt: 'one', i:1}, 2: {txt: 'one', i:3}, 3: {txt: 'two', i:2} })");

        assertRows(execute("SELECT " + fName + "(lst) FROM %s WHERE key = 1"),
                   row("three"));
        assertRows(execute("SELECT " + fName + "(st) FROM %s WHERE key = 1"),
                   row("one"));
        assertRows(execute("SELECT " + fName + "(mp) FROM %s WHERE key = 1"),
                   row("two"));

        String cqlSelect = "SELECT " + fName + "(lst), " + fName + "(st), " + fName + "(mp) FROM %s WHERE key = 1";
        assertRows(execute(cqlSelect),
                   row("three", "one", "two"));

        // same test - but via native protocol
        for (ProtocolVersion version : PROTOCOL_VERSIONS)
            assertRowsNet(version,
                          executeNet(version, cqlSelect),
                          row("three", "one", "two"));
    }

    @Test
    public void testJavascriptFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = '\n' +
                              "  Math.sin(val);\n";

        String fName = createFunction(KEYSPACE, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE javascript\n" +
                                      "AS '" + functionBody + "';");

        FunctionName fNameName = parseFunctionName(fName);

        assertRows(execute("SELECT language, body FROM system_schema.functions WHERE keyspace_name=? AND function_name=?",
                           fNameName.keyspace, fNameName.name),
                   row("javascript", functionBody));

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
    public void testJavascriptBadReturnType() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String fName = createFunction(KEYSPACE, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE javascript\n" +
                                      "AS '\"string\";';");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        // throws IRE with ClassCastException
        assertInvalidMessage("Invalid value for CQL type double", "SELECT key, val, " + fName + "(val) FROM %s");
    }

    @Test
    public void testJavascriptThrow() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String fName = createFunction(KEYSPACE, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE javascript\n" +
                                      "AS 'throw \"fool\";';");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        // throws IRE with ScriptException
        assertInvalidThrowMessage("fool", FunctionExecutionException.class,
                                  "SELECT key, val, " + fName + "(val) FROM %s");
    }

    @Test
    public void testScriptReturnTypeCasting() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);

        Object[][] variations = {
        new Object[]    {   "true",     "boolean",  true    },
        new Object[]    {   "false",    "boolean",  false   },
        new Object[]    {   "100",      "tinyint",  (byte)100 },
        new Object[]    {   "100.",     "tinyint",  (byte)100 },
        new Object[]    {   "100",      "smallint", (short)100 },
        new Object[]    {   "100.",     "smallint", (short)100 },
        new Object[]    {   "100",      "int",      100     },
        new Object[]    {   "100.",     "int",      100     },
        new Object[]    {   "100",      "double",   100d    },
        new Object[]    {   "100.",     "double",   100d    },
        new Object[]    {   "100",      "bigint",   100L    },
        new Object[]    {   "100.",     "bigint",   100L    },
        new Object[]    { "100", "varint", BigInteger.valueOf(100L)    },
        new Object[]    {   "100.",     "varint",   BigInteger.valueOf(100L)    },
        new Object[]    { "parseInt(\"100\");", "decimal", BigDecimal.valueOf(100d)    },
        new Object[]    {   "100.",     "decimal",  BigDecimal.valueOf(100d)    },
        };

        for (Object[] variation : variations)
        {
            Object functionBody = variation[0];
            Object returnType = variation[1];
            Object expectedResult = variation[2];

            String fName = createFunction(KEYSPACE, "double",
                                          "CREATE OR REPLACE FUNCTION %s(val double) " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS " +returnType + ' ' +
                                          "LANGUAGE javascript " +
                                          "AS '" + functionBody + ";';");
            assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                       row(1, 1d, expectedResult));
        }
    }

    @Test
    public void testScriptParamReturnTypes() throws Throwable
    {
        UUID ruuid = UUID.randomUUID();
        UUID tuuid = UUIDGen.getTimeUUID();

        createTable("CREATE TABLE %s (key int primary key, " +
                    "tival tinyint, sival smallint, ival int, lval bigint, fval float, dval double, vval varint, ddval decimal, " +
                    "timval time, dtval date, tsval timestamp, uval uuid, tuval timeuuid)");
        execute("INSERT INTO %s (key, tival, sival, ival, lval, fval, dval, vval, ddval, timval, dtval, tsval, uval, tuval) VALUES " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 1,
                (byte)1, (short)1, 1, 1L, 1f, 1d, BigInteger.valueOf(1L), BigDecimal.valueOf(1d), 1L, Integer.MAX_VALUE, new Date(1), ruuid, tuuid);

        Object[][] variations = {
        new Object[] {  "tinyint",  "tival",    (byte)1,                (byte)2  },
        new Object[] {  "smallint", "sival",    (short)1,               (short)2  },
        new Object[] {  "int",      "ival",     1,                      2  },
        new Object[] {  "bigint",   "lval",     1L,                     2L  },
        new Object[] {  "float",    "fval",     1f,                     2f  },
        new Object[] {  "double",   "dval",     1d,                     2d  },
        new Object[] {  "varint",   "vval",     BigInteger.valueOf(1L), BigInteger.valueOf(2L)  },
        new Object[] {  "decimal",  "ddval",    BigDecimal.valueOf(1d), BigDecimal.valueOf(2d)  },
        new Object[] {  "time",     "timval",   1L,                     2L  },
        };

        for (Object[] variation : variations)
        {
            Object type = variation[0];
            Object col = variation[1];
            Object expected1 = variation[2];
            Object expected2 = variation[3];
            String fName = createFunction(KEYSPACE, type.toString(),
                                          "CREATE OR REPLACE FUNCTION %s(val " + type + ") " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS " + type + ' ' +
                                          "LANGUAGE javascript " +
                                          "AS 'val+1;';");
            assertRows(execute("SELECT key, " + col + ", " + fName + '(' + col + ") FROM %s"),
                       row(1, expected1, expected2));
        }

        variations = new Object[][] {
        new Object[] {  "timestamp","tsval",    new Date(1),            new Date(1)  },
        new Object[] {  "uuid",     "uval",     ruuid,                  ruuid  },
        new Object[] {  "timeuuid", "tuval",    tuuid,                  tuuid  },
        new Object[] {  "date",     "dtval",    Integer.MAX_VALUE,      Integer.MAX_VALUE },
        };

        for (Object[] variation : variations)
        {
            Object type = variation[0];
            Object col = variation[1];
            Object expected1 = variation[2];
            Object expected2 = variation[3];
            String fName = createFunction(KEYSPACE, type.toString(),
                                          "CREATE OR REPLACE FUNCTION %s(val " + type + ") " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS " + type + ' ' +
                                          "LANGUAGE javascript " +
                                          "AS 'val;';");
            assertRows(execute("SELECT key, " + col + ", " + fName + '(' + col + ") FROM %s"),
                       row(1, expected1, expected2));
        }
    }

    @Test
    public void testJavascriptDisabled() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        DatabaseDescriptor.enableScriptedUserDefinedFunctions(false);
        try
        {
            assertInvalid("CREATE OR REPLACE FUNCTION " + KEYSPACE + ".assertNotEnabled(val double) " +
                          "RETURNS NULL ON NULL INPUT " +
                          "RETURNS double " +
                          "LANGUAGE javascript\n" +
                          "AS 'Math.sin(val);';");
        }
        finally
        {
            DatabaseDescriptor.enableScriptedUserDefinedFunctions(true);
        }
    }

    @Test
    public void testJavascriptCompileFailure() throws Throwable
    {
        assertInvalidMessage("Failed to compile function 'cql_test_keyspace.scrinv'",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".scrinv(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE javascript\n" +
                             "AS 'foo bar';");
    }

    @Test
    public void testScriptInvalidLanguage() throws Throwable
    {
        assertInvalidMessage("Invalid language 'artificial_intelligence' for function 'cql_test_keyspace.scrinv'",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".scrinv(val double) " +
                             "RETURNS NULL ON NULL INPUT " +
                             "RETURNS double " +
                             "LANGUAGE artificial_intelligence\n" +
                             "AS 'question for 42?';");
    }
}
