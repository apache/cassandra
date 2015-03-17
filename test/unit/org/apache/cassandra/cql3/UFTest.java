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
package org.apache.cassandra.cql3;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.*;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.messages.ResultMessage;

public class UFTest extends CQLTester
{

    @Test
    public void testSchemaChange() throws Throwable
    {
        String f = createFunction(KEYSPACE,
                                  "double, double",
                                  "CREATE OR REPLACE FUNCTION %s(state double, val double) " +
                                  "RETURNS double " +
                                  "LANGUAGE javascript " +
                                  "AS '\"string\";';");

        assertLastSchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.FUNCTION,
                               KEYSPACE, parseFunctionName(f).name,
                               "double", "double");

        createFunctionOverload(f,
                               "double, double",
                               "CREATE OR REPLACE FUNCTION %s(state int, val int) " +
                               "RETURNS int " +
                               "LANGUAGE javascript " +
                               "AS '\"string\";';");

        assertLastSchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.FUNCTION,
                               KEYSPACE, parseFunctionName(f).name,
                               "int", "int");

        schemaChange("CREATE OR REPLACE FUNCTION " + f + "(state int, val int) " +
                     "RETURNS int " +
                     "LANGUAGE javascript " +
                     "AS '\"string\";';");

        assertLastSchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.FUNCTION,
                               KEYSPACE, parseFunctionName(f).name,
                               "int", "int");

        schemaChange("DROP FUNCTION " + f + "(double, double)");

        assertLastSchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.FUNCTION,
                               KEYSPACE, parseFunctionName(f).name,
                               "double", "double");
    }

    @Test
    public void testFunctionDropOnKeyspaceDrop() throws Throwable
    {
        String fSin = createFunction(KEYSPACE_PER_TEST, "double",
                                     "CREATE FUNCTION %s ( input double ) " +
                                     "RETURNS double " +
                                     "LANGUAGE java " +
                                     "AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        FunctionName fSinName = parseFunctionName(fSin);

        Assert.assertEquals(1, Functions.find(parseFunctionName(fSin)).size());

        assertRows(execute("SELECT function_name, language FROM system.schema_functions WHERE keyspace_name=?", KEYSPACE_PER_TEST),
                   row(fSinName.name, "java"));

        dropPerTestKeyspace();

        assertRows(execute("SELECT function_name, language FROM system.schema_functions WHERE keyspace_name=?", KEYSPACE_PER_TEST));

        Assert.assertEquals(0, Functions.find(fSinName).size());
    }

    @Test
    public void testFunctionDropPreparedStatement() throws Throwable
    {
        createTable("CREATE TABLE %s (key int PRIMARY KEY, d double)");

        String fSin = createFunction(KEYSPACE_PER_TEST, "double",
                                     "CREATE FUNCTION %s ( input double ) " +
                                     "RETURNS double " +
                                     "LANGUAGE java " +
                                     "AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        FunctionName fSinName = parseFunctionName(fSin);

        Assert.assertEquals(1, Functions.find(parseFunctionName(fSin)).size());

        ResultMessage.Prepared prepared = QueryProcessor.prepare(
                                                    String.format("SELECT key, %s(d) FROM %s.%s", fSin, KEYSPACE, currentTable()),
                                                    ClientState.forInternalCalls(), false);
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));

        execute("DROP FUNCTION " + fSin + "(double);");

        Assert.assertNull(QueryProcessor.instance.getPrepared(prepared.statementId));

        //

        execute("CREATE FUNCTION " + fSin + " ( input double ) " +
                "RETURNS double " +
                "LANGUAGE java " +
                "AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        Assert.assertEquals(1, Functions.find(fSinName).size());

        prepared = QueryProcessor.prepare(
                                         String.format("SELECT key, %s(d) FROM %s.%s", fSin, KEYSPACE, currentTable()),
                                         ClientState.forInternalCalls(), false);
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));

        dropPerTestKeyspace();

        Assert.assertNull(QueryProcessor.instance.getPrepared(prepared.statementId));
    }

    @Test
    public void testFunctionCreationAndDrop() throws Throwable
    {
        createTable("CREATE TABLE %s (key int PRIMARY KEY, d double)");

        execute("INSERT INTO %s(key, d) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s(key, d) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s(key, d) VALUES (?, ?)", 3, 3d);

        // simple creation
        String fSin = createFunction(KEYSPACE_PER_TEST, "double",
                                     "CREATE FUNCTION %s ( input double ) " +
                                     "RETURNS double " +
                                     "LANGUAGE java " +
                                     "AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");
        // check we can't recreate the same function
        assertInvalidMessage("already exists", "CREATE FUNCTION " + fSin + " ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");
        // but that it doesn't comply with "IF NOT EXISTS"
        execute("CREATE FUNCTION IF NOT EXISTS " + fSin + " ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        // Validate that it works as expected
        assertRows(execute("SELECT key, " + fSin + "(d) FROM %s"),
            row(1, Math.sin(1d)),
            row(2, Math.sin(2d)),
            row(3, Math.sin(3d))
        );

        // Replace the method with incompatible return type
        assertInvalidMessage("the new return type text is not compatible with the return type double of existing function",
                             "CREATE OR REPLACE FUNCTION " + fSin + " ( input double ) RETURNS text LANGUAGE java AS 'return Double.valueOf(42d);'");
        // proper replacement
        execute("CREATE OR REPLACE FUNCTION " + fSin + " ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(42d);'");

        // Validate the method as been replaced
        assertRows(execute("SELECT key, " + fSin + "(d) FROM %s"),
            row(1, 42.0),
            row(2, 42.0),
            row(3, 42.0)
        );

        // same function but other keyspace
        String fSin2 = createFunction(KEYSPACE, "double",
                                      "CREATE FUNCTION %s ( input double ) " +
                                      "RETURNS double " +
                                      "LANGUAGE java " +
                                      "AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");
        assertRows(execute("SELECT key, " + fSin2 + "(d) FROM %s"),
            row(1, Math.sin(1d)),
            row(2, Math.sin(2d)),
            row(3, Math.sin(3d))
        );

        // Drop
        execute("DROP FUNCTION " + fSin);
        execute("DROP FUNCTION " + fSin2);

        // Drop unexisting function
        assertInvalidMessage("Cannot drop non existing function", "DROP FUNCTION " + fSin);
        // but don't complain with "IF EXISTS"
        execute("DROP FUNCTION IF EXISTS " + fSin);

        // can't drop native functions
        assertInvalidMessage("system keyspace is not user-modifiable", "DROP FUNCTION dateof");
        assertInvalidMessage("system keyspace is not user-modifiable", "DROP FUNCTION uuid");

        // sin() no longer exists
        assertInvalidMessage("Unknown function", "SELECT key, sin(d) FROM %s");
    }

    @Test
    public void testFunctionExecution() throws Throwable
    {
        createTable("CREATE TABLE %s (v text PRIMARY KEY)");

        execute("INSERT INTO %s(v) VALUES (?)", "aaa");

        String fRepeat = createFunction(KEYSPACE_PER_TEST, "text,int",
                                        "CREATE FUNCTION %s(v text, n int) " +
                                        "RETURNS text " +
                                        "LANGUAGE java " +
                                        "AS 'StringBuilder sb = new StringBuilder();\n" +
                                        "    for (int i = 0; i < n.intValue(); i++)\n" +
                                        "        sb.append(v);\n" +
                                        "    return sb.toString();'");

        assertRows(execute("SELECT v FROM %s WHERE v=" + fRepeat + "(?, ?)", "a", 3), row("aaa"));
        assertEmpty(execute("SELECT v FROM %s WHERE v=" + fRepeat + "(?, ?)", "a", 2));
    }

    @Test
    public void testFunctionOverloading() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v int)");

        execute("INSERT INTO %s(k, v) VALUES (?, ?)", "f2", 1);

        String fOverload = createFunction(KEYSPACE_PER_TEST, "varchar",
                                          "CREATE FUNCTION %s ( input varchar ) " +
                                          "RETURNS text " +
                                          "LANGUAGE java " +
                                          "AS 'return \"f1\";'");
        createFunctionOverload(fOverload,
                               "int",
                               "CREATE OR REPLACE FUNCTION %s(i int) RETURNS text LANGUAGE java AS 'return \"f2\";'");
        createFunctionOverload(fOverload,
                               "text,text",
                               "CREATE OR REPLACE FUNCTION %s(v1 text, v2 text) RETURNS text LANGUAGE java AS 'return \"f3\";'");
        createFunctionOverload(fOverload,
                               "ascii",
                               "CREATE OR REPLACE FUNCTION %s(v ascii) RETURNS text LANGUAGE java AS 'return \"f1\";'");

        // text == varchar, so this should be considered as a duplicate
        assertInvalidMessage("already exists",
                             "CREATE FUNCTION " + fOverload + "(v varchar) RETURNS text LANGUAGE java AS 'return \"f1\";'");

        assertRows(execute("SELECT " + fOverload + "(k), " + fOverload + "(v), " + fOverload + "(k, k) FROM %s"),
            row("f1", "f2", "f3")
        );

        forcePreparedValues();
        // This shouldn't work if we use preparation since there no way to know which overload to use
        assertInvalidMessage("Ambiguous call to function", "SELECT v FROM %s WHERE k = " + fOverload + "(?)", "foo");
        stopForcingPreparedValues();

        // but those should since we specifically cast
        assertEmpty(execute("SELECT v FROM %s WHERE k = " + fOverload + "((text)?)", "foo"));
        assertRows(execute("SELECT v FROM %s WHERE k = " + fOverload + "((int)?)", 3), row(1));
        assertEmpty(execute("SELECT v FROM %s WHERE k = " + fOverload + "((ascii)?)", "foo"));
        // And since varchar == text, this should work too
        assertEmpty(execute("SELECT v FROM %s WHERE k = " + fOverload + "((varchar)?)", "foo"));

        // no such functions exist...
        assertInvalidMessage("non existing function", "DROP FUNCTION " + fOverload + "(boolean)");
        assertInvalidMessage("non existing function", "DROP FUNCTION " + fOverload + "(bigint)");

        // 'overloaded' has multiple overloads - so it has to fail (CASSANDRA-7812)
        assertInvalidMessage("matches multiple function definitions", "DROP FUNCTION " + fOverload);
        execute("DROP FUNCTION " + fOverload + "(varchar)");
        assertInvalidMessage("none of its type signatures match", "SELECT v FROM %s WHERE k = " + fOverload + "((text)?)", "foo");
        execute("DROP FUNCTION " + fOverload + "(text, text)");
        assertInvalidMessage("none of its type signatures match", "SELECT v FROM %s WHERE k = " + fOverload + "((text)?,(text)?)", "foo", "bar");
        execute("DROP FUNCTION " + fOverload + "(ascii)");
        assertInvalidMessage("cannot be passed as argument 0 of function", "SELECT v FROM %s WHERE k = " + fOverload + "((ascii)?)", "foo");
        // single-int-overload must still work
        assertRows(execute("SELECT v FROM %s WHERE k = " + fOverload + "((int)?)", 3), row(1));
        // overloaded has just one overload now - so the following DROP FUNCTION is not ambigious (CASSANDRA-7812)
        execute("DROP FUNCTION " + fOverload);
    }

    @Test
    public void testCreateOrReplaceJavaFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                "CREATE FUNCTION %s( input double ) " +
                "RETURNS double LANGUAGE java\n" +
                "AS '\n" +
                "  // parameter val is of type java.lang.Double\n" +
                "  /* return type is of type java.lang.Double */\n" +
                "  if (input == null) {\n" +
                "    return null;\n" +
                "  }\n" +
                "  double v = Math.sin( input.doubleValue() );\n" +
                "  return Double.valueOf(v);\n" +
                "';");

        // just check created function
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );

        execute("CREATE OR REPLACE FUNCTION " + fName + "( input double ) " +
                "RETURNS double LANGUAGE java\n" +
                "AS '\n" +
                "  return input;\n" +
                "';");

        // check if replaced function returns correct result
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                   row(1, 1d, 1d),
                   row(2, 2d, 2d),
                   row(3, 3d, 3d)
        );
    }

    @Test
    public void testJavaFunctionNoParameters() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = "\n  return Long.valueOf(1L);\n";

        String fName = createFunction(KEYSPACE, "",
                                      "CREATE OR REPLACE FUNCTION %s() RETURNS bigint LANGUAGE JAVA\n" +
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
            execute("CREATE OR REPLACE FUNCTION " + KEYSPACE + ".jfinv() RETURNS bigint LANGUAGE JAVA\n" +
                    "AS '\n" +
                    "foobarbaz" +
                    "\n';");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("[source error]"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("; is missing"));
        }

        try
        {
            execute("CREATE OR REPLACE FUNCTION " + KEYSPACE + ".jfinv() RETURNS bigint LANGUAGE JAVA\n" +
                    "AS '\n" +
                    "foobarbaz;" +
                    "\n';");
            Assert.fail();
        }
        catch (InvalidRequestException e)
        {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("[source error]"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("no such field: foobarbaz"));
        }
    }

    @Test
    public void testJavaFunctionInvalidReturn() throws Throwable
    {
        assertInvalidMessage("system keyspace is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION jfir(val double) RETURNS double LANGUAGE JAVA\n" +
                             "AS 'return Long.valueOf(1L);';");
    }

    @Test
    public void testJavaFunctionArgumentTypeMismatch() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val bigint)");

        String fName = createFunction(KEYSPACE, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double)" +
                                      "RETURNS double LANGUAGE JAVA " +
                                      "AS 'return val;';");

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

        String functionBody = "\n" +
                              "  // parameter val is of type java.lang.Double\n" +
                              "  /* return type is of type java.lang.Double */\n" +
                              "  if (val == null) {\n" +
                              "    return null;\n" +
                              "  }\n" +
                              "  double v = Math.sin( val.doubleValue() );\n" +
                              "  return Double.valueOf(v);\n";

        String fName = createFunction(KEYSPACE, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
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
    public void testFunctionInTargetKeyspace() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        execute("CREATE TABLE " + KEYSPACE_PER_TEST + ".second_tab (key int primary key, val double)");

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) RETURNS double LANGUAGE JAVA " +
                                      "AS 'return val;';");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertInvalidMessage("Unknown function",
                             "SELECT key, val, " + parseFunctionName(fName).name + "(val) FROM %s");

        execute("INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, " + fName + "(val) FROM " + KEYSPACE_PER_TEST + ".second_tab"),
                   row(1, 1d, 1d),
                   row(2, 2d, 2d),
                   row(3, 3d, 3d)
        );
    }

    @Test
    public void testFunctionWithReservedName() throws Throwable
    {
        execute("CREATE TABLE " + KEYSPACE_PER_TEST + ".second_tab (key int primary key, val double)");

        String fName = createFunction(KEYSPACE_PER_TEST, "",
                                      "CREATE OR REPLACE FUNCTION %s() RETURNS timestamp LANGUAGE JAVA " +
                                      "AS 'return null;';");

        execute("INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO " + KEYSPACE_PER_TEST + ".second_tab (key, val) VALUES (?, ?)", 3, 3d);

        // ensure that system now() is executed
        UntypedResultSet rows = execute("SELECT key, val, now() FROM " + KEYSPACE_PER_TEST + ".second_tab");
        Assert.assertEquals(3, rows.size());
        UntypedResultSet.Row row = rows.iterator().next();
        Date ts = row.getTimestamp(row.getColumns().get(2).name.toString());
        Assert.assertNotNull(ts);

        // ensure that KEYSPACE_PER_TEST's now() is executed
        rows = execute("SELECT key, val, " + fName + "() FROM " + KEYSPACE_PER_TEST + ".second_tab");
        Assert.assertEquals(3, rows.size());
        row = rows.iterator().next();
        Assert.assertFalse(row.has(row.getColumns().get(2).name.toString()));
    }

    @Test
    public void testFunctionInSystemKS() throws Throwable
    {
        execute("CREATE OR REPLACE FUNCTION " + KEYSPACE + ".dateof(val timeuuid) RETURNS timestamp LANGUAGE JAVA\n" +
                "AS 'return null;';");

        assertInvalidMessage("system keyspace is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION system.jnft(val double) RETURNS double LANGUAGE JAVA\n" +
                             "AS 'return null;';");
        assertInvalidMessage("system keyspace is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION system.dateof(val timeuuid) RETURNS timestamp LANGUAGE JAVA\n" +
                             "AS 'return null;';");
        assertInvalidMessage("system keyspace is not user-modifiable",
                             "DROP FUNCTION system.now");

        // KS for executeInternal() is system
        assertInvalidMessage("system keyspace is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION jnft(val double) RETURNS double LANGUAGE JAVA\n" +
                             "AS 'return null;';");
        assertInvalidMessage("system keyspace is not user-modifiable",
                             "CREATE OR REPLACE FUNCTION dateof(val timeuuid) RETURNS timestamp LANGUAGE JAVA\n" +
                             "AS 'return null;';");
        assertInvalidMessage("system keyspace is not user-modifiable",
                             "DROP FUNCTION now");
    }

    @Test
    public void testFunctionNonExistingKeyspace() throws Throwable
    {
        assertInvalidMessage("to non existing keyspace",
                             "CREATE OR REPLACE FUNCTION this_ks_does_not_exist.jnft(val double) RETURNS double LANGUAGE JAVA\n" +
                             "AS 'return null;';");
    }

    @Test
    public void testFunctionAfterOnDropKeyspace() throws Throwable
    {
        dropPerTestKeyspace();

        assertInvalidMessage("to non existing keyspace",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE_PER_TEST + ".jnft(val double) RETURNS double LANGUAGE JAVA\n" +
                             "AS 'return null;';");
    }

    @Test
    public void testJavaKeyspaceFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = "\n" +
                              "  // parameter val is of type java.lang.Double\n" +
                              "  /* return type is of type java.lang.Double */\n" +
                              "  if (val == null) {\n" +
                              "    return null;\n" +
                              "  }\n" +
                              "  double v = Math.sin( val.doubleValue() );\n" +
                              "  return Double.valueOf(v);\n";

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                     "CREATE OR REPLACE FUNCTION %s(val double) RETURNS double LANGUAGE JAVA " +
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

        String functionBody = "\n" +
                              "  throw new RuntimeException(\"oh no!\");\n";

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) RETURNS double LANGUAGE JAVA\n" +
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
        String functionBody = "\n" +
                              "  // parameter val is of type java.lang.Double\n" +
                              "  /* return type is of type java.lang.Double */\n" +
                              "  if (input == null) {\n" +
                              "    return null;\n" +
                              "  }\n" +
                              "  double v = Math.sin( input.doubleValue() );\n" +
                              "  return \"'\" + Double.valueOf(v)+'\\\'';\n";

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE FUNCTION %s( input double ) " +
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
                                     "RETURNS list<double> LANGUAGE java\n" +
                                     "AS $$return lst;$$;");
        String fSet = createFunction(KEYSPACE_PER_TEST, "set<text>",
                                     "CREATE FUNCTION %s( st set<text> ) " +
                                     "RETURNS set<text> LANGUAGE java\n" +
                                     "AS $$return st;$$;");
        String fMap = createFunction(KEYSPACE_PER_TEST, "map<int, boolean>",
                                     "CREATE FUNCTION %s( mp map<int, boolean> ) " +
                                     "RETURNS map<int, boolean> LANGUAGE java\n" +
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
    public void testComplexNullValues() throws Throwable
    {
        String type = KEYSPACE + "." + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, lst list<double>, st set<text>, mp map<int, boolean>," +
                    "tup frozen<tuple<double, text, int, boolean>>, udt frozen<" + type + ">)");

        String fList = createFunction(KEYSPACE, "list<double>",
                                      "CREATE FUNCTION %s( coll list<double> ) " +
                                      "RETURNS list<double> " +
                                      "LANGUAGE java\n" +
                                      "AS $$return coll;$$;");
        String fSet = createFunction(KEYSPACE, "set<text>",
                                     "CREATE FUNCTION %s( coll set<text> ) " +
                                     "RETURNS set<text> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return coll;$$;");
        String fMap = createFunction(KEYSPACE, "map<int, boolean>",
                                     "CREATE FUNCTION %s( coll map<int, boolean> ) " +
                                     "RETURNS map<int, boolean> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return coll;$$;");
        String fTup = createFunction(KEYSPACE, "frozen<tuple<double, text, int, boolean>>",
                                     "CREATE FUNCTION %s( val frozen<tuple<double, text, int, boolean>> ) " +
                                     "RETURNS frozen<tuple<double, text, int, boolean>> " +
                                     "LANGUAGE java\n" +
                                     "AS $$return val;$$;");
        String fUdt = createFunction(KEYSPACE, "frozen<" + type+'>',
                                     "CREATE FUNCTION %s( val frozen<" + type + "> ) " +
                                     "RETURNS frozen<" + type + "> " +
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

        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
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

    @Test
    public void testJavaTupleType() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, tup frozen<tuple<double, text, int, boolean>>)");

        String fName = createFunction(KEYSPACE, "frozen<tuple<double, text, int, boolean>>",
                                     "CREATE FUNCTION %s( tup frozen<tuple<double, text, int, boolean>> ) " +
                                     "RETURNS frozen<tuple<double, text, int, boolean>> " +
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
        String tupleTypeDef = "frozen<tuple<double, list<double>, set<text>, map<int, boolean>>>";

        createTable("CREATE TABLE %s (key int primary key, tup " + tupleTypeDef + ")");

        String fTup0 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS " + tupleTypeDef + " " +
                "LANGUAGE java\n" +
                "AS $$return " +
                "       tup;$$;");
        String fTup1 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS double " +
                "LANGUAGE java\n" +
                "AS $$return " +
                "       Double.valueOf(tup.getDouble(0));$$;");
        String fTup2 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS list<double> " +
                "LANGUAGE java\n" +
                "AS $$return " +
                "       tup.getList(1, Double.class);$$;");
        String fTup3 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS set<text> " +
                "LANGUAGE java\n" +
                "AS $$return " +
                "       tup.getSet(2, String.class);$$;");
        String fTup4 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
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
        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + KEYSPACE + "." + type + ">)");
        execute("INSERT INTO %s (key, udt) VALUES (1, {txt: 'one', i:1})");

        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
        {
            executeNet(version, "USE " + KEYSPACE);

            executeNet(version,
                       "CREATE FUNCTION f_use1( udt frozen<" + type + "> ) " +
                       "RETURNS frozen<" + type + "> " +
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
    public void testJavaUserTypeOtherKeyspace() throws Throwable
    {
        String type = KEYSPACE + "." + createType("CREATE TYPE %s (txt text, i int)");

        String fName = createFunction(KEYSPACE_PER_TEST, "frozen<" + type + ">",
                                      "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                                      "RETURNS frozen<" + type + "> " +
                                      "LANGUAGE java " +
                                      "AS $$return " +
                                      "     udt;$$;");

        execute("DROP FUNCTION " + fName);
    }

    @Test
    public void testJavaUserType() throws Throwable
    {
        String type = KEYSPACE + "." + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fUdt0 = createFunction(KEYSPACE, "frozen<" + type + ">",
                                      "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                                      "RETURNS frozen<" + type + "> " +
                                      "LANGUAGE java " +
                                      "AS $$return " +
                                      "     udt;$$;");
        String fUdt1 = createFunction(KEYSPACE, "frozen<" + type + ">",
                                      "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                                      "RETURNS text " +
                                      "LANGUAGE java " +
                                      "AS $$return " +
                                      "     udt.getString(\"txt\");$$;");
        String fUdt2 = createFunction(KEYSPACE, "frozen<" + type + ">",
                                      "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
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
    public void testUserTypeDrop() throws Throwable
    {
        String type = KEYSPACE + "." + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fName = createFunction(KEYSPACE, "frozen<" + type + ">",
                                      "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                                      "RETURNS int " +
                                      "LANGUAGE java " +
                                      "AS $$return " +
                                      "     Integer.valueOf(udt.getInt(\"i\"));$$;");

        FunctionName fNameName = parseFunctionName(fName);

        Assert.assertEquals(1, Functions.find(fNameName).size());

        ResultMessage.Prepared prepared = QueryProcessor.prepare(String.format("SELECT key, %s(udt) FROM %s.%s", fName, KEYSPACE, currentTable()),
                                                                 ClientState.forInternalCalls(), false);
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));

        // UT still referenced by table
        assertInvalidMessage("Cannot drop user type", "DROP TYPE " + type);

        execute("DROP TABLE %s");

        // UT still referenced by UDF
        assertInvalidMessage("as it is still used by function", "DROP TYPE " + type);

        Assert.assertNull(QueryProcessor.instance.getPrepared(prepared.statementId));

        // function stays
        Assert.assertEquals(1, Functions.find(fNameName).size());
    }

    @Test
    public void testJavaUserTypeRenameField() throws Throwable
    {
        String type = KEYSPACE + "." + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fName = createFunction(KEYSPACE, "frozen<" + type + ">",
                                      "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                                      "RETURNS text LANGUAGE java\n" +
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
        String type = KEYSPACE + "." + createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fName1replace = createFunction(KEYSPACE, "frozen<" + type + ">",
                                              "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                                              "RETURNS text LANGUAGE java\n" +
                                              "AS $$return udt.getString(\"txt\");$$;");
        String fName2replace = createFunction(KEYSPACE, "frozen<" + type + ">",
                                              "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                                              "RETURNS int LANGUAGE java\n" +
                                              "AS $$return Integer.valueOf(udt.getInt(\"i\"));$$;");
        String fName3replace = createFunction(KEYSPACE, "frozen<" + type + ">",
                                              "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                                              "RETURNS double LANGUAGE java\n" +
                                              "AS $$return Double.valueOf(udt.getDouble(\"added\"));$$;");
        String fName4replace = createFunction(KEYSPACE, "frozen<" + type + ">",
                                              "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                                              "RETURNS frozen<" + type + "> LANGUAGE java\n" +
                                              "AS $$return udt;$$;");

        String fName1noReplace = createFunction(KEYSPACE, "frozen<" + type + ">",
                                              "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                                              "RETURNS text LANGUAGE java\n" +
                                              "AS $$return udt.getString(\"txt\");$$;");
        String fName2noReplace = createFunction(KEYSPACE, "frozen<" + type + ">",
                                              "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                                              "RETURNS int LANGUAGE java\n" +
                                              "AS $$return Integer.valueOf(udt.getInt(\"i\"));$$;");
        String fName3noReplace = createFunction(KEYSPACE, "frozen<" + type + ">",
                                                "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                                                "RETURNS double LANGUAGE java\n" +
                                                "AS $$return Double.valueOf(udt.getDouble(\"added\"));$$;");
        String fName4noReplace = createFunction(KEYSPACE, "frozen<" + type + ">",
                                                "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                                                "RETURNS frozen<" + type + "> LANGUAGE java\n" +
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

        execute(String.format("CREATE OR REPLACE FUNCTION %s( udt frozen<%s> ) " +
                              "RETURNS text LANGUAGE java\n" +
                              "AS $$return " +
                              "     udt.getString(\"txt\");$$;",
                              fName1replace, type));
        Assert.assertEquals(1, Functions.find(parseFunctionName(fName1replace)).size());
        execute(String.format("CREATE OR REPLACE FUNCTION %s( udt frozen<%s> ) " +
                              "RETURNS int LANGUAGE java\n" +
                              "AS $$return " +
                              "     Integer.valueOf(udt.getInt(\"i\"));$$;",
                              fName2replace, type));
        Assert.assertEquals(1, Functions.find(parseFunctionName(fName2replace)).size());
        execute(String.format("CREATE OR REPLACE FUNCTION %s( udt frozen<%s> ) " +
                              "RETURNS double LANGUAGE java\n" +
                              "AS $$return " +
                              "     Double.valueOf(udt.getDouble(\"added\"));$$;",
                              fName3replace, type));
        Assert.assertEquals(1, Functions.find(parseFunctionName(fName3replace)).size());
        execute(String.format("CREATE OR REPLACE FUNCTION %s( udt frozen<%s> ) " +
                              "RETURNS frozen<%s> LANGUAGE java\n" +
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
        String type = KEYSPACE + "." + createType("CREATE TYPE %s (txt text, i int)");

        createTable(String.format("CREATE TABLE %%s " +
                                  "(key int primary key, lst list<frozen<%s>>, st set<frozen<%s>>, mp map<int, frozen<%s>>)",
                                  type, type, type));

        String fName1 = createFunction(KEYSPACE, "list<frozen<" + type + ">>",
                              "CREATE FUNCTION %s( lst list<frozen<" + type + ">> ) " +
                              "RETURNS text LANGUAGE java\n" +
                              "AS $$" +
                              "     com.datastax.driver.core.UDTValue udtVal = (com.datastax.driver.core.UDTValue)lst.get(1);" +
                              "     return udtVal.getString(\"txt\");$$;");
        String fName2 = createFunction(KEYSPACE, "set<frozen<" + type + ">>",
                              "CREATE FUNCTION %s( st set<frozen<" + type + ">> ) " +
                              "RETURNS text LANGUAGE java\n" +
                              "AS $$" +
                              "     com.datastax.driver.core.UDTValue udtVal = (com.datastax.driver.core.UDTValue)st.iterator().next();" +
                              "     return udtVal.getString(\"txt\");$$;");
        String fName3 = createFunction(KEYSPACE, "map<int, frozen<" + type + ">>",
                              "CREATE FUNCTION %s( mp map<int, frozen<" + type + ">> ) " +
                              "RETURNS text LANGUAGE java\n" +
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
    public void testJavascriptSimpleCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, lst list<double>, st set<text>, mp map<int, boolean>)");

        String fName1 = createFunction(KEYSPACE_PER_TEST, "list<double>",
                "CREATE FUNCTION %s( lst list<double> ) " +
                "RETURNS list<double> " +
                "LANGUAGE javascript\n" +
                "AS 'lst;';");
        String fName2 = createFunction(KEYSPACE_PER_TEST, "set<text>",
                "CREATE FUNCTION %s( st set<text> ) " +
                "RETURNS set<text> " +
                "LANGUAGE javascript\n" +
                "AS 'st;';");
        String fName3 = createFunction(KEYSPACE_PER_TEST, "map<int, boolean>",
                "CREATE FUNCTION %s( mp map<int, boolean> ) " +
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

        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fName1 + "(lst), " + fName2 + "(st), " + fName3 + "(mp) FROM %s WHERE key = 1"),
                          row(list, set, map));
    }

    @Test
    public void testJavascriptTupleType() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, tup frozen<tuple<double, text, int, boolean>>)");

        String fName = createFunction(KEYSPACE_PER_TEST, "frozen<tuple<double, text, int, boolean>>",
                "CREATE FUNCTION %s( tup frozen<tuple<double, text, int, boolean>> ) " +
                "RETURNS frozen<tuple<double, text, int, boolean>> " +
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
    public void testJavascriptTupleTypeCollection() throws Throwable
    {
        String tupleTypeDef = "frozen<tuple<double, list<double>, set<text>, map<int, boolean>>>";
        createTable("CREATE TABLE %s (key int primary key, tup " + tupleTypeDef + ")");

        String fTup1 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS frozen<tuple<double, list<double>, set<text>, map<int, boolean>>> " +
                "LANGUAGE javascript\n" +
                "AS $$" +
                "       tup;$$;");
        String fTup2 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS double LANGUAGE " +
                "javascript\n" +
                "AS $$" +
                "       tup.getDouble(0);$$;");
        String fTup3 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS list<double> " +
                "LANGUAGE javascript\n" +
                "AS $$" +
                "       tup.getList(1, java.lang.Class.forName(\"java.lang.Double\"));$$;");
        String fTup4 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS set<text> " +
                "LANGUAGE javascript\n" +
                "AS $$" +
                "       tup.getSet(2, java.lang.Class.forName(\"java.lang.String\"));$$;");
        String fTup5 = createFunction(KEYSPACE_PER_TEST, tupleTypeDef,
                "CREATE FUNCTION %s( tup " + tupleTypeDef + " ) " +
                "RETURNS map<int, boolean> " +
                "LANGUAGE javascript\n" +
                "AS $$" +
                "       tup.getMap(3, java.lang.Class.forName(\"java.lang.Integer\"), java.lang.Class.forName(\"java.lang.Boolean\"));$$;");

        List<Double> list = Arrays.asList(1d, 2d, 3d);
        Set<String> set = new TreeSet<>(Arrays.asList("one", "three", "two"));
        Map<Integer, Boolean> map = new TreeMap<>();
        map.put(1, true);
        map.put(2, false);
        map.put(3, true);

        Object t = tuple(1d, list, set, map);

        execute("INSERT INTO %s (key, tup) VALUES (1, ?)", t);

        assertRows(execute("SELECT " + fTup1 + "(tup) FROM %s WHERE key = 1"),
                   row(t));
        assertRows(execute("SELECT " + fTup2 + "(tup) FROM %s WHERE key = 1"),
                   row(1d));
        assertRows(execute("SELECT " + fTup3 + "(tup) FROM %s WHERE key = 1"),
                   row(list));
        assertRows(execute("SELECT " + fTup4 + "(tup) FROM %s WHERE key = 1"),
                   row(set));
        assertRows(execute("SELECT " + fTup5 + "(tup) FROM %s WHERE key = 1"),
                   row(map));

        // same test - but via native protocol
        TupleType tType = TupleType.of(DataType.cdouble(),
                                       DataType.list(DataType.cdouble()),
                                       DataType.set(DataType.text()),
                                       DataType.map(DataType.cint(), DataType.cboolean()));
        TupleValue tup = tType.newValue(1d, list, set, map);
        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
        {
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup1 + "(tup) FROM %s WHERE key = 1"),
                          row(tup));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup2 + "(tup) FROM %s WHERE key = 1"),
                          row(1d));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup3 + "(tup) FROM %s WHERE key = 1"),
                          row(list));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup4 + "(tup) FROM %s WHERE key = 1"),
                          row(set));
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fTup5 + "(tup) FROM %s WHERE key = 1"),
                          row(map));
        }
    }

    @Test
    public void testJavascriptUserType() throws Throwable
    {
        String type = createType("CREATE TYPE %s (txt text, i int)");

        createTable("CREATE TABLE %s (key int primary key, udt frozen<" + type + ">)");

        String fUdt1 = createFunction(KEYSPACE, "frozen<" + type + ">",
                              "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                              "RETURNS frozen<" + type + "> " +
                              "LANGUAGE javascript\n" +
                              "AS $$" +
                              "     udt;$$;");
        String fUdt2 = createFunction(KEYSPACE, "frozen<" + type + ">",
                              "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
                              "RETURNS text " +
                              "LANGUAGE javascript\n" +
                              "AS $$" +
                              "     udt.getString(\"txt\");$$;");
        String fUdt3 = createFunction(KEYSPACE, "frozen<" + type + ">",
                              "CREATE FUNCTION %s( udt frozen<" + type + "> ) " +
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
                       "RETURNS text " +
                       "LANGUAGE javascript\n" +
                       "AS $$" +
                       "        lst.get(1).getString(\"txt\");$$;");
        createFunctionOverload(fName, "set<frozen<" + type + ">>",
                               "CREATE FUNCTION %s( st set<frozen<" + type + ">> ) " +
                               "RETURNS text " +
                               "LANGUAGE javascript\n" +
                               "AS $$" +
                               "        st.iterator().next().getString(\"txt\");$$;");
        createFunctionOverload(fName, "map<int, frozen<" + type + ">>",
                               "CREATE FUNCTION %s( mp map<int, frozen<" + type + ">> ) " +
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
        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
            assertRowsNet(version,
                          executeNet(version, cqlSelect),
                          row("three", "one", "two"));
    }

    @Test
    public void testJavascriptFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = "\n" +
                              "  Math.sin(val);\n";

        String fName = createFunction(KEYSPACE, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) " +
                                      "RETURNS double LANGUAGE javascript\n" +
                                      "AS '" + functionBody + "';");

        FunctionName fNameName = parseFunctionName(fName);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE keyspace_name=? AND function_name=?",
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
                       "RETURNS double " +
                       "LANGUAGE javascript\n" +
                       "AS 'throw \"fool\";';");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        // throws IRE with ScriptException
        assertInvalidThrowMessage("fool", FunctionExecutionException.class,
                                  "SELECT key, val, " + fName + "(val) FROM %s");
    }

    @Test
    public void testDuplicateArgNames() throws Throwable
    {
        assertInvalidMessage("duplicate argument names for given function",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".scrinv(val double, val text) " +
                             "RETURNS text LANGUAGE javascript\n" +
                             "AS '\"foo bar\";';");
    }

    @Test
    public void testJavascriptCompileFailure() throws Throwable
    {
        assertInvalidMessage("Failed to compile function 'cql_test_keyspace.scrinv'",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".scrinv(val double) " +
                             "RETURNS double LANGUAGE javascript\n" +
                             "AS 'foo bar';");
    }

    @Test
    public void testScriptInvalidLanguage() throws Throwable
    {
        assertInvalidMessage("Invalid language 'artificial_intelligence' for function 'cql_test_keyspace.scrinv'",
                             "CREATE OR REPLACE FUNCTION " + KEYSPACE + ".scrinv(val double) " +
                             "RETURNS double LANGUAGE artificial_intelligence\n" +
                             "AS 'question for 42?';");
    }

    @Test
    public void testScriptReturnTypeCasting() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);

        Object[][] variations = new Object[][]
                                {
                                new Object[]    {   "true",     "boolean",  true    },
                                new Object[]    {   "false",    "boolean",  false   },
                                new Object[]    {   "100",      "int",      100     },
                                new Object[]    {   "100.",     "int",      100     },
                                new Object[]    {   "100",      "double",   100d    },
                                new Object[]    {   "100.",     "double",   100d    },
                                new Object[]    {   "100",      "bigint",   100L    },
                                new Object[]    {   "100.",     "bigint",   100L    },
                                new Object[]    {   "100",      "varint",   BigInteger.valueOf(100L)    },
                                new Object[]    {   "100.",     "varint",   BigInteger.valueOf(100L)    },
                                new Object[]    {   "parseInt(\"100\");", "decimal",  BigDecimal.valueOf(100d)    },
                                new Object[]    {   "100.",     "decimal",  BigDecimal.valueOf(100d)    },
                                };

        for (Object[] variation : variations)
        {
            Object functionBody = variation[0];
            Object returnType = variation[1];
            Object expectedResult = variation[2];

            String fName = createFunction(KEYSPACE, "double",
                                          "CREATE OR REPLACE FUNCTION %s(val double) " +
                                          "RETURNS " +returnType + " " +
                                          "LANGUAGE javascript " +
                                          "AS '" + functionBody + ";';");
            assertRows(execute("SELECT key, val, " + fName + "(val) FROM %s"),
                       row(1, 1d, expectedResult));
        }
    }

    @Test
    public void testScriptParamReturnTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, ival int, lval bigint, fval float, dval double, vval varint, ddval decimal)");
        execute("INSERT INTO %s (key, ival, lval, fval, dval, vval, ddval) VALUES (?, ?, ?, ?, ?, ?, ?)", 1,
                1, 1L, 1f, 1d, BigInteger.valueOf(1L), BigDecimal.valueOf(1d));

        Object[][] variations = new Object[][]
                                {
                                new Object[] {  "int",      "ival",     1,                      2  },
                                new Object[] {  "bigint",   "lval",     1L,                     2L  },
                                new Object[] {  "float",    "fval",     1f,                     2f  },
                                new Object[] {  "double",   "dval",     1d,                     2d  },
                                new Object[] {  "varint",   "vval",     BigInteger.valueOf(1L), BigInteger.valueOf(2L)  },
                                new Object[] {  "decimal",  "ddval",    BigDecimal.valueOf(1d), BigDecimal.valueOf(2d)  },
                                };

        for (Object[] variation : variations)
        {
            Object type = variation[0];
            Object col = variation[1];
            Object expected1 = variation[2];
            Object expected2 = variation[3];
            String fName = createFunction(KEYSPACE, type.toString(),
                           "CREATE OR REPLACE FUNCTION %s(val " + type + ") " +
                           "RETURNS " + type + " " +
                           "LANGUAGE javascript " +
                           "AS 'val+1;';");
            assertRows(execute("SELECT key, " + col + ", " + fName + "(" + col + ") FROM %s"),
                       row(1, expected1, expected2));
        }
    }

    @Test
    public void testBrokenFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, dval double)");
        execute("INSERT INTO %s (key, dval) VALUES (?, ?)", 1, 1d);

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) RETURNS double LANGUAGE JAVA\n" +
                                      "AS 'throw new RuntimeException();';");

        UDFunction f = (UDFunction) Functions.find(parseFunctionName(fName)).get(0);

        Functions.replaceFunction(UDFunction.createBrokenFunction(f.name(), f.argNames(), f.argTypes(), f.returnType(),
                                                                  "java", f.body(), new InvalidRequestException("foo bar is broken")));

        assertInvalidThrowMessage("foo bar is broken", InvalidRequestException.class,
                                  "SELECT key, " + fName + "(dval) FROM %s");
    }

    @Test
    @Ignore("implement this unit test when Java Driver can handle new ExceptionCode.")
    public void testFunctionExecutionExceptionNet() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, dval double)");
        execute("INSERT INTO %s (key, dval) VALUES (?, ?)", 1, 1d);

        String fName = createFunction(KEYSPACE_PER_TEST, "double",
                                      "CREATE OR REPLACE FUNCTION %s(val double) RETURNS double LANGUAGE JAVA\n" +
                                      "AS 'throw new RuntimeException()';");

        for (int version = Server.VERSION_2; version <= maxProtocolVersion; version++)
        {
            // TODO replace with appropiate code
            assertRowsNet(version,
                          executeNet(version, "SELECT " + fName + "(dval) FROM %s WHERE key = 1"));
        }
    }
}
