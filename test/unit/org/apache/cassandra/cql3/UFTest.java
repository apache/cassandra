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
import java.util.Date;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class UFTest extends CQLTester
{
    private static final String KS_FOO = "cqltest_foo";

    @Before
    public void createKsFoo() throws Throwable
    {
        execute("CREATE KEYSPACE IF NOT EXISTS "+KS_FOO+" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
    }

    @After
    public void dropKsFoo() throws Throwable
    {
        execute("DROP KEYSPACE IF EXISTS "+KS_FOO+";");
    }

    @Test
    public void testFunctionDropOnKeyspaceDrop() throws Throwable
    {
        execute("CREATE FUNCTION " + KS_FOO + ".sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        Assert.assertEquals(1, Functions.find(new FunctionName(KS_FOO, "sin")).size());

        assertRows(execute("SELECT function_name, language FROM system.schema_functions WHERE keyspace_name=?", KS_FOO),
                   row("sin", "java"));

        execute("DROP KEYSPACE "+KS_FOO+";");

        assertRows(execute("SELECT function_name, language FROM system.schema_functions WHERE keyspace_name=?", KS_FOO));

        Assert.assertEquals(0, Functions.find(new FunctionName(KS_FOO, "sin")).size());
    }

    @Test
    public void testFunctionDropPreparedStatement() throws Throwable
    {
        createTable("CREATE TABLE %s (key int PRIMARY KEY, d double)");

        execute("CREATE FUNCTION " + KS_FOO + ".sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        Assert.assertEquals(1, Functions.find(new FunctionName(KS_FOO, "sin")).size());

        ResultMessage.Prepared prepared = QueryProcessor.prepare("SELECT key, "+KS_FOO+".sin(d) FROM "+KEYSPACE+'.'+currentTable(), ClientState.forInternalCalls(), false);
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));

        execute("DROP FUNCTION " + KS_FOO + ".sin(double);");

        Assert.assertNull(QueryProcessor.instance.getPrepared(prepared.statementId));

        //

        execute("CREATE FUNCTION " + KS_FOO + ".sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        Assert.assertEquals(1, Functions.find(new FunctionName(KS_FOO, "sin")).size());

        prepared = QueryProcessor.prepare("SELECT key, "+KS_FOO+".sin(d) FROM "+KEYSPACE+'.'+currentTable(), ClientState.forInternalCalls(), false);
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(prepared.statementId));

        execute("DROP KEYSPACE " + KS_FOO + ";");

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
        execute("CREATE FUNCTION "+KS_FOO+".sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");
        // check we can't recreate the same function
        assertInvalid("CREATE FUNCTION "+KS_FOO+".sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");
        // but that it doesn't complay with "IF NOT EXISTS"
        execute("CREATE FUNCTION IF NOT EXISTS "+KS_FOO+".sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        // Validate that it works as expected
        assertRows(execute("SELECT key, "+KS_FOO+".sin(d) FROM %s"),
            row(1, Math.sin(1d)),
            row(2, Math.sin(2d)),
            row(3, Math.sin(3d))
        );

        // Replace the method with incompatible return type
        assertInvalid("CREATE OR REPLACE FUNCTION " + KS_FOO + ".sin ( input double ) RETURNS text LANGUAGE java AS 'return Double.valueOf(42d);'");
        // proper replacement
        execute("CREATE OR REPLACE FUNCTION "+KS_FOO+".sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(42d);'");

        // Validate the method as been replaced
        assertRows(execute("SELECT key, "+KS_FOO+".sin(d) FROM %s"),
            row(1, 42.0),
            row(2, 42.0),
            row(3, 42.0)
        );

        // same function but without namespace
        execute("CREATE FUNCTION "+KEYSPACE+".sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");
        assertRows(execute("SELECT key, "+KEYSPACE+".sin(d) FROM %s"),
            row(1, Math.sin(1d)),
            row(2, Math.sin(2d)),
            row(3, Math.sin(3d))
        );

        // Drop with and without keyspace
        execute("DROP FUNCTION "+KS_FOO+".sin");
        execute("DROP FUNCTION "+KEYSPACE+".sin");

        // Drop unexisting function
        assertInvalid("DROP FUNCTION "+KS_FOO+".sin");
        // but don't complain with "IF EXISTS"
        execute("DROP FUNCTION IF EXISTS "+KS_FOO+".sin");

        // can't drop native functions
        assertInvalid("DROP FUNCTION dateof");
        assertInvalid("DROP FUNCTION uuid");

        // sin() no longer exists
        assertInvalid("SELECT key, sin(d) FROM %s");
    }

    @Test
    public void testFunctionExecution() throws Throwable
    {
        createTable("CREATE TABLE %s (v text PRIMARY KEY)");

        execute("INSERT INTO %s(v) VALUES (?)", "aaa");

        execute("CREATE FUNCTION "+KEYSPACE+".repeat (v text, n int) RETURNS text LANGUAGE java AS 'StringBuilder sb = new StringBuilder();\n" +
                "        for (int i = 0; i < n.intValue(); i++)\n" +
                "            sb.append(v);\n" +
                "        return sb.toString();'");

        assertRows(execute("SELECT v FROM %s WHERE v="+KEYSPACE+".repeat(?, ?)", "a", 3), row("aaa"));
        assertEmpty(execute("SELECT v FROM %s WHERE v="+KEYSPACE+".repeat(?, ?)", "a", 2));
    }

    @Test
    public void testFunctionOverloading() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v int)");

        execute("INSERT INTO %s(k, v) VALUES (?, ?)", "f2", 1);

        execute("CREATE FUNCTION "+KEYSPACE+".overloaded(v varchar) RETURNS text LANGUAGE java AS 'return \"f1\";'");
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".overloaded(i int) RETURNS text LANGUAGE java AS 'return \"f2\";'");
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".overloaded(v1 text, v2 text) RETURNS text LANGUAGE java AS 'return \"f3\";'");
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".overloaded(v ascii) RETURNS text LANGUAGE java AS 'return \"f1\";'");

        // text == varchar, so this should be considered as a duplicate
        assertInvalid("CREATE FUNCTION "+KEYSPACE+".overloaded(v varchar) RETURNS text LANGUAGE java AS 'return \"f1\";'");

        assertRows(execute("SELECT "+KEYSPACE+".overloaded(k), "+KEYSPACE+".overloaded(v), "+KEYSPACE+".overloaded(k, k) FROM %s"),
            row("f1", "f2", "f3")
        );

        forcePreparedValues();
        // This shouldn't work if we use preparation since there no way to know which overload to use
        assertInvalid("SELECT v FROM %s WHERE k = "+KEYSPACE+".overloaded(?)", "foo");
        stopForcingPreparedValues();

        // but those should since we specifically cast
        assertEmpty(execute("SELECT v FROM %s WHERE k = "+KEYSPACE+".overloaded((text)?)", "foo"));
        assertRows(execute("SELECT v FROM %s WHERE k = "+KEYSPACE+".overloaded((int)?)", 3), row(1));
        assertEmpty(execute("SELECT v FROM %s WHERE k = "+KEYSPACE+".overloaded((ascii)?)", "foo"));
        // And since varchar == text, this should work too
        assertEmpty(execute("SELECT v FROM %s WHERE k = "+KEYSPACE+".overloaded((varchar)?)", "foo"));

        // no such functions exist...
        assertInvalid("DROP FUNCTION "+KEYSPACE+".overloaded(boolean)");
        assertInvalid("DROP FUNCTION "+KEYSPACE+".overloaded(bigint)");

        // 'overloaded' has multiple overloads - so it has to fail (CASSANDRA-7812)
        assertInvalid("DROP FUNCTION "+KEYSPACE+".overloaded");
        execute("DROP FUNCTION " + KEYSPACE + ".overloaded(varchar)");
        assertInvalid("SELECT v FROM %s WHERE k = " + KEYSPACE + ".overloaded((text)?)", "foo");
        execute("DROP FUNCTION " + KEYSPACE + ".overloaded(text, text)");
        assertInvalid("SELECT v FROM %s WHERE k = " + KEYSPACE + ".overloaded((text)?,(text)?)", "foo", "bar");
        execute("DROP FUNCTION " + KEYSPACE + ".overloaded(ascii)");
        assertInvalid("SELECT v FROM %s WHERE k = "+KEYSPACE+".overloaded((ascii)?)", "foo");
        // single-int-overload must still work
        assertRows(execute("SELECT v FROM %s WHERE k = " + KEYSPACE + ".overloaded((int)?)", 3), row(1));
        // overloaded has just one overload now - so the following DROP FUNCTION is not ambigious (CASSANDRA-7812)
        execute("DROP FUNCTION "+KEYSPACE+".overloaded");
    }

    @Test
    public void testCreateOrReplaceJavaFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);

        execute("create function "+KS_FOO+".corjf ( input double ) returns double language java\n" +
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
        assertRows(execute("SELECT key, val, "+KS_FOO+".corjf(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );

        execute("create or replace function "+KS_FOO+".corjf ( input double ) returns double language java\n" +
                "AS '\n" +
                "  return input;\n" +
                "';");

        // check if replaced function returns correct result
        assertRows(execute("SELECT key, val, "+KS_FOO+".corjf(val) FROM %s"),
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

        String cql = String.format("CREATE OR REPLACE FUNCTION %s.jfnpt() RETURNS bigint LANGUAGE JAVA\n" +
                     "AS '%s';", KEYSPACE, functionBody);

        execute(cql);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE keyspace_name=? AND function_name='jfnpt'", KEYSPACE),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, "+KEYSPACE+".jfnpt() FROM %s"),
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
            execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".jfinv() RETURNS bigint LANGUAGE JAVA\n" +
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
            execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".jfinv() RETURNS bigint LANGUAGE JAVA\n" +
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
        String functionBody = "\n" +
                              "  return Long.valueOf(1L);\n";

        String cql = "CREATE OR REPLACE FUNCTION jfir(val double) RETURNS double LANGUAGE JAVA\n" +
                     "AS '" + functionBody + "';";

        assertInvalid(cql);
    }

    @Test
    public void testJavaFunctionArgumentTypeMismatch() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val bigint)");

        String functionBody = "\n" +
                              "  return val;\n";

        String cql = "CREATE OR REPLACE FUNCTION "+KEYSPACE+".jft(val double) RETURNS double LANGUAGE JAVA\n" +
                     "AS '" + functionBody + "';";

        execute(cql);

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1L);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2L);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3L);
        assertInvalid("SELECT key, val, "+KEYSPACE+".jft(val) FROM %s");
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

        String cql = String.format("CREATE OR REPLACE FUNCTION %s.jft(val double) RETURNS double LANGUAGE JAVA\n" +
                     "AS '%s';", KEYSPACE, functionBody);

        execute(cql);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE keyspace_name=? AND function_name='jft'", KEYSPACE),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, " + KEYSPACE + ".jft(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );
    }

    @Test
    public void testFunctionInTargetKeyspace() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        execute("CREATE TABLE "+KS_FOO+".second_tab (key int primary key, val double)");

        String functionBody = "\n" +
                              "  return val;\n";

        String cql = "CREATE OR REPLACE FUNCTION "+KS_FOO+".jfitks(val double) RETURNS double LANGUAGE JAVA\n" +
                     "AS '" + functionBody + "';";

        execute(cql);

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertInvalid("SELECT key, val, " + KEYSPACE + ".jfitks(val) FROM %s");

        execute("INSERT INTO "+KS_FOO+".second_tab (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO "+KS_FOO+".second_tab (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO "+KS_FOO+".second_tab (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, jfitks(val) FROM " + KS_FOO + ".second_tab"),
                   row(1, 1d, 1d),
                   row(2, 2d, 2d),
                   row(3, 3d, 3d)
        );
    }

    @Test
    public void testFunctionWithReservedName() throws Throwable
    {
        execute("CREATE TABLE " + KS_FOO + ".second_tab (key int primary key, val double)");

        String cql = "CREATE OR REPLACE FUNCTION "+KS_FOO+".now() RETURNS timestamp LANGUAGE JAVA\n" +
                     "AS 'return null;';";

        execute(cql);

        execute("INSERT INTO "+KS_FOO+".second_tab (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO "+KS_FOO+".second_tab (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO "+KS_FOO+".second_tab (key, val) VALUES (?, ?)", 3, 3d);

        // ensure that system now() is executed
        UntypedResultSet rows = execute("SELECT key, val, now() FROM " + KS_FOO + ".second_tab");
        Assert.assertEquals(3, rows.size());
        UntypedResultSet.Row row = rows.iterator().next();
        Date ts = row.getTimestamp(row.getColumns().get(2).name.toString());
        Assert.assertNotNull(ts);

        // ensure that KS_FOO's now() is executed
        rows = execute("SELECT key, val, "+KS_FOO+".now() FROM " + KS_FOO + ".second_tab");
        Assert.assertEquals(3, rows.size());
        row = rows.iterator().next();
        Assert.assertFalse(row.has(row.getColumns().get(2).name.toString()));
    }

    @Test
    public void testFunctionInSystemKS() throws Throwable
    {
        execute("CREATE OR REPLACE FUNCTION " + KEYSPACE + ".dateof(val timeuuid) RETURNS timestamp LANGUAGE JAVA\n" +
                "AS 'return null;';");

        assertInvalid("CREATE OR REPLACE FUNCTION system.jnft(val double) RETURNS double LANGUAGE JAVA\n" +
                      "AS 'return null;';");
        assertInvalid("CREATE OR REPLACE FUNCTION system.dateof(val timeuuid) RETURNS timestamp LANGUAGE JAVA\n" +
                      "AS 'return null;';");
        assertInvalid("DROP FUNCTION system.now");

        // KS for executeInternal() is system
        assertInvalid("CREATE OR REPLACE FUNCTION jnft(val double) RETURNS double LANGUAGE JAVA\n" +
                      "AS 'return null;';");
        assertInvalid("CREATE OR REPLACE FUNCTION dateof(val timeuuid) RETURNS timestamp LANGUAGE JAVA\n" +
                      "AS 'return null;';");
        assertInvalid("DROP FUNCTION now");
    }

    @Test
    public void testFunctionNonExistingKeyspace() throws Throwable
    {
        String cql = "CREATE OR REPLACE FUNCTION this_ks_does_not_exist.jnft(val double) RETURNS double LANGUAGE JAVA\n" +
                     "AS 'return null;';";
        assertInvalid(cql);
    }

    @Test
    public void testFunctionAfterOnDropKeyspace() throws Throwable
    {
        dropKsFoo();

        String cql = "CREATE OR REPLACE FUNCTION "+KS_FOO+".jnft(val double) RETURNS double LANGUAGE JAVA\n" +
                     "AS 'return null;';";
        assertInvalid(cql);
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

        String cql = "CREATE OR REPLACE FUNCTION "+KS_FOO+".jnft(val double) RETURNS double LANGUAGE JAVA\n" +
                     "AS '" + functionBody + "';";

        execute(cql);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE keyspace_name='"+KS_FOO+"' AND function_name='jnft'"),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, "+KS_FOO+".jnft(val) FROM %s"),
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

        String cql = "CREATE OR REPLACE FUNCTION "+KS_FOO+".jrtef(val double) RETURNS double LANGUAGE JAVA\n" +
                     "AS '" + functionBody + "';";

        execute(cql);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE keyspace_name='"+KS_FOO+"' AND function_name='jrtef'"),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);

        // function throws a RuntimeException which is wrapped by InvalidRequestException
        assertInvalid("SELECT key, val, "+KS_FOO+".jrtef(val) FROM %s");
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
                              "  return \"'\"+Double.valueOf(v)+'\\\'';\n";

        execute("create function "+KS_FOO+".pgfun1 ( input double ) returns text language java\n" +
                "AS $$" + functionBody + "$$;");
        execute("CREATE FUNCTION "+KS_FOO+".pgsin ( input double ) RETURNS double LANGUAGE java AS $$return Double.valueOf(Math.sin(input.doubleValue()));$$");

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE keyspace_name='"+KS_FOO+"' AND function_name='pgfun1'"),
                   row("java", functionBody));
    }

    @Test
    public void testJavascriptFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = "\n" +
                              "  Math.sin(val);\n";

        String cql = "CREATE OR REPLACE FUNCTION "+KEYSPACE+".jsft(val double) RETURNS double LANGUAGE javascript\n" +
                     "AS '" + functionBody + "';";

        execute(cql);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE keyspace_name='"+KEYSPACE+"' AND function_name='jsft'"),
                   row("javascript", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, jsft(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );
    }

    @Test
    public void testJavascriptBadReturnType() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".jsft(val double) RETURNS double LANGUAGE javascript\n" +
                "AS '\"string\";';");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        // throws IRE with ClassCastException
        assertInvalid("SELECT key, val, jsft(val) FROM %s");
    }

    @Test
    public void testJavascriptThrow() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".jsft(val double) RETURNS double LANGUAGE javascript\n" +
                "AS 'throw \"fool\";';");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        // throws IRE with ScriptException
        assertInvalid("SELECT key, val, jsft(val) FROM %s");
    }

    @Test
    public void testDuplicateArgNames() throws Throwable
    {
        assertInvalid("CREATE OR REPLACE FUNCTION "+KEYSPACE+".scrinv(val double, val text) RETURNS text LANGUAGE javascript\n" +
                      "AS '\"foo bar\";';");
    }

    @Test
    public void testJavascriptCompileFailure() throws Throwable
    {
        assertInvalid("CREATE OR REPLACE FUNCTION "+KEYSPACE+".scrinv(val double) RETURNS double LANGUAGE javascript\n" +
                      "AS 'foo bar';");
    }

    @Test
    public void testScriptInvalidLanguage() throws Throwable
    {
        assertInvalid("CREATE OR REPLACE FUNCTION "+KEYSPACE+".scrinv(val double) RETURNS double LANGUAGE artificial_intelligence\n" +
                      "AS 'question for 42?';");
    }

    @Test
    public void testScriptReturnTypeCasting() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);

        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".js(val double) RETURNS boolean LANGUAGE javascript\n" +
                "AS 'true;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, true));
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".js(val double) RETURNS boolean LANGUAGE javascript\n" +
                "AS 'false;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, false));
        execute("DROP FUNCTION "+KEYSPACE+".js(double)");

        // declared rtype = int , return type = int
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".js(val double) RETURNS int LANGUAGE javascript\n" +
                "AS '100;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, 100));
        execute("DROP FUNCTION "+KEYSPACE+".js(double)");

        // declared rtype = int , return type = double
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".js(val double) RETURNS int LANGUAGE javascript\n" +
                "AS '100.;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, 100));
        execute("DROP FUNCTION "+KEYSPACE+".js(double)");

        // declared rtype = double , return type = int
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".js(val double) RETURNS double LANGUAGE javascript\n" +
                "AS '100;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, 100d));
        execute("DROP FUNCTION "+KEYSPACE+".js(double)");

        // declared rtype = double , return type = double
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".js(val double) RETURNS double LANGUAGE javascript\n" +
                "AS '100.;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, 100d));
        execute("DROP FUNCTION "+KEYSPACE+".js(double)");

        // declared rtype = bigint , return type = int
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".js(val double) RETURNS bigint LANGUAGE javascript\n" +
                "AS '100;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, 100L));
        execute("DROP FUNCTION "+KEYSPACE+".js(double)");

        // declared rtype = bigint , return type = double
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".js(val double) RETURNS bigint LANGUAGE javascript\n" +
                "AS '100.;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, 100L));
        execute("DROP FUNCTION "+KEYSPACE+".js(double)");

        // declared rtype = varint , return type = int
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".js(val double) RETURNS varint LANGUAGE javascript\n" +
                "AS '100;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, BigInteger.valueOf(100L)));
        execute("DROP FUNCTION "+KEYSPACE+".js(double)");

        // declared rtype = varint , return type = double
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".js(val double) RETURNS varint LANGUAGE javascript\n" +
                "AS '100.;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, BigInteger.valueOf(100L)));
        execute("DROP FUNCTION "+KEYSPACE+".js(double)");

        // declared rtype = decimal , return type = int
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".js(val double) RETURNS decimal LANGUAGE javascript\n" +
                "AS 'parseInt(\"100\");';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, BigDecimal.valueOf(100d)));
        execute("DROP FUNCTION "+KEYSPACE+".js(double)");

        // declared rtype = decimal , return type = double
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".js(val double) RETURNS decimal LANGUAGE javascript\n" +
                "AS '100.;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, BigDecimal.valueOf(100d)));
        execute("DROP FUNCTION "+KEYSPACE+".js(double)");
    }

    @Test
    public void testScriptParamReturnTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, ival int, lval bigint, fval float, dval double, vval varint, ddval decimal)");
        execute("INSERT INTO %s (key, ival, lval, fval, dval, vval, ddval) VALUES (?, ?, ?, ?, ?, ?, ?)", 1,
                1, 1L, 1f, 1d, BigInteger.valueOf(1L), BigDecimal.valueOf(1d));

        // type = int
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".jsint(val int) RETURNS int LANGUAGE javascript\n" +
                "AS 'val+1;';");
        assertRows(execute("SELECT key, ival, jsint(ival) FROM %s"),
                   row(1, 1, 2));
        execute("DROP FUNCTION "+KEYSPACE+".jsint(int)");

        // bigint
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".jsbigint(val bigint) RETURNS bigint LANGUAGE javascript\n" +
                "AS 'val+1;';");
        assertRows(execute("SELECT key, lval, jsbigint(lval) FROM %s"),
                   row(1, 1L, 2L));
        execute("DROP FUNCTION "+KEYSPACE+".jsbigint(bigint)");

        // float
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".jsfloat(val float) RETURNS float LANGUAGE javascript\n" +
                "AS 'val+1;';");
        assertRows(execute("SELECT key, fval, jsfloat(fval) FROM %s"),
                   row(1, 1f, 2f));
        execute("DROP FUNCTION "+KEYSPACE+".jsfloat(float)");

        // double
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".jsdouble(val double) RETURNS double LANGUAGE javascript\n" +
                "AS 'val+1;';");
        assertRows(execute("SELECT key, dval, jsdouble(dval) FROM %s"),
                   row(1, 1d, 2d));
        execute("DROP FUNCTION "+KEYSPACE+".jsdouble(double)");

        // varint
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".jsvarint(val varint) RETURNS varint LANGUAGE javascript\n" +
                "AS 'val+1;';");
        assertRows(execute("SELECT key, vval, jsvarint(vval) FROM %s"),
                   row(1, BigInteger.valueOf(1L), BigInteger.valueOf(2L)));
        execute("DROP FUNCTION "+KEYSPACE+".jsvarint(varint)");

        // decimal
        execute("CREATE OR REPLACE FUNCTION "+KEYSPACE+".jsdecimal(val decimal) RETURNS decimal LANGUAGE javascript\n" +
                "AS 'val+1;';");
        assertRows(execute("SELECT key, ddval, jsdecimal(ddval) FROM %s"),
                   row(1, BigDecimal.valueOf(1d), BigDecimal.valueOf(2d)));
        execute("DROP FUNCTION "+KEYSPACE+".jsdecimal(decimal)");
    }
}
