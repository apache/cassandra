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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;

public class UFTest extends CQLTester
{
    @Test
    public void testFunctionCreationAndDrop() throws Throwable
    {
        createTable("CREATE TABLE %s (key int PRIMARY KEY, d double)");

        execute("INSERT INTO %s(key, d) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s(key, d) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s(key, d) VALUES (?, ?)", 3, 3d);

        // simple creation
        execute("CREATE FUNCTION foo::sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");
        // check we can't recreate the same function
        assertInvalid("CREATE FUNCTION foo::sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");
        // but that it doesn't complay with "IF NOT EXISTS"
        execute("CREATE FUNCTION IF NOT EXISTS foo::sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        // Validate that it works as expected
        assertRows(execute("SELECT key, foo::sin(d) FROM %s"),
            row(1, Math.sin(1d)),
            row(2, Math.sin(2d)),
            row(3, Math.sin(3d))
        );

        // proper replacement
        execute("CREATE OR REPLACE FUNCTION foo::sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(42d);'");

        // Validate the method as been replaced
        assertRows(execute("SELECT key, foo::sin(d) FROM %s"),
            row(1, 42.0),
            row(2, 42.0),
            row(3, 42.0)
        );

        // same function but without namespace
        execute("CREATE FUNCTION sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");
        assertRows(execute("SELECT key, sin(d) FROM %s"),
            row(1, Math.sin(1d)),
            row(2, Math.sin(2d)),
            row(3, Math.sin(3d))
        );

        // Drop with and without namespace
        execute("DROP FUNCTION foo::sin");
        execute("DROP FUNCTION sin");

        // Drop unexisting function
        assertInvalid("DROP FUNCTION foo::sin");
        // but don't complain with "IF EXISTS"
        execute("DROP FUNCTION IF EXISTS foo::sin");

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

        execute("CREATE FUNCTION repeat (v text, n int) RETURNS text LANGUAGE java AS 'StringBuilder sb = new StringBuilder();\n" +
                "        for (int i = 0; i < n.intValue(); i++)\n" +
                "            sb.append(v);\n" +
                "        return sb.toString();'");

        assertRows(execute("SELECT v FROM %s WHERE v=repeat(?, ?)", "a", 3), row("aaa"));
        assertEmpty(execute("SELECT v FROM %s WHERE v=repeat(?, ?)", "a", 2));
    }

    @Test
    public void testFunctionOverloading() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v int)");

        execute("INSERT INTO %s(k, v) VALUES (?, ?)", "f2", 1);

        execute("CREATE FUNCTION overloaded(v varchar) RETURNS text LANGUAGE java AS 'return \"f1\";'");
        execute("CREATE OR REPLACE FUNCTION overloaded(i int) RETURNS text LANGUAGE java AS 'return \"f2\";'");
        execute("CREATE OR REPLACE FUNCTION overloaded(v1 text, v2 text) RETURNS text LANGUAGE java AS 'return \"f3\";'");
        execute("CREATE OR REPLACE FUNCTION overloaded(v ascii) RETURNS text LANGUAGE java AS 'return \"f1\";'");

        // text == varchar, so this should be considered as a duplicate
        assertInvalid("CREATE FUNCTION overloaded(v varchar) RETURNS text LANGUAGE java AS 'return \"f1\";'");

        assertRows(execute("SELECT overloaded(k), overloaded(v), overloaded(k, k) FROM %s"),
            row("f1", "f2", "f3")
        );

        forcePreparedValues();
        // This shouldn't work if we use preparation since there no way to know which overload to use
        assertInvalid("SELECT v FROM %s WHERE k = overloaded(?)", "foo");
        stopForcingPreparedValues();

        // but those should since we specifically cast
        assertEmpty(execute("SELECT v FROM %s WHERE k = overloaded((text)?)", "foo"));
        assertRows(execute("SELECT v FROM %s WHERE k = overloaded((int)?)", 3), row(1));
        assertEmpty(execute("SELECT v FROM %s WHERE k = overloaded((ascii)?)", "foo"));
        // And since varchar == text, this should work too
        assertEmpty(execute("SELECT v FROM %s WHERE k = overloaded((varchar)?)", "foo"));

        // no such functions exist...
        assertInvalid("DROP FUNCTION overloaded(boolean)");
        assertInvalid("DROP FUNCTION overloaded(bigint)");

        // 'overloaded' has multiple overloads - so it has to fail (CASSANDRA-7812)
        assertInvalid("DROP FUNCTION overloaded");
        execute("DROP FUNCTION overloaded(varchar)");
        assertInvalid("SELECT v FROM %s WHERE k = overloaded((text)?)", "foo");
        execute("DROP FUNCTION overloaded(text, text)");
        assertInvalid("SELECT v FROM %s WHERE k = overloaded((text)?,(text)?)", "foo", "bar");
        execute("DROP FUNCTION overloaded(ascii)");
        assertInvalid("SELECT v FROM %s WHERE k = overloaded((ascii)?)", "foo");
        // single-int-overload must still work
        assertRows(execute("SELECT v FROM %s WHERE k = overloaded((int)?)", 3), row(1));
        // overloaded has just one overload now - so the following DROP FUNCTION is not ambigious (CASSANDRA-7812)
        execute("DROP FUNCTION overloaded");
    }

    @Test
    public void testCreateOrReplaceJavaFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);

        execute("create function foo::corjf ( input double ) returns double language java\n" +
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
        assertRows(execute("SELECT key, val, foo::corjf(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );

        execute("create or replace function foo::corjf ( input double ) returns double language java\n" +
                "AS '\n" +
                "  return input;\n" +
                "';");

        // check if replaced function returns correct result
        assertRows(execute("SELECT key, val, foo::corjf(val) FROM %s"),
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

        String cql = "CREATE OR REPLACE FUNCTION jfnpt() RETURNS bigint LANGUAGE JAVA\n" +
                     "AS '" + functionBody + "';";

        execute(cql);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE namespace='' AND name='jfnpt'"),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, jfnpt() FROM %s"),
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
            execute("CREATE OR REPLACE FUNCTION jfinv() RETURNS bigint LANGUAGE JAVA\n" +
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
            execute("CREATE OR REPLACE FUNCTION jfinv() RETURNS bigint LANGUAGE JAVA\n" +
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

        String cql = "CREATE OR REPLACE FUNCTION jft(val double) RETURNS double LANGUAGE JAVA\n" +
                     "AS '" + functionBody + "';";

        execute(cql);

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1L);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2L);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3L);
        assertInvalid("SELECT key, val, jft(val) FROM %s");
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

        String cql = "CREATE OR REPLACE FUNCTION jft(val double) RETURNS double LANGUAGE JAVA\n" +
                     "AS '" + functionBody + "';";

        execute(cql);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE namespace='' AND name='jft'"),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, jft(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );
    }

    @Test
    public void testJavaNamespaceFunction() throws Throwable
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

        String cql = "CREATE OR REPLACE FUNCTION foo::jnft(val double) RETURNS double LANGUAGE JAVA\n" +
                     "AS '" + functionBody + "';";

        execute(cql);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE namespace='foo' AND name='jnft'"),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);
        assertRows(execute("SELECT key, val, foo::jnft(val) FROM %s"),
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

        String cql = "CREATE OR REPLACE FUNCTION foo::jrtef(val double) RETURNS double LANGUAGE JAVA\n" +
                     "AS '" + functionBody + "';";

        execute(cql);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE namespace='foo' AND name='jrtef'"),
                   row("java", functionBody));

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);

        // function throws a RuntimeException which is wrapped by InvalidRequestException
        assertInvalid("SELECT key, val, foo::jrtef(val) FROM %s");
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

        execute("create function foo::pgfun1 ( input double ) returns text language java\n" +
                "AS $$" + functionBody + "$$;");
        execute("CREATE FUNCTION foo::pgsin ( input double ) RETURNS double LANGUAGE java AS $$return Double.valueOf(Math.sin(input.doubleValue()));$$");

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE namespace='foo' AND name='pgfun1'"),
                   row("java", functionBody));
    }

    @Test
    public void testJavascriptFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        String functionBody = "\n" +
                              "  Math.sin(val);\n";

        String cql = "CREATE OR REPLACE FUNCTION jsft(val double) RETURNS double LANGUAGE javascript\n" +
                     "AS '" + functionBody + "';";

        execute(cql);

        assertRows(execute("SELECT language, body FROM system.schema_functions WHERE namespace='' AND name='jsft'"),
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

        execute("CREATE OR REPLACE FUNCTION jsft(val double) RETURNS double LANGUAGE javascript\n" +
                "AS '\"string\";';");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        // throws IRE with ClassCastException
        assertInvalid("SELECT key, val, jsft(val) FROM %s");
    }

    @Test
    public void testJavascriptThrow() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        execute("CREATE OR REPLACE FUNCTION jsft(val double) RETURNS double LANGUAGE javascript\n" +
                "AS 'throw \"fool\";';");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        // throws IRE with ScriptException
        assertInvalid("SELECT key, val, jsft(val) FROM %s");
    }

    @Test
    public void testDuplicateArgNames() throws Throwable
    {
        assertInvalid("CREATE OR REPLACE FUNCTION scrinv(val double, val text) RETURNS text LANGUAGE javascript\n" +
                      "AS '\"foo bar\";';");
    }

    @Test
    public void testJavascriptCompileFailure() throws Throwable
    {
        assertInvalid("CREATE OR REPLACE FUNCTION scrinv(val double) RETURNS double LANGUAGE javascript\n" +
                      "AS 'foo bar';");
    }

    @Test
    public void testScriptInvalidLanguage() throws Throwable
    {
        assertInvalid("CREATE OR REPLACE FUNCTION scrinv(val double) RETURNS double LANGUAGE artificial_intelligence\n" +
                      "AS 'question for 42?';");
    }

    @Test
    public void testScriptReturnTypeCasting() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);

        execute("CREATE OR REPLACE FUNCTION js(val double) RETURNS boolean LANGUAGE javascript\n" +
                "AS 'true;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, true));
        execute("CREATE OR REPLACE FUNCTION js(val double) RETURNS boolean LANGUAGE javascript\n" +
                "AS 'false;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, false));
        execute("DROP FUNCTION js(double)");

        // declared rtype = int , return type = int
        execute("CREATE OR REPLACE FUNCTION js(val double) RETURNS int LANGUAGE javascript\n" +
                "AS '100;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, 100));
        execute("DROP FUNCTION js(double)");

        // declared rtype = int , return type = double
        execute("CREATE OR REPLACE FUNCTION js(val double) RETURNS int LANGUAGE javascript\n" +
                "AS '100.;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, 100));
        execute("DROP FUNCTION js(double)");

        // declared rtype = double , return type = int
        execute("CREATE OR REPLACE FUNCTION js(val double) RETURNS double LANGUAGE javascript\n" +
                "AS '100;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, 100d));
        execute("DROP FUNCTION js(double)");

        // declared rtype = double , return type = double
        execute("CREATE OR REPLACE FUNCTION js(val double) RETURNS double LANGUAGE javascript\n" +
                "AS '100.;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, 100d));
        execute("DROP FUNCTION js(double)");

        // declared rtype = bigint , return type = int
        execute("CREATE OR REPLACE FUNCTION js(val double) RETURNS bigint LANGUAGE javascript\n" +
                "AS '100;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, 100L));
        execute("DROP FUNCTION js(double)");

        // declared rtype = bigint , return type = double
        execute("CREATE OR REPLACE FUNCTION js(val double) RETURNS bigint LANGUAGE javascript\n" +
                "AS '100.;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, 100L));
        execute("DROP FUNCTION js(double)");

        // declared rtype = varint , return type = int
        execute("CREATE OR REPLACE FUNCTION js(val double) RETURNS varint LANGUAGE javascript\n" +
                "AS '100;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, BigInteger.valueOf(100L)));
        execute("DROP FUNCTION js(double)");

        // declared rtype = varint , return type = double
        execute("CREATE OR REPLACE FUNCTION js(val double) RETURNS varint LANGUAGE javascript\n" +
                "AS '100.;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, BigInteger.valueOf(100L)));
        execute("DROP FUNCTION js(double)");

        // declared rtype = decimal , return type = int
        execute("CREATE OR REPLACE FUNCTION js(val double) RETURNS decimal LANGUAGE javascript\n" +
                "AS '100;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, BigDecimal.valueOf(100L)));
        execute("DROP FUNCTION js(double)");

        // declared rtype = decimal , return type = double
        execute("CREATE OR REPLACE FUNCTION js(val double) RETURNS decimal LANGUAGE javascript\n" +
                "AS '100.;';");
        assertRows(execute("SELECT key, val, js(val) FROM %s"),
                   row(1, 1d, BigDecimal.valueOf(100d)));
        execute("DROP FUNCTION js(double)");
    }

    @Test
    public void testScriptParamReturnTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, ival int, lval bigint, fval float, dval double, vval varint, ddval decimal)");
        execute("INSERT INTO %s (key, ival, lval, fval, dval, vval, ddval) VALUES (?, ?, ?, ?, ?, ?, ?)", 1,
                1, 1L, 1f, 1d, BigInteger.valueOf(1L), BigDecimal.valueOf(1d));

        // type = int
        execute("CREATE OR REPLACE FUNCTION jsint(val int) RETURNS int LANGUAGE javascript\n" +
                "AS 'val+1;';");
        assertRows(execute("SELECT key, ival, jsint(ival) FROM %s"),
                   row(1, 1, 2));
        execute("DROP FUNCTION jsint(int)");

        // bigint
        execute("CREATE OR REPLACE FUNCTION jsbigint(val bigint) RETURNS bigint LANGUAGE javascript\n" +
                "AS 'val+1;';");
        assertRows(execute("SELECT key, lval, jsbigint(lval) FROM %s"),
                   row(1, 1L, 2L));
        execute("DROP FUNCTION jsbigint(bigint)");

        // float
        execute("CREATE OR REPLACE FUNCTION jsfloat(val float) RETURNS float LANGUAGE javascript\n" +
                "AS 'val+1;';");
        assertRows(execute("SELECT key, fval, jsfloat(fval) FROM %s"),
                   row(1, 1f, 2f));
        execute("DROP FUNCTION jsfloat(float)");

        // double
        execute("CREATE OR REPLACE FUNCTION jsdouble(val double) RETURNS double LANGUAGE javascript\n" +
                "AS 'val+1;';");
        assertRows(execute("SELECT key, dval, jsdouble(dval) FROM %s"),
                   row(1, 1d, 2d));
        execute("DROP FUNCTION jsdouble(double)");

        // varint
        execute("CREATE OR REPLACE FUNCTION jsvarint(val varint) RETURNS varint LANGUAGE javascript\n" +
                "AS 'val+1;';");
        assertRows(execute("SELECT key, vval, jsvarint(vval) FROM %s"),
                   row(1, BigInteger.valueOf(1L), BigInteger.valueOf(2L)));
        execute("DROP FUNCTION jsvarint(varint)");

        // decimal
        execute("CREATE OR REPLACE FUNCTION jsdecimal(val decimal) RETURNS decimal LANGUAGE javascript\n" +
                "AS 'val+1;';");
        assertRows(execute("SELECT key, ddval, jsdecimal(ddval) FROM %s"),
                   row(1, BigDecimal.valueOf(1d), BigDecimal.valueOf(2d)));
        execute("DROP FUNCTION jsdecimal(decimal)");
    }
}
