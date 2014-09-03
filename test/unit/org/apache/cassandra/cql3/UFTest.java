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

import org.junit.Test;

public class UFTest extends CQLTester
{
    public static Double sin(Double val)
    {
        return val != null ? Math.sin(val) : null;
    }

    public static Float sin(Float val)
    {
        return val != null ? (float)Math.sin(val) : null;
    }

    public static Double badSin(Double val)
    {
        return 42.0;
    }

    public static String badSinBadReturn(Double val)
    {
        return "foo";
    }

    public Float nonStaticMethod(Float val)
    {
        return new Float(1.0);
    }

    private static Float privateMethod(Float val)
    {
        return new Float(1.0);
    }

    public static String repeat(String v, Integer n)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++)
            sb.append(v);
        return sb.toString();
    }

    public static String overloaded(String v)
    {
        return "f1";
    }

    public static String overloaded(Integer v)
    {
        return "f2";
    }

    public static String overloaded(String v1, String v2)
    {
        return "f3";
    }

    @Test
    public void testFunctionCreationAndDrop() throws Throwable
    {
        createTable("CREATE TABLE %s (key int PRIMARY KEY, d double)");

        execute("INSERT INTO %s(key, d) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s(key, d) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s(key, d) VALUES (?, ?)", 3, 3d);

        // creation with a bad class
        assertInvalid("CREATE FUNCTION foo::sin1 ( input double ) RETURNS double USING 'org.apache.cassandra.cql3.DoesNotExist#doesnotexist'");
        // and a good class but inexisting method
        assertInvalid("CREATE FUNCTION foo::sin2 ( input double ) RETURNS double USING 'org.apache.cassandra.cql3.UFTest#doesnotexist'");
        // with a non static method
        assertInvalid("CREATE FUNCTION foo::sin3 ( input float ) RETURNS float USING 'org.apache.cassandra.cql3.UFTest#nonStaticMethod'");
        // with a non public method
        assertInvalid("CREATE FUNCTION foo::sin4 ( input float ) RETURNS float USING 'org.apache.cassandra.cql3.UFTest#privateMethod'");

        // creation with bad argument types
        assertInvalid("CREATE FUNCTION foo::sin5 ( input text ) RETURNS double USING 'org.apache.cassandra.cql3.UFTest#sin'");
        // with bad return types
        assertInvalid("CREATE FUNCTION foo::sin6 ( input double ) RETURNS text USING 'org.apache.cassandra.cql3.UFTest#sin'");

        // simple creation
        execute("CREATE FUNCTION foo::sin ( input double ) RETURNS double USING 'org.apache.cassandra.cql3.UFTest#sin'");
        // check we can't recreate the same function
        assertInvalid("CREATE FUNCTION foo::sin ( input double ) RETURNS double USING 'org.apache.cassandra.cql3.UFTest#sin'");
        // but that it doesn't complay with "IF NOT EXISTS"
        execute("CREATE FUNCTION IF NOT EXISTS foo::sin ( input double ) RETURNS double USING 'org.apache.cassandra.cql3.UFTest#sin'");

        // Validate that it works as expected
        assertRows(execute("SELECT key, foo::sin(d) FROM %s"),
            row(1, Math.sin(1d)),
            row(2, Math.sin(2d)),
            row(3, Math.sin(3d))
        );

        // Replace the method with incompatible return type
        assertInvalid("CREATE OR REPLACE FUNCTION foo::sin ( input double ) RETURNS text USING 'org.apache.cassandra.cql3.UFTest#badSinBadReturn'");
        // proper replacement
        execute("CREATE OR REPLACE FUNCTION foo::sin ( input double ) RETURNS double USING 'org.apache.cassandra.cql3.UFTest#badSin'");

        // Validate the method as been replaced
        assertRows(execute("SELECT key, foo::sin(d) FROM %s"),
            row(1, 42.0),
            row(2, 42.0),
            row(3, 42.0)
        );

        // same function but without namespace
        execute("CREATE FUNCTION sin ( input double ) RETURNS double USING 'org.apache.cassandra.cql3.UFTest#sin'");
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
    }

    @Test
    public void testFunctionExecution() throws Throwable
    {
        createTable("CREATE TABLE %s (v text PRIMARY KEY)");

        execute("INSERT INTO %s(v) VALUES (?)", "aaa");

        execute("CREATE FUNCTION repeat (v text, n int) RETURNS text USING 'org.apache.cassandra.cql3.UFTest#repeat'");

        assertRows(execute("SELECT v FROM %s WHERE v=repeat(?, ?)", "a", 3), row("aaa"));
        assertEmpty(execute("SELECT v FROM %s WHERE v=repeat(?, ?)", "a", 2));
    }

    @Test
    public void testFunctionOverloading() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v int)");

        execute("INSERT INTO %s(k, v) VALUES (?, ?)", "f2", 1);

        execute("CREATE FUNCTION overloaded(v varchar) RETURNS text USING 'org.apache.cassandra.cql3.UFTest'");
        execute("CREATE OR REPLACE FUNCTION overloaded(i int) RETURNS text USING 'org.apache.cassandra.cql3.UFTest'");
        execute("CREATE OR REPLACE FUNCTION overloaded(v1 text, v2 text) RETURNS text USING 'org.apache.cassandra.cql3.UFTest'");
        execute("CREATE OR REPLACE FUNCTION overloaded(v ascii) RETURNS text USING 'org.apache.cassandra.cql3.UFTest'");

        // text == varchar, so this should be considered as a duplicate
        assertInvalid("CREATE FUNCTION overloaded(v varchar) RETURNS text USING 'org.apache.cassandra.cql3.UFTest'");

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
}
