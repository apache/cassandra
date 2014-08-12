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

import org.apache.cassandra.exceptions.InvalidRequestException;

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

    public Float nonStaticMethod(Float val)
    {
        return new Float(1.0);
    }

    private static Float privateMethod(Float val)
    {
        return new Float(1.0);
    }

    @Test
    public void ddlCreateFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)"); // not used, but required by CQLTester

        execute("create function foo::cf ( input double ) returns double 'org.apache.cassandra.cql3.UFTest#sin'");
        execute("drop function foo::cf");
    }

    @Test(expected = InvalidRequestException.class)
    public void ddlCreateFunctionFail() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)"); // not used, but required by CQLTester

        execute("create function foo::cff ( input double ) returns double 'org.apache.cassandra.cql3.UFTest#sin'");
        execute("create function foo::cff ( input double ) returns double 'org.apache.cassandra.cql3.UFTest#sin'");
    }


    @Test
    public void ddlCreateIfNotExistsFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)"); // not used, but required by CQLTester

        execute("create function if not exists foo::cfine ( input double ) returns double 'org.apache.cassandra.cql3.UFTest#sin'");
        execute("drop function foo::cfine");
    }


    @Test(expected = InvalidRequestException.class)
    public void ddlCreateFunctionBadClass() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)"); // not used, but required by CQLTester
        execute("create function foo::cff ( input double ) returns double 'org.apache.cassandra.cql3.DoesNotExist#doesnotexist'");
    }

    @Test(expected = InvalidRequestException.class)
    public void ddlCreateFunctionBadMethod() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)"); // not used, but required by CQLTester
        execute("create function foo::cff ( input double ) returns double 'org.apache.cassandra.cql3.UFTest#doesnotexist'");
    }

    @Test(expected = InvalidRequestException.class)
    public void ddlCreateFunctionBadArgType() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)"); // not used, but required by CQLTester
        execute("create function foo::cff ( input text ) returns double 'org.apache.cassandra.cql3.UFTest#sin'");
    }

    @Test(expected = InvalidRequestException.class)
    public void ddlCreateFunctionBadReturnType() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)"); // not used, but required by CQLTester
        execute("create function foo::cff ( input double ) returns text 'org.apache.cassandra.cql3.UFTest#sin'");
    }

    @Test(expected = InvalidRequestException.class)
    public void ddlCreateFunctionNonStaticMethod() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)"); // not used, but required by CQLTester
        execute("create function foo::cff ( input float ) returns float 'org.apache.cassandra.cql3.UFTest#nonStaticMethod'");
    }

    @Test(expected = InvalidRequestException.class)
    public void ddlCreateFunctionNonPublicMethod() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)"); // not used, but required by CQLTester
        execute("create function foo::cff ( input float ) returns float 'org.apache.cassandra.cql3.UFTest#privateMethod'");
    }

    @Test(expected = InvalidRequestException.class)
    public void ddlCreateIfNotExistsFunctionFail() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)"); // not used, but required by CQLTester

        execute("create function if not exists foo::cfinef ( input double ) returns double 'org.apache.cassandra.cql3.UFTest#sin'");
        execute("create function if not exists foo::cfinef ( input double ) returns double 'org.apache.cassandra.cql3.UFTest#sin'");
    }

    @Test
    public void ddlCreateOrReplaceFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)"); // not used, but required by CQLTester

        execute("create function foo::corf ( input double ) returns double 'org.apache.cassandra.cql3.UFTest#sin'");
        execute("create or replace function foo::corf ( input double ) returns double 'org.apache.cassandra.cql3.UFTest#sin'");
    }

    @Test(expected = InvalidRequestException.class)
    public void ddlDropNonExistingFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)"); // not used, but required by CQLTester

        execute("drop function foo::dnef");
    }

    @Test
    public void ddlDropIfExistsNonExistingFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)"); // not used, but required by CQLTester

        execute("drop function if exists foo::dienef");
    }

    @Test
    public void namespaceUserFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        execute("create or replace function math::sin ( input double ) returns double 'org.apache.cassandra.cql3.UFTest'");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);

        assertRows(execute("SELECT key, val, math::sin(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );
    }

    @Test
    public void nonNamespaceUserFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, val double)");

        execute("create or replace function sin ( input double ) returns double 'org.apache.cassandra.cql3.UFTest'");

        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 1, 1d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 2, 2d);
        execute("INSERT INTO %s (key, val) VALUES (?, ?)", 3, 3d);

        assertRows(execute("SELECT key, val, sin(val) FROM %s"),
                   row(1, 1d, Math.sin(1d)),
                   row(2, 2d, Math.sin(2d)),
                   row(3, 3d, Math.sin(3d))
        );
    }
}
