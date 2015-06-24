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

import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.ByteOrderedPartitioner;

public class UserTypesTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.setPartitioner(new ByteOrderedPartitioner());
    }

    @Test
    public void testInvalidField() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (f int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<" + myType + ">)");

        // 's' is not a field of myType
        assertInvalid("INSERT INTO %s (k, v) VALUES (?, {s : ?})", 0, 1);
    }

    @Test
    public void testCassandra8105() throws Throwable
    {
        String ut1 = createType("CREATE TYPE %s (a int, b int)");
        String ut2 = createType("CREATE TYPE %s (j frozen<" + KEYSPACE + "." + ut1 + ">, k int)");
        createTable("CREATE TABLE %s (x int PRIMARY KEY, y set<frozen<" + KEYSPACE + "." + ut2 + ">>)");
        execute("INSERT INTO %s (x, y) VALUES (1, { { k: 1 } })");

        String ut3 = createType("CREATE TYPE %s (a int, b int)");
        String ut4 = createType("CREATE TYPE %s (j frozen<" + KEYSPACE + "." + ut3 + ">, k int)");
        createTable("CREATE TABLE %s (x int PRIMARY KEY, y list<frozen<" + KEYSPACE + "." + ut4 + ">>)");
        execute("INSERT INTO %s (x, y) VALUES (1, [ { k: 1 } ])");

        String ut5 = createType("CREATE TYPE %s (a int, b int)");
        String ut6 = createType("CREATE TYPE %s (i int, j frozen<" + KEYSPACE + "." + ut5 + ">)");
        createTable("CREATE TABLE %s (x int PRIMARY KEY, y set<frozen<" + KEYSPACE + "." + ut6 + ">>)");
        execute("INSERT INTO %s (x, y) VALUES (1, { { i: 1 } })");
    }

    @Test
    public void testFor7684() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (x double)");
        createTable("CREATE TABLE %s (k int, v frozen<" + myType + ">, b boolean static, PRIMARY KEY (k, v))");

        execute("INSERT INTO %s(k, v) VALUES (?, {x:?})", 1, -104.99251);
        execute("UPDATE %s SET b = ? WHERE k = ?", true, 1);

        assertRows(execute("SELECT v.x FROM %s WHERE k = ? AND v = {x:?}", 1, -104.99251),
            row(-104.99251)
        );

        flush();

        assertRows(execute("SELECT v.x FROM %s WHERE k = ? AND v = {x:?}", 1, -104.99251),
                   row(-104.99251)
        );
    }

    @Test
    public void testCreateInvalidTablesWithUDT() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (f int)");

        // Using a UDT without frozen shouldn't work
        assertInvalidMessage("Non-frozen User-Defined types are not supported, please use frozen<>",
                             "CREATE TABLE " + KEYSPACE + ".wrong (k int PRIMARY KEY, v " + KEYSPACE + '.' + myType + ")");

        assertInvalidMessage("Statement on keyspace " + KEYSPACE + " cannot refer to a user type in keyspace otherkeyspace;" +
                             " user types can only be used in the keyspace they are defined in",
                             "CREATE TABLE " + KEYSPACE + ".wrong (k int PRIMARY KEY, v frozen<otherKeyspace.myType>)");

        assertInvalidMessage("Unknown type " + KEYSPACE + ".unknowntype",
                             "CREATE TABLE " + KEYSPACE + ".wrong (k int PRIMARY KEY, v frozen<" + KEYSPACE + '.' + "unknownType>)");
    }

    @Test
    public void testAlterUDT() throws Throwable
    {
        String myType = KEYSPACE + '.' + createType("CREATE TYPE %s (a int)");
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<" + myType + ">)");
        execute("INSERT INTO %s (a, b) VALUES (1, {a: 1})");

        assertRows(execute("SELECT b.a FROM %s"), row(1));

        flush();

        execute("ALTER TYPE " + myType + " ADD b int");
        execute("INSERT INTO %s (a, b) VALUES (2, {a: 2, b :2})");

        assertRows(execute("SELECT b.a, b.b FROM %s"),
                   row(1, null),
                   row(2, 2));

        flush();

        assertRows(execute("SELECT b.a, b.b FROM %s"),
                   row(1, null),
                   row(2, 2));
    }

    @Test
    public void testAlteringUserTypeNestedWithinMap() throws Throwable
    {
        // test frozen and non-frozen collections
        String[] columnTypePrefixes = {"frozen<map<text, ", "map<text, frozen<"};
        for (String columnTypePrefix : columnTypePrefixes)
        {
            String ut1 = createType("CREATE TYPE %s (a int)");
            String columnType = columnTypePrefix + KEYSPACE + "." + ut1 + ">>";

            createTable("CREATE TABLE %s (x int PRIMARY KEY, y " + columnType + ")");

            execute("INSERT INTO %s (x, y) VALUES(1, {'firstValue':{a:1}})");
            assertRows(execute("SELECT * FROM %s"), row(1, map("firstValue", userType(1))));
            flush();

            execute("ALTER TYPE " + KEYSPACE + "." + ut1 + " ADD b int");
            execute("INSERT INTO %s (x, y) VALUES(2, {'secondValue':{a:2, b:2}})");
            execute("INSERT INTO %s (x, y) VALUES(3, {'thirdValue':{a:3}})");
            execute("INSERT INTO %s (x, y) VALUES(4, {'fourthValue':{b:4}})");

            assertRows(execute("SELECT * FROM %s"),
                    row(1, map("firstValue", userType(1))),
                    row(2, map("secondValue", userType(2, 2))),
                    row(3, map("thirdValue", userType(3, null))),
                    row(4, map("fourthValue", userType(null, 4))));

            flush();

            assertRows(execute("SELECT * FROM %s"),
                    row(1, map("firstValue", userType(1))),
                    row(2, map("secondValue", userType(2, 2))),
                    row(3, map("thirdValue", userType(3, null))),
                    row(4, map("fourthValue", userType(null, 4))));
        }
    }

    @Test
    public void testAlteringUserTypeNestedWithinSet() throws Throwable
    {
        // test frozen and non-frozen collections
        String[] columnTypePrefixes = {"frozen<set<", "set<frozen<"};
        for (String columnTypePrefix : columnTypePrefixes)
        {
            String ut1 = createType("CREATE TYPE %s (a int)");
            String columnType = columnTypePrefix + KEYSPACE + "." + ut1 + ">>";

            createTable("CREATE TABLE %s (x int PRIMARY KEY, y " + columnType + ")");

            execute("INSERT INTO %s (x, y) VALUES(1, {1} )");
            assertRows(execute("SELECT * FROM %s"), row(1, set(userType(1))));
            flush();

            execute("ALTER TYPE " + KEYSPACE + "." + ut1 + " ADD b int");
            execute("INSERT INTO %s (x, y) VALUES(2, {{a:2, b:2}})");
            execute("INSERT INTO %s (x, y) VALUES(3, {{a:3}})");
            execute("INSERT INTO %s (x, y) VALUES(4, {{b:4}})");

            assertRows(execute("SELECT * FROM %s"),
                    row(1, set(userType(1))),
                    row(2, set(userType(2, 2))),
                    row(3, set(userType(3, null))),
                    row(4, set(userType(null, 4))));

            flush();

            assertRows(execute("SELECT * FROM %s"),
                    row(1, set(userType(1))),
                    row(2, set(userType(2, 2))),
                    row(3, set(userType(3, null))),
                    row(4, set(userType(null, 4))));
        }
    }

    @Test
    public void testAlteringUserTypeNestedWithinList() throws Throwable
    {
        // test frozen and non-frozen collections
        String[] columnTypePrefixes = {"frozen<list<", "list<frozen<"};
        for (String columnTypePrefix : columnTypePrefixes)
        {
            String ut1 = createType("CREATE TYPE %s (a int)");
            String columnType = columnTypePrefix + KEYSPACE + "." + ut1 + ">>";

            createTable("CREATE TABLE %s (x int PRIMARY KEY, y " + columnType + ")");

            execute("INSERT INTO %s (x, y) VALUES(1, [1] )");
            assertRows(execute("SELECT * FROM %s"), row(1, list(userType(1))));
            flush();

            execute("ALTER TYPE " + KEYSPACE + "." + ut1 + " ADD b int");
            execute("INSERT INTO %s (x, y) VALUES(2, [{a:2, b:2}])");
            execute("INSERT INTO %s (x, y) VALUES(3, [{a:3}])");
            execute("INSERT INTO %s (x, y) VALUES(4, [{b:4}])");

            assertRows(execute("SELECT * FROM %s"),
                    row(1, list(userType(1))),
                    row(2, list(userType(2, 2))),
                    row(3, list(userType(3, null))),
                    row(4, list(userType(null, 4))));

            flush();

            assertRows(execute("SELECT * FROM %s"),
                    row(1, list(userType(1))),
                    row(2, list(userType(2, 2))),
                    row(3, list(userType(3, null))),
                    row(4, list(userType(null, 4))));
        }
    }

    @Test
    public void testAlteringUserTypeNestedWithinTuple() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");

        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<tuple<int, " + KEYSPACE + "." + type + ">>)");

        execute("INSERT INTO %s (a, b) VALUES(1, (1, {a:1, b:1}))");
        assertRows(execute("SELECT * FROM %s"), row(1, tuple(1, userType(1, 1))));
        flush();

        execute("ALTER TYPE " + KEYSPACE + "." + type + " ADD c int");
        execute("INSERT INTO %s (a, b) VALUES(2, (2, {a: 2, b: 2, c: 2}))");
        execute("INSERT INTO %s (a, b) VALUES(3, (3, {a: 3, b: 3}))");
        execute("INSERT INTO %s (a, b) VALUES(4, (4, {b:4}))");

        assertRows(execute("SELECT * FROM %s"),
                   row(1, tuple(1, userType(1, 1))),
                   row(2, tuple(2, userType(2, 2, 2))),
                   row(3, tuple(3, userType(3, 3, null))),
                   row(4, tuple(4, userType(null, 4, null))));

        flush();

        assertRows(execute("SELECT * FROM %s"),
                   row(1, tuple(1, userType(1, 1))),
                   row(2, tuple(2, userType(2, 2, 2))),
                   row(3, tuple(3, userType(3, 3, null))),
                   row(4, tuple(4, userType(null, 4, null))));
    }

    @Test
    public void testAlteringUserTypeNestedWithinNestedTuple() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");

        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<tuple<int, tuple<int, " + KEYSPACE + "." + type + ">>>)");

        execute("INSERT INTO %s (a, b) VALUES(1, (1, (1, {a:1, b:1})))");
        assertRows(execute("SELECT * FROM %s"), row(1, tuple(1, tuple(1, userType(1, 1)))));
        flush();

        execute("ALTER TYPE " + KEYSPACE + "." + type + " ADD c int");
        execute("INSERT INTO %s (a, b) VALUES(2, (2, (1, {a: 2, b: 2, c: 2})))");
        execute("INSERT INTO %s (a, b) VALUES(3, (3, (1, {a: 3, b: 3})))");
        execute("INSERT INTO %s (a, b) VALUES(4, (4, (1, {b:4})))");

        assertRows(execute("SELECT * FROM %s"),
                   row(1, tuple(1, tuple(1, userType(1, 1)))),
                   row(2, tuple(2, tuple(1, userType(2, 2, 2)))),
                   row(3, tuple(3, tuple(1, userType(3, 3, null)))),
                   row(4, tuple(4, tuple(1, userType(null, 4, null)))));

        flush();

        assertRows(execute("SELECT * FROM %s"),
                   row(1, tuple(1, tuple(1, userType(1, 1)))),
                   row(2, tuple(2, tuple(1, userType(2, 2, 2)))),
                   row(3, tuple(3, tuple(1, userType(3, 3, null)))),
                   row(4, tuple(4, tuple(1, userType(null, 4, null)))));
    }

    @Test
    public void testAlteringUserTypeNestedWithinUserType() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");
        String otherType = createType("CREATE TYPE %s (x frozen<" + KEYSPACE + "." + type + ">)");

        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<" + KEYSPACE + "." + otherType + ">)");

        execute("INSERT INTO %s (a, b) VALUES(1, {x: {a:1, b:1}})");
        assertRows(execute("SELECT b.x.a, b.x.b FROM %s"), row(1, 1));
        flush();

        execute("ALTER TYPE " + KEYSPACE + "." + type + " ADD c int");
        execute("INSERT INTO %s (a, b) VALUES(2, {x: {a: 2, b: 2, c: 2}})");
        execute("INSERT INTO %s (a, b) VALUES(3, {x: {a: 3, b: 3}})");
        execute("INSERT INTO %s (a, b) VALUES(4, {x: {b:4}})");

        assertRows(execute("SELECT b.x.a, b.x.b, b.x.c FROM %s"),
                   row(1, 1, null),
                   row(2, 2, 2),
                   row(3, 3, null),
                   row(null, 4, null));

        flush();

        assertRows(execute("SELECT b.x.a, b.x.b, b.x.c FROM %s"),
                   row(1, 1, null),
                   row(2, 2, 2),
                   row(3, 3, null),
                   row(null, 4, null));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.user_types_test()
     */
    @Test
    public void testUserTypes() throws Throwable
    {
        UUID userID_1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");

        String addressType = createType("CREATE TYPE %s (street text, city text, zip_code int, phones set<text >)");

        String nameType = createType("CREATE TYPE %s (firstname text, lastname text)");

        createTable("CREATE TABLE %s (id uuid PRIMARY KEY, name frozen < " + nameType + " >, addresses map < text, frozen < " + addressType + " >> )");

        execute("INSERT INTO %s (id, name) VALUES(?, { firstname: 'Paul', lastname: 'smith' } )", userID_1);

        assertRows(execute("SELECT name.firstname FROM %s WHERE id = ?", userID_1), row("Paul"));

        execute("UPDATE %s SET addresses = addresses + { 'home': { street: '...', city:'SF', zip_code:94102, phones:{ } } } WHERE id = ?", userID_1);

        // TODO: deserialize the value here and check it 's right.
        execute("SELECT addresses FROM %s WHERE id = ? ", userID_1);
    }

    /**
     * Test user type test that does a little more nesting,
     * migrated from cql_tests.py:TestCQL.more_user_types_test()
     */
    @Test
    public void testNestedUserTypes() throws Throwable
    {
        String type1 = createType("CREATE TYPE %s ( s set<text>, m map<text, text>, l list<text>)");

        String type2 = createType("CREATE TYPE %s ( s set < frozen < " + type1 + " >>,)");

        createTable("CREATE TABLE %s (id int PRIMARY KEY, val frozen<" + type2 + ">)");

        execute("INSERT INTO %s (id, val) VALUES (0, { s : {{ s : {'foo', 'bar'}, m : { 'foo' : 'bar' }, l : ['foo', 'bar']} }})");

        // TODO: check result once we have an easy way to do it. For now we just check it doesn't crash
        execute("SELECT * FROM %s");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.add_field_to_udt_test()
     */
    @Test
    public void testAddFieldToUdt() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (fooint int, fooset set <text>)");
        createTable("CREATE TABLE %s (key int PRIMARY KEY, data frozen <" + typeName + ">)");

        execute("INSERT INTO %s (key, data) VALUES (1, {fooint: 1, fooset: {'2'}})");
        execute("ALTER TYPE " + keyspace() + "." + typeName + " ADD foomap map <int,text>");
        execute("INSERT INTO %s (key, data) VALUES (1, {fooint: 1, fooset: {'2'}, foomap: {3 : 'bar'}})");
    }
}
