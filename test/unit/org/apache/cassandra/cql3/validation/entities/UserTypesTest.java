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

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.StorageService;

public class UserTypesTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()     // overrides CQLTester.setUpClass()
    {
        // Selecting partitioner for a table is not exposed on CREATE TABLE.
        StorageService.instance.setPartitionerUnsafe(ByteOrderedPartitioner.instance);

        prepareServer();
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
    public void testInvalidInputForUserType() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (f int)");
        createTable("CREATE TABLE %s(pk int PRIMARY KEY, t frozen<" + myType + ">)");
        assertInvalidMessage("Not enough bytes to read 0th field f",
                             "INSERT INTO %s (pk, t) VALUES (?, ?)", 1, "test");
        assertInvalidMessage("Not enough bytes to read 0th field f",
                             "INSERT INTO %s (pk, t) VALUES (?, ?)", 1, Long.MAX_VALUE);

        String type = createType("CREATE TYPE %s (a int, b tuple<int, text, double>)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t frozen<" + type + ">)");
        assertInvalidMessage("Invalid remaining data after end of tuple value",
                             "INSERT INTO %s (k, t) VALUES (0, ?)",
                             userType("a", 1, "b", tuple(1, "1", 1.0, 1)));

        assertInvalidMessage("Invalid user type literal for t: field b is not of type frozen<tuple<int, text, double>>",
                             "INSERT INTO %s (k, t) VALUES (0, {a: 1, b: (1, '1', 1.0, 1)})");
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

        beforeAndAfterFlush(() ->
            assertRows(execute("SELECT v.x FROM %s WHERE k = ? AND v = {x:?}", 1, -104.99251),
                row(-104.99251)
            )
        );
    }

    @Test
    public void testInvalidUDTStatements() throws Throwable
    {
        String typename = createType("CREATE TYPE %s (a int)");
        String myType = KEYSPACE + '.' + typename;

        // non-frozen UDTs in a table PK
        assertInvalidMessage("Invalid non-frozen user-defined type for PRIMARY KEY component k",
                "CREATE TABLE " + KEYSPACE + ".wrong (k " + myType + " PRIMARY KEY , v int)");
        assertInvalidMessage("Invalid non-frozen user-defined type for PRIMARY KEY component k2",
                "CREATE TABLE " + KEYSPACE + ".wrong (k1 int, k2 " + myType + ", v int, PRIMARY KEY (k1, k2))");

        // non-frozen UDTs in a collection
        assertInvalidMessage("Non-frozen UDTs are not allowed inside collections: list<" + myType + ">",
                "CREATE TABLE " + KEYSPACE + ".wrong (k int PRIMARY KEY, v list<" + myType + ">)");
        assertInvalidMessage("Non-frozen UDTs are not allowed inside collections: set<" + myType + ">",
                "CREATE TABLE " + KEYSPACE + ".wrong (k int PRIMARY KEY, v set<" + myType + ">)");
        assertInvalidMessage("Non-frozen UDTs are not allowed inside collections: map<" + myType + ", int>",
                "CREATE TABLE " + KEYSPACE + ".wrong (k int PRIMARY KEY, v map<" + myType + ", int>)");
        assertInvalidMessage("Non-frozen UDTs are not allowed inside collections: map<int, " + myType + ">",
                "CREATE TABLE " + KEYSPACE + ".wrong (k int PRIMARY KEY, v map<int, " + myType + ">)");

        // non-frozen UDT in a collection (as part of a UDT definition)
        assertInvalidMessage("Non-frozen UDTs are not allowed inside collections: list<" + myType + ">",
                "CREATE TYPE " + KEYSPACE + ".wrong (a int, b list<" + myType + ">)");

        // non-frozen UDT in a UDT
        assertInvalidMessage("A user type cannot contain non-frozen UDTs",
                "CREATE TYPE " + KEYSPACE + ".wrong (a int, b " + myType + ")");

        // referencing a UDT in another keyspace
        assertInvalidMessage("Statement on keyspace " + KEYSPACE + " cannot refer to a user type in keyspace otherkeyspace;" +
                             " user types can only be used in the keyspace they are defined in",
                             "CREATE TABLE " + KEYSPACE + ".wrong (k int PRIMARY KEY, v frozen<otherKeyspace.myType>)");

        // referencing an unknown UDT
        assertInvalidMessage("Unknown type " + KEYSPACE + ".unknowntype",
                             "CREATE TABLE " + KEYSPACE + ".wrong (k int PRIMARY KEY, v frozen<" + KEYSPACE + '.' + "unknownType>)");

        // bad deletions on frozen UDTs
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<" + myType + ">, c int)");
        assertInvalidMessage("Frozen UDT column b does not support field deletion", "DELETE b.a FROM %s WHERE a = 0");
        assertInvalidMessage("Invalid field deletion operation for non-UDT column c", "DELETE c.a FROM %s WHERE a = 0");

        // bad updates on frozen UDTs
        assertInvalidMessage("Invalid operation (b.a = 0) for frozen UDT column b", "UPDATE %s SET b.a = 0 WHERE a = 0");
        assertInvalidMessage("Invalid operation (c.a = 0) for non-UDT column c", "UPDATE %s SET c.a = 0 WHERE a = 0");

        // bad deletions on non-frozen UDTs
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b " + myType + ", c int)");
        assertInvalidMessage("UDT column b does not have a field named foo", "DELETE b.foo FROM %s WHERE a = 0");

        // bad updates on non-frozen UDTs
        assertInvalidMessage("UDT column b does not have a field named foo", "UPDATE %s SET b.foo = 0 WHERE a = 0");

        // bad insert on non-frozen UDTs
        assertInvalidMessage("Unknown field 'foo' in value of user defined type", "INSERT INTO %s (a, b, c) VALUES (0, {a: 0, foo: 0}, 0)");
        if (usePrepared())
        {
            assertInvalidMessage("Invalid remaining data after end of UDT value",
                    "INSERT INTO %s (a, b, c) VALUES (0, ?, 0)", userType("a", 0, "foo", 0));
        }
        else
        {
            assertInvalidMessage("Unknown field 'foo' in value of user defined type " + typename,
                    "INSERT INTO %s (a, b, c) VALUES (0, ?, 0)", userType("a", 0, "foo", 0));
        }

        // non-frozen UDT with non-frozen nested collection
        String typename2 = createType("CREATE TYPE %s (bar int, foo list<int>)");
        String myType2 = KEYSPACE + '.' + typename2;
        assertInvalidMessage("Non-frozen UDTs with nested non-frozen collections are not supported",
                "CREATE TABLE " + KEYSPACE + ".wrong (k int PRIMARY KEY, v " + myType2 + ")");
    }

    @Test
    public void testAlterUDT() throws Throwable
    {
        String myType = KEYSPACE + '.' + createType("CREATE TYPE %s (a int)");
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<" + myType + ">)");
        execute("INSERT INTO %s (a, b) VALUES (1, ?)", userType("a", 1));

        assertRows(execute("SELECT b.a FROM %s"), row(1));

        flush();

        schemaChange("ALTER TYPE " + myType + " ADD b int");
        execute("INSERT INTO %s (a, b) VALUES (2, ?)", userType("a", 2, "b", 2));

        beforeAndAfterFlush(() ->
            assertRows(execute("SELECT b.a, b.b FROM %s"),
                       row(1, null),
                       row(2, 2))
        );
    }

    @Test
    public void testAlterNonFrozenUDT() throws Throwable
    {
        String myType = KEYSPACE + '.' + createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + myType + ")");
        execute("INSERT INTO %s (k, v) VALUES (0, ?)", userType("a", 1, "b", "abc"));

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT v FROM %s"), row(userType("a", 1, "b", "abc")));
            assertRows(execute("SELECT v.a FROM %s"), row(1));
            assertRows(execute("SELECT v.b FROM %s"), row("abc"));
        });

        schemaChange("ALTER TYPE " + myType + " RENAME b TO foo");
        assertRows(execute("SELECT v FROM %s"), row(userType("a", 1, "b", "abc")));
        assertRows(execute("SELECT v.a FROM %s"), row(1));
        assertRows(execute("SELECT v.foo FROM %s"), row("abc"));

        execute("UPDATE %s SET v.foo = 'def' WHERE k = 0");
        assertRows(execute("SELECT v FROM %s"), row(userType("a", 1, "foo", "def")));
        assertRows(execute("SELECT v.a FROM %s"), row(1));
        assertRows(execute("SELECT v.foo FROM %s"), row("def"));

        execute("INSERT INTO %s (k, v) VALUES (0, ?)", userType("a", 2, "foo", "def"));
        assertRows(execute("SELECT v FROM %s"), row(userType("a", 2, "foo", "def")));
        assertRows(execute("SELECT v.a FROM %s"), row(2));
        assertRows(execute("SELECT v.foo FROM %s"), row("def"));

        schemaChange("ALTER TYPE " + myType + " ADD c int");
        assertRows(execute("SELECT v FROM %s"), row(userType("a", 2, "foo", "def", "c", null)));
        assertRows(execute("SELECT v.a FROM %s"), row(2));
        assertRows(execute("SELECT v.foo FROM %s"), row("def"));
        assertRows(execute("SELECT v.c FROM %s"), row(new Object[] {null}));

        execute("INSERT INTO %s (k, v) VALUES (0, ?)", userType("a", 3, "foo", "abc", "c", 0));
        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT v FROM %s"), row(userType("a", 3, "foo", "abc", "c", 0)));
            assertRows(execute("SELECT v.c FROM %s"), row(0));
        });
    }

    @Test
    public void testUDTWithUnsetValues() throws Throwable
    {
        // set up
        String myType = createType("CREATE TYPE %s (x int, y int)");
        String myOtherType = createType("CREATE TYPE %s (a frozen<" + myType + ">)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<" + myType + ">, z frozen<" + myOtherType + ">)");

        if (usePrepared())
        {
            assertInvalidMessage("Invalid unset value for field 'y' of user defined type " + myType,
                    "INSERT INTO %s (k, v) VALUES (10, {x:?, y:?})", 1, unset());

            assertInvalidMessage("Invalid unset value for field 'y' of user defined type " + myType,
                    "INSERT INTO %s (k, v, z) VALUES (10, {x:?, y:?}, {a:{x: ?, y: ?}})", 1, 1, 1, unset());
        }
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

            execute("INSERT INTO %s (x, y) VALUES(1, ?)", map("firstValue", userType("a", 1)));
            assertRows(execute("SELECT * FROM %s"), row(1, map("firstValue", userType("a", 1))));
            flush();

            execute("ALTER TYPE " + KEYSPACE + "." + ut1 + " ADD b int");
            execute("INSERT INTO %s (x, y) VALUES(2, ?)", map("secondValue", userType("a", 2, "b", 2)));
            execute("INSERT INTO %s (x, y) VALUES(3, ?)", map("thirdValue", userType("a", 3, "b", null)));
            execute("INSERT INTO %s (x, y) VALUES(4, ?)", map("fourthValue", userType("a", null, "b", 4)));

            beforeAndAfterFlush(() ->
                assertRows(execute("SELECT * FROM %s"),
                        row(1, map("firstValue", userType("a", 1))),
                        row(2, map("secondValue", userType("a", 2, "b", 2))),
                        row(3, map("thirdValue", userType("a", 3, "b", null))),
                        row(4, map("fourthValue", userType("a", null, "b", 4))))
            );
        }
    }

    @Test
    public void testAlteringUserTypeNestedWithinNonFrozenMap() throws Throwable
    {
        String ut1 = createType("CREATE TYPE %s (a int)");
        String columnType = KEYSPACE + "." + ut1;

        createTable("CREATE TABLE %s (x int PRIMARY KEY, y map<text, frozen<" + columnType + ">>)");

        execute("INSERT INTO %s (x, y) VALUES(1, {'firstValue': {a: 1}})");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, map("firstValue", userType("a", 1))));

        flush();

        execute("ALTER TYPE " + columnType + " ADD b int");
        execute("UPDATE %s SET y['secondValue'] = {a: 2, b: 2} WHERE x = 1");

        beforeAndAfterFlush(() ->
                            assertRows(execute("SELECT * FROM %s"),
                                       row(1, map("firstValue", userType("a", 1),
                                                  "secondValue", userType("a", 2, "b", 2))))
        );
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

            execute("INSERT INTO %s (x, y) VALUES(1, ?)", set(userType("a", 1)));
            assertRows(execute("SELECT * FROM %s"), row(1, set(userType("a", 1))));
            flush();

            execute("ALTER TYPE " + KEYSPACE + "." + ut1 + " ADD b int");
            execute("INSERT INTO %s (x, y) VALUES(2, ?)", set(userType("a", 2, "b", 2)));
            execute("INSERT INTO %s (x, y) VALUES(3, ?)", set(userType("a", 3, "b", null)));
            execute("INSERT INTO %s (x, y) VALUES(4, ?)", set(userType("a", null, "b", 4)));

            beforeAndAfterFlush(() ->
                assertRows(execute("SELECT * FROM %s"),
                        row(1, set(userType("a", 1))),
                        row(2, set(userType("a", 2, "b", 2))),
                        row(3, set(userType("a", 3, "b", null))),
                        row(4, set(userType("a", null, "b", 4))))
            );
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

            execute("INSERT INTO %s (x, y) VALUES(1, ?)", list(userType("a", 1)));
            assertRows(execute("SELECT * FROM %s"), row(1, list(userType("a", 1))));
            flush();

            execute("ALTER TYPE " + KEYSPACE + "." + ut1 + " ADD b int");
            execute("INSERT INTO %s (x, y) VALUES (2, ?)", list(userType("a", 2, "b", 2)));
            execute("INSERT INTO %s (x, y) VALUES (3, ?)", list(userType("a", 3, "b", null)));
            execute("INSERT INTO %s (x, y) VALUES (4, ?)", list(userType("a", null, "b", 4)));

            beforeAndAfterFlush(() ->
                assertRows(execute("SELECT * FROM %s"),
                        row(1, list(userType("a", 1))),
                        row(2, list(userType("a", 2, "b", 2))),
                        row(3, list(userType("a", 3, "b", null))),
                        row(4, list(userType("a", null, "b", 4))))
            );
        }
    }

    @Test
    public void testAlteringUserTypeNestedWithinTuple() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");

        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<tuple<int, " + KEYSPACE + "." + type + ">>)");

        execute("INSERT INTO %s (a, b) VALUES(1, (1, ?))", userType("a", 1, "b", 1));
        assertRows(execute("SELECT * FROM %s"), row(1, tuple(1, userType("a", 1, "b", 1))));
        flush();

        execute("ALTER TYPE " + KEYSPACE + "." + type + " ADD c int");
        execute("INSERT INTO %s (a, b) VALUES (2, (2, ?))", userType("a", 2, "b", 2, "c", 2));
        execute("INSERT INTO %s (a, b) VALUES (3, (3, ?))", userType("a", 3, "b", 3, "c", null));
        execute("INSERT INTO %s (a, b) VALUES (4, (4, ?))", userType("a", null, "b", 4, "c", null));

        beforeAndAfterFlush(() ->
            assertRows(execute("SELECT * FROM %s"),
                    row(1, tuple(1, userType("a", 1, "b", 1))),
                    row(2, tuple(2, userType("a", 2, "b", 2, "c", 2))),
                    row(3, tuple(3, userType("a", 3, "b", 3, "c", null))),
                    row(4, tuple(4, userType("a", null, "b", 4, "c", null))))
        );
    }

    @Test
    public void testAlteringUserTypeNestedWithinNestedTuple() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");

        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<tuple<int, tuple<int, " + KEYSPACE + "." + type + ">>>)");

        execute("INSERT INTO %s (a, b) VALUES(1, (1, (1, ?)))", userType("a", 1, "b", 1));
        assertRows(execute("SELECT * FROM %s"), row(1, tuple(1, tuple(1, userType("a", 1, "b", 1)))));
        flush();

        execute("ALTER TYPE " + KEYSPACE + "." + type + " ADD c int");
        execute("INSERT INTO %s (a, b) VALUES(2, (2, (1, ?)))", userType("a", 2, "b", 2, "c", 2));
        execute("INSERT INTO %s (a, b) VALUES(3, (3, ?))", tuple(1, userType("a", 3, "b", 3, "c", null)));
        execute("INSERT INTO %s (a, b) VALUES(4, ?)", tuple(4, tuple(1, userType("a", null, "b", 4, "c", null))));

        beforeAndAfterFlush(() ->
            assertRows(execute("SELECT * FROM %s"),
                    row(1, tuple(1, tuple(1, userType("a", 1, "b", 1)))),
                    row(2, tuple(2, tuple(1, userType("a", 2, "b", 2, "c", 2)))),
                    row(3, tuple(3, tuple(1, userType("a", 3, "b", 3, "c", null)))),
                    row(4, tuple(4, tuple(1, userType("a", null, "b", 4, "c", null)))))
        );
    }

    @Test
    public void testAlteringUserTypeNestedWithinUserType() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int, b int)");
        String otherType = createType("CREATE TYPE %s (x frozen<" + KEYSPACE + "." + type + ">)");

        createTable("CREATE TABLE %s (a int PRIMARY KEY, b frozen<" + KEYSPACE + "." + otherType + ">)");

        execute("INSERT INTO %s (a, b) VALUES(1, {x: ?})", userType("a", 1, "b", 1));
        assertRows(execute("SELECT b.x.a, b.x.b FROM %s"), row(1, 1));
        execute("INSERT INTO %s (a, b) VALUES(1, ?)", userType("x", userType("a", 1, "b", 1)));
        assertRows(execute("SELECT b.x.a, b.x.b FROM %s"), row(1, 1));
        flush();

        execute("ALTER TYPE " + KEYSPACE + "." + type + " ADD c int");
        execute("INSERT INTO %s (a, b) VALUES(2, {x: ?})", userType("a", 2, "b", 2, "c", 2));
        execute("INSERT INTO %s (a, b) VALUES(3, {x: ?})", userType("a", 3, "b", 3));
        execute("INSERT INTO %s (a, b) VALUES(4, {x: ?})", userType("a", null, "b", 4));

        beforeAndAfterFlush(() ->
            assertRows(execute("SELECT b.x.a, b.x.b, b.x.c FROM %s"),
                       row(1, 1, null),
                       row(2, 2, 2),
                       row(3, 3, null),
                       row(null, 4, null))
        );
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

        execute("INSERT INTO %s (id, val) VALUES (0, ?)",
                userType("s", set(userType("s", set("foo", "bar"), "m", map("foo", "bar"), "l", list("foo", "bar")))));

        assertRows(execute("SELECT * FROM %s"),
                row(0, userType("s", set(userType("s", set("foo", "bar"), "m", map("foo", "bar"), "l", list("foo", "bar"))))));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.add_field_to_udt_test()
     */
    @Test
    public void testAddFieldToUdt() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (fooint int, fooset set <text>)");
        createTable("CREATE TABLE %s (key int PRIMARY KEY, data frozen <" + typeName + ">)");

        execute("INSERT INTO %s (key, data) VALUES (1, ?)", userType("fooint", 1, "fooset", set("2")));
        execute("ALTER TYPE " + keyspace() + "." + typeName + " ADD foomap map <int,text>");
        execute("INSERT INTO %s (key, data) VALUES (1, ?)", userType("fooint", 1, "fooset", set("2"), "foomap", map(3, "bar")));
        assertRows(execute("SELECT * FROM %s"),
                row(1, userType("fooint", 1, "fooset", set("2"), "foomap", map(3, "bar"))));
    }

    @Test
    public void testCircularReferences() throws Throwable
    {
        String type1 = createType("CREATE TYPE %s (foo int)");

        String typeX = createType("CREATE TYPE %s (bar frozen<" + typeWithKs(type1) + ">)");
        assertInvalidMessage("would create a circular reference", "ALTER TYPE " + typeWithKs(type1) + " ADD needs_to_fail frozen<" + typeWithKs(typeX) + '>');

        typeX = createType("CREATE TYPE %s (bar frozen<list<" + typeWithKs(type1) + ">>)");
        assertInvalidMessage("would create a circular reference", "ALTER TYPE " + typeWithKs(type1) + " ADD needs_to_fail frozen<" + typeWithKs(typeX) + '>');

        typeX = createType("CREATE TYPE %s (bar frozen<set<" + typeWithKs(type1) + ">>)");
        assertInvalidMessage("would create a circular reference", "ALTER TYPE " + typeWithKs(type1) + " ADD needs_to_fail frozen<" + typeWithKs(typeX) + '>');

        typeX = createType("CREATE TYPE %s (bar frozen<map<text, " + typeWithKs(type1) + ">>)");
        assertInvalidMessage("would create a circular reference", "ALTER TYPE " + typeWithKs(type1) + " ADD needs_to_fail frozen<" + typeWithKs(typeX) + '>');

        typeX = createType("CREATE TYPE %s (bar frozen<map<" + typeWithKs(type1) + ", text>>)");
        assertInvalidMessage("would create a circular reference", "ALTER TYPE " + typeWithKs(type1) + " ADD needs_to_fail frozen<" + typeWithKs(typeX) + '>');

        //

        String type2 = createType("CREATE TYPE %s (foo frozen<" + typeWithKs(type1) + ">)");

        typeX = createType("CREATE TYPE %s (bar frozen<" + keyspace() + '.' + type2 + ">)");
        assertInvalidMessage("would create a circular reference", "ALTER TYPE " + typeWithKs(type1) + " ADD needs_to_fail frozen<" + typeWithKs(typeX) + '>');

        typeX = createType("CREATE TYPE %s (bar frozen<list<" + keyspace() + '.' + type2 + ">>)");
        assertInvalidMessage("would create a circular reference", "ALTER TYPE " + typeWithKs(type1) + " ADD needs_to_fail frozen<" + typeWithKs(typeX) + '>');

        typeX = createType("CREATE TYPE %s (bar frozen<set<" + keyspace() + '.' + type2 + ">>)");
        assertInvalidMessage("would create a circular reference", "ALTER TYPE " + typeWithKs(type1) + " ADD needs_to_fail frozen<" + typeWithKs(typeX) + '>');

        typeX = createType("CREATE TYPE %s (bar frozen<map<text, " + keyspace() + '.' + type2 + ">>)");
        assertInvalidMessage("would create a circular reference", "ALTER TYPE " + typeWithKs(type1) + " ADD needs_to_fail frozen<" + typeWithKs(typeX) + '>');

        typeX = createType("CREATE TYPE %s (bar frozen<map<" + keyspace() + '.' + type2 + ", text>>)");
        assertInvalidMessage("would create a circular reference", "ALTER TYPE " + typeWithKs(type1) + " ADD needs_to_fail frozen<" + typeWithKs(typeX) + '>');

        //

        assertInvalidMessage("would create a circular reference", "ALTER TYPE " + typeWithKs(type1) + " ADD needs_to_fail frozen<list<" + typeWithKs(type1) + ">>");
    }

    @Test
    public void testTypeAlterUsedInFunction() throws Throwable
    {
        String type1 = createType("CREATE TYPE %s (foo ascii)");
        assertComplexInvalidAlterDropStatements(type1, type1, "{foo: 'abc'}");

        type1 = createType("CREATE TYPE %s (foo ascii)");
        assertComplexInvalidAlterDropStatements(type1, "list<frozen<" + type1 + ">>", "[{foo: 'abc'}]");

        type1 = createType("CREATE TYPE %s (foo ascii)");
        assertComplexInvalidAlterDropStatements(type1, "set<frozen<" + type1 + ">>", "{{foo: 'abc'}}");

        type1 = createType("CREATE TYPE %s (foo ascii)");
        assertComplexInvalidAlterDropStatements(type1, "map<text, frozen<" + type1 + ">>", "{'key': {foo: 'abc'}}");

        type1 = createType("CREATE TYPE %s (foo ascii)");
        String type2 = createType("CREATE TYPE %s (foo frozen<" + type1 + ">)");
        assertComplexInvalidAlterDropStatements(type1, type2, "{foo: {foo: 'abc'}}");

        type1 = createType("CREATE TYPE %s (foo ascii)");
        type2 = createType("CREATE TYPE %s (foo frozen<" + type1 + ">)");
        assertComplexInvalidAlterDropStatements(type1,
                                                "list<frozen<" + type2 + ">>",
                                                "[{foo: {foo: 'abc'}}]");

        type1 = createType("CREATE TYPE %s (foo ascii)");
        type2 = createType("CREATE TYPE %s (foo frozen<set<" + type1 + ">>)");
        assertComplexInvalidAlterDropStatements(type1,
                                                "map<text, frozen<" + type2 + ">>",
                                                "{'key': {foo: {{foo: 'abc'}}}}");

        type1 = createType("CREATE TYPE %s (foo ascii)");
        assertComplexInvalidAlterDropStatements(type1,
                                                "tuple<text, frozen<" + type1 + ">>",
                                                "('key', {foo: 'abc'})");

        type1 = createType("CREATE TYPE %s (foo ascii)");
        assertComplexInvalidAlterDropStatements(type1,
                                                "tuple<text, frozen<tuple<tuple<" + type1 + ", int>, int>>>",
                                                "('key', (({foo: 'abc'}, 0), 0))");

        type1 = createType("CREATE TYPE %s (foo ascii)");
        type2 = createType("CREATE TYPE %s (foo frozen<set<" + type1 + ">>)");
        assertComplexInvalidAlterDropStatements(type1,
                                                "tuple<text, frozen<" + type2 + ">>",
                                                "('key', {foo: {{foo: 'abc'}}})");
    }

    private void assertComplexInvalidAlterDropStatements(String type1, String fArgType, String initcond) throws Throwable
    {
        String f = createFunction(KEYSPACE, type1, "CREATE FUNCTION %s(arg " + fArgType + ", col int) " +
                                                   "RETURNS NULL ON NULL INPUT " +
                                                   "RETURNS " + fArgType + ' ' +
                                                   "LANGUAGE java AS 'return arg;'");
        createAggregate(KEYSPACE, "int", "CREATE AGGREGATE %s(int) " +
                                         "SFUNC " + shortFunctionName(f) + ' ' +
                                         "STYPE " + fArgType + ' ' +
                                         "INITCOND " + initcond);
        assertInvalidAlterDropStatements(type1);
    }

    private void assertInvalidAlterDropStatements(String t) throws Throwable
    {
        assertInvalidMessage("Cannot alter user type " + typeWithKs(t), "ALTER TYPE " + typeWithKs(t) + " RENAME foo TO bar;");
        assertInvalidMessage("Cannot drop user type " + typeWithKs(t), "DROP TYPE " + typeWithKs(t) + ';');
    }

    @Test
    public void testInsertNonFrozenUDT() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + typeName + ")");

        execute("INSERT INTO %s (k, v) VALUES (?, {a: ?, b: ?})", 0, 0, "abc");
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", 0, "b", "abc")));

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 0, userType("a", 0, "b", "abc"));
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", 0, "b", "abc")));

        execute("INSERT INTO %s (k, v) VALUES (?, {a: ?, b: ?})", 0, 0, null);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", 0, "b", null)));

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 0, userType("a", null, "b", "abc"));
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", null, "b", "abc")));
    }

    @Test
    public void testUpdateNonFrozenUDT() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + typeName + ")");

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 0, userType("a", 0, "b", "abc"));
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", 0, "b", "abc")));

        // overwrite the whole UDT
        execute("UPDATE %s SET v = ? WHERE k = ?", userType("a", 1, "b", "def"), 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", 1, "b", "def")));

        execute("UPDATE %s SET v = ? WHERE k = ?", userType("a", 0, "b", null), 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", 0, "b", null)));

        execute("UPDATE %s SET v = ? WHERE k = ?", userType("a", null, "b", "abc"), 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", null, "b", "abc")));

        // individually set fields to non-null values
        execute("UPDATE %s SET v.a = ? WHERE k = ?", 1, 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", 1, "b", "abc")));

        execute("UPDATE %s SET v.b = ? WHERE k = ?", "def", 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", 1, "b", "def")));

        execute("UPDATE %s SET v.a = ?, v.b = ? WHERE k = ?", 0, "abc", 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", 0, "b", "abc")));

        execute("UPDATE %s SET v.b = ?, v.a = ? WHERE k = ?", "abc", 0, 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", 0, "b", "abc")));

        // individually set fields to null values
        execute("UPDATE %s SET v.a = ? WHERE k = ?", null, 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", null, "b", "abc")));

        execute("UPDATE %s SET v.a = ? WHERE k = ?", 0, 0);
        execute("UPDATE %s SET v.b = ? WHERE k = ?", null, 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", 0, "b", null)));

        execute("UPDATE %s SET v.a = ? WHERE k = ?", null, 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, null));

        assertInvalid("UPDATE %s SET v.bad = ? FROM %s WHERE k = ?", 0, 0);
        assertInvalid("UPDATE %s SET v = ? FROM %s WHERE k = ?", 0, 0);
        assertInvalid("UPDATE %s SET v = ? FROM %s WHERE k = ?", userType("a", 1, "b", "abc", "bad", 123), 0);
    }

    @Test
    public void testDeleteNonFrozenUDT() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + typeName + ")");

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 0, userType("a", 0, "b", "abc"));
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", 0, "b", "abc")));

        execute("DELETE v.b FROM %s WHERE k = ?", 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", 0, "b", null)));

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 0, userType("a", 0, "b", "abc"));
        execute("DELETE v.a FROM %s WHERE k = ?", 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, userType("a", null, "b", "abc")));

        execute("DELETE v.b FROM %s WHERE k = ?", 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, null));

        // delete both fields at once
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 0, userType("a", 0, "b", "abc"));
        execute("DELETE v.a, v.b FROM %s WHERE k = ?", 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, null));

        // same, but reverse field order
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 0, userType("a", 0, "b", "abc"));
        execute("DELETE v.b, v.a FROM %s WHERE k = ?", 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, null));

        // delete the whole thing at once
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 0, userType("a", 0, "b", "abc"));
        execute("DELETE v FROM %s WHERE k = ?", 0);
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 0), row(0, null));

        assertInvalid("DELETE v.bad FROM %s WHERE k = ?", 0);
    }

    @Test
    public void testReadAfterAlteringUserTypeNestedWithinSet() throws Throwable
    {
        String columnType = typeWithKs(createType("CREATE TYPE %s (a int)"));

        try
        {
            createTable("CREATE TABLE %s (x int PRIMARY KEY, y set<frozen<" + columnType + ">>)");
            disableCompaction();

            execute("INSERT INTO %s (x, y) VALUES(1, ?)", set(userType("a", 1), userType("a", 2)));
            assertRows(execute("SELECT * FROM %s"), row(1, set(userType("a", 1), userType("a", 2))));
            flush();

            assertRows(execute("SELECT * FROM %s WHERE x = 1"),
                       row(1, set(userType("a", 1), userType("a", 2))));

            execute("ALTER TYPE " + columnType + " ADD b int");
            execute("UPDATE %s SET y = y + ? WHERE x = 1",
                    set(userType("a", 1, "b", 1), userType("a", 1, "b", 2), userType("a", 2, "b", 1)));

            flush();
            assertRows(execute("SELECT * FROM %s WHERE x = 1"),
                           row(1, set(userType("a", 1),
                                      userType("a", 1, "b", 1),
                                      userType("a", 1, "b", 2),
                                      userType("a", 2),
                                      userType("a", 2, "b", 1))));

            compact();

            assertRows(execute("SELECT * FROM %s WHERE x = 1"),
                       row(1, set(userType("a", 1),
                                  userType("a", 1, "b", 1),
                                  userType("a", 1, "b", 2),
                                  userType("a", 2),
                                  userType("a", 2, "b", 1))));
        }
        finally
        {
            enableCompaction();
        }
    }

    @Test
    public void testReadAfterAlteringUserTypeNestedWithinMap() throws Throwable
    {
        String columnType = typeWithKs(createType("CREATE TYPE %s (a int)"));

        try
        {
            createTable("CREATE TABLE %s (x int PRIMARY KEY, y map<frozen<" + columnType + ">, int>)");
            disableCompaction();

            execute("INSERT INTO %s (x, y) VALUES(1, ?)", map(userType("a", 1), 1, userType("a", 2), 2));
            assertRows(execute("SELECT * FROM %s"), row(1, map(userType("a", 1), 1, userType("a", 2), 2)));
            flush();

            assertRows(execute("SELECT * FROM %s WHERE x = 1"),
                       row(1, map(userType("a", 1), 1, userType("a", 2), 2)));

            execute("ALTER TYPE " + columnType + " ADD b int");
            execute("UPDATE %s SET y = y + ? WHERE x = 1",
                    map(userType("a", 1, "b", 1), 1, userType("a", 1, "b", 2), 1, userType("a", 2, "b", 1), 2));

            flush();
            assertRows(execute("SELECT * FROM %s WHERE x = 1"),
                           row(1, map(userType("a", 1), 1,
                                      userType("a", 1, "b", 1), 1,
                                      userType("a", 1, "b", 2), 1,
                                      userType("a", 2), 2,
                                      userType("a", 2, "b", 1), 2)));

            compact();

            assertRows(execute("SELECT * FROM %s WHERE x = 1"),
                       row(1, map(userType("a", 1), 1,
                                  userType("a", 1, "b", 1), 1,
                                  userType("a", 1, "b", 2), 1,
                                  userType("a", 2), 2,
                                  userType("a", 2, "b", 1), 2)));
        }
        finally
        {
            enableCompaction();
        }
    }

    @Test
    public void testReadAfterAlteringUserTypeNestedWithinList() throws Throwable
    {
        String columnType = typeWithKs(createType("CREATE TYPE %s (a int)"));

        try
        {
            createTable("CREATE TABLE %s (x int PRIMARY KEY, y list<frozen<" + columnType + ">>)");
            disableCompaction();

            execute("INSERT INTO %s (x, y) VALUES(1, ?)", list(userType("a", 1), userType("a", 2)));
            assertRows(execute("SELECT * FROM %s"), row(1, list(userType("a", 1), userType("a", 2))));
            flush();

            assertRows(execute("SELECT * FROM %s WHERE x = 1"),
                       row(1, list(userType("a", 1), userType("a", 2))));

            execute("ALTER TYPE " + columnType + " ADD b int");
            execute("UPDATE %s SET y = y + ? WHERE x = 1",
                    list(userType("a", 1, "b", 1), userType("a", 1, "b", 2), userType("a", 2, "b", 1)));

            flush();
            assertRows(execute("SELECT * FROM %s WHERE x = 1"),
                           row(1, list(userType("a", 1),
                                       userType("a", 2),
                                       userType("a", 1, "b", 1),
                                       userType("a", 1, "b", 2),
                                       userType("a", 2, "b", 1))));

            compact();

            assertRows(execute("SELECT * FROM %s WHERE x = 1"),
                       row(1, list(userType("a", 1),
                                   userType("a", 2),
                                   userType("a", 1, "b", 1),
                                   userType("a", 1, "b", 2),
                                   userType("a", 2, "b", 1))));
        }
        finally
        {
            enableCompaction();
        }
    }

    @Test
    public void testAlteringUserTypeNestedWithinSetWithView() throws Throwable
    {
        String columnType = typeWithKs(createType("CREATE TYPE %s (a int)"));

        createTable("CREATE TABLE %s (pk int, c int, v int, s set<frozen<" + columnType + ">>, PRIMARY KEY (pk, c))");
        execute("CREATE MATERIALIZED VIEW " + keyspace() + ".view1 AS SELECT c, pk, v FROM %s WHERE pk IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL PRIMARY KEY (c, pk)");

        execute("INSERT INTO %s (pk, c, v, s) VALUES(?, ?, ?, ?)", 1, 1, 1, set(userType("a", 1), userType("a", 2)));
        flush();

        execute("ALTER TYPE " + columnType + " ADD b int");
        execute("UPDATE %s SET s = s + ?, v = ? WHERE pk = ? AND c = ?",
                set(userType("a", 1, "b", 1), userType("a", 1, "b", 2), userType("a", 2, "b", 1)), 2, 1, 1);

        assertRows(execute("SELECT * FROM %s WHERE pk = ? AND c = ?", 1, 1),
                       row(1, 1,set(userType("a", 1), userType("a", 1, "b", 1), userType("a", 1, "b", 2), userType("a", 2), userType("a", 2, "b", 1)), 2));
    }

    @Test(expected = SyntaxException.class)
    public void emptyTypeNameTest() throws Throwable
    {
        execute("CREATE TYPE \"\" (a int, b int)");
    }

    @Test(expected = SyntaxException.class)
    public void emptyFieldNameTest() throws Throwable
    {
        execute("CREATE TYPE mytype (\"\" int, b int)");
    }

    @Test(expected = SyntaxException.class)
    public void renameColumnToEmpty() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (a int, b int)");
        execute(String.format("ALTER TYPE %s.%s RENAME b TO \"\"", keyspace(), typeName));
    }

    private String typeWithKs(String type1)
    {
        return keyspace() + '.' + type1;
    }
}
