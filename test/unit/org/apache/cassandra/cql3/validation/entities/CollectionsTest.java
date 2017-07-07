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

import java.util.*;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class CollectionsTest extends CQLTester
{
    @Test
    public void testMapBulkRemoval() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<text, text>)");

        execute("INSERT INTO %s(k, m) VALUES (?, ?)", 0, map("k1", "v1", "k2", "v2", "k3", "v3"));

        assertRows(execute("SELECT * FROM %s"),
            row(0, map("k1", "v1", "k2", "v2", "k3", "v3"))
        );

        execute("UPDATE %s SET m = m - ? WHERE k = ?", set("k2"), 0);

        assertRows(execute("SELECT * FROM %s"),
            row(0, map("k1", "v1", "k3", "v3"))
        );

        execute("UPDATE %s SET m = m + ?, m = m - ? WHERE k = ?", map("k4", "v4"), set("k3"), 0);

        assertRows(execute("SELECT * FROM %s"),
            row(0, map("k1", "v1", "k4", "v4"))
        );
    }

    @Test
    public void testInvalidCollectionsMix() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<text>, s set<text>, m map<text, text>)");

        // Note: we force the non-prepared form for some of those tests because a list and a set
        // have the same serialized format in practice and CQLTester don't validate that the type
        // of what's passed as a value in the prepared case, so the queries would work (which is ok,
        // CQLTester is just a "dumb" client).

        assertInvalid("UPDATE %s SET l = l + { 'a', 'b' } WHERE k = 0");
        assertInvalid("UPDATE %s SET l = l - { 'a', 'b' } WHERE k = 0");
        assertInvalid("UPDATE %s SET l = l + ? WHERE k = 0", map("a", "b", "c", "d"));
        assertInvalid("UPDATE %s SET l = l - ? WHERE k = 0", map("a", "b", "c", "d"));

        assertInvalid("UPDATE %s SET s = s + [ 'a', 'b' ] WHERE k = 0");
        assertInvalid("UPDATE %s SET s = s - [ 'a', 'b' ] WHERE k = 0");
        assertInvalid("UPDATE %s SET s = s + ? WHERE k = 0", map("a", "b", "c", "d"));
        assertInvalid("UPDATE %s SET s = s - ? WHERE k = 0", map("a", "b", "c", "d"));

        assertInvalid("UPDATE %s SET m = m + ? WHERE k = 0", list("a", "b"));
        assertInvalid("UPDATE %s SET m = m - [ 'a', 'b' ] WHERE k = 0");
        assertInvalid("UPDATE %s SET m = m + ? WHERE k = 0", set("a", "b"));
        assertInvalid("UPDATE %s SET m = m - ? WHERE k = 0", map("a", "b", "c", "d"));
    }

    @Test
    public void testSets() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<text>)");

        execute("INSERT INTO %s(k, s) VALUES (0, ?)", set("v1", "v2", "v3", "v4"));

        assertRows(execute("SELECT s FROM %s WHERE k = 0"),
            row(set("v1", "v2", "v3", "v4"))
        );

        execute("DELETE s[?] FROM %s WHERE k = 0", "v1");

        assertRows(execute("SELECT s FROM %s WHERE k = 0"),
                   row(set("v2", "v3", "v4"))
        );

        // Full overwrite
        execute("UPDATE %s SET s = ? WHERE k = 0", set("v6", "v5"));

        assertRows(execute("SELECT s FROM %s WHERE k = 0"),
                   row(set("v5", "v6"))
        );

        execute("UPDATE %s SET s = s + ? WHERE k = 0", set("v7"));

        assertRows(execute("SELECT s FROM %s WHERE k = 0"),
                   row(set("v5", "v6", "v7"))
        );

        execute("UPDATE %s SET s = s - ? WHERE k = 0", set("v6", "v5"));

        assertRows(execute("SELECT s FROM %s WHERE k = 0"),
                   row(set("v7"))
        );

        execute("UPDATE %s SET s += ? WHERE k = 0", set("v5"));
        execute("UPDATE %s SET s += ? WHERE k = 0", set("v6"));

        assertRows(execute("SELECT s FROM %s WHERE k = 0"),
                   row(set("v5", "v6", "v7"))
        );

        execute("UPDATE %s SET s -= ? WHERE k = 0", set("v6", "v5"));

        assertRows(execute("SELECT s FROM %s WHERE k = 0"),
                   row(set("v7"))
        );

        execute("DELETE s[?] FROM %s WHERE k = 0", set("v7"));

        // Deleting an element that does not exist will succeed
        execute("DELETE s[?] FROM %s WHERE k = 0", set("v7"));

        execute("DELETE s FROM %s WHERE k = 0");

        assertRows(execute("SELECT s FROM %s WHERE k = 0"),
                   row((Object) null)
        );
    }

    @Test
    public void testMaps() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<text, int>)");

        execute("INSERT INTO %s(k, m) VALUES (0, ?)", map("v1", 1, "v2", 2));

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
            row(map("v1", 1, "v2", 2))
        );

        execute("UPDATE %s SET m[?] = ?, m[?] = ? WHERE k = 0", "v3", 3, "v4", 4);

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
            row(map("v1", 1, "v2", 2, "v3", 3, "v4", 4))
        );

        execute("DELETE m[?] FROM %s WHERE k = 0", "v1");

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
            row(map("v2", 2, "v3", 3, "v4", 4))
        );

        // Full overwrite
        execute("UPDATE %s SET m = ? WHERE k = 0", map("v6", 6, "v5", 5));

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map("v5", 5, "v6", 6))
        );

        execute("UPDATE %s SET m = m + ? WHERE k = 0", map("v7", 7));

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map("v5", 5, "v6", 6, "v7", 7))
        );

        execute("UPDATE %s SET m = m - ? WHERE k = 0", set("v7"));

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map("v5", 5, "v6", 6))
        );

        execute("UPDATE %s SET m += ? WHERE k = 0", map("v7", 7));

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map("v5", 5, "v6", 6, "v7", 7))
        );

        execute("UPDATE %s SET m -= ? WHERE k = 0", set("v7"));

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map("v5", 5, "v6", 6))
        );

        execute("DELETE m[?] FROM %s WHERE k = 0", "v6");

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map("v5", 5))
        );

        execute("DELETE m[?] FROM %s WHERE k = 0", "v5");

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row((Object) null)
        );

        // Deleting a non-existing key should succeed
        execute("DELETE m[?] FROM %s WHERE k = 0", "v5");

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row((Object) null)
        );

        // The empty map is parsed as an empty set (because we don't have enough info at parsing
        // time when we see a {}) and special cased later. This test checks this work properly
        execute("UPDATE %s SET m = {} WHERE k = 0");

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
            row((Object)null)
        );
    }

    @Test
    public void testLists() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<text>)");

        execute("INSERT INTO %s(k, l) VALUES (0, ?)", list("v1", "v2", "v3"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v1", "v2", "v3")));

        execute("DELETE l[?] FROM %s WHERE k = 0", 1);

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v1", "v3")));

        execute("UPDATE %s SET l[?] = ? WHERE k = 0", 1, "v4");

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v1", "v4")));

        // Full overwrite
        execute("UPDATE %s SET l = ? WHERE k = 0", list("v6", "v5"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v6", "v5")));

        execute("UPDATE %s SET l = l + ? WHERE k = 0", list("v7", "v8"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v6", "v5", "v7", "v8")));

        execute("UPDATE %s SET l = ? + l WHERE k = 0", list("v9"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v9", "v6", "v5", "v7", "v8")));

        execute("UPDATE %s SET l = l - ? WHERE k = 0", list("v5", "v8"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v9", "v6", "v7")));

        execute("UPDATE %s SET l += ? WHERE k = 0", list("v8"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v9", "v6", "v7", "v8")));

        execute("UPDATE %s SET l -= ? WHERE k = 0", list("v6", "v8"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v9", "v7")));

        execute("DELETE l FROM %s WHERE k = 0");

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row((Object) null));

        assertInvalidMessage("Attempted to delete an element from a list which is null",
                             "DELETE l[0] FROM %s WHERE k=0 ");

        assertInvalidMessage("Attempted to set an element on a list which is null",
                             "UPDATE %s SET l[0] = ? WHERE k=0", list("v10"));

        execute("UPDATE %s SET l = l - ? WHERE k=0", list("v11"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row((Object) null));
    }

    @Test
    public void testMapWithUnsetValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<text,text>)");
        // set up
        Object m = map("k", "v");
        execute("INSERT INTO %s (k, m) VALUES (10, ?)", m);
        assertRows(execute("SELECT m FROM %s WHERE k = 10"),
                   row(m)
        );

        // test putting an unset map, should not delete the contents
        execute("INSERT INTO %s (k, m) VALUES (10, ?)", unset());
        assertRows(execute("SELECT m FROM %s WHERE k = 10"),
                   row(m)
        );
        // test unset variables in a map update operaiotn, should not delete the contents
        execute("UPDATE %s SET m['k'] = ? WHERE k = 10", unset());
        assertRows(execute("SELECT m FROM %s WHERE k = 10"),
                   row(m)
        );
        assertInvalidMessage("Invalid unset map key", "UPDATE %s SET m[?] = 'foo' WHERE k = 10", unset());

        // test unset value for map key
        assertInvalidMessage("Invalid unset map key", "DELETE m[?] FROM %s WHERE k = 10", unset());
    }

    @Test
    public void testListWithUnsetValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<text>)");
        // set up
        Object l = list("foo", "foo");
        execute("INSERT INTO %s (k, l) VALUES (10, ?)", l);
        assertRows(execute("SELECT l FROM %s WHERE k = 10"),
                   row(l)
        );

        // replace list with unset value
        execute("INSERT INTO %s (k, l) VALUES (10, ?)", unset());
        assertRows(execute("SELECT l FROM %s WHERE k = 10"),
                   row(l)
        );

        // add to position
        execute("UPDATE %s SET l[1] = ? WHERE k = 10", unset());
        assertRows(execute("SELECT l FROM %s WHERE k = 10"),
                   row(l)
        );

        // set in index
        assertInvalidMessage("Invalid unset value for list index", "UPDATE %s SET l[?] = 'foo' WHERE k = 10", unset());

        // remove element by index
        execute("DELETE l[?] FROM %s WHERE k = 10", unset());
        assertRows(execute("SELECT l FROM %s WHERE k = 10"),
                   row(l)
        );

        // remove all occurrences of element
        execute("UPDATE %s SET l = l - ? WHERE k = 10", unset());
        assertRows(execute("SELECT l FROM %s WHERE k = 10"),
                   row(l)
        );

        // select with in clause
        assertInvalidMessage("Invalid unset value for column k", "SELECT * FROM %s WHERE k IN ?", unset());
        assertInvalidMessage("Invalid unset value for column k", "SELECT * FROM %s WHERE k IN (?)", unset());
    }

    @Test
    public void testSetWithUnsetValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<text>)");

        Object s = set("bar", "baz", "foo");
        execute("INSERT INTO %s (k, s) VALUES (10, ?)", s);
        assertRows(execute("SELECT s FROM %s WHERE k = 10"),
                   row(s)
        );

        // replace set with unset value
        execute("INSERT INTO %s (k, s) VALUES (10, ?)", unset());
        assertRows(execute("SELECT s FROM %s WHERE k = 10"),
                   row(s)
        );

        // add to set
        execute("UPDATE %s SET s = s + ? WHERE k = 10", unset());
        assertRows(execute("SELECT s FROM %s WHERE k = 10"),
                   row(s)
        );

        // remove all occurrences of element
        execute("UPDATE %s SET s = s - ? WHERE k = 10", unset());
        assertRows(execute("SELECT s FROM %s WHERE k = 10"),
                   row(s)
        );
    }

    /**
     * Migrated from cql_tests.py:TestCQL.set_test()
     */
    @Test
    public void testSet() throws Throwable
    {
        createTable("CREATE TABLE %s ( fn text, ln text, tags set<text>, PRIMARY KEY (fn, ln) )");

        execute("UPDATE %s SET tags = tags + { 'foo' } WHERE fn='Tom' AND ln='Bombadil'");
        execute("UPDATE %s SET tags = tags + { 'bar' } WHERE fn='Tom' AND ln='Bombadil'");
        execute("UPDATE %s SET tags = tags + { 'foo' } WHERE fn='Tom' AND ln='Bombadil'");
        execute("UPDATE %s SET tags = tags + { 'foobar' } WHERE fn='Tom' AND ln='Bombadil'");
        execute("UPDATE %s SET tags = tags - { 'bar' } WHERE fn='Tom' AND ln='Bombadil'");

        assertRows(execute("SELECT tags FROM %s"),
                   row(set("foo", "foobar")));

        execute("UPDATE %s SET tags = { 'a', 'c', 'b' } WHERE fn='Bilbo' AND ln='Baggins'");
        assertRows(execute("SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   row(set("a", "b", "c")));

        execute("UPDATE %s SET tags = { 'm', 'n' } WHERE fn='Bilbo' AND ln='Baggins'");
        assertRows(execute("SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   row(set("m", "n")));

        execute("DELETE tags['m'] FROM %s WHERE fn='Bilbo' AND ln='Baggins'");
        assertRows(execute("SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   row(set("n")));

        execute("DELETE tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'");
        assertEmpty(execute("SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.map_test()
     */
    @Test
    public void testMap() throws Throwable
    {
        createTable("CREATE TABLE %s (fn text, ln text, m map<text, int>, PRIMARY KEY (fn, ln))");

        execute("UPDATE %s SET m['foo'] = 3 WHERE fn='Tom' AND ln='Bombadil'");
        execute("UPDATE %s SET m['bar'] = 4 WHERE fn='Tom' AND ln='Bombadil'");
        execute("UPDATE %s SET m['woot'] = 5 WHERE fn='Tom' AND ln='Bombadil'");
        execute("UPDATE %s SET m['bar'] = 6 WHERE fn='Tom' AND ln='Bombadil'");
        execute("DELETE m['foo'] FROM %s WHERE fn='Tom' AND ln='Bombadil'");

        assertRows(execute("SELECT m FROM %s"),
                   row(map("bar", 6, "woot", 5)));

        execute("UPDATE %s SET m = { 'a' : 4 , 'c' : 3, 'b' : 2 } WHERE fn='Bilbo' AND ln='Baggins'");
        assertRows(execute("SELECT m FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   row(map("a", 4, "b", 2, "c", 3)));

        execute("UPDATE %s SET m =  { 'm' : 4 , 'n' : 1, 'o' : 2 } WHERE fn='Bilbo' AND ln='Baggins'");
        assertRows(execute("SELECT m FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   row(map("m", 4, "n", 1, "o", 2)));

        execute("UPDATE %s SET m = {} WHERE fn='Bilbo' AND ln='Baggins'");
        assertEmpty(execute("SELECT m FROM %s WHERE fn='Bilbo' AND ln='Baggins'"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.list_test()
     */
    @Test
    public void testList() throws Throwable
    {
        createTable("CREATE TABLE %s (fn text, ln text, tags list<text>, PRIMARY KEY (fn, ln))");

        execute("UPDATE %s SET tags = tags + [ 'foo' ] WHERE fn='Tom' AND ln='Bombadil'");
        execute("UPDATE %s SET tags = tags + [ 'bar' ] WHERE fn='Tom' AND ln='Bombadil'");
        execute("UPDATE %s SET tags = tags + [ 'foo' ] WHERE fn='Tom' AND ln='Bombadil'");
        execute("UPDATE %s SET tags = tags + [ 'foobar' ] WHERE fn='Tom' AND ln='Bombadil'");

        assertRows(execute("SELECT tags FROM %s"),
                   row(list("foo", "bar", "foo", "foobar")));

        execute("UPDATE %s SET tags = [ 'a', 'c', 'b', 'c' ] WHERE fn='Bilbo' AND ln='Baggins'");
        assertRows(execute("SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   row(list("a", "c", "b", "c")));

        execute("UPDATE %s SET tags = [ 'm', 'n' ] + tags WHERE fn='Bilbo' AND ln='Baggins'");
        assertRows(execute("SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   row(list("m", "n", "a", "c", "b", "c")));

        execute("UPDATE %s SET tags[2] = 'foo', tags[4] = 'bar' WHERE fn='Bilbo' AND ln='Baggins'");
        assertRows(execute("SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   row(list("m", "n", "foo", "c", "bar", "c")));

        execute("DELETE tags[2] FROM %s WHERE fn='Bilbo' AND ln='Baggins'");
        assertRows(execute("SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   row(list("m", "n", "c", "bar", "c")));

        execute("UPDATE %s SET tags = tags - [ 'bar' ] WHERE fn='Bilbo' AND ln='Baggins'");
        assertRows(execute("SELECT tags FROM %s WHERE fn='Bilbo' AND ln='Baggins'"),
                   row(list("m", "n", "c", "c")));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.multi_collection_test()
     */
    @Test
    public void testMultiCollections() throws Throwable
    {
        UUID id = UUID.fromString("b017f48f-ae67-11e1-9096-005056c00008");

        createTable("CREATE TABLE %s (k uuid PRIMARY KEY, L list<int>, M map<text, int>, S set<int> )");

        execute("UPDATE %s SET L = [1, 3, 5] WHERE k = ?", id);
        execute("UPDATE %s SET L = L + [7, 11, 13] WHERE k = ?;", id);
        execute("UPDATE %s SET S = {1, 3, 5} WHERE k = ?", id);
        execute("UPDATE %s SET S = S + {7, 11, 13} WHERE k = ?", id);
        execute("UPDATE %s SET M = {'foo': 1, 'bar' : 3} WHERE k = ?", id);
        execute("UPDATE %s SET M = M + {'foobar' : 4} WHERE k = ?", id);

        assertRows(execute("SELECT L, M, S FROM %s WHERE k = ?", id),
                   row(list(1, 3, 5, 7, 11, 13),
                       map("bar", 3, "foo", 1, "foobar", 4),
                       set(1, 3, 5, 7, 11, 13)));
    }


    /**
     * Migrated from cql_tests.py:TestCQL.collection_and_regular_test()
     */
    @Test
    public void testCollectionAndRegularColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<int>, c int)");

        execute("INSERT INTO %s (k, l, c) VALUES(3, [0, 1, 2], 4)");
        execute("UPDATE %s SET l[0] = 1, c = 42 WHERE k = 3");
        assertRows(execute("SELECT l, c FROM %s WHERE k = 3"),
                   row(list(1, 1, 2), 42));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.multi_list_set_test()
     */
    @Test
    public void testMultipleLists() throws Throwable
    {
        createTable(" CREATE TABLE %s (k int PRIMARY KEY, l1 list<int>, l2 list<int>)");

        execute("INSERT INTO %s (k, l1, l2) VALUES (0, [1, 2, 3], [4, 5, 6])");
        execute("UPDATE %s SET l2[1] = 42, l1[1] = 24  WHERE k = 0");

        assertRows(execute("SELECT l1, l2 FROM %s WHERE k = 0"),
                   row(list(1, 24, 3), list(4, 42, 6)));
    }

    /**
     * Test you can add columns in a table with collections (#4982 bug),
     * migrated from cql_tests.py:TestCQL.alter_with_collections_test()
     */
    @Test
    public void testAlterCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (key int PRIMARY KEY, aset set<text>)");
        execute("ALTER TABLE %s ADD c text");
        execute("ALTER TABLE %s ADD alist list<text>");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.collection_compact_test()
     */
    @Test
    public void testCompactCollections() throws Throwable
    {
        String tableName = KEYSPACE + "." + createTableName();
        assertInvalid(String.format("CREATE TABLE %s (user ascii PRIMARY KEY, mails list < text >) WITH COMPACT STORAGE;", tableName));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.collection_function_test()
     */
    @Test
    public void testFunctionsOnCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l set<int>)");

        assertInvalid("SELECT ttl(l) FROM %s WHERE k = 0");
        assertInvalid("SELECT writetime(l) FROM %s WHERE k = 0");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.bug_5376()
     */
    @Test
    public void testInClauseWithCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, c bigint, v text, x set < text >, PRIMARY KEY(key, c) )");

        assertInvalid("select * from %s where key = 'foo' and c in (1,3,4)");
    }

    /**
     * Test for bug #5795,
     * migrated from cql_tests.py:TestCQL.nonpure_function_collection_test()
     */
    @Test
    public void testNonPureFunctionCollection() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<timeuuid>)");

        // we just want to make sure this doesn't throw
        execute("INSERT INTO %s (k, v) VALUES (0, [now()])");
    }

    /**
     * Test for 5805 bug,
     * migrated from cql_tests.py:TestCQL.collection_flush_test()
     */
    @Test
    public void testCollectionFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<int>)");

        execute("INSERT INTO %s (k, s) VALUES (1, {1})");
        flush();

        execute("INSERT INTO %s (k, s) VALUES (1, {2})");
        flush();

        assertRows(execute("SELECT * FROM %s"),
                   row(1, set(2)));
    }

    /**
     * Test for 6276,
     * migrated from cql_tests.py:TestCQL.drop_and_readd_collection_test()
     */
    @Test
    public void testDropAndReaddCollection() throws Throwable
    {
        createTable("create table %s (k int primary key, v set<text>, x int)");
        execute("insert into %s (k, v) VALUES (0, {'fffffffff'})");
        flush();
        execute("alter table %s drop v");
        assertInvalid("alter table %s add v set<int>");
    }

    @Test
    public void testDropAndReaddDroppedCollection() throws Throwable
    {
        createTable("create table %s (k int primary key, v frozen<set<text>>, x int)");
        execute("insert into %s (k, v) VALUES (0, {'fffffffff'})");
        flush();
        execute("alter table %s drop v");
        execute("alter table %s add v set<int>");
    }

    @Test
    public void testMapWithLargePartition() throws Throwable
    {
        Random r = new Random();
        long seed = System.nanoTime();
        System.out.println("Seed " + seed);
        r.setSeed(seed);

        int len = (1024 * 1024)/100;
        createTable("CREATE TABLE %s (userid text PRIMARY KEY, properties map<int, text>) with compression = {}");

        final int numKeys = 200;
        for (int i = 0; i < numKeys; i++)
        {
            byte[] b = new byte[len];
            r.nextBytes(b);
            execute("UPDATE %s SET properties[?] = ? WHERE userid = 'user'", i, new String(b));
        }

        flush();

        Object[][] rows = getRows(execute("SELECT properties from %s where userid = 'user'"));
        assertEquals(1, rows.length);
        assertEquals(numKeys, ((Map) rows[0][0]).size());
    }

    @Test
    public void testMapWithTwoSStables() throws Throwable
    {
        createTable("CREATE TABLE %s (userid text PRIMARY KEY, properties map<int, text>) with compression = {}");

        final int numKeys = 100;
        for (int i = 0; i < numKeys; i++)
            execute("UPDATE %s SET properties[?] = ? WHERE userid = 'user'", i, "prop_" + Integer.toString(i));

        flush();

        for (int i = numKeys; i < 2*numKeys; i++)
            execute("UPDATE %s SET properties[?] = ? WHERE userid = 'user'", i, "prop_" + Integer.toString(i));

        flush();

        Object[][] rows = getRows(execute("SELECT properties from %s where userid = 'user'"));
        assertEquals(1, rows.length);
        assertEquals(numKeys * 2, ((Map) rows[0][0]).size());
    }

    @Test
    public void testSetWithTwoSStables() throws Throwable
    {
        createTable("CREATE TABLE %s (userid text PRIMARY KEY, properties set<text>) with compression = {}");

        final int numKeys = 100;
        for (int i = 0; i < numKeys; i++)
            execute("UPDATE %s SET properties = properties + ? WHERE userid = 'user'", set("prop_" + Integer.toString(i)));

        flush();

        for (int i = numKeys; i < 2*numKeys; i++)
            execute("UPDATE %s SET properties = properties + ? WHERE userid = 'user'", set("prop_" + Integer.toString(i)));

        flush();

        Object[][] rows = getRows(execute("SELECT properties from %s where userid = 'user'"));
        assertEquals(1, rows.length);
        assertEquals(numKeys * 2, ((Set) rows[0][0]).size());
    }

    @Test
    public void testUpdateStaticList() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 text, k2 text, s_list list<int> static, PRIMARY KEY (k1, k2))");

        execute("insert into %s (k1, k2) VALUES ('a','b')");
        execute("update %s set s_list = s_list + [0] where k1='a'");
        assertRows(execute("select s_list from %s where k1='a'"), row(list(0)));

        execute("update %s set s_list[0] = 100 where k1='a'");
        assertRows(execute("select s_list from %s where k1='a'"), row(list(100)));

        execute("update %s set s_list = s_list + [0] where k1='a'");
        assertRows(execute("select s_list from %s where k1='a'"), row(list(100, 0)));

        execute("delete s_list[0] from %s where k1='a'");
        assertRows(execute("select s_list from %s where k1='a'"), row(list(0)));
    }

    @Test
    public void testListWithElementsBiggerThan64K() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<text>)");

        byte[] bytes = new byte[FBUtilities.MAX_UNSIGNED_SHORT + 10];
        Arrays.fill(bytes, (byte) 1);
        String largeText = new String(bytes);

        bytes = new byte[FBUtilities.MAX_UNSIGNED_SHORT + 10];
        Arrays.fill(bytes, (byte) 2);
        String largeText2 = new String(bytes);

        execute("INSERT INTO %s(k, l) VALUES (0, ?)", list(largeText, "v2"));
        flush();

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list(largeText, "v2")));

        execute("DELETE l[?] FROM %s WHERE k = 0", 0);

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v2")));

        execute("UPDATE %s SET l[?] = ? WHERE k = 0", 0, largeText);

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list(largeText)));

        // Full overwrite
        execute("UPDATE %s SET l = ? WHERE k = 0", list("v1", largeText));
        flush();

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v1", largeText)));

        execute("UPDATE %s SET l = l + ? WHERE k = 0", list("v2", largeText2));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v1", largeText, "v2", largeText2)));

        execute("UPDATE %s SET l = l - ? WHERE k = 0", list(largeText, "v2"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list("v1", largeText2)));

        execute("DELETE l FROM %s WHERE k = 0");

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row((Object) null));

        execute("INSERT INTO %s(k, l) VALUES (0, ['" + largeText + "', 'v2'])");
        flush();

        assertRows(execute("SELECT l FROM %s WHERE k = 0"), row(list(largeText, "v2")));
    }

    @Test
    public void testMapsWithElementsBiggerThan64K() throws Throwable
    {
        byte[] bytes = new byte[FBUtilities.MAX_UNSIGNED_SHORT + 10];
        Arrays.fill(bytes, (byte) 1);
        String largeText = new String(bytes);
        bytes = new byte[FBUtilities.MAX_UNSIGNED_SHORT + 10];
        Arrays.fill(bytes, (byte) 2);
        String largeText2 = new String(bytes);

        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<text, text>)");

        execute("INSERT INTO %s(k, m) VALUES (0, ?)", map("k1", largeText, largeText, "v2"));
        flush();

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map("k1", largeText, largeText, "v2")));

        execute("UPDATE %s SET m[?] = ? WHERE k = 0", "k3", largeText);

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map("k1", largeText, largeText, "v2", "k3", largeText)));

        execute("UPDATE %s SET m[?] = ? WHERE k = 0", largeText2, "v4");

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map("k1", largeText, largeText, "v2", "k3", largeText, largeText2, "v4")));

        execute("DELETE m[?] FROM %s WHERE k = 0", "k1");

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map(largeText, "v2", "k3", largeText, largeText2, "v4")));

        execute("DELETE m[?] FROM %s WHERE k = 0", largeText2);
        flush();

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map(largeText, "v2", "k3", largeText)));

        // Full overwrite
        execute("UPDATE %s SET m = ? WHERE k = 0", map("k5", largeText, largeText, "v6"));
        flush();

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map("k5", largeText, largeText, "v6")));

        execute("UPDATE %s SET m = m + ? WHERE k = 0", map("k7", largeText));

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map("k5", largeText, largeText, "v6", "k7", largeText)));

        execute("UPDATE %s SET m = m + ? WHERE k = 0", map(largeText2, "v8"));
        flush();

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map("k5", largeText, largeText, "v6", "k7", largeText, largeText2, "v8")));

        execute("DELETE m FROM %s WHERE k = 0");

        assertRows(execute("SELECT m FROM %s WHERE k = 0"), row((Object) null));

        execute("INSERT INTO %s(k, m) VALUES (0, {'" + largeText + "' : 'v1', 'k2' : '" + largeText + "'})");
        flush();

        assertRows(execute("SELECT m FROM %s WHERE k = 0"),
                   row(map(largeText, "v1", "k2", largeText)));
    }

    @Test
    public void testSetsWithElementsBiggerThan64K() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<text>)");

        byte[] bytes = new byte[FBUtilities.MAX_UNSIGNED_SHORT + 10];
        Arrays.fill(bytes, (byte) 1);
        String largeText = new String(bytes);

        bytes = new byte[FBUtilities.MAX_UNSIGNED_SHORT + 10];
        Arrays.fill(bytes, (byte) 2);
        String largeText2 = new String(bytes);

        execute("INSERT INTO %s(k, s) VALUES (0, ?)", set(largeText, "v2"));
        flush();

        assertRows(execute("SELECT s FROM %s WHERE k = 0"), row(set(largeText, "v2")));

        execute("DELETE s[?] FROM %s WHERE k = 0", largeText);

        assertRows(execute("SELECT s FROM %s WHERE k = 0"), row(set("v2")));

        // Full overwrite
        execute("UPDATE %s SET s = ? WHERE k = 0", set("v1", largeText));
        flush();

        assertRows(execute("SELECT s FROM %s WHERE k = 0"), row(set("v1", largeText)));

        execute("UPDATE %s SET s = s + ? WHERE k = 0", set("v2", largeText2));

        assertRows(execute("SELECT s FROM %s WHERE k = 0"), row(set("v1", largeText, "v2", largeText2)));

        execute("UPDATE %s SET s = s - ? WHERE k = 0", set(largeText, "v2"));

        assertRows(execute("SELECT s FROM %s WHERE k = 0"), row(set("v1", largeText2)));

        execute("DELETE s FROM %s WHERE k = 0");

        assertRows(execute("SELECT s FROM %s WHERE k = 0"), row((Object) null));

        execute("INSERT INTO %s(k, s) VALUES (0, {'" + largeText + "', 'v2'})");
        flush();

        assertRows(execute("SELECT s FROM %s WHERE k = 0"), row(set(largeText, "v2")));
    }

    @Test
    public void testRemovalThroughUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<int>)");

         execute("INSERT INTO %s(k, l) VALUES(?, ?)", 0, list(1, 2, 3));
         assertRows(execute("SELECT * FROM %s"), row(0, list(1, 2, 3)));

         execute("UPDATE %s SET l[0] = null WHERE k=0");
         assertRows(execute("SELECT * FROM %s"), row(0, list(2, 3)));
    }

    @Test
    public void testInvalidInputForList() throws Throwable
    {
        createTable("CREATE TABLE %s(pk int PRIMARY KEY, l list<text>)");
        assertInvalidMessage("Not enough bytes to read a list",
                             "INSERT INTO %s (pk, l) VALUES (?, ?)", 1, "test");
        assertInvalidMessage("Not enough bytes to read a list",
                             "INSERT INTO %s (pk, l) VALUES (?, ?)", 1, Long.MAX_VALUE);
        assertInvalidMessage("Not enough bytes to read a list",
                             "INSERT INTO %s (pk, l) VALUES (?, ?)", 1, "");
        assertInvalidMessage("The data cannot be deserialized as a list",
                             "INSERT INTO %s (pk, l) VALUES (?, ?)", 1, -1);
    }

    @Test
    public void testInvalidInputForSet() throws Throwable
    {
        createTable("CREATE TABLE %s(pk int PRIMARY KEY, s set<text>)");
        assertInvalidMessage("Not enough bytes to read a set",
                             "INSERT INTO %s (pk, s) VALUES (?, ?)", 1, "test");
        assertInvalidMessage("String didn't validate.",
                             "INSERT INTO %s (pk, s) VALUES (?, ?)", 1, Long.MAX_VALUE);
        assertInvalidMessage("Not enough bytes to read a set",
                             "INSERT INTO %s (pk, s) VALUES (?, ?)", 1, "");
        assertInvalidMessage("The data cannot be deserialized as a set",
                             "INSERT INTO %s (pk, s) VALUES (?, ?)", 1, -1);
    }

    @Test
    public void testInvalidInputForMap() throws Throwable
    {
        createTable("CREATE TABLE %s(pk int PRIMARY KEY, m map<text, text>)");
        assertInvalidMessage("Not enough bytes to read a map",
                             "INSERT INTO %s (pk, m) VALUES (?, ?)", 1, "test");
        assertInvalidMessage("String didn't validate.",
                             "INSERT INTO %s (pk, m) VALUES (?, ?)", 1, Long.MAX_VALUE);
        assertInvalidMessage("Not enough bytes to read a map",
                             "INSERT INTO %s (pk, m) VALUES (?, ?)", 1, "");
        assertInvalidMessage("The data cannot be deserialized as a map",
                             "INSERT INTO %s (pk, m) VALUES (?, ?)", 1, -1);
    }

    @Test
    public void testMultipleOperationOnListWithinTheSameQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, l list<int>)");
        execute("INSERT INTO %s (pk, l) VALUES (1, [1, 2, 3, 4])");

        // Checks that when the same element is updated twice the update with the greatest value is the one taken into account
        execute("UPDATE %s SET l[?] = ?, l[?] = ?  WHERE pk = ?", 2, 7, 2, 8, 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, list(1, 2, 8, 4)));

        execute("UPDATE %s SET l[?] = ?, l[?] = ?  WHERE pk = ?", 2, 9, 2, 6, 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, list(1, 2, 9, 4)));

        // Checks that deleting twice the same element will result in the deletion of the element with the index
        // and of the following element.
        execute("DELETE l[?], l[?] FROM %s WHERE pk = ?", 2, 2, 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, list(1, 2)));

        // Checks that the set operation is performed on the added elements and that the greatest value win
        execute("UPDATE %s SET l = l + ?, l[?] = ?  WHERE pk = ?", list(3, 4), 3, 7, 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, list(1, 2, 3, 7)));

        execute("UPDATE %s SET l = l + ?, l[?] = ?  WHERE pk = ?", list(6, 8), 4, 5, 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, list(1, 2, 3, 7, 6, 8)));

        // Checks that the order of the operations matters
        assertInvalidMessage("List index 6 out of bound, list has size 6",
                             "UPDATE %s SET l[?] = ?, l = l + ? WHERE pk = ?", 6, 5, list(9), 1);

        // Checks that the updated element is deleted.
        execute("UPDATE %s SET l[?] = ? , l = l - ? WHERE pk = ?", 2, 8, list(8), 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, list(1, 2, 7, 6)));

        // Checks that we cannot update an element that has been removed.
        assertInvalidMessage("List index 3 out of bound, list has size 3",
                             "UPDATE %s SET l = l - ?, l[?] = ?  WHERE pk = ?", list(6), 3, 4, 1);

        // Checks that the element is updated before the other ones are shifted.
        execute("UPDATE %s SET l[?] = ? , l = l - ? WHERE pk = ?", 2, 8, list(1), 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, list(2, 8, 6)));

        // Checks that the element are shifted before the element is updated.
        execute("UPDATE %s SET l = l - ?, l[?] = ?  WHERE pk = ?", list(2, 6), 0, 9, 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, list(9)));
    }

    @Test
    public void testMultipleOperationOnMapWithinTheSameQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, m map<int, int>)");
        execute("INSERT INTO %s (pk, m) VALUES (1, {0 : 1, 1 : 2, 2 : 3, 3 : 4})");

        // Checks that when the same element is updated twice the update with the greatest value is the one taken into account
        execute("UPDATE %s SET m[?] = ?, m[?] = ?  WHERE pk = ?", 2, 7, 2, 8, 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = 1") , row(1, map(0, 1, 1, 2, 2, 8, 3, 4)));

        execute("UPDATE %s SET m[?] = ?, m[?] = ?  WHERE pk = ?", 2, 9, 2, 6, 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = 1") , row(1, map(0, 1, 1, 2, 2, 9, 3, 4)));

        // Checks that deleting twice the same element has no side effect
        execute("DELETE m[?], m[?] FROM %s WHERE pk = ?", 2, 2, 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, map(0, 1, 1, 2, 3, 4)));

        // Checks that the set operation is performed on the added elements and that the greatest value win
        execute("UPDATE %s SET m = m + ?, m[?] = ?  WHERE pk = ?", map(4, 5), 4, 7, 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, map(0, 1, 1, 2, 3, 4, 4, 7)));

        execute("UPDATE %s SET m = m + ?, m[?] = ?  WHERE pk = ?", map(4, 8), 4, 6, 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, map(0, 1, 1, 2, 3, 4, 4, 8)));

        // Checks that, as tombstones win over updates for the same timestamp, the removed element is not readded
        execute("UPDATE %s SET m = m - ?, m[?] = ?  WHERE pk = ?", set(4), 4, 9, 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, map(0, 1, 1, 2, 3, 4)));

        // Checks that the update is taken into account before the removal
        execute("UPDATE %s SET m[?] = ?,  m = m - ?  WHERE pk = ?", 5, 9, set(5), 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, map(0, 1, 1, 2, 3, 4)));

        // Checks that the set operation is merged with the change of the append and that the greatest value win
        execute("UPDATE %s SET m[?] = ?, m = m + ?  WHERE pk = ?", 5, 9, map(5, 8, 6, 9), 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, map(0, 1, 1, 2, 3, 4, 5, 9, 6, 9)));

        execute("UPDATE %s SET m[?] = ?, m = m + ?  WHERE pk = ?", 7, 1, map(7, 2), 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 1) , row(1, map(0, 1, 1, 2, 3, 4, 5, 9, 6, 9, 7, 2)));
    }

    @Test
    public void testMultipleOperationOnSetWithinTheSameQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, s set<int>)");
        execute("INSERT INTO %s (pk, s) VALUES (1, {0, 1, 2})");

        // Checks that the two operation are merged and that the tombstone always win
        execute("UPDATE %s SET s = s + ? , s = s - ?  WHERE pk = ?", set(3, 4), set(3), 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = 1") , row(1, set(0, 1, 2, 4)));

        execute("UPDATE %s SET s = s - ? , s = s + ?  WHERE pk = ?", set(3), set(3, 4), 1);
        assertRows(execute("SELECT * FROM %s WHERE pk = 1") , row(1, set(0, 1, 2, 4)));
    }

    @Test
    public void testInsertingCollectionsWithInvalidElements() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s frozen<set<tuple<int, text, double>>>)");
        assertInvalidMessage("Invalid remaining data after end of tuple value",
                             "INSERT INTO %s (k, s) VALUES (0, ?)",
                             set(tuple(1, "1", 1.0, 1), tuple(2, "2", 2.0, 2)));

        assertInvalidMessage("Invalid set literal for s: value (1, '1', 1.0, 1) is not of type frozen<tuple<int, text, double>>",
                             "INSERT INTO %s (k, s) VALUES (0, {(1, '1', 1.0, 1)})");

        createTable("CREATE TABLE %s (k int PRIMARY KEY, l frozen<list<tuple<int, text, double>>>)");
        assertInvalidMessage("Invalid remaining data after end of tuple value",
                             "INSERT INTO %s (k, l) VALUES (0, ?)",
                             list(tuple(1, "1", 1.0, 1), tuple(2, "2", 2.0, 2)));

        assertInvalidMessage("Invalid list literal for l: value (1, '1', 1.0, 1) is not of type frozen<tuple<int, text, double>>",
                             "INSERT INTO %s (k, l) VALUES (0, [(1, '1', 1.0, 1)])");

        createTable("CREATE TABLE %s (k int PRIMARY KEY, m frozen<map<tuple<int, text, double>, int>>)");
        assertInvalidMessage("Invalid remaining data after end of tuple value",
                             "INSERT INTO %s (k, m) VALUES (0, ?)",
                             map(tuple(1, "1", 1.0, 1), 1, tuple(2, "2", 2.0, 2), 2));

        assertInvalidMessage("Invalid map literal for m: key (1, '1', 1.0, 1) is not of type frozen<tuple<int, text, double>>",
                             "INSERT INTO %s (k, m) VALUES (0, {(1, '1', 1.0, 1) : 1})");

        createTable("CREATE TABLE %s (k int PRIMARY KEY, m frozen<map<int, tuple<int, text, double>>>)");
        assertInvalidMessage("Invalid remaining data after end of tuple value",
                             "INSERT INTO %s (k, m) VALUES (0, ?)",
                             map(1, tuple(1, "1", 1.0, 1), 2, tuple(2, "2", 2.0, 2)));

        assertInvalidMessage("Invalid map literal for m: value (1, '1', 1.0, 1) is not of type frozen<tuple<int, text, double>>",
                             "INSERT INTO %s (k, m) VALUES (0, {1 : (1, '1', 1.0, 1)})");
    }
}
