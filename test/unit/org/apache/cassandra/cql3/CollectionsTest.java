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

        execute("DELETE s FROM %s WHERE k = 0");

        assertRows(execute("SELECT s FROM %s WHERE k = 0"),
            row((Object)null)
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

        assertRows(execute("SELECT l FROM %s WHERE k = 0"),
            row(list("v1", "v2", "v3"))
        );

        execute("DELETE l[?] FROM %s WHERE k = 0", 1);

        assertRows(execute("SELECT l FROM %s WHERE k = 0"),
            row(list("v1", "v3"))
        );

        execute("UPDATE %s SET l[?] = ? WHERE k = 0", 1, "v4");

        assertRows(execute("SELECT l FROM %s WHERE k = 0"),
            row(list("v1", "v4"))
        );

        // Full overwrite
        execute("UPDATE %s SET l = ? WHERE k = 0", list("v6", "v5"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"),
            row(list("v6", "v5"))
        );

        execute("UPDATE %s SET l = l + ? WHERE k = 0", list("v7", "v8"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"),
            row(list("v6", "v5", "v7", "v8"))
        );

        execute("UPDATE %s SET l = ? + l WHERE k = 0", list("v9"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"),
            row(list("v9", "v6", "v5", "v7", "v8"))
        );

        execute("UPDATE %s SET l = l - ? WHERE k = 0", list("v5", "v8"));

        assertRows(execute("SELECT l FROM %s WHERE k = 0"),
            row(list("v9", "v6", "v7"))
        );

        execute("DELETE l FROM %s WHERE k = 0");

        assertRows(execute("SELECT l FROM %s WHERE k = 0"),
            row((Object)null)
        );
    }
}
