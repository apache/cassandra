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

public class ContainsRelationTest extends CQLTester
{
    @Test
    public void testSetContains() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories set<text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, set("lmn"));

        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "xyz", "lmn"));

        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS ?", "lmn"),
            row("test", 5, set("lmn"))
        );

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "test", "lmn"),
            row("test", 5, set("lmn"))
        );

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, "lmn"),
                   row("test", 5, set("lmn"))
        );

        assertInvalid("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND categories CONTAINS ?", "xyz", "lmn", "notPresent");
        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND categories CONTAINS ? ALLOW FILTERING", "xyz", "lmn", "notPresent"));
    }

    @Test
    public void testListContains() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories list<text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, list("lmn"));

        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "xyz", "lmn"));

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?;", "test", "lmn"),
            row("test", 5, list("lmn"))
        );

        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS ?", "lmn"),
            row("test", 5, list("lmn"))
        );

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?;", "test", 5, "lmn"),
                   row("test", 5, list("lmn"))
        );

        assertInvalid("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ?",
                      "test", 5, "lmn", "notPresent");
        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ? ALLOW FILTERING",
                            "test", 5, "lmn", "notPresent"));
    }

    @Test
    public void testListContainsWithFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (e int PRIMARY KEY, f list<text>, s int)");
        createIndex("CREATE INDEX ON %s(f)");
        for(int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (e, f, s) VALUES (?, ?, ?)", i, list("Dubai"), 4);
        }
        for(int i = 3; i < 5; i++)
        {
            execute("INSERT INTO %s (e, f, s) VALUES (?, ?, ?)", i, list("Dubai"), 3);
        }
        assertRows(execute("SELECT * FROM %s WHERE f CONTAINS ? AND s=? allow filtering", "Dubai", 3),
                   row(3, list("Dubai"), 3),
                   row(4, list("Dubai"), 3));
    }

    @Test
    public void testMapKeyContains() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(keys(categories))");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));

        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ?", "xyz", "lmn"));

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ?", "test", "lmn"),
            row("test", 5, map("lmn", "foo"))
        );
        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS KEY ?", "lmn"),
            row("test", 5, map("lmn", "foo"))
        );

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ?", "test", 5, "lmn"),
                   row("test", 5, map("lmn", "foo"))
        );

        assertInvalid("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ? AND categories CONTAINS KEY ?",
                      "test", 5, "lmn", "notPresent");
        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ? AND categories CONTAINS KEY ? ALLOW FILTERING",
                            "test", 5, "lmn", "notPresent"));

        assertInvalid("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS KEY ? AND categories CONTAINS ?",
                      "test", 5, "lmn", "foo");
    }

    @Test
    public void testMapValueContains() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));

        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "xyz", "foo"));

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "test", "foo"),
            row("test", 5, map("lmn", "foo"))
        );

        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS ?", "foo"),
            row("test", 5, map("lmn", "foo"))
        );

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ?", "test", 5, "foo"),
                   row("test", 5, map("lmn", "foo"))
        );

        assertInvalid("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ?"
                           , "test", 5, "foo", "notPresent");

        assertEmpty(execute("SELECT * FROM %s WHERE account = ? AND id = ? AND categories CONTAINS ? AND categories CONTAINS ? ALLOW FILTERING"
                           , "test", 5, "foo", "notPresent"));
    }

    // See CASSANDRA-7525
    @Test
    public void testQueryMultipleIndexTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");

        // create an index on
        createIndex("CREATE INDEX id_index ON %s(id)");
        createIndex("CREATE INDEX categories_values_index ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));

        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS ? AND id = ? ALLOW FILTERING", "foo", 5),
                row("test", 5, map("lmn", "foo"))
        );

        assertRows(
            execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND id = ? ALLOW FILTERING", "test", "foo", 5),
            row("test", 5, map("lmn", "foo"))
        );
    }

    // See CASSANDRA-8033
    @Test
    public void testFilterForContains() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v set<int>, PRIMARY KEY ((k1, k2)))");
        createIndex("CREATE INDEX ON %s(k2)");

        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 0, set(1, 2, 3));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 1, set(2, 3, 4));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 0, set(3, 4, 5));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 1, set(4, 5, 6));

        assertRows(execute("SELECT * FROM %s WHERE k2 = ?", 1),
            row(0, 1, set(2, 3, 4)),
            row(1, 1, set(4, 5, 6))
        );

        assertRows(execute("SELECT * FROM %s WHERE k2 = ? AND v CONTAINS ? ALLOW FILTERING", 1, 6),
            row(1, 1, set(4, 5, 6))
        );

        assertEmpty(execute("SELECT * FROM %s WHERE k2 = ? AND v CONTAINS ? ALLOW FILTERING", 1, 7));
    }

    // See CASSANDRA-8073
    @Test
    public void testIndexLookupWithClusteringPrefix() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d set<int>, PRIMARY KEY (a, b, c))");
        createIndex("CREATE INDEX ON %s(d)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, set(1, 2, 3));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, set(3, 4, 5));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, set(1, 2, 3));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, set(3, 4, 5));

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=? AND d CONTAINS ?", 0, 1, 3),
            row(0, 1, 0, set(1, 2, 3)),
            row(0, 1, 1, set(3, 4, 5))
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=? AND d CONTAINS ?", 0, 1, 2),
            row(0, 1, 0, set(1, 2, 3))
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? AND b=? AND d CONTAINS ?", 0, 1, 5),
            row(0, 1, 1, set(3, 4, 5))
        );
    }

    @Test
    public void testContainsKeyAndContainsWithIndexOnMapKey() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(keys(categories))");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));
        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 6, map("lmn", "foo2"));

        assertInvalid("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "test", "foo");

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ?", "test", "lmn"),
                   row("test", 5, map("lmn", "foo")),
                   row("test", 6, map("lmn", "foo2")));
        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ? AND categories CONTAINS ? ALLOW FILTERING",
                           "test", "lmn", "foo"),
                   row("test", 5, map("lmn", "foo")));
        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND categories CONTAINS KEY ? ALLOW FILTERING",
                           "test", "foo", "lmn"),
                   row("test", 5, map("lmn", "foo")));
    }

    @Test
    public void testContainsKeyAndContainsWithIndexOnMapValue() throws Throwable
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id))");
        createIndex("CREATE INDEX ON %s(categories)");

        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 5, map("lmn", "foo"));
        execute("INSERT INTO %s (account, id , categories) VALUES (?, ?, ?)", "test", 6, map("lmn2", "foo"));

        assertInvalid("SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ?", "test", "lmn");

        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ?", "test", "foo"),
                   row("test", 5, map("lmn", "foo")),
                   row("test", 6, map("lmn2", "foo")));
        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS KEY ? AND categories CONTAINS ? ALLOW FILTERING",
                           "test", "lmn", "foo"),
                   row("test", 5, map("lmn", "foo")));
        assertRows(execute("SELECT * FROM %s WHERE account = ? AND categories CONTAINS ? AND categories CONTAINS KEY ? ALLOW FILTERING",
                           "test", "foo", "lmn"),
                   row("test", 5, map("lmn", "foo")));
    }
}
