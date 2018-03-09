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

import org.junit.Test;
import org.apache.cassandra.cql3.CQLTester;

public class SecondaryIndexOnStaticColumnTest extends CQLTester
{
    @Test
    public void testSimpleStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, name text, age int static, PRIMARY KEY (id, name))");

        createIndex("CREATE INDEX static_age on %s(age)");
        int id1 = 1, id2 = 2, age1 = 24, age2 = 32;
        String name1A = "Taylor", name1B = "Swift",
               name2 = "Jamie";

        execute("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", id1, name1A, age1);
        execute("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", id1, name1B, age1);
        execute("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", id2, name2, age2);

        assertRows(execute("SELECT id, name, age FROM %s WHERE age=?", age1),
              row(id1, name1B, age1), row(id1, name1A, age1));
        assertRows(execute("SELECT id, name, age FROM %s WHERE age=?", age2),
                row(id2, name2, age2));

        // Update the rows. Validate that updated values will be reflected in the index.
        int newAge1 = 40;
        execute("UPDATE %s SET age = ? WHERE id = ?", newAge1, id1);
        assertEmpty(execute("SELECT id, name, age FROM %s WHERE age=?", age1));
        assertRows(execute("SELECT id, name, age FROM %s WHERE age=?", newAge1),
                row(id1, name1B, newAge1), row(id1, name1A, newAge1));
        execute("DELETE FROM %s WHERE id = ?", id2);
        assertEmpty(execute("SELECT id, name, age FROM %s WHERE age=?", age2));
    }

    @Test
    public void testIndexOnCompoundRowKey() throws Throwable
    {
        createTable("CREATE TABLE %s (interval text, seq int, id int, severity int static, PRIMARY KEY ((interval, seq), id) ) WITH CLUSTERING ORDER BY (id DESC)");

        execute("CREATE INDEX ON %s (severity)");

        execute("insert into %s (interval, seq, id , severity) values('t',1, 3, 10)");
        execute("insert into %s (interval, seq, id , severity) values('t',1, 4, 10)");
        execute("insert into %s (interval, seq, id , severity) values('t',2, 3, 10)");
        execute("insert into %s (interval, seq, id , severity) values('t',2, 4, 10)");
        execute("insert into %s (interval, seq, id , severity) values('m',1, 3, 11)");
        execute("insert into %s (interval, seq, id , severity) values('m',1, 4, 11)");
        execute("insert into %s (interval, seq, id , severity) values('m',2, 3, 11)");
        execute("insert into %s (interval, seq, id , severity) values('m',2, 4, 11)");

        assertRows(execute("select * from %s where severity = 10 and interval = 't' and seq = 1"),
                   row("t", 1, 4, 10), row("t", 1, 3, 10));
    }

    @Test
    public void testIndexOnCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, l list<int> static, s set<text> static, m map<text, int> static, PRIMARY KEY (k, v))");

        createIndex("CREATE INDEX ON %s (l)");
        createIndex("CREATE INDEX ON %s (s)");
        createIndex("CREATE INDEX ON %s (m)");
        createIndex("CREATE INDEX ON %s (keys(m))");

        execute("INSERT INTO %s (k, v, l, s, m) VALUES (0, 0, [1, 2],    {'a'},      {'a' : 1, 'b' : 2})");
        execute("INSERT INTO %s (k, v)          VALUES (0, 1)                                  ");
        execute("INSERT INTO %s (k, v, l, s, m) VALUES (1, 0, [4, 5],    {'d'},      {'b' : 1, 'c' : 4})");

        // lists
        assertRows(execute("SELECT k, v FROM %s WHERE l CONTAINS 1"), row(0, 0), row(0, 1));
        assertEmpty(execute("SELECT k, v FROM %s WHERE k = 1 AND l CONTAINS 1"));
        assertRows(execute("SELECT k, v FROM %s WHERE l CONTAINS 4"), row(1, 0));
        assertEmpty(execute("SELECT k, v FROM %s WHERE l CONTAINS 6"));

        // update lists
        execute("UPDATE %s SET l = l + [3] WHERE k = ?", 0);
        assertRows(execute("SELECT k, v FROM %s WHERE l CONTAINS 3"), row(0, 0), row(0, 1));

        // sets
        assertRows(execute("SELECT k, v FROM %s WHERE s CONTAINS 'a'"), row(0, 0), row(0, 1));
        assertRows(execute("SELECT k, v FROM %s WHERE k = 0 AND s CONTAINS 'a'"), row(0, 0), row(0, 1));
        assertRows(execute("SELECT k, v FROM %s WHERE s CONTAINS 'd'"), row(1, 0));
        assertEmpty(execute("SELECT k, v FROM %s  WHERE s CONTAINS 'e'"));

        // update sets
        execute("UPDATE %s SET s = s + {'b'} WHERE k = ?", 0);
        assertRows(execute("SELECT k, v FROM %s WHERE s CONTAINS 'b'"), row(0, 0), row(0, 1));
        execute("UPDATE %s SET s = s - {'a'} WHERE k = ?", 0);
        assertEmpty(execute("SELECT k, v FROM %s WHERE s CONTAINS 'a'"));

        // maps
        assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS 1"), row(1, 0), row(0, 0), row(0, 1));
        assertRows(execute("SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS 1"), row(0, 0), row(0, 1));
        assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS 4"), row(1, 0));
        assertEmpty(execute("SELECT k, v FROM %s  WHERE m CONTAINS 5"));

        assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS KEY 'b'"), row(1, 0), row(0, 0), row(0, 1));
        assertRows(execute("SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS KEY 'b'"), row(0, 0), row(0, 1));
        assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS KEY 'c'"), row(1, 0));
        assertEmpty(execute("SELECT k, v FROM %s  WHERE m CONTAINS KEY 'd'"));

        // update maps.
        execute("UPDATE %s SET m['c'] = 5 WHERE k = 0");
        assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS 5"), row(0, 0), row(0, 1));
        assertRows(execute("SELECT k, v FROM %s WHERE m CONTAINS KEY 'c'"), row(1, 0), row(0, 0), row(0, 1));
        execute("DELETE m['a'] FROM %s WHERE k = 0");
        assertEmpty(execute("SELECT k, v FROM %s  WHERE m CONTAINS KEY 'a'"));
    }

    @Test
    public void testIndexOnFrozenCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, l frozen<list<int>> static, s frozen<set<text>> static, m frozen<map<text, int>> static, PRIMARY KEY (k, v))");

        createIndex("CREATE INDEX ON %s (FULL(l))");
        createIndex("CREATE INDEX ON %s (FULL(s))");
        createIndex("CREATE INDEX ON %s (FULL(m))");

        execute("INSERT INTO %s (k, v, l, s, m) VALUES (0, 0, [1, 2],    {'a'},      {'a' : 1, 'b' : 2})");
        execute("INSERT INTO %s (k, v)          VALUES (0, 1)                                  ");
        execute("INSERT INTO %s (k, v, l, s, m) VALUES (1, 0, [4, 5],    {'d'},      {'b' : 1, 'c' : 4})");
        execute("UPDATE %s SET l=[3], s={'3'}, m={'3': 3} WHERE k=3" );

        // lists
        assertRows(execute("SELECT k, v FROM %s WHERE l = [1, 2]"), row(0, 0), row(0, 1));
        assertEmpty(execute("SELECT k, v FROM %s WHERE k = 1 AND l = [1, 2]"));
        assertEmpty(execute("SELECT k, v FROM %s WHERE l = [4]"));
        assertRows(execute("SELECT k, v FROM %s WHERE l = [3]"), row(3, null));

        // update lists
        execute("UPDATE %s SET l = [1, 2, 3] WHERE k = ?", 0);
        assertEmpty(execute("SELECT k, v FROM %s WHERE l = [1, 2]"));
        assertRows(execute("SELECT k, v FROM %s WHERE l = [1, 2, 3]"), row(0, 0), row(0, 1));

        // sets
        assertRows(execute("SELECT k, v FROM %s WHERE s = {'a'}"), row(0, 0), row(0, 1));
        assertEmpty(execute("SELECT k, v FROM %s WHERE k = 1 AND s = {'a'}"));
        assertEmpty(execute("SELECT k, v FROM %s WHERE s = {'b'}"));
        assertRows(execute("SELECT k, v FROM %s WHERE s = {'3'}"), row(3, null));

        // update sets
        execute("UPDATE %s SET s = {'a', 'b'} WHERE k = ?", 0);
        assertEmpty(execute("SELECT k, v FROM %s WHERE s = {'a'}"));
        assertRows(execute("SELECT k, v FROM %s WHERE s = {'a', 'b'}"), row(0, 0), row(0, 1));

        // maps
        assertRows(execute("SELECT k, v FROM %s WHERE m = {'a' : 1, 'b' : 2}"), row(0, 0), row(0, 1));
        assertEmpty(execute("SELECT k, v FROM %s WHERE k = 1 AND m = {'a' : 1, 'b' : 2}"));
        assertEmpty(execute("SELECT k, v FROM %s WHERE m = {'a' : 1, 'b' : 3}"));
        assertEmpty(execute("SELECT k, v FROM %s WHERE m = {'a' : 1, 'c' : 2}"));
        assertRows(execute("SELECT k, v FROM %s WHERE m = {'3': 3}"), row(3, null));

        // update maps.
        execute("UPDATE %s SET m = {'a': 2, 'b': 3} WHERE k = ?", 0);
        assertEmpty(execute("SELECT k, v FROM %s WHERE m = {'a': 1, 'b': 2}"));
        assertRows(execute("SELECT k, v FROM %s WHERE m = {'a': 2, 'b': 3}"), row(0, 0), row(0, 1));
    }

    @Test
    public void testStaticIndexAndNonStaticIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, company text, age int static, salary int, PRIMARY KEY(id, company))");
        createIndex("CREATE INDEX on %s(age)");
        createIndex("CREATE INDEX on %s(salary)");

        String company1 = "company1", company2 = "company2";

        execute("INSERT INTO %s(id, company, age, salary) VALUES(?, ?, ?, ?)", 1, company1, 20, 1000);
        execute("INSERT INTO %s(id, company,      salary) VALUES(?, ?,    ?)", 1, company2,     2000);
        execute("INSERT INTO %s(id, company, age, salary) VALUES(?, ?, ?, ?)", 2, company1, 40, 2000);

        assertRows(execute("SELECT id, company, age, salary FROM %s WHERE age = 20 AND salary = 2000 ALLOW FILTERING"),
                   row(1, company2, 20, 2000));
    }

    @Test
    public void testIndexOnUDT() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (street text, city text)");

        createTable(String.format(
            "CREATE TABLE %%s (id int, company text, home frozen<%s> static, price int, PRIMARY KEY(id, company))",
            typeName));
        createIndex("CREATE INDEX on %s(home)");

        String addressString = "{street: 'Centre', city: 'C'}";
        String companyName = "Random";

        execute("INSERT INTO %s(id, company, home, price) "
                + "VALUES(1, '" + companyName + "', " + addressString + ", 10000)");
        assertRows(execute("SELECT id, company FROM %s WHERE home = " + addressString), row(1, companyName));
        String newAddressString = "{street: 'Fifth', city: 'P'}";

        execute("UPDATE %s SET home = " + newAddressString + " WHERE id = 1");
        assertEmpty(execute("SELECT id, company FROM %s WHERE home = " + addressString));
        assertRows(execute("SELECT id, company FROM %s WHERE home = " + newAddressString), row(1, companyName));
    }
}