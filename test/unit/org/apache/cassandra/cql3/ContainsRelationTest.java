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
}
