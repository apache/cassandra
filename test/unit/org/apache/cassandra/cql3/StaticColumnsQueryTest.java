package org.apache.cassandra.cql3;

import org.junit.Test;

/**
 * Test column ranges and ordering with static column in table
 */
public class StaticColumnsQueryTest extends CQLTester
{
    @Test
    public void testSingleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c text, v text, s text static, PRIMARY KEY (p, c))");

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p2"),
            row("p2", null, "sv2", null)
        );

        // Ascending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p2"),
            row("p2", null, "sv2", null)
        );

        // Descending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p2"),
            row("p2", null, "sv2", null)
        );

        // No order with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c =?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k0"));

        // Ascending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c =? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k0"));

        // Descending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c =? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k0"));

        // IN

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?)", "p1", "k1", "k2"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c ASC", "p1", "k1", "k2"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c DESC", "p1", "k1", "k2"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );
    }

    @Test
    public void testSingleClusteringReversed() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c text, v text, s text static, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p2"),
            row("p2", null, "sv2", null)
        );

        // Ascending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p2"),
            row("p2", null, "sv2", null)
        );

        // Descending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p2"),
            row("p2", null, "sv2", null)
        );

        // No order with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c=?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k0"));

        // Ascending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k0"));

        // Descending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k0"));

        // IN

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?)", "p1", "k1", "k2"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c ASC", "p1", "k1", "k2"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c DESC", "p1", "k1", "k2"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );
    }
}
