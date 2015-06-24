package org.apache.cassandra.cql3.validation.operations;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class SelectLimitTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setPartitioner(new ByteOrderedPartitioner());
    }

    /**
     * Test limit across a partition range, requires byte ordered partitioner,
     * migrated from cql_tests.py:TestCQL.limit_range_test()
     */
    @Test
    public void testPartitionRange() throws Throwable
    {
        createTable("CREATE TABLE %s (userid int, url text, time bigint, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE");

        for (int i = 0; i < 100; i++)
            for (String tld : new String[] { "com", "org", "net" })
                execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", i, String.format("http://foo.%s", tld), 42L);

        assertRows(execute("SELECT * FROM %s WHERE token(userid) >= token(2) LIMIT 1"),
                   row(2, "http://foo.com", 42L));

        assertRows(execute("SELECT * FROM %s WHERE token(userid) > token(2) LIMIT 1"),
                   row(3, "http://foo.com", 42L));
    }

    /**
     * Test limit across a column range,
     * migrated from cql_tests.py:TestCQL.limit_multiget_test()
     */
    @Test
    public void testColumnRange() throws Throwable
    {
        createTable("CREATE TABLE %s (userid int, url text, time bigint, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE");

        for (int i = 0; i < 100; i++)
            for (String tld : new String[] { "com", "org", "net" })
                execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", i, String.format("http://foo.%s", tld), 42L);

        // Check that we do limit the output to 1 *and* that we respect query
        // order of keys (even though 48 is after 2)
        assertRows(execute("SELECT * FROM %s WHERE userid IN (48, 2) LIMIT 1"),
                   row(48, "http://foo.com", 42L));
    }

    /**
     * Test limit queries on a sparse table,
     * migrated from cql_tests.py:TestCQL.limit_sparse_test()
     */
    @Test
    public void testSparseTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid int, url text, day int, month text, year int, PRIMARY KEY (userid, url))");

        for (int i = 0; i < 100; i++)
            for (String tld : new String[] { "com", "org", "net" })
                execute("INSERT INTO %s (userid, url, day, month, year) VALUES (?, ?, 1, 'jan', 2012)", i, String.format("http://foo.%s", tld));

        assertRowCount(execute("SELECT * FROM %s LIMIT 4"), 4);

    }

    /**
     * Check for #7052 bug,
     * migrated from cql_tests.py:TestCQL.limit_compact_table()
     */
    @Test
    public void testLimitInCompactTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY (k, v) ) WITH COMPACT STORAGE ");

        for (int i = 0; i < 4; i++)
            for (int j = 0; j < 4; j++)
                execute("INSERT INTO %s(k, v) VALUES (?, ?)", i, j);

        assertRows(execute("SELECT v FROM %s WHERE k=0 AND v > 0 AND v <= 4 LIMIT 2"),
                   row(1),
                   row(2));
        assertRows(execute("SELECT v FROM %s WHERE k=0 AND v > -1 AND v <= 4 LIMIT 2"),
                   row(0),
                   row(1));
        assertRows(execute("SELECT * FROM %s WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 2"),
                   row(0, 1),
                   row(0, 2));
        assertRows(execute("SELECT * FROM %s WHERE k IN (0, 1, 2) AND v > -1 AND v <= 4 LIMIT 2"),
                   row(0, 0),
                   row(0, 1));
        assertRows(execute("SELECT * FROM %s WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 6"),
                   row(0, 1),
                   row(0, 2),
                   row(0, 3),
                   row(1, 1),
                   row(1, 2),
                   row(1, 3));

        // strict bound (v > 1) over a range of partitions is not supported for compact storage if limit is provided
        assertInvalidThrow(InvalidRequestException.class, "SELECT * FROM %s WHERE v > 1 AND v <= 3 LIMIT 6 ALLOW FILTERING");
    }
}
