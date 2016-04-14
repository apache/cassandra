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
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
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
                   row(2, "http://foo.com", 42L));

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
        assertRows(execute("SELECT * FROM %s WHERE v > 1 AND v <= 3 LIMIT 6 ALLOW FILTERING"),
                   row(0, 2),
                   row(0, 3),
                   row(1, 2),
                   row(1, 3),
                   row(2, 2),
                   row(2, 3));
    }

    @Test
    public void testPerPartitionLimit() throws Throwable
    {
        perPartitionLimitTest(false);
    }

    @Test
    public void testPerPartitionLimitWithCompactStorage() throws Throwable
    {
        perPartitionLimitTest(true);
    }

    private void perPartitionLimitTest(boolean withCompactStorage) throws Throwable
    {
        String query = "CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))";

        if (withCompactStorage)
            createTable(query + " WITH COMPACT STORAGE");
        else
            createTable(query);

        for (int i = 0; i < 5; i++)
        {
            for (int j = 0; j < 5; j++)
            {
                execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", i, j, j);
            }
        }

        assertInvalidMessage("LIMIT must be strictly positive",
                             "SELECT * FROM %s PER PARTITION LIMIT ?", 0);
        assertInvalidMessage("LIMIT must be strictly positive",
                             "SELECT * FROM %s PER PARTITION LIMIT ?", -1);

        assertRowsIgnoringOrder(execute("SELECT * FROM %s PER PARTITION LIMIT ?", 2),
                                row(0, 0, 0),
                                row(0, 1, 1),
                                row(1, 0, 0),
                                row(1, 1, 1),
                                row(2, 0, 0),
                                row(2, 1, 1),
                                row(3, 0, 0),
                                row(3, 1, 1),
                                row(4, 0, 0),
                                row(4, 1, 1));

        // Combined Per Partition and "global" limit
        assertRowCount(execute("SELECT * FROM %s PER PARTITION LIMIT ? LIMIT ?", 2, 6),
                       6);

        // odd amount of results
        assertRowCount(execute("SELECT * FROM %s PER PARTITION LIMIT ? LIMIT ?", 2, 5),
                       5);

        // IN query
        assertRows(execute("SELECT * FROM %s WHERE a IN (2,3) PER PARTITION LIMIT ?", 2),
                   row(2, 0, 0),
                   row(2, 1, 1),
                   row(3, 0, 0),
                   row(3, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE a IN (2,3) PER PARTITION LIMIT ? LIMIT 3", 2),
                   row(2, 0, 0),
                   row(2, 1, 1),
                   row(3, 0, 0));

        assertRows(execute("SELECT * FROM %s WHERE a IN (1,2,3) PER PARTITION LIMIT ? LIMIT 3", 2),
                   row(1, 0, 0),
                   row(1, 1, 1),
                   row(2, 0, 0));

        // with restricted partition key
        assertRows(execute("SELECT * FROM %s WHERE a = ? PER PARTITION LIMIT ?", 2, 3),
                   row(2, 0, 0),
                   row(2, 1, 1),
                   row(2, 2, 2));

        // with ordering
        assertRows(execute("SELECT * FROM %s WHERE a IN (3, 2) ORDER BY b DESC PER PARTITION LIMIT ?", 2),
                   row(2, 4, 4),
                   row(3, 4, 4),
                   row(2, 3, 3),
                   row(3, 3, 3));

        assertRows(execute("SELECT * FROM %s WHERE a IN (3, 2) ORDER BY b DESC PER PARTITION LIMIT ? LIMIT ?", 3, 4),
                   row(2, 4, 4),
                   row(3, 4, 4),
                   row(2, 3, 3),
                   row(3, 3, 3));

        assertRows(execute("SELECT * FROM %s WHERE a = ? ORDER BY b DESC PER PARTITION LIMIT ?", 2, 3),
                   row(2, 4, 4),
                   row(2, 3, 3),
                   row(2, 2, 2));

        // with filtering
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b > ? PER PARTITION LIMIT ? ALLOW FILTERING", 2, 0, 2),
                   row(2, 1, 1),
                   row(2, 2, 2));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b > ? ORDER BY b DESC PER PARTITION LIMIT ? ALLOW FILTERING", 2, 2, 2),
                   row(2, 4, 4),
                   row(2, 3, 3));

        assertInvalidMessage("PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries",
                             "SELECT DISTINCT a FROM %s PER PARTITION LIMIT ?", 3);
        assertInvalidMessage("PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries",
                             "SELECT DISTINCT a FROM %s PER PARTITION LIMIT ? LIMIT ?", 3, 4);
        assertInvalidMessage("PER PARTITION LIMIT is not allowed with aggregate queries.",
                             "SELECT COUNT(*) FROM %s PER PARTITION LIMIT ?", 3);
    }
}
