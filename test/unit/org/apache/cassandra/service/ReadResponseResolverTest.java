package org.apache.cassandra.service;

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Row;
import static org.apache.cassandra.db.TableTest.assertColumns;
import static org.apache.cassandra.Util.column;
import static junit.framework.Assert.assertNull;

public class ReadResponseResolverTest
{
    @Test
    public void testResolveSupersetNewer()
    {
        ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
        cf1.addColumn(column("c1", "v1", 0));
        Row row1 = new Row("key1", cf1);

        ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
        cf2.addColumn(column("c1", "v2", 1));
        Row row2 = new Row("key1", cf2);

        Row resolved = ReadResponseResolver.resolveSuperset(Arrays.asList(row1, row2));
        assertColumns(resolved.cf, "c1");
        assertColumns(row1.diff(resolved), "c1");
        assertNull(row2.diff(resolved));
    }

    @Test
    public void testResolveSupersetDisjoint()
    {
        ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
        cf1.addColumn(column("c1", "v1", 0));
        Row row1 = new Row("key1", cf1);

        ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
        cf2.addColumn(column("c2", "v2", 1));
        Row row2 = new Row("key1", cf2);

        Row resolved = ReadResponseResolver.resolveSuperset(Arrays.asList(row1, row2));
        assertColumns(resolved.cf, "c1", "c2");
        assertColumns(row1.diff(resolved), "c2");
        assertColumns(row2.diff(resolved), "c1");
    }

    @Test
    public void testResolveSupersetNullOne()
    {
        Row row1 = new Row("key1", null);

        ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
        cf2.addColumn(column("c2", "v2", 1));
        Row row2 = new Row("key1", cf2);

        Row resolved = ReadResponseResolver.resolveSuperset(Arrays.asList(row1, row2));
        assertColumns(resolved.cf, "c2");
        assertColumns(row1.diff(resolved), "c2");
        assertNull(row2.diff(resolved));
    }

    @Test
    public void testResolveSupersetNullTwo()
    {
        ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
        cf1.addColumn(column("c1", "v1", 0));
        Row row1 = new Row("key1", cf1);

        Row row2 = new Row("key1", null);

        Row resolved = ReadResponseResolver.resolveSuperset(Arrays.asList(row1, row2));
        assertColumns(resolved.cf, "c1");
        assertNull(row1.diff(resolved));
        assertColumns(row2.diff(resolved), "c1");
    }

    @Test
    public void testResolveSupersetNullBoth()
    {
        Row row1 = new Row("key1", null);
        Row row2 = new Row("key1", null);

        Row resolved = ReadResponseResolver.resolveSuperset(Arrays.asList(row1, row2));
        assertNull(resolved.cf);
        assertNull(row1.diff(resolved));
        assertNull(row2.diff(resolved));
    }
}
