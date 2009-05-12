package org.apache.cassandra.db;

import java.util.Arrays;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class RowTest
{
    @Test
    public void testDiffColumnFamily()
    {
        ColumnFamily cf1 = new ColumnFamily("Standard1", "Standard");
        cf1.addColumn("one", "onev".getBytes(), 0);

        ColumnFamily cf2 = new ColumnFamily("Standard1", "Standard");
        cf2.delete(0, 0);

        ColumnFamily cfDiff = cf1.diff(cf2);
        assertEquals(cfDiff.getColumns().size(), 0);
        assertEquals(cfDiff.getMarkedForDeleteAt(), 0);
    }

    @Test
    public void testDiffSuperColumn()
    {
        SuperColumn sc1 = new SuperColumn("one");
        sc1.addColumn(new Column("subcolumn", "A".getBytes(), 0));

        SuperColumn sc2 = new SuperColumn("one");
        sc2.markForDeleteAt(0, 0);

        SuperColumn scDiff = (SuperColumn)sc1.diff(sc2);
        assertEquals(scDiff.getSubColumns().size(), 0);
        assertEquals(scDiff.getMarkedForDeleteAt(), 0);
    }

    @Test
    public void testRepair()
    {
        Row row1 = new Row();
        ColumnFamily cf1 = new ColumnFamily("Standard1", "Standard");
        cf1.addColumn("one", "A".getBytes(), 0);
        row1.addColumnFamily(cf1);

        Row row2 = new Row();
        ColumnFamily cf2 = new ColumnFamily("Standard1", "Standard");
        cf2.addColumn("one", "B".getBytes(), 1);
        cf2.addColumn("two", "C".getBytes(), 1);
        ColumnFamily cf3 = new ColumnFamily("Standard2", "Standard");
        cf3.addColumn("three", "D".getBytes(), 1);
        row2.addColumnFamily(cf2);
        row2.addColumnFamily(cf3);

        row1.repair(row2);
        cf1 = row1.getColumnFamily("Standard1");
        assert Arrays.equals(cf1.getColumn("one").value(), "B".getBytes());
        assert Arrays.equals(cf2.getColumn("two").value(), "C".getBytes());
        assert row1.getColumnFamily("Standard2") != null;
    }
}
