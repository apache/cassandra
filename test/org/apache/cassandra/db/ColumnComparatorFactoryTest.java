package org.apache.cassandra.db;

import org.testng.annotations.Test;

import java.util.Comparator;

public class ColumnComparatorFactoryTest {
    public Comparator<IColumn> nameComparator  = ColumnComparatorFactory.getComparator(ColumnComparatorFactory.ComparatorType.NAME);

    @Test
    public void testLT() {
        IColumn col1 = new Column("Column-8");
        IColumn col2 = new Column("Column-9");
        assert nameComparator.compare(col1, col2) < 0;
    }

    @Test
    public void testGT() {
        IColumn col1 = new Column("Column-9");
        IColumn col2 = new Column("Column-10");
        // tricky -- remember we're comparing _lexically_
        assert nameComparator.compare(col1, col2) > 0;
	}
}
