package org.apache.cassandra.stress.generate;

public class Row
{

    final Object[] partitionKey;
    final Object[] row;

    public Row(Object[] partitionKey, Object[] row)
    {
        this.partitionKey = partitionKey;
        this.row = row;
    }

    public Object get(int column)
    {
        if (column < 0)
            return partitionKey[-1-column];
        return row[column];
    }

}
