package org.apache.cassandra.cql;

import java.util.ArrayList;
import java.util.List;

public class Row
{
    private final Term key;
    private List<Column> columns = new ArrayList<Column>();
    
    public Row(Term key, Column firstColumn)
    {
        this.key = key;
        columns.add(firstColumn);
    }
    
    public void and(Column col)
    {
        columns.add(col);
    }

    public Term getKey()
    {
        return key;
    }

    public List<Column> getColumns()
    {
        return columns;
    }
}
