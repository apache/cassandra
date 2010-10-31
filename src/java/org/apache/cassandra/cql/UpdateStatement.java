package org.apache.cassandra.cql;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.ConsistencyLevel;

public class UpdateStatement
{
    private String columnFamily;
    private List<Row> rows = new ArrayList<Row>();
    private ConsistencyLevel cLevel;
    
    public UpdateStatement(String columnFamily, Row first, ConsistencyLevel cLevel)
    {
        this.columnFamily = columnFamily;
        this.cLevel = cLevel;
        and(first);
    }
    
    public void and(Row row)
    {
        rows.add(row);
    }

    public List<Row> getRows()
    {
        return rows;
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return cLevel;
    }

    public String getColumnFamily()
    {
        return columnFamily;
    }
}
