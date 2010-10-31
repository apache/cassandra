package org.apache.cassandra.cql;

import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 * 
 * @author eevans
 *
 */
public class SelectStatement
{
    private final String columnFamily;
    private final ConsistencyLevel cLevel;
    private final SelectExpression expression;
    private final int numRecords;
    private final int numColumns;
    private final boolean reverse;
    
    public SelectStatement(String columnFamily, ConsistencyLevel cLevel, SelectExpression expression,
            int numRecords, int numColumns, boolean reverse)
    {
        this.columnFamily = columnFamily;
        this.cLevel = cLevel;
        this.expression = expression;
        this.numRecords = numRecords;
        this.numColumns = numColumns;
        this.reverse = reverse;
    }
    
    public Predicates getKeyPredicates()
    {
        return expression.getKeyPredicates();
    }
    
    public Predicates getColumnPredicates()
    {
        return expression.getColumnPredicates();
    }
    
    public String getColumnFamily()
    {
        return columnFamily;
    }
    
    public boolean reversed()
    {
        return reverse;
    }
    
    public ConsistencyLevel getConsistencyLevel()
    {
        return cLevel;
    }

    public int getNumRecords()
    {
        return numRecords;
    }

    public int getNumColumns()
    {
        return numColumns;
    }
}
