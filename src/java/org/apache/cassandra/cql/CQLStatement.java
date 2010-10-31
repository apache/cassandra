package org.apache.cassandra.cql;

public class CQLStatement
{
    public StatementType type;
    public Object statement;
    
    public CQLStatement(StatementType type, Object statement)
    {
        this.type = type;
        this.statement = statement;
    }
}
