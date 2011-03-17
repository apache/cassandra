package org.apache.cassandra.cql.jdbc;

/**
 * This interface lets AbstractTypes derived outside of the cassandra project answer JDBC questions
 * relating to type.  Only bother implmenting this on your AbstractTypes if you're going to use JDBC
 * and ResultSetMetaData.
 */
public interface ColumnMetaData
{
    public boolean isSigned();
    public boolean isCaseSensitive();
    public boolean isCurrency();
    public int getPrecision();
    public int getScale();
    public int getType();
}
