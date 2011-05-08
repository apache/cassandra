package org.apache.cassandra.cql.jdbc;

import java.sql.ResultSet;

public interface CassandraResultSet extends ResultSet
{
    /**
     * @return the current row key
     */
    public byte[] getKey();

    /**
     * @return the raw column data for the given column offset
     */
    public TypedColumn getColumn(int i);

    /**
     * @return the raw column data for the given column name
     */
    public TypedColumn getColumn(String name);
}
