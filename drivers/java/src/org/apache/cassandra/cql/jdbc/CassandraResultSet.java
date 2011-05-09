package org.apache.cassandra.cql.jdbc;

import java.math.BigInteger;
import java.sql.ResultSet;

public interface CassandraResultSet extends ResultSet
{
    /**
     * @return the current row key
     */
    public byte[] getKey();

    /** @return a BigInteger value for the given column offset*/
    public BigInteger getBigInteger(int i);
    /** @return a BigInteger value for the given column name */
    public BigInteger getBigInteger(String name);

    /** @return the raw column data for the given column offset */
    public TypedColumn getColumn(int i);
    /** @return the raw column data for the given column name */
    public TypedColumn getColumn(String name);
}
