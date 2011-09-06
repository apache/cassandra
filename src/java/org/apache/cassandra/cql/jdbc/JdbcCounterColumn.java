package org.apache.cassandra.cql.jdbc;

public class JdbcCounterColumn extends JdbcLong
{
    public static final JdbcCounterColumn instance = new JdbcCounterColumn();
    
    JdbcCounterColumn() {}
}
