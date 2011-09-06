package org.apache.cassandra.cql.jdbc;

public class JdbcCounterColumn extends LongTerm
{
    public static final JdbcCounterColumn instance = new JdbcCounterColumn();
    
    JdbcCounterColumn() {}
}
