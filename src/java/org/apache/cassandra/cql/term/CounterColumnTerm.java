package org.apache.cassandra.cql.term;

public class CounterColumnTerm extends LongTerm
{
    public static final CounterColumnTerm instance = new CounterColumnTerm();
    
    CounterColumnTerm() {}
}
