package org.apache.cassandra.cql;

public class Column
{
    private final Term name;
    private final Term value;
    
    public Column(Term name, Term value)
    {
        this.name = name;
        this.value = value;
    }

    public Term getName()
    {
        return name;
    }

    public Term getValue()
    {
        return value;
    }
}
