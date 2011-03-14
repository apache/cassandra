package org.apache.cassandra.cql.jdbc;

class TypedColumn<N, V>
{
    public final N name;
    public final V value;
    
    public TypedColumn(N name, V value)
    {
        this.name = name;
        this.value = value;
    }
    
    public N getName()
    {
        return name;
    }
    
    public V getValue()
    {
        return value;
    }
}
