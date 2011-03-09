package org.apache.cassandra.cql.driver;

public class Col<N, V>
{
    public final N name;
    public final V value;
    
    public Col(N name, V value)
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
