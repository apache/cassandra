package org.apache.cassandra.cql.jdbc;

import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;

class TypedColumn<N, V>
{
    private final N name;
    private final V value;
    
    // we cache the string versions of the byte buffers here.  It turns out that {N|V}.toString() isn't always the same
    // (a good example is byte buffers) as the stringified versions supplied by the AbstractTypes.
    private final String nameString;
    private final String valueString;
    
    public TypedColumn(AbstractType<N> comparator, byte[] name, AbstractType<V> validator, byte[] value)
    {
        ByteBuffer bbName = ByteBuffer.wrap(name);
        ByteBuffer bbValue = ByteBuffer.wrap(value);
        this.name = comparator.compose(bbName);
        this.value = validator.compose(bbValue);
        nameString = comparator.getString(bbName);
        valueString = validator.getString(bbValue);
    }
    
    public N getName()
    {
        return name;
    }
    
    public V getValue()
    {
        return value;
    }
    
    public String getNameString()
    {
        return nameString;
    }
    
    public String getValueString()
    {
        return valueString;
    }
}
