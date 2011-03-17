package org.apache.cassandra.cql.jdbc;

import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;

class TypedColumn<N, V>
{
    private final N name;
    private final V value;
    private final AbstractType<N> nameType;
    private final AbstractType<V> valueType;
    private final String nameString;
    private final String valueString;
    
    public TypedColumn(AbstractType<N> comparator, byte[] name, AbstractType<V> validator, byte[] value)
    {
        ByteBuffer bbName = ByteBuffer.wrap(name);
        ByteBuffer bbValue = ByteBuffer.wrap(value);
        this.name = comparator.compose(bbName);
        this.value = validator.compose(bbValue);
        nameType = comparator;
        valueType = validator;
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
    
    public AbstractType<N> getNameType()
    {
        return nameType;
    }
    
    public AbstractType<V> getValueType()
    {
        return valueType;
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
