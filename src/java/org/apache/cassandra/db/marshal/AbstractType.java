package org.apache.cassandra.db.marshal;

import java.util.Comparator;
import java.util.Collection;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SuperColumn;

public abstract class AbstractType implements Comparator<byte[]>
{
    /** get a string representation of the bytes suitable for log messages */
    public abstract String getString(byte[] bytes);

    /** validate that the byte array is a valid sequence for the type we are supposed to be comparing */
    public void validate(byte[] bytes)
    {
        getString(bytes);
    }

    /** convenience method */
    public String getString(Collection<byte[]> names)
    {
        StringBuilder builder = new StringBuilder();
        for (byte[] name : names)
        {
            builder.append(getString(name)).append(",");
        }
        return builder.toString();
    }

    /** convenience method */
    public String getColumnsString(Collection<IColumn> columns)
    {
        StringBuilder builder = new StringBuilder();
        for (IColumn column : columns)
        {
            builder.append(getString(column.name())).append(",");
        }
        return builder.toString();
    }
}
