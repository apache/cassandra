package org.apache.cassandra.config;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.FBUtilities;

public class ColumnDefinition {
    public final byte[] name;
    public final AbstractType validator;
    public final IndexType index_type;
    public final String index_name;

    public ColumnDefinition(byte[] name, String validation_class, IndexType index_type, String index_name) throws ConfigurationException
    {
        this.name = name;
        this.index_type = index_type;
        this.index_name = index_name;
        this.validator = DatabaseDescriptor.getComparator(validation_class);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ColumnDefinition that = (ColumnDefinition) o;
        if (index_name != null ? !index_name.equals(that.index_name) : that.index_name != null)
            return false;
        if (index_type != null ? !index_type.equals(that.index_type) : that.index_type != null)
            return false;
        if (!Arrays.equals(name, that.name))
            return false;
        return !(validator != null ? !validator.equals(that.validator) : that.validator != null);
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? Arrays.hashCode(name) : 0;
        result = 31 * result + (validator != null ? validator.hashCode() : 0);
        result = 31 * result + (index_type != null ? index_type.hashCode() : 0);
        result = 31 * result + (index_name != null ? index_name.hashCode() : 0);
        return result;
    }

    public org.apache.cassandra.avro.ColumnDef deflate()
    {
        org.apache.cassandra.avro.ColumnDef cd = new org.apache.cassandra.avro.ColumnDef();
        cd.name = ByteBuffer.wrap(name);
        cd.validation_class = new Utf8(validator.getClass().getName());
        cd.index_type = index_type == null ? null :
            Enum.valueOf(org.apache.cassandra.avro.IndexType.class, index_type.name());
        cd.index_name = index_name == null ? null : new Utf8(index_name);
        return cd;
    }

    public static ColumnDefinition inflate(org.apache.cassandra.avro.ColumnDef cd) throws ConfigurationException
    {
        byte[] name = new byte[cd.name.remaining()];
        cd.name.get(name, 0, name.length);
        IndexType index_type = cd.index_type == null ? null :
            Enum.valueOf(IndexType.class, cd.index_type.name());
        String index_name = cd.index_name == null ? null : cd.index_name.toString();
        try
        {
            return new ColumnDefinition(name, cd.validation_class.toString(), index_type, index_name);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static ColumnDefinition fromColumnDef(ColumnDef cd) throws ConfigurationException
    {
        return new ColumnDefinition(cd.name, cd.validation_class, cd.index_type, cd.index_name);
    }

    public static Map<byte[], ColumnDefinition> fromColumnDef(List<ColumnDef> thriftDefs) throws ConfigurationException
    {
        if (thriftDefs == null)
            return Collections.emptyMap();

        Map<byte[], ColumnDefinition> cds = new TreeMap<byte[], ColumnDefinition>(FBUtilities.byteArrayComparator);
        for (ColumnDef thriftColumnDef : thriftDefs)
        {
            cds.put(thriftColumnDef.name, fromColumnDef(thriftColumnDef));
        }

        return Collections.unmodifiableMap(cds);
    }
}
