package org.apache.cassandra.config;

import java.io.*;
import java.util.*;

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

    public static byte[] serialize(ColumnDefinition cd) throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bout);
        out.writeInt(cd.name.length);
        out.write(cd.name);
        out.writeUTF(cd.validator.getClass().getName());

        out.writeBoolean(cd.index_type != null);
        if (cd.index_type != null)
            out.writeInt(cd.index_type.ordinal());

        out.writeBoolean(cd.index_name != null);
        if (cd.index_name != null)
            out.writeUTF(cd.index_name);

        out.close();
        return bout.toByteArray();
    }

    public static ColumnDefinition deserialize(byte[] bytes) throws IOException
    {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        int nameSize = in.readInt();
        byte[] name = new byte[nameSize];
        in.readFully(name);
        String validation_class = in.readUTF();

        IndexType index_type = null;
        if (in.readBoolean())
            index_type = IndexType.values()[in.readInt()];

        String index_name = null;
        if (in.readBoolean())
            index_name = in.readUTF();

        try
        {
            return new ColumnDefinition(name, validation_class, index_type, index_name);
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
