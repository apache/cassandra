package org.apache.cassandra.config;

import java.io.*;
import java.util.*;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.utils.FBUtilities;

public class ColumnDefinition {
    public byte[] name;
    public String validation_class;
    public String index_type;
    public String index_name;

    public ColumnDefinition()
    {
        this(null, null, null, null);
    }

    public ColumnDefinition(byte[] name, String validation_class, String index_type, String index_name)
    {
        this.name = name;
        this.validation_class = validation_class;
        this.index_type = index_type;
        this.index_name = index_name;
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
                .append(name)
                .append(validation_class)
                .append(index_type)
                .append(index_name)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null || obj.getClass() != getClass())
        {
            return false;
        }

        ColumnDefinition rhs = (ColumnDefinition) obj;
        return new EqualsBuilder()
                .append(name, rhs.name)
                .append(validation_class, rhs.validation_class)
                .append(index_name, rhs.index_name)
                .append(index_type, rhs.index_type)
                .isEquals();
    }

    public static byte[] serialize(ColumnDefinition cd) throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bout);
        out.writeInt(cd.name.length);
        out.write(cd.name);

        out.writeBoolean(cd.validation_class != null);
        if (cd.validation_class != null)
        {
            out.writeUTF(cd.validation_class);
        }

        out.writeBoolean(cd.index_type != null);
        if (cd.index_type != null)
        {
            out.writeUTF(cd.index_type);
        }

        out.writeBoolean(cd.index_name != null);
        if (cd.index_name != null)
        {
            out.writeUTF(cd.index_name);
        }

        out.close();
        return bout.toByteArray();
    }

    public static ColumnDefinition deserialize(byte[] bytes) throws IOException
    {
        ColumnDefinition cd = new ColumnDefinition();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        int nameSize = in.readInt();
        cd.name = new byte[nameSize];
        if (in.read(cd.name, 0, nameSize) != nameSize)
            throw new IOException("short read of ColumnDefinition name");

        if (in.readBoolean())
            cd.validation_class = in.readUTF();

        if (in.readBoolean())
            cd.index_type = in.readUTF();

        if (in.readBoolean())
            cd.index_name = in.readUTF();

        return cd;
    }

    public static ColumnDefinition fromColumnDef(ColumnDef thriftColumnDef)
    {
        assert thriftColumnDef != null;
        ColumnDefinition cd = new ColumnDefinition();
        cd.name = thriftColumnDef.name;
        cd.validation_class = thriftColumnDef.validation_class;
        cd.index_type = thriftColumnDef.index_type;
        cd.index_name = thriftColumnDef.index_name;
        return cd;
    }

    public static Map<byte[], ColumnDefinition> fromColumnDef(List<ColumnDef> thriftDefs)
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
