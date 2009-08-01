package org.apache.cassandra.db;

import java.io.*;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.FBUtilities;

public class ColumnSerializer implements ICompactSerializer2<IColumn>
{
    public static void writeName(byte[] name, DataOutput out) throws IOException
    {
        int length = name.length;
        assert length <= 65535;
        out.writeByte((length >> 8) & 0xFF);
        out.writeByte(length & 0xFF);
        out.write(name);
    }

    public static byte[] readName(DataInput in) throws IOException
    {
        int length = 0;
        length |= (in.readByte() << 8);
        length |= in.readByte();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    public void serialize(IColumn column, DataOutput dos) throws IOException
    {
        ColumnSerializer.writeName(column.name(), dos);
        dos.writeBoolean(column.isMarkedForDelete());
        dos.writeLong(column.timestamp());
        FBUtilities.writeByteArray(column.value(), dos);
    }

    public Column deserialize(DataInput dis) throws IOException
    {
        byte[] name = ColumnSerializer.readName(dis);
        boolean delete = dis.readBoolean();
        long ts = dis.readLong();
        byte[] value = FBUtilities.readByteArray(dis);
        return new Column(name, value, ts, delete);
    }
}
