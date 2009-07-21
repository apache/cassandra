package org.apache.cassandra;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;

public class Util
{
    public static Column column(String name, String value, long timestamp)
    {
        return new Column(name.getBytes(), value.getBytes(), timestamp);
    }

    public static void addMutation(RowMutation rm, String columnFamilyName, String superColumnName, long columnName, String value, long timestamp)
    {
        rm.add(new QueryPath(columnFamilyName, superColumnName.getBytes(), getBytes(columnName)), value.getBytes(), timestamp);
    }

    public static byte[] getBytes(long v)
    {
        byte[] bytes = new byte[8];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putLong(v);
        return bytes;
    }
}
