package org.apache.cassandra.db.marshal;

import java.util.UUID;

public class TimeUUIDType extends AbstractType
{
    public int compare(byte[] o1, byte[] o2)
    {
        long t1 = LexicalUUIDType.getUUID(o1).timestamp();
        long t2 = LexicalUUIDType.getUUID(o2).timestamp();
        return t1 < t2 ? -1 : (t1 > t2 ? 1 : 0);
    }

    public String getString(byte[] bytes)
    {
        if (bytes.length != 16)
        {
            throw new MarshalException("UUIDs must be exactly 16 bytes");
        }
        UUID uuid = LexicalUUIDType.getUUID(bytes);
        if (uuid.version() != 1)
        {
            throw new MarshalException("TimeUUID only makes sense with version 1 UUIDs");
        }
        return uuid.toString();
    }
}
