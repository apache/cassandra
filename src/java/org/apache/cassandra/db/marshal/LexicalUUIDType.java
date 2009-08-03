package org.apache.cassandra.db.marshal;

import java.util.UUID;
import java.nio.ByteBuffer;

public class LexicalUUIDType extends AbstractType
{
    static UUID getUUID(byte[] bytes)
    {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return new UUID(bb.getLong(), bb.getLong());
    }

    public int compare(byte[] o1, byte[] o2)
    {
        if (o1.length == 0)
        {
            return o2.length == 0 ? 0 : -1;
        }
        if (o2.length == 0)
        {
            return 1;
        }

        return getUUID(o1).compareTo(getUUID(o2));
    }

    public String getString(byte[] bytes)
    {
        if (bytes.length != 16)
        {
            throw new MarshalException("UUIDs must be exactly 16 bytes");
        }
        return getUUID(bytes).toString();
    }
}
