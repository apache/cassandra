package org.apache.cassandra.db.marshal;

import java.util.UUID;
import java.nio.ByteBuffer;

public class UUIDType extends AbstractType
{
    private UUID getUUID(byte[] bytes)
    {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return new UUID(bb.getLong(), bb.getLong());
    }

    public int compare(byte[] o1, byte[] o2)
    {
        return getUUID(o1).compareTo(getUUID(o2));
    }

    public String getString(byte[] bytes)
    {
        return getUUID(bytes).toString();
    }
}
