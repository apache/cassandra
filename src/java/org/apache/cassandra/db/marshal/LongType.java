package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongType extends AbstractType
{
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

        long L1 = ByteBuffer.wrap(o1).order(ByteOrder.LITTLE_ENDIAN).getLong();
        long L2 = ByteBuffer.wrap(o2).order(ByteOrder.LITTLE_ENDIAN).getLong();
        return new Long(L1).compareTo(L2);
    }

    public String getString(byte[] bytes)
    {
        if (bytes.length != 8)
        {
            throw new MarshalException("A long is exactly 8 bytes");
        }
        return String.valueOf(ByteBuffer.wrap(bytes).getLong());
    }
}
