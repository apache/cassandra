package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongType extends AbstractType
{
    public int compare(byte[] o1, byte[] o2)
    {
        long L1 = ByteBuffer.wrap(o1).order(ByteOrder.LITTLE_ENDIAN).getLong();
        long L2 = ByteBuffer.wrap(o2).order(ByteOrder.LITTLE_ENDIAN).getLong();
        return new Long(L1).compareTo(L2);
    }

    public String getString(byte[] bytes)
    {
        return String.valueOf(ByteBuffer.wrap(bytes).getLong());
    }
}
