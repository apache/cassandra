package org.apache.cassandra.stress.generatedata;

import java.nio.ByteBuffer;

public abstract class DataGenHex extends DataGen
{

    abstract long next(long operationIndex);

    @Override
    public final void generate(ByteBuffer fill, long operationIndex)
    {
        fill.clear();
        fillKeyStringBytes(next(operationIndex), fill.array());
    }

    public static void fillKeyStringBytes(long key, byte[] fill)
    {
        int ub = fill.length - 1;
        int offset = 0;
        while (key != 0)
        {
            int digit = ((int) key) & 15;
            key >>>= 4;
            fill[ub - offset++] = digit(digit);
        }
        while (offset < fill.length)
            fill[ub - offset++] = '0';
    }

    // needs to be UTF-8, but for these chars there is no difference
    private static byte digit(int num)
    {
        if (num < 10)
            return (byte)('0' + num);
        return (byte)('A' + (num - 10));
    }

}
