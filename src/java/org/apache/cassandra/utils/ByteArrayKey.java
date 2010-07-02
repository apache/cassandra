package org.apache.cassandra.utils;

import java.util.Arrays;

/**
 * A wrapper class for making a byte[] suitable for use as keys (i.e. hashCode/equals)
 */
public class ByteArrayKey
{
    private final byte[] bytes;

    public ByteArrayKey(byte[] bytes)
    {
        this.bytes = bytes;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(this.bytes);
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

        return Arrays.equals(this.bytes, ((ByteArrayKey) obj).bytes);
    }
}

