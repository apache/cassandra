package org.apache.cassandra.db.marshal;

import java.io.UnsupportedEncodingException;

public class AsciiType extends BytesType
{
    @Override
    public String getString(byte[] bytes)
    {
        try
        {
            return new String(bytes, "US-ASCII");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }
}
