package org.apache.cassandra.db.marshal;

import java.io.UnsupportedEncodingException;

public class UTF8Type extends AbstractType
{
    public int compare(byte[] o1, byte[] o2)
    {
        try
        {
            return new String(o1, "UTF-8").compareTo(new String(o2, "UTF-8"));
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    public String getString(byte[] bytes)
    {
        try
        {
            return new String(bytes, "UTF-8");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }
}
