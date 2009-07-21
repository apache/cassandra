package org.apache.cassandra.db.marshal;

import java.io.UnsupportedEncodingException;

public class UTF8Type extends AbstractType
{
    public int compare(byte[] o1, byte[] o2)
    {
        // TODO rm hack to support the "i'm going to pretend [] is an index entry if I didn't actually index anything" hack
        if (o1.length == 0)
        {
            return o2.length == 0 ? 0 : -1;
        }
        if (o2.length == 0)
        {
            return -1;
        }

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
