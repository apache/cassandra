package org.apache.cassandra.cache.capi;

import java.util.Arrays;

import org.apache.cassandra.cache.CapiRowCacheProvider.HashFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YCSBKeyHashFunction implements HashFunction
{
    private static final Logger logger = LoggerFactory.getLogger(YCSBKeyHashFunction.class);

    public YCSBKeyHashFunction()
    {
    }
    
    public String toString(byte[] key)
    {
        try
        {
            /*
             * u:117 s:115 e:101 r:114
             */
            if (key.length > "user".length() //
                    && key[0] == (byte) 117//
                    && key[1] == (byte) 115//
                    && key[2] == (byte) 101//
                    && key[3] == (byte) 114//
            )
            {
                boolean fail = false;
                long num = 0L;
                for (int i = 4; i < key.length; ++i)
                {
                    if (key[i] < 48 || key[i] > 57)
                    {
                        fail = true;
                        break;
                    }
                    num = num * 10L + (int) key[i] - 48;
                }
                if (!fail)
                    return "user" + (int) num;
            }
        }
        catch (Throwable th)
        {
            logger.error(th.getMessage(), th);
        }
        return "unknown";        
    }

    @Override
    public int hashCode(byte[] key)
    {
        try
        {
            /*
             * u:117 s:115 e:101 r:114
             */
            if (key.length > "user".length() //
                    && key[0] == (byte) 117//
                    && key[1] == (byte) 115//
                    && key[2] == (byte) 101//
                    && key[3] == (byte) 114//
            )
            {
                boolean fail = false;
                long num = 0L;
                for (int i = 4; i < key.length; ++i)
                {
                    if (key[i] < 48 || key[i] > 57)
                    {
                        fail = true;
                        break;
                    }
                    num = num * 10L + (int) key[i] - 48;
                }
                if (!fail)
                    return (int) num;
            }
        }
        catch (Throwable th)
        {
            logger.error(th.getMessage(), th);
        }
        return Arrays.hashCode(key);
    }
}
