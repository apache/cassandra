package org.apache.cassandra.io.util;

import java.io.File;
import java.io.FileNotFoundException;

import com.google.common.util.concurrent.RateLimiter;

public class ThrottledReader extends RandomAccessReader
{
    private final RateLimiter limiter;

    protected ThrottledReader(File file, RateLimiter limiter) throws FileNotFoundException
    {
        super(file, RandomAccessReader.DEFAULT_BUFFER_SIZE, true, null);
        this.limiter = limiter;
    }

    protected void reBuffer()
    {
        limiter.acquire(buffer.length);
        super.reBuffer();
    }

    public static ThrottledReader open(File file, RateLimiter limiter)
    {
        try
        {
            return new ThrottledReader(file, limiter);
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }
}
