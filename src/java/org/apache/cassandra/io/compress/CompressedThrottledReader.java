package org.apache.cassandra.io.compress;

import java.io.File;
import java.io.FileNotFoundException;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.io.util.PoolingSegmentedFile;
import org.apache.cassandra.io.util.RandomAccessReader;

public class CompressedThrottledReader extends CompressedRandomAccessReader
{
    private final RateLimiter limiter;

    public CompressedThrottledReader(String file, CompressionMetadata metadata, RateLimiter limiter) throws FileNotFoundException
    {
        super(file, metadata, true, null);
        this.limiter = limiter;
    }

    protected void reBuffer()
    {
        limiter.acquire(buffer.length);
        super.reBuffer();
    }

    public static CompressedThrottledReader open(String file, CompressionMetadata metadata, RateLimiter limiter)
    {
        try
        {
            return new CompressedThrottledReader(file, metadata, limiter);
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }
}
