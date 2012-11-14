package org.apache.cassandra.io.util;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class PoolingSegmentedFile extends SegmentedFile
{
    public final Queue<RandomAccessReader> pool = new ConcurrentLinkedQueue<RandomAccessReader>();

    protected PoolingSegmentedFile(String path, long length)
    {
        super(path, length);
    }

    protected PoolingSegmentedFile(String path, long length, long onDiskLength)
    {
        super(path, length, onDiskLength);
    }

    public FileDataInput getSegment(long position)
    {
        RandomAccessReader reader = pool.poll();
        if (reader == null)
            reader = createReader(path);
        reader.seek(position);
        return reader;
    }

    protected abstract RandomAccessReader createReader(String path);

    public void recycle(RandomAccessReader reader)
    {
        pool.add(reader);
    }

    public void cleanup()
    {
        for (RandomAccessReader reader : pool)
            reader.deallocate();
    }
}
