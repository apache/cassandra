package org.apache.cassandra.stress.generatedata;

public class DistributionSeqBatch extends DataGenHex
{

    final Distribution delegate;
    final int batchSize;
    final long maxKey;

    private int batchIndex;
    private long batchKey;

    // object must be published safely if passed between threadCount, due to batchIndex not being volatile. various
    // hacks possible, but not ideal. don't want to use volatile as object intended for single threaded use.
    public DistributionSeqBatch(int batchSize, long maxKey, Distribution delegate)
    {
        this.batchIndex = batchSize;
        this.batchSize = batchSize;
        this.maxKey = maxKey;
        this.delegate = delegate;
    }

    @Override
    long next(long operationIndex)
    {
        if (batchIndex >= batchSize)
        {
            batchKey = delegate.next();
            batchIndex = 0;
        }
        long r = batchKey + batchIndex++;
        if (r > maxKey)
        {
            batchKey = delegate.next();
            batchIndex = 1;
            r = batchKey;
        }
        return r;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

}
