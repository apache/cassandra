package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrder;

public class HeapSlabPool extends Pool
{
    public HeapSlabPool(long maxOnHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        super(maxOnHeapMemory, cleanupThreshold, cleaner);
    }

    public HeapSlabAllocator newAllocator(OpOrder writes)
    {
        return new HeapSlabAllocator(this);
    }
}
