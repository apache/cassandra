package org.apache.cassandra.utils.memory;

import com.google.common.base.*;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.db.ColumnFamilyStore;

import java.nio.ByteBuffer;

/**
 * Wraps calls to a PoolAllocator with the provided writeOp. Also doubles as a Function that clones Cells
 * using itself
 */
public final class ContextAllocator extends AbstractAllocator implements Function<Cell, Cell>
{
    private final OpOrder.Group opGroup;
    private final PoolAllocator allocator;
    private final ColumnFamilyStore cfs;

    public ContextAllocator(OpOrder.Group opGroup, PoolAllocator allocator, ColumnFamilyStore cfs)
    {
        this.opGroup = opGroup;
        this.allocator = allocator;
        this.cfs = cfs;
    }

    @Override
    public ByteBuffer clone(ByteBuffer buffer)
    {
        return allocator.clone(buffer, opGroup);
    }

    public ByteBuffer allocate(int size)
    {
        return allocator.allocate(size, opGroup);
    }

    public Cell apply(Cell column)
    {
        return column.localCopy(cfs, this);
    }

    public long owns()
    {
        return allocator.owns();
    }

    @Override
    public float ownershipRatio()
    {
        return allocator.ownershipRatio();
    }

    @Override
    public long reclaiming()
    {
        return allocator.reclaiming();
    }
}
