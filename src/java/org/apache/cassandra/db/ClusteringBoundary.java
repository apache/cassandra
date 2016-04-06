package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * The threshold between two different ranges, i.e. a shortcut for the combination of two ClusteringBounds -- one
 * specifying the end of one of the ranges, and its (implicit) complement specifying the beginning of the other.
 */
public class ClusteringBoundary extends ClusteringBoundOrBoundary
{
    protected ClusteringBoundary(Kind kind, ByteBuffer[] values)
    {
        super(kind, values);
    }

    public static ClusteringBoundary create(Kind kind, ByteBuffer[] values)
    {
        assert kind.isBoundary();
        return new ClusteringBoundary(kind, values);
    }

    @Override
    public ClusteringBoundary invert()
    {
        return create(kind().invert(), values);
    }

    @Override
    public ClusteringBoundary copy(AbstractAllocator allocator)
    {
        return (ClusteringBoundary) super.copy(allocator);
    }

    public ClusteringBound openBound(boolean reversed)
    {
        return ClusteringBound.create(kind.openBoundOfBoundary(reversed), values);
    }

    public ClusteringBound closeBound(boolean reversed)
    {
        return ClusteringBound.create(kind.closeBoundOfBoundary(reversed), values);
    }
}