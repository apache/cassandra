package org.apache.cassandra.io;

public class SSTableIndex
{
    /**
     * This class holds the position of a key in a block
     * and the size of the data associated with this key.
     */
    public static class BlockMetadata
    {
        protected static final BlockMetadata NULL = new BlockMetadata(-1L, -1L);

        long position_;
        long size_;

        BlockMetadata(long position, long size)
        {
            position_ = position;
            size_ = size;
        }
    }
}
