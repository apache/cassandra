package org.apache.cassandra.cache;

import java.io.IOException;

import com.sun.jna.Memory;

public class FreeableMemory extends Memory
{
	protected volatile boolean valid = true;
	
    public FreeableMemory(long size)
    {
        super(size);
    }

    public void free()
    {
        assert peer != 0;
        super.finalize(); // calls free and sets peer to zero
    }

    /**
     * avoid re-freeing already-freed memory
     */
    @Override
    protected void finalize()
    {
        if (peer != 0)
            super.finalize();
    }
    
    public byte getValidByte(long offset)
    {
        assert peer != 0;
        return super.getByte(offset);
    }
}
