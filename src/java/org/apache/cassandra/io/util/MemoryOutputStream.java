package org.apache.cassandra.io.util;

import java.io.OutputStream;

import com.sun.jna.Memory;

/**
 * This class provides a way to stream the writes into the {@link Memory}
 */
public class MemoryOutputStream extends OutputStream
{
    
    private final Memory mem;
    private int position = 0;
    
    public MemoryOutputStream(Memory mem)
    {
        this.mem = mem;
    }
    
    @Override
    public void write(int b)
    {
        mem.setByte(this.position, (byte)b);
        this.position++;
    }
    
    public int position()
    {
        return this.position;
    }
    
}
