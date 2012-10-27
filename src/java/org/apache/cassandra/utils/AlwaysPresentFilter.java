package org.apache.cassandra.utils;

import java.nio.ByteBuffer;

public class AlwaysPresentFilter implements IFilter
{
    public boolean isPresent(ByteBuffer key)
    {
        return true;
    }

    public void add(ByteBuffer key) { }

    public void clear() { }

    public void close() throws IOException { }
}
