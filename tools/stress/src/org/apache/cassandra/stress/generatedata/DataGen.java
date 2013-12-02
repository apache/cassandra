package org.apache.cassandra.stress.generatedata;

import java.nio.ByteBuffer;
import java.util.List;

public abstract class DataGen
{

    public abstract void generate(ByteBuffer fill, long offset);
    public abstract boolean isDeterministic();

    public void generate(List<ByteBuffer> fills, long offset)
    {
        for (ByteBuffer fill : fills)
            generate(fill, offset++);
    }

}
