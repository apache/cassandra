package org.apache.cassandra.stress.generatedata;

import java.nio.ByteBuffer;
import java.util.List;

public abstract class DataGen
{

    public abstract void generate(ByteBuffer fill, long index, ByteBuffer seed);
    public abstract boolean isDeterministic();

    public void generate(List<ByteBuffer> fills, long index, ByteBuffer seed)
    {
        for (ByteBuffer fill : fills)
            generate(fill, index++, seed);
    }

}
