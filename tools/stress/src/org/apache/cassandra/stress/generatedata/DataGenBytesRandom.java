package org.apache.cassandra.stress.generatedata;

import java.nio.ByteBuffer;
import java.util.Random;

public class DataGenBytesRandom extends DataGen
{

    private final Random rnd = new Random();

    @Override
    public void generate(ByteBuffer fill, long offset)
    {
        fill.clear();
        rnd.nextBytes(fill.array());
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

}
