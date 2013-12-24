package org.apache.cassandra.stress.generatedata;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KeyGen
{

    final DataGen dataGen;
    final int keySize;
    final List<ByteBuffer> keyBuffers = new ArrayList<>();

    public KeyGen(DataGen dataGen, int keySize)
    {
        this.dataGen = dataGen;
        this.keySize = keySize;
    }

    public List<ByteBuffer> getKeys(int n, long index)
    {
        while (keyBuffers.size() < n)
            keyBuffers.add(ByteBuffer.wrap(new byte[keySize]));
        dataGen.generate(keyBuffers, index);
        return keyBuffers;
    }

    public boolean isDeterministic()
    {
        return dataGen.isDeterministic();
    }

}
