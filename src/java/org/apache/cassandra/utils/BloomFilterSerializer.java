package org.apache.cassandra.utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.cassandra.utils.obs.OpenBitSet;
import org.apache.cassandra.io.ICompactSerializer;

class BloomFilterSerializer implements ICompactSerializer<BloomFilter>
{
    public void serialize(BloomFilter bf, DataOutputStream dos) throws IOException
    {
        long[] bits = bf.bitset.getBits();
        int bitLength = bits.length;

        dos.writeInt(bf.getHashCount());
        dos.writeInt(bitLength);

        for (int i = 0; i < bitLength; i++)
            dos.writeLong(bits[i]);
        dos.flush();
    }

    public BloomFilter deserialize(DataInputStream dis) throws IOException
    {
        int hashes = dis.readInt();
        int bitLength = dis.readInt();
        long[] bits = new long[bitLength];
        for (int i = 0; i < bitLength; i++)
            bits[i] = dis.readLong();
        OpenBitSet bs = new OpenBitSet(bits, bitLength);
        return new BloomFilter(hashes, bs);
    }
}


