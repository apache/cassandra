package org.apache.cassandra.utils;

import java.util.BitSet;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import org.apache.cassandra.utils.obs.OpenBitSet;
import org.apache.cassandra.io.ICompactSerializer;

class LegacyBloomFilterSerializer implements ICompactSerializer<LegacyBloomFilter>
{
    public void serialize(LegacyBloomFilter bf, DataOutputStream dos)
            throws IOException
    {
        dos.writeInt(bf.getHashCount());
        ObjectOutputStream oos = new ObjectOutputStream(dos);
        oos.writeObject(bf.getBitSet());
        oos.flush();
    }

    public LegacyBloomFilter deserialize(DataInputStream dis) throws IOException
    {
        int hashes = dis.readInt();
        ObjectInputStream ois = new ObjectInputStream(dis);
        try
        {
          BitSet bs = (BitSet) ois.readObject();
          return new LegacyBloomFilter(hashes, bs);
        } catch (ClassNotFoundException e)
        {
          throw new RuntimeException(e);
        }
    }
}
