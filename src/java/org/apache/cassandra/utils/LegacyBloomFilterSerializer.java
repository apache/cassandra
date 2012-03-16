/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.utils;

import java.io.*;
import java.util.BitSet;

public class LegacyBloomFilterSerializer
{
    public void serialize(LegacyBloomFilter bf, DataOutput dos)
            throws IOException
    {
        throw new UnsupportedOperationException("Shouldn't be serializing legacy bloom filters");
//        dos.writeInt(bf.getHashCount());
//        ObjectOutputStream oos = new ObjectOutputStream(dos);
//        oos.writeObject(bf.getBitSet());
//        oos.flush();
    }

    public LegacyBloomFilter deserialize(final DataInput dis) throws IOException
    {
        int hashes = dis.readInt();
        ObjectInputStream ois = new ObjectInputStream(new InputStream()
        {
            @Override
            public int read() throws IOException
            {
                return dis.readByte() & 0xFF;
            }
        });
        try
        {
          BitSet bs = (BitSet) ois.readObject();
          return new LegacyBloomFilter(hashes, bs);
        } catch (ClassNotFoundException e)
        {
          throw new RuntimeException(e);
        }
    }

    public long serializedSize(LegacyBloomFilter legacyBloomFilter)
    {
        throw new UnsupportedOperationException();
    }
}
