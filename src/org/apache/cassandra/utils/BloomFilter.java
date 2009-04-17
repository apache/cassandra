/**
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

import java.math.*;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.io.*;
import java.security.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.*;

import javax.xml.bind.annotation.XmlElement;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.SSTable;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class BloomFilter implements Serializable
{    
    private static List<ISimpleHash> hashLibrary_ = new ArrayList<ISimpleHash>();
    private static ICompactSerializer<BloomFilter> serializer_;

    static
    {
        serializer_ = new BloomFilterSerializer();
        hashLibrary_.add(new RSHash());
        hashLibrary_.add(new JSHash());
        hashLibrary_.add(new PJWHash());
        hashLibrary_.add(new ELFHash());
        hashLibrary_.add(new BKDRHash());
        hashLibrary_.add(new SDBMHash());
        hashLibrary_.add(new DJBHash());
        hashLibrary_.add(new DEKHash());
        hashLibrary_.add(new BPHash());
        hashLibrary_.add(new FNVHash());
        hashLibrary_.add(new APHash());
    }

    public static ICompactSerializer<BloomFilter> serializer()
    {
        return serializer_;
    }

    private BitSet filter_;
    private int count_;
    private int size_;
    private int hashes_;
    private Random random_ = new Random(System.currentTimeMillis());

    public BloomFilter(int numElements, int bitsPerElement)
    {
        // TODO -- think about the trivial cases more.
        // Note that it should indeed be possible to send a bloom filter that
        // encodes the empty set.
        if (numElements < 0 || bitsPerElement < 1)
            throw new IllegalArgumentException("Number of elements and bits "
                    + "must be non-negative.");
        // Adding a small random number of bits so that even if the set
        // of elements hasn't changed, we'll get different false positives.
        count_ = numElements;
        size_ = numElements * bitsPerElement + 20 + random_.nextInt(64);
        filter_ = new BitSet(size_);
        //hashes_ = BloomCalculations.computeBestK(bitsPerElement);
        hashes_ = 8;
    }

    /*
     * This version is only used by the deserializer. 
     */
    BloomFilter(int count, int hashes, int size, BitSet filter)
    {
        count_ = count;
        hashes_ = hashes;
        size_ = size;
        filter_ = filter;
    }

    int count()
    {
        return count_;
    }

    int size()
    {        
        return size_;
    }

    int hashes()
    {
        return hashes_;
    }

    BitSet filter()
    {
        return filter_;
    }

    public boolean isPresent(String key)
    {
        boolean bVal = true;
        for (int i = 0; i < hashes_; ++i)
        {
            ISimpleHash hash = hashLibrary_.get(i);
            int hashValue = hash.hash(key);
            int index = Math.abs(hashValue % size_);
            if (!filter_.get(index))
            {
                bVal = false;
                break;
            }
        }
        return bVal;
    }

    /*
     param@ key -- value whose hash is used to fill
     the filter_.
     This is a general purpose API.
     */
    public void add(String key)
    {
        for (int i = 0; i < hashes_; ++i)
        {
            ISimpleHash hash = hashLibrary_.get(i);
            int hashValue = hash.hash(key);
            int index = Math.abs(hashValue % size_);
            filter_.set(index);
        }
    }

    public String toString()
    {
        return filter_.toString();
    }
}

class BloomFilterSerializer implements ICompactSerializer<BloomFilter>
{
    /* 
     * The following methods are used for compact representation
     * of BloomFilter. This is essential, since we want to determine
     * the size of the serialized Bloom Filter blob before it is
     * populated armed with the knowledge of how many elements are
     * going to reside in it.
     */

    public void serialize(BloomFilter bf, DataOutputStream dos) throws IOException
    {
        /* write out the count of the BloomFilter */
        dos.writeInt(bf.count());
        /* write the number of hash functions used */
        dos.writeInt(bf.hashes());
        /* write the size of the BloomFilter */
        dos.writeInt(bf.size());
        BitSet.serializer().serialize(bf.filter(), dos);
    }

    public BloomFilter deserialize(DataInputStream dis) throws IOException
    {
        /* read the count of the BloomFilter */
        int count = dis.readInt();
        /* read the number of hash functions */
        int hashes = dis.readInt();
        /* read the size of the bloom filter */
        int size = dis.readInt();
        BitSet bs = BitSet.serializer().deserialize(dis);
        return new BloomFilter(count, hashes, size, bs);
    }
}

interface ISimpleHash
{
    public int hash(String str);
}

class RSHash implements ISimpleHash
{
    public int hash(String str)
    {
        int b = 378551;
        int a = 63689;
        int hash = 0;

        for (int i = 0; i < str.length(); i++)
        {
            hash = hash * a + str.charAt(i);
            a = a * b;
        }
        return hash;
    }
}

class JSHash implements ISimpleHash
{
    public int hash(String str)
    {
        int hash = 1315423911;
        for (int i = 0; i < str.length(); i++)
        {
            hash ^= ((hash << 5) + str.charAt(i) + (hash >> 2));
        }
        return hash;
    }
}

class PJWHash implements ISimpleHash
{
    public int hash(String str)
    {
        int bitsInUnsignedInt = (4 * 8);
        int threeQuarters = (bitsInUnsignedInt * 3) / 4;
        int oneEighth = bitsInUnsignedInt / 8;
        int highBits = (0xFFFFFFFF) << (bitsInUnsignedInt - oneEighth);
        int hash = 0;
        int test = 0;

        for (int i = 0; i < str.length(); i++)
        {
            hash = (hash << oneEighth) + str.charAt(i);

            if ((test = hash & highBits) != 0)
            {
                hash = ((hash ^ (test >> threeQuarters)) & (~highBits));
            }
        }
        return hash;
    }
}

class ELFHash implements ISimpleHash
{
    public int hash(String str)
    {
        int hash = 0;
        int x = 0;
        for (int i = 0; i < str.length(); i++)
        {
            hash = (hash << 4) + str.charAt(i);

            if ((x = hash & 0xF0000000) != 0)
            {
                hash ^= (x >> 24);
            }
            hash &= ~x;
        }
        return hash;
    }
}

class BKDRHash implements ISimpleHash
{
    public int hash(String str)
    {
        int seed = 131; // 31 131 1313 13131 131313 etc..
        int hash = 0;
        for (int i = 0; i < str.length(); i++)
        {
            hash = (hash * seed) + str.charAt(i);
        }
        return hash;
    }
}

class SDBMHash implements ISimpleHash
{
    public int hash(String str)
    {
        int hash = 0;
        for (int i = 0; i < str.length(); i++)
        {
            hash = str.charAt(i) + (hash << 6) + (hash << 16) - hash;
        }
        return hash;
    }
}

class DJBHash implements ISimpleHash
{
    public int hash(String str)
    {
        int hash = 5381;
        for (int i = 0; i < str.length(); i++)
        {
            hash = ((hash << 5) + hash) + str.charAt(i);
        }
        return hash;
    }
}

class DEKHash implements ISimpleHash
{
    public int hash(String str)
    {
        int hash = str.length();
        for (int i = 0; i < str.length(); i++)
        {
            hash = ((hash << 5) ^ (hash >> 27)) ^ str.charAt(i);
        }
        return hash;
    }
}

class BPHash implements ISimpleHash
{
    public int hash(String str)
    {
        int hash = 0;
        for (int i = 0; i < str.length(); i++)
        {
            hash = hash << 7 ^ str.charAt(i);
        }
        return hash;
    }
}

class FNVHash implements ISimpleHash
{
    public int hash(String str)
    {
        int fnv_prime = 0x811C9DC5;
        int hash = 0;
        for (int i = 0; i < str.length(); i++)
        {
            hash *= fnv_prime;
            hash ^= str.charAt(i);
        }
        return hash;
    }
}

class APHash implements ISimpleHash
{
    public int hash(String str)
    {
        int hash = 0xAAAAAAAA;
        for (int i = 0; i < str.length(); i++)
        {
            if ((i & 1) == 0)
            {
                hash ^= ((hash << 7) ^ str.charAt(i) ^ (hash >> 3));
            }
            else
            {
                hash ^= (~((hash << 11) ^ str.charAt(i) ^ (hash >> 5)));
            }
        }
        return hash;
    }
}
