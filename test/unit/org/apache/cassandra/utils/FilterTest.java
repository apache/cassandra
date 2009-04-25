package org.apache.cassandra.utils;

import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.io.IOException;

import org.testng.annotations.Test;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;

public class FilterTest
{
    public void testManyHashes(Iterator<String> keys)
    {
        int MAX_HASH_COUNT = 128;
        Set<Integer> hashes = new HashSet<Integer>();
        int collisions = 0;
        while (keys.hasNext())
        {
            hashes.clear();
            for (int hashIndex : Filter.getHashBuckets(keys.next(), MAX_HASH_COUNT, 1024 * 1024))
            {
                hashes.add(hashIndex);
            }
            collisions += (MAX_HASH_COUNT - hashes.size());
        }
        assert collisions <= 100;
    }

    @Test
    public void testManyRandom()
    {
        testManyHashes(randomKeys());
    }

    // used by filter subclass tests

    static final double MAX_FAILURE_RATE = 0.1;
    public static final BloomCalculations.BloomSpecification spec = BloomCalculations.computeBucketsAndK(MAX_FAILURE_RATE);
    static final int ELEMENTS = 10000;

    static final ResetableIterator<String> intKeys()
    {
        return new KeyGenerator.IntGenerator(ELEMENTS);
    }

    static final ResetableIterator<String> randomKeys()
    {
        return new KeyGenerator.RandomStringGenerator(314159, ELEMENTS);
    }

    static final ResetableIterator<String> randomKeys2()
    {
        return new KeyGenerator.RandomStringGenerator(271828, ELEMENTS);
    }

    public static void testFalsePositives(Filter f, ResetableIterator<String> keys, ResetableIterator<String> otherkeys)
    {
        assert keys.size() == otherkeys.size();

        while (keys.hasNext())
        {
            f.add(keys.next());
        }

        int fp = 0;
        while (otherkeys.hasNext())
        {
            if (f.isPresent(otherkeys.next()))
            {
                fp++;
            }
        }

        double fp_ratio = fp / (keys.size() * BloomCalculations.probs[spec.bucketsPerElement][spec.K]);
        assert fp_ratio < 1.03 : fp_ratio;
    }

    public static Filter testSerialize(Filter f) throws IOException
    {
        f.add("a");
        DataOutputBuffer out = new DataOutputBuffer();
        f.getSerializer().serialize(f, out);

        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());
        Filter f2 = f.getSerializer().deserialize(in);

        assert f2.isPresent("a");
        assert !f2.isPresent("b");
        return f2;
    }

}
