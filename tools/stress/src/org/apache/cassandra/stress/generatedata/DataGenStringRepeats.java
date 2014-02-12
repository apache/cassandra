package org.apache.cassandra.stress.generatedata;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.base.Charsets.UTF_8;

public class DataGenStringRepeats extends DataGen
{

    private static final ConcurrentHashMap<Integer, ConcurrentHashMap<Long, byte[]>> CACHE_LOOKUP = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Long, byte[]> cache;
    private final int repeatFrequency;
    public DataGenStringRepeats(int repeatFrequency)
    {
        if (!CACHE_LOOKUP.containsKey(repeatFrequency))
            CACHE_LOOKUP.putIfAbsent(repeatFrequency, new ConcurrentHashMap<Long, byte[]>());
        cache = CACHE_LOOKUP.get(repeatFrequency);
        this.repeatFrequency = repeatFrequency;
    }

    @Override
    public void generate(ByteBuffer fill, long index, ByteBuffer seed)
    {
        fill(fill, index, 0, seed);
    }

    @Override
    public void generate(List<ByteBuffer> fills, long index, ByteBuffer seed)
    {
        for (int i = 0 ; i < fills.size() ; i++)
        {
            fill(fills.get(i), index, i, seed);
        }
    }

    private void fill(ByteBuffer fill, long index, int column, ByteBuffer seed)
    {
        fill.clear();
        byte[] trg = fill.array();
        byte[] src = getData(index, column, seed);
        for (int j = 0 ; j < trg.length ; j += src.length)
            System.arraycopy(src, 0, trg, j, Math.min(src.length, trg.length - j));
    }

    private byte[] getData(long index, int column, ByteBuffer seed)
    {
        final long key = (column * repeatFrequency) + ((seed == null ? index : Math.abs(seed.hashCode())) % repeatFrequency);
        byte[] r = cache.get(key);
        if (r != null)
            return r;
        MessageDigest md = FBUtilities.threadLocalMD5Digest();
        r = md.digest(Long.toString(key).getBytes(UTF_8));
        cache.putIfAbsent(key, r);
        return r;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

}
