package org.apache.cassandra.stress.generatedata;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RowGenDistributedSize extends RowGen
{

    // TODO - make configurable
    static final int MAX_SINGLE_CACHE_SIZE = 16 * 1024;

    final Distribution countDistribution;
    final Distribution sizeDistribution;

    final TreeMap<Integer, ByteBuffer> cache = new TreeMap<>();

    // array re-used for returning columns
    final ByteBuffer[] ret;
    final int[] sizes;

    public RowGenDistributedSize(DataGen dataGenerator, Distribution countDistribution, Distribution sizeDistribution)
    {
        super(dataGenerator);
        this.countDistribution = countDistribution;
        this.sizeDistribution = sizeDistribution;
        ret = new ByteBuffer[(int) countDistribution.maxValue()];
        sizes = new int[ret.length];
    }

    ByteBuffer getBuffer(int size)
    {
        if (size >= MAX_SINGLE_CACHE_SIZE)
            return ByteBuffer.allocate(size);
        Map.Entry<Integer, ByteBuffer> found = cache.ceilingEntry(size);
        if (found == null)
        {
            // remove the next entry down, and replace it with a cache of this size
            Integer del = cache.lowerKey(size);
            if (del != null)
                cache.remove(del);
            return ByteBuffer.allocate(size);
        }
        ByteBuffer r = found.getValue();
        cache.remove(found.getKey());
        return r;
    }

    @Override
    List<ByteBuffer> getColumns(long operationIndex)
    {
        int i = 0;
        int count = (int) countDistribution.next();
        while (i < count)
        {
            int columnSize = (int) sizeDistribution.next();
            sizes[i] = columnSize;
            ret[i] = getBuffer(columnSize);
            i++;
        }
        while (i < ret.length && ret[i] != null)
            ret[i] = null;
        i = 0;
        while (i < count)
        {
            ByteBuffer b = ret[i];
            cache.put(b.capacity(), b);
            b.position(b.capacity() - sizes[i]);
            ret[i] = b.slice();
            b.position(0);
            i++;
        }
        return Arrays.asList(ret).subList(0, count);
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

}
