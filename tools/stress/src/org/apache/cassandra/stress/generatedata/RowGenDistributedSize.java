package org.apache.cassandra.stress.generatedata;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


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

    final boolean isDeterministic;

    public RowGenDistributedSize(DataGen dataGenerator, Distribution countDistribution, Distribution sizeDistribution)
    {
        super(dataGenerator);
        this.countDistribution = countDistribution;
        this.sizeDistribution = sizeDistribution;
        ret = new ByteBuffer[(int) countDistribution.maxValue()];
        sizes = new int[ret.length];
        this.isDeterministic = dataGen.isDeterministic() && countDistribution.maxValue() == countDistribution.minValue()
            && sizeDistribution.minValue() == sizeDistribution.maxValue();
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
        return isDeterministic;
    }

}
