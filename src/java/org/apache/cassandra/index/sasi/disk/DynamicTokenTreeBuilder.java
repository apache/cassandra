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
package org.apache.cassandra.index.sasi.disk;

import java.nio.ByteBuffer;
import java.util.*;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.utils.*;

public class DynamicTokenTreeBuilder extends AbstractTokenTreeBuilder
{

    private final SortedMap<Long, KeyOffsets> tokens = new TreeMap<>();

    public DynamicTokenTreeBuilder()
    {}

    public DynamicTokenTreeBuilder(TokenTreeBuilder data)
    {
        add(data);
    }

    public DynamicTokenTreeBuilder(SortedMap<Long, KeyOffsets> data)
    {
        add(data);
    }

    public void add(Long token, long partitionOffset, long rowOffset)
    {
        KeyOffsets found = tokens.get(token);
        if (found == null)
            tokens.put(token, (found = new KeyOffsets(2)));

        found.put(partitionOffset, rowOffset);
    }

    public void add(Iterator<Pair<Long, KeyOffsets>> data)
    {
        while (data.hasNext())
        {
            Pair<Long, KeyOffsets> entry = data.next();
            for (LongObjectCursor<long[]> cursor : entry.right)
                for (long l : cursor.value)
                    add(entry.left, cursor.key, l);
        }
    }

    public void add(SortedMap<Long, KeyOffsets> data)
    {
        for (Map.Entry<Long, KeyOffsets> newEntry : data.entrySet())
        {
            for (LongObjectCursor<long[]> cursor : newEntry.getValue())
                for (long l : cursor.value)
                    add(newEntry.getKey(), cursor.key, l);
        }
    }

    public Iterator<Pair<Long, KeyOffsets>> iterator()
    {
        final Iterator<Map.Entry<Long, KeyOffsets>> iterator = tokens.entrySet().iterator();
        return new AbstractIterator<Pair<Long, KeyOffsets>>()
        {
            protected Pair<Long, KeyOffsets> computeNext()
            {
                if (!iterator.hasNext())
                    return endOfData();

                Map.Entry<Long, KeyOffsets> entry = iterator.next();
                return Pair.create(entry.getKey(), entry.getValue());
            }
        };
    }

    public boolean isEmpty()
    {
        return tokens.size() == 0;
    }

    protected void constructTree()
    {
        tokenCount = tokens.size();
        treeMinToken = tokens.firstKey();
        treeMaxToken = tokens.lastKey();
        numBlocks = 1;

        // special case the tree that only has a single block in it (so we don't create a useless root)
        if (tokenCount <= TOKENS_PER_BLOCK)
        {
            leftmostLeaf = new DynamicLeaf(tokens);
            rightmostLeaf = leftmostLeaf;
            root = leftmostLeaf;
        }
        else
        {
            root = new InteriorNode();
            rightmostParent = (InteriorNode) root;

            int i = 0;
            Leaf lastLeaf = null;
            Long firstToken = tokens.firstKey();
            Long finalToken = tokens.lastKey();
            Long lastToken;
            for (Long token : tokens.keySet())
            {
                if (i == 0 || (i % TOKENS_PER_BLOCK != 0 && i != (tokenCount - 1)))
                {
                    i++;
                    continue;
                }

                lastToken = token;
                Leaf leaf = (i != (tokenCount - 1) || token.equals(finalToken)) ?
                        new DynamicLeaf(tokens.subMap(firstToken, lastToken)) : new DynamicLeaf(tokens.tailMap(firstToken));

                if (i == TOKENS_PER_BLOCK)
                    leftmostLeaf = leaf;
                else
                    lastLeaf.next = leaf;

                rightmostParent.add(leaf);
                lastLeaf = leaf;
                rightmostLeaf = leaf;
                firstToken = lastToken;
                i++;
                numBlocks++;

                if (token.equals(finalToken))
                {
                    Leaf finalLeaf = new DynamicLeaf(tokens.tailMap(token));
                    lastLeaf.next = finalLeaf;
                    rightmostParent.add(finalLeaf);
                    rightmostLeaf = finalLeaf;
                    numBlocks++;
                }
            }

        }
    }

    private class DynamicLeaf extends Leaf
    {
        private final SortedMap<Long, KeyOffsets> tokens;

        DynamicLeaf(SortedMap<Long, KeyOffsets> data)
        {
            super(data.firstKey(), data.lastKey());
            tokens = data;
        }

        public int tokenCount()
        {
            return tokens.size();
        }

        public boolean isSerializable()
        {
            return true;
        }

        protected void serializeData(ByteBuffer buf)
        {
            for (Map.Entry<Long, KeyOffsets> entry : tokens.entrySet())
                createEntry(entry.getKey(), entry.getValue()).serialize(buf);
        }

    }
}
