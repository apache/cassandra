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
package org.apache.cassandra.index.sasi.utils;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.index.sasi.disk.OnDiskIndex.DataTerm;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.disk.TokenTree;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;
import org.apache.cassandra.db.marshal.AbstractType;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;

public class CombinedTerm implements CombinedValue<DataTerm>
{
    private final AbstractType<?> comparator;
    private final DataTerm term;
    private final TreeMap<Long, LongSet> tokens;

    public CombinedTerm(AbstractType<?> comparator, DataTerm term)
    {
        this.comparator = comparator;
        this.term = term;
        this.tokens = new TreeMap<>();

        RangeIterator<Long, Token> tokens = term.getTokens();
        while (tokens.hasNext())
        {
            Token current = tokens.next();
            LongSet offsets = this.tokens.get(current.get());
            if (offsets == null)
                this.tokens.put(current.get(), (offsets = new LongOpenHashSet()));

            for (Long offset : ((TokenTree.OnDiskToken) current).getOffsets())
                offsets.add(offset);
        }
    }

    public ByteBuffer getTerm()
    {
        return term.getTerm();
    }

    public Map<Long, LongSet> getTokens()
    {
        return tokens;
    }

    public TokenTreeBuilder getTokenTreeBuilder()
    {
        return new TokenTreeBuilder(tokens).finish();
    }

    public void merge(CombinedValue<DataTerm> other)
    {
        if (!(other instanceof CombinedTerm))
            return;

        CombinedTerm o = (CombinedTerm) other;

        assert comparator == o.comparator;

        for (Map.Entry<Long, LongSet> token : o.tokens.entrySet())
        {
            LongSet offsets = this.tokens.get(token.getKey());
            if (offsets == null)
                this.tokens.put(token.getKey(), (offsets = new LongOpenHashSet()));

            for (LongCursor offset : token.getValue())
                offsets.add(offset.value);
        }
    }

    public DataTerm get()
    {
        return term;
    }

    public int compareTo(CombinedValue<DataTerm> o)
    {
        return term.compareTo(comparator, o.get().getTerm());
    }
}