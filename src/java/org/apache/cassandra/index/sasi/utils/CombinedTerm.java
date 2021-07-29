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
import java.util.*;

import org.apache.cassandra.index.sasi.disk.*;
import org.apache.cassandra.index.sasi.disk.OnDiskIndex.DataTerm;
import org.apache.cassandra.db.marshal.AbstractType;

public class CombinedTerm implements CombinedValue<DataTerm>
{
    private final AbstractType<?> comparator;
    private final DataTerm term;
    private final List<DataTerm> mergedTerms = new ArrayList<>();

    public CombinedTerm(AbstractType<?> comparator, DataTerm term)
    {
        this.comparator = comparator;
        this.term = term;
    }

    public ByteBuffer getTerm()
    {
        return term.getTerm();
    }

    public boolean isPartial()
    {
        return term.isPartial();
    }

    public RangeIterator<Long, Token> getTokenIterator()
    {
        RangeIterator.Builder<Long, Token> union = RangeUnionIterator.builder();
        union.add(term.getTokens());
        mergedTerms.stream().map(OnDiskIndex.DataTerm::getTokens).forEach(union::add);

        return union.build();
    }

    public TokenTreeBuilder getTokenTreeBuilder()
    {
        return new StaticTokenTreeBuilder(this).finish();
    }

    public void merge(CombinedValue<DataTerm> other)
    {
        if (!(other instanceof CombinedTerm))
            return;

        CombinedTerm o = (CombinedTerm) other;

        assert comparator == o.comparator;

        mergedTerms.add(o.term);
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
