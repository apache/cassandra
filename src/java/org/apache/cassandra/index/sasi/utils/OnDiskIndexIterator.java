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

import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.index.sasi.disk.OnDiskIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndex.DataTerm;
import org.apache.cassandra.db.marshal.AbstractType;

public class OnDiskIndexIterator extends RangeIterator<DataTerm, CombinedTerm>
{
    private final AbstractType<?> comparator;
    private final Iterator<DataTerm> terms;

    public OnDiskIndexIterator(OnDiskIndex index)
    {
        super(index.min(), index.max(), Long.MAX_VALUE);

        this.comparator = index.getComparator();
        this.terms = index.iterator();
    }

    public static RangeIterator<DataTerm, CombinedTerm> union(OnDiskIndex... union)
    {
        RangeUnionIterator.Builder<DataTerm, CombinedTerm> builder = RangeUnionIterator.builder();
        for (OnDiskIndex e : union)
        {
            if (e != null)
                builder.add(new OnDiskIndexIterator(e));
        }

        return builder.build();
    }

    protected CombinedTerm computeNext()
    {
        return terms.hasNext() ? new CombinedTerm(comparator, terms.next()) : endOfData();
    }

    protected void performSkipTo(DataTerm nextToken)
    {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException
    {}
}
