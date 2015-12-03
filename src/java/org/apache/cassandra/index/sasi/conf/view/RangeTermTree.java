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
package org.apache.cassandra.index.sasi.conf.view;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.index.sasi.SSTableIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class RangeTermTree implements TermTree
{
    protected final ByteBuffer min, max;
    protected final IntervalTree<ByteBuffer, SSTableIndex, Interval<ByteBuffer, SSTableIndex>> rangeTree;

    public RangeTermTree(ByteBuffer min, ByteBuffer max, IntervalTree<ByteBuffer, SSTableIndex, Interval<ByteBuffer, SSTableIndex>> rangeTree)
    {
        this.min = min;
        this.max = max;
        this.rangeTree = rangeTree;
    }

    public Set<SSTableIndex> search(Expression e)
    {
        ByteBuffer minTerm = e.lower == null ? min : e.lower.value;
        ByteBuffer maxTerm = e.upper == null ? max : e.upper.value;

        return new HashSet<>(rangeTree.search(Interval.create(minTerm, maxTerm, (SSTableIndex) null)));
    }

    public int intervalCount()
    {
        return rangeTree.intervalCount();
    }

    static class Builder extends TermTree.Builder
    {
        protected final List<Interval<ByteBuffer, SSTableIndex>> intervals = new ArrayList<>();

        protected Builder(OnDiskIndexBuilder.Mode mode, AbstractType<?> comparator)
        {
            super(mode, comparator);
        }

        public void addIndex(SSTableIndex index)
        {
            intervals.add(Interval.create(index.minTerm(), index.maxTerm(), index));
        }

        public TermTree build()
        {
            return new RangeTermTree(min, max, IntervalTree.build(intervals));
        }
    }
}
