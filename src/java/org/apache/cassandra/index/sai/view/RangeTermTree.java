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

package org.apache.cassandra.index.sai.view;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class RangeTermTree
{
    private static final Logger logger = LoggerFactory.getLogger(RangeTermTree.class);

    protected final ByteBuffer min, max;
    protected final AbstractType<?> comparator;
    
    private final IntervalTree<Term, SSTableIndex, Interval<Term, SSTableIndex>> rangeTree;

    private RangeTermTree(IntervalTree<Term, SSTableIndex, Interval<Term, SSTableIndex>> rangeTree, AbstractType<?> comparator)
    {
        this.min = rangeTree.isEmpty() ? null : rangeTree.min().term;
        this.max = rangeTree.isEmpty() ? null : rangeTree.max().term;
        this.rangeTree = rangeTree;
        this.comparator = comparator;
    }

    public List<SSTableIndex> search(Expression e)
    {
        ByteBuffer minTerm = e.getOp().isNonEquality() || e.lower == null ? min : e.lower.value.encoded;
        ByteBuffer maxTerm = e.getOp().isNonEquality() || e.upper == null ? max : e.upper.value.encoded;

        return rangeTree.search(Interval.create(new Term(minTerm, comparator),
                                                new Term(maxTerm, comparator),
                                                null));
    }

    static class Builder
    {
        private final AbstractType<?> comparator;

        final List<Interval<Term, SSTableIndex>> intervals = new ArrayList<>();

        protected Builder(AbstractType<?> comparator)
        {
            this.comparator = comparator;
        }

        public final void add(SSTableIndex index)
        {
            addIndex(index);
        }

        public void addIndex(SSTableIndex index)
        {
            Interval<Term, SSTableIndex> interval =
                    Interval.create(new Term(index.minTerm(), comparator), new Term(index.maxTerm(), comparator), index);

            if (logger.isTraceEnabled())
            {
                IndexContext context = index.getIndexContext();
                logger.trace(context.logMessage("Adding index for SSTable {} with minTerm={} and maxTerm={}..."), 
                                                index.getSSTable().descriptor, 
                                                comparator.compose(index.minTerm()), 
                                                comparator.compose(index.maxTerm()));
            }

            intervals.add(interval);
        }

        public RangeTermTree build()
        {
            return new RangeTermTree(IntervalTree.build(intervals), comparator);
        }
    }

    /**
     * This is required since IntervalTree doesn't support custom Comparator
     * implementations and relied on items to be comparable which "raw" terms are not.
     */
    protected static class Term implements Comparable<Term>
    {
        private final ByteBuffer term;
        private final AbstractType<?> comparator;

        Term(ByteBuffer term, AbstractType<?> comparator)
        {
            this.term = term;
            this.comparator = comparator;
        }

        @Override
        public int compareTo(Term o)
        {
            if (term == null && o.term == null)
                return 0;
            if (term == null)
                return -1;
            if (o.term == null)
                return 1;
            return TypeUtil.compare(term, o.term, comparator);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this).add("term", comparator.getString(term)).toString();
        }
    }
}
