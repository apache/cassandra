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

import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class RangeTermTree
{
    private static final Logger logger = LoggerFactory.getLogger(RangeTermTree.class);

    protected final ByteBuffer min, max;
    protected final IndexTermType indexTermType;
    
    private final IntervalTree<Term, SSTableIndex, Interval<Term, SSTableIndex>> rangeTree;

    private RangeTermTree(ByteBuffer min, ByteBuffer max, IntervalTree<Term, SSTableIndex, Interval<Term, SSTableIndex>> rangeTree, IndexTermType indexTermType)
    {
        this.min = min;
        this.max = max;
        this.rangeTree = rangeTree;
        this.indexTermType = indexTermType;
    }

    public List<SSTableIndex> search(Expression e)
    {
        ByteBuffer minTerm = e.lower() == null ? min : e.lower().value.encoded;
        ByteBuffer maxTerm = e.upper() == null ? max : e.upper().value.encoded;

        return rangeTree.search(Interval.create(new Term(minTerm, indexTermType),
                                                new Term(maxTerm, indexTermType),
                                                null));
    }

    static class Builder
    {
        private final IndexTermType indexTermType;
        private ByteBuffer min, max;

        final List<Interval<Term, SSTableIndex>> intervals = new ArrayList<>();

        protected Builder(IndexTermType indexTermType)
        {
            this.indexTermType = indexTermType;
        }

        public final void add(SSTableIndex index)
        {
            addIndex(index);

            min = min == null || index.getIndexTermType().compare(min, index.minTerm()) > 0 ? index.minTerm() : min;
            max = max == null || index.getIndexTermType().compare(max, index.maxTerm()) < 0 ? index.maxTerm() : max;
        }

        public void addIndex(SSTableIndex index)
        {
            Interval<Term, SSTableIndex> interval =
                    Interval.create(new Term(index.minTerm(), indexTermType), new Term(index.maxTerm(), indexTermType), index);

            if (logger.isTraceEnabled())
            {
                logger.trace(index.getIndexIdentifier().logMessage("Adding index for SSTable {} with minTerm={} and maxTerm={}..."),
                                                                   index.getSSTable().descriptor,
                                                                   indexTermType.indexType().compose(index.minTerm()),
                                                                   indexTermType.indexType().compose(index.maxTerm()));
            }

            intervals.add(interval);
        }

        public RangeTermTree build()
        {
            return new RangeTermTree(min, max, IntervalTree.build(intervals), indexTermType);
        }
    }

    /**
     * This is required since IntervalTree doesn't support custom Comparator
     * implementations and relied on items to be comparable which "raw" terms are not.
     */
    protected static class Term implements Comparable<Term>
    {
        private final ByteBuffer term;
        private final IndexTermType indexTermType;

        Term(ByteBuffer term, IndexTermType indexTermType)
        {
            this.term = term;
            this.indexTermType = indexTermType;
        }

        @Override
        public int compareTo(Term o)
        {
            return indexTermType.compare(term, o.term);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this).add("term", indexTermType.asString(term)).toString();
        }
    }
}
