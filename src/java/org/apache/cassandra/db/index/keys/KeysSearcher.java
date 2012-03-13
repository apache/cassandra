/**
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
package org.apache.cassandra.db.index.keys;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.MultiRowIndexSearcherIterator;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeysSearcher extends SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(KeysSearcher.class);

    public KeysSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
    {
        super(indexManager, columns);
    }

    private IndexExpression highestSelectivityPredicate(List<IndexExpression> clause)
    {
        IndexExpression best = null;
        int bestMeanCount = Integer.MAX_VALUE;
        for (IndexExpression expression : clause)
        {
            //skip columns belonging to a different index type
            if(!columns.contains(expression.column_name))
                continue;

            SecondaryIndex index = indexManager.getIndexForColumn(expression.column_name);
            if (index == null || (expression.op != IndexOperator.EQ))
                continue;
            int columns = index.getIndexCfs().getMeanColumns();
            if (columns < bestMeanCount)
            {
                best = expression;
                bestMeanCount = columns;
            }
        }
        return best;
    }

    public boolean isIndexing(List<IndexExpression> clause)
    {
        return highestSelectivityPredicate(clause) != null;
    }

    @Override
    public List<Row> search(List<IndexExpression> clause, AbstractBounds<RowPosition> range, int maxResults, IFilter dataFilter, boolean maxIsColumns)
    {
        assert clause != null && !clause.isEmpty();
        ExtendedFilter filter = ExtendedFilter.create(baseCfs, dataFilter, clause, maxResults, maxIsColumns);
        return baseCfs.filter(getIndexedIterator(range, filter), filter);
    }

    public ColumnFamilyStore.AbstractScanIterator getIndexedIterator(final AbstractBounds<RowPosition> range, final ExtendedFilter filter)
    {
        // Start with the most-restrictive indexed clause, then apply remaining clauses
        // to each row matching that clause.
        // TODO: allow merge join instead of just one index + loop
        final IndexExpression primary = highestSelectivityPredicate(filter.getClause());
        final SecondaryIndex index = indexManager.getIndexForColumn(primary.column_name);

        if (logger.isDebugEnabled())
            logger.debug("Primary scan clause is " + baseCfs.getComparator().getString(primary.column_name));

        assert index != null;
        return new KeysMultiRowIndexSearcherIterator(primary, filter, range, indexManager, index);
    }

    public class KeysMultiRowIndexSearcherIterator extends MultiRowIndexSearcherIterator
    {
        final DecoratedKey indexKey;

        public KeysMultiRowIndexSearcherIterator(IndexExpression expression,
                                                 ExtendedFilter filter,
                                                 AbstractBounds<RowPosition> range,
                                                 SecondaryIndexManager indexManager,
                                                 SecondaryIndex index)
        {
            super(expression, baseCfs, index.getIndexCfs(), filter, range);
            indexKey = indexManager.getIndexKeyFor(expression.column_name, expression.value);
        }

        @Override
        protected final DecoratedKey nextIndexKey()
        {
            return curIndexKey == null ? indexKey : null; // keys index always scan single row in indexCfs
        }
    }
}
