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
package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

public abstract class SecondaryIndexSearcher
{
    protected final SecondaryIndexManager indexManager;
    protected final Set<ByteBuffer> columns;
    protected final ColumnFamilyStore baseCfs;

    public SecondaryIndexSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
    {
        this.indexManager = indexManager;
        this.columns = columns;
        this.baseCfs = indexManager.baseCfs;
    }

    public SecondaryIndex highestSelectivityIndex(List<IndexExpression> clause)
    {
        IndexExpression expr = highestSelectivityPredicate(clause);
        return expr == null ? null : indexManager.getIndexForColumn(expr.column);
    }

    public abstract List<Row> search(ExtendedFilter filter);

    /**
     * @return true this index is able to handle the given index expressions.
     */
    public boolean canHandleIndexClause(List<IndexExpression> clause)
    {
        for (IndexExpression expression : clause)
        {
            if (!columns.contains(expression.column))
                continue;

            SecondaryIndex index = indexManager.getIndexForColumn(expression.column);
            if (index != null && index.getIndexCfs() != null && index.supportsOperator(expression.operator))
                return true;
        }
        return false;
    }
    
    /**
     * Validates the specified {@link IndexExpression}. It will throw an {@link org.apache.cassandra.exceptions.InvalidRequestException}
     * if the provided clause is not valid for the index implementation.
     *
     * @param indexExpression An {@link IndexExpression} to be validated
     * @throws org.apache.cassandra.exceptions.InvalidRequestException in case of validation errors
     */
    public void validate(IndexExpression indexExpression) throws InvalidRequestException
    {
    }

    protected IndexExpression highestSelectivityPredicate(List<IndexExpression> clause)
    {
        IndexExpression best = null;
        int bestMeanCount = Integer.MAX_VALUE;
        Map<SecondaryIndex, Integer> candidates = new HashMap<>();

        for (IndexExpression expression : clause)
        {
            // skip columns belonging to a different index type
            if (!columns.contains(expression.column))
                continue;

            SecondaryIndex index = indexManager.getIndexForColumn(expression.column);
            if (index == null || index.getIndexCfs() == null || !index.supportsOperator(expression.operator))
                continue;

            int columns = index.getIndexCfs().getMeanColumns();
            candidates.put(index, columns);
            if (columns < bestMeanCount)
            {
                best = expression;
                bestMeanCount = columns;
            }
        }

        if (best == null)
            Tracing.trace("No applicable indexes found");
        else
            Tracing.trace("Candidate index mean cardinalities are {}. Scanning with {}.",
                          FBUtilities.toString(candidates), indexManager.getIndexForColumn(best.column).getIndexName());

        return best;
    }
}
