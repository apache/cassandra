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
package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.google.common.collect.ListMultimap;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sai.plan.Expression.Op;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.ColumnMetadata.Kind;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.index.sai.plan.Operation.OperationType;

/**
 * Tree-like structure to filter base table data using indexed expressions and non-user-defined filters.
 *
 * This is needed because:
 * 1. SAI doesn't index tombstones, base data may have been shadowed.
 * 2. SAI indexes partition offset, not all rows in partition match index condition.
 * 3. Replica filter protecting may fetch data that doesn't match index expressions.
 */
public class FilterTree
{
    protected final OperationType op;
    protected final ListMultimap<ColumnMetadata, Expression> expressions;

    protected final FilterTree left;
    protected final FilterTree right;

    FilterTree(OperationType operation,
               ListMultimap<ColumnMetadata, Expression> expressions,
               FilterTree left, FilterTree right)
    {
        this.op = operation;
        this.expressions = expressions;

        this.left = left;
        this.right = right;
    }

    /**
     * Recursive "satisfies" checks based on operation
     * and data from the lower level members using depth-first search
     * and bubbling the results back to the top level caller.
     *
     * Most of the work here is done by localSatisfiedBy(Unfiltered, Row, boolean)
     * see it's comment for details, if there are no local expressions
     * assigned to Operation it will call satisfiedBy(Row) on it's children.
     *
     * Query: first_name = X AND (last_name = Y OR address = XYZ AND street = IL AND city = C) OR (state = 'CA' AND country = 'US')
     * Row: key1: (first_name: X, last_name: Z, address: XYZ, street: IL, city: C, state: NY, country:US)
     *
     * #1                       OR
     *                        /    \
     * #2       (first_name) AND   AND (state, country)
     *                          \
     * #3            (last_name) OR
     *                             \
     * #4                          AND (address, street, city)
     *
     *
     * Evaluation of the key1 is top-down depth-first search:
     *
     * --- going down ---
     * Level #1 is evaluated, OR expression has to pull results from it's children which are at level #2 and OR them together,
     * Level #2 AND (state, country) could be be evaluated right away, AND (first_name) refers to it's "right" child from level #3
     * Level #3 OR (last_name) requests results from level #4
     * Level #4 AND (address, street, city) does logical AND between it's 3 fields, returns result back to level #3.
     * --- bubbling up ---
     * Level #3 computes OR between AND (address, street, city) result and it's "last_name" expression
     * Level #2 computes AND between "first_name" and result of level #3, AND (state, country) which is already computed
     * Level #1 does OR between results of AND (first_name) and AND (state, country) and returns final result.
     *
     * @param key The partition key for the row.
     * @param currentCluster The row cluster to check.
     * @param staticRow The static row associated with current cluster.
     * @return true if give Row satisfied all of the expressions in the tree,
     *         false otherwise.
     */
    public boolean satisfiedBy(DecoratedKey key, Unfiltered currentCluster, Row staticRow)
    {
        boolean sideL, sideR;

        if (expressions == null || expressions.isEmpty())
        {
            sideL =  left != null &&  left.satisfiedBy(key, currentCluster, staticRow);
            sideR = right != null && right.satisfiedBy(key, currentCluster, staticRow);

            // one of the expressions was skipped
            // because it had no indexes attached
            if (left == null)
                return sideR;
        }
        else
        {
            sideL = localSatisfiedBy(key, currentCluster, staticRow);

            // if there is no right it means that this expression
            // is last in the sequence, we can just return result from local expressions
            if (right == null)
                return sideL;

            sideR = right.satisfiedBy(key, currentCluster, staticRow);
        }

        return op.apply(sideL, sideR);
    }

    /**
     * Check every expression in the analyzed list to figure out if the
     * columns in the give row match all of the based on the operation
     * set to the current operation node.
     *
     * The algorithm is as follows: for every given expression from analyzed
     * list get corresponding column from the Row:
     *   - apply {@link Expression#isSatisfiedBy(ByteBuffer)}
     *     method to figure out if it's satisfied;
     *   - apply logical operation between boolean accumulator and current boolean result;
     *   - if result == false and node's operation is AND return right away;
     *
     * After all of the expressions have been evaluated return resulting accumulator variable.
     *
     * Example:
     *
     * Operation = (op: AND, columns: [first_name = p, 5 < age < 7, last_name: y])
     * Row = (first_name: pavel, last_name: y, age: 6, timestamp: 15)
     *
     * #1 get "first_name" = p (expressions)
     *      - row-get "first_name"                      => "pavel"
     *      - compare "pavel" against "p"               => true (current)
     *      - set accumulator current                   => true (because this is expression #1)
     *
     * #2 get "last_name" = y (expressions)
     *      - row-get "last_name"                       => "y"
     *      - compare "y" against "y"                   => true (current)
     *      - set accumulator to accumulator & current  => true
     *
     * #3 get 5 < "age" < 7 (expressions)
     *      - row-get "age"                             => "6"
     *      - compare 5 < 6 < 7                         => true (current)
     *      - set accumulator to accumulator & current  => true
     *
     * #4 return accumulator => true (row satisfied all of the conditions)
     *
     * @param key The partition key for the row.
     * @param currentCluster The row cluster to check.
     * @param staticRow The static row associated with current cluster.
     * @return true if give Row satisfied all of the analyzed expressions,
     *         false otherwise.
     */
    private boolean localSatisfiedBy(DecoratedKey key, Unfiltered currentCluster, Row staticRow)
    {
        if (currentCluster == null || !currentCluster.isRow())
            return false;

        final int now = FBUtilities.nowInSeconds();
        boolean result = op == OperationType.AND;

        Iterator<ColumnMetadata> columnIterator = expressions.keySet().iterator();
        while(columnIterator.hasNext())
        {
            ColumnMetadata column = columnIterator.next();
            Row row = column.kind == Kind.STATIC ? staticRow : (Row)currentCluster;

            // If there is a column with multiple expressions that can mean an OR or (in the case of map
            // collections) it can mean different map indexes.
            List<Expression> filters = expressions.get(column);

            // We do a reverse iteration over the filters because NOT_EQ operations will be at the end
            // of the filter list and we want to check them first.
            ListIterator<Expression> filterIterator = filters.listIterator(filters.size());
            while(filterIterator.hasPrevious())
            {
                Expression filter = filterIterator.previous();

                if (TypeUtil.isNonFrozenCollection(column.type))
                {
                    Iterator<ByteBuffer> valueIterator = filter.context.getValuesOf(row, now);
                    result = op.apply(result, collectionMatch(valueIterator, filter));
                }
                else
                {
                    ByteBuffer value = filter.context.getValueOf(key, row, now);
                    result = op.apply(result, singletonMatch(value, filter));
                }

                // If the operation is an AND then exit early if we get a single false
                if (op == OperationType.AND && !result)
                    return false;
            }
        }
        return result;
    }

    private boolean singletonMatch(ByteBuffer value, Expression filter)
    {
        boolean match = value != null && filter.isSatisfiedBy(value);
        // If this is NOT_EQ operation we have to
        // inverse match flag (to check against other expressions),
        if (filter.getOp() == Op.NOT_EQ)
            match = !match;
        return match;
    }

    private boolean collectionMatch(Iterator<ByteBuffer> valueIterator, Expression filter)
    {
        if (valueIterator == null)
            return false;

        while (valueIterator.hasNext())
        {
            ByteBuffer value = valueIterator.next();
            if (value == null)
                continue;
            if (filter.isSatisfiedBy(value))
                return true;
        }
        return false;
    }
}
