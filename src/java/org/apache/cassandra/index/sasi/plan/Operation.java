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
package org.apache.cassandra.index.sasi.plan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.ColumnMetadata.Kind;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.plan.Expression.Op;
import org.apache.cassandra.index.sasi.utils.RangeIntersectionIterator;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;

import org.apache.cassandra.utils.FBUtilities;

public class Operation extends RangeIterator<Long, Token>
{
    public enum OperationType
    {
        AND, OR;

        public boolean apply(boolean a, boolean b)
        {
            switch (this)
            {
                case OR:
                    return a | b;

                case AND:
                    return a & b;

                default:
                    throw new AssertionError();
            }
        }
    }

    private final QueryController controller;

    protected final OperationType op;
    protected final ListMultimap<ColumnMetadata, Expression> expressions;
    protected final RangeIterator<Long, Token> range;

    protected Operation left, right;

    private Operation(OperationType operation,
                      QueryController controller,
                      ListMultimap<ColumnMetadata, Expression> expressions,
                      RangeIterator<Long, Token> range,
                      Operation left, Operation right)
    {
        super(range);

        this.op = operation;
        this.controller = controller;
        this.expressions = expressions;
        this.range = range;

        this.left = left;
        this.right = right;
    }

    /**
     * Recursive "satisfies" checks based on operation
     * and data from the lower level members using depth-first search
     * and bubbling the results back to the top level caller.
     *
     * Most of the work here is done by {@link #localSatisfiedBy(Unfiltered, Row, boolean)}
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
     * @param currentCluster The row cluster to check.
     * @param staticRow The static row associated with current cluster.
     * @param allowMissingColumns allow columns value to be null.
     * @return true if give Row satisfied all of the expressions in the tree,
     *         false otherwise.
     */
    public boolean satisfiedBy(Unfiltered currentCluster, Row staticRow, boolean allowMissingColumns)
    {
        boolean sideL, sideR;

        if (expressions == null || expressions.isEmpty())
        {
            sideL =  left != null &&  left.satisfiedBy(currentCluster, staticRow, allowMissingColumns);
            sideR = right != null && right.satisfiedBy(currentCluster, staticRow, allowMissingColumns);

            // one of the expressions was skipped
            // because it had no indexes attached
            if (left == null)
                return sideR;
        }
        else
        {
            sideL = localSatisfiedBy(currentCluster, staticRow, allowMissingColumns);

            // if there is no right it means that this expression
            // is last in the sequence, we can just return result from local expressions
            if (right == null)
                return sideL;

            sideR = right.satisfiedBy(currentCluster, staticRow, allowMissingColumns);
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
     * @param currentCluster The row cluster to check.
     * @param staticRow The static row associated with current cluster.
     * @param allowMissingColumns allow columns value to be null.
     * @return true if give Row satisfied all of the analyzed expressions,
     *         false otherwise.
     */
    private boolean localSatisfiedBy(Unfiltered currentCluster, Row staticRow, boolean allowMissingColumns)
    {
        if (currentCluster == null || !currentCluster.isRow())
            return false;

        final long now = FBUtilities.nowInSeconds();
        boolean result = false;
        int idx = 0;

        for (ColumnMetadata column : expressions.keySet())
        {
            if (column.kind == Kind.PARTITION_KEY)
                continue;

            ByteBuffer value = ColumnIndex.getValueOf(column, column.kind == Kind.STATIC ? staticRow : (Row) currentCluster, now);
            boolean isMissingColumn = value == null;

            if (!allowMissingColumns && isMissingColumn)
                throw new IllegalStateException("All indexed columns should be included into the column slice, missing: " + column);

            boolean isMatch = false;
            // If there is a column with multiple expressions that effectively means an OR
            // e.g. comment = 'x y z' could be split into 'comment' EQ 'x', 'comment' EQ 'y', 'comment' EQ 'z'
            // by analyzer, in situation like that we only need to check if at least one of expressions matches,
            // and there is no hit on the NOT_EQ (if any) which are always at the end of the filter list.
            // Loop always starts from the end of the list, which makes it possible to break after the last
            // NOT_EQ condition on first EQ/RANGE condition satisfied, instead of checking every
            // single expression in the column filter list.
            List<Expression> filters = expressions.get(column);
            for (int i = filters.size() - 1; i >= 0; i--)
            {
                Expression expression = filters.get(i);
                isMatch = !isMissingColumn && expression.isSatisfiedBy(value);
                if (expression.getOp() == Op.NOT_EQ)
                {
                    // since this is NOT_EQ operation we have to
                    // inverse match flag (to check against other expressions),
                    // and break in case of negative inverse because that means
                    // that it's a positive hit on the not-eq clause.
                    isMatch = !isMatch;
                    if (!isMatch)
                        break;
                } // if it was a match on EQ/RANGE or column is missing
                else if (isMatch || isMissingColumn)
                    break;
            }

            if (idx++ == 0)
            {
                result = isMatch;
                continue;
            }

            result = op.apply(result, isMatch);

            // exit early because we already got a single false
            if (op == OperationType.AND && !result)
                return false;
        }

        return idx == 0 || result;
    }

    @VisibleForTesting
    protected static ListMultimap<ColumnMetadata, Expression> analyzeGroup(QueryController controller,
                                                                           OperationType op,
                                                                           List<RowFilter.Expression> expressions)
    {
        ListMultimap<ColumnMetadata, Expression> analyzed = ArrayListMultimap.create();

        // sort all of the expressions in the operation by name and priority of the logical operator
        // this gives us an efficient way to handle inequality and combining into ranges without extra processing
        // and converting expressions from one type to another.
        Collections.sort(expressions, (a, b) -> {
            int cmp = a.column().compareTo(b.column());
            return cmp == 0 ? -Integer.compare(getPriority(a.operator()), getPriority(b.operator())) : cmp;
        });

        for (final RowFilter.Expression e : expressions)
        {
            ColumnIndex columnIndex = controller.getIndex(e);
            List<Expression> perColumn = analyzed.get(e.column());

            if (columnIndex == null)
                columnIndex = new ColumnIndex(controller.getKeyValidator(), e.column(), null);

            AbstractAnalyzer analyzer = columnIndex.getAnalyzer();
            analyzer.reset(e.getIndexValue().duplicate());

            // EQ/LIKE_*/NOT_EQ can have multiple expressions e.g. text = "Hello World",
            // becomes text = "Hello" OR text = "World" because "space" is always interpreted as a split point (by analyzer),
            // NOT_EQ is made an independent expression only in case of pre-existing multiple EQ expressions, or
            // if there is no EQ operations and NOT_EQ is met or a single NOT_EQ expression present,
            // in such case we know exactly that there would be no more EQ/RANGE expressions for given column
            // since NOT_EQ has the lowest priority.
            boolean isMultiExpression = false;
            switch (e.operator())
            {
                case EQ:
                    isMultiExpression = false;
                    break;

                case LIKE_PREFIX:
                case LIKE_SUFFIX:
                case LIKE_CONTAINS:
                case LIKE_MATCHES:
                    isMultiExpression = true;
                    break;

                case NEQ:
                    isMultiExpression = (perColumn.size() == 0 || perColumn.size() > 1
                                     || (perColumn.size() == 1 && perColumn.get(0).getOp() == Op.NOT_EQ));
                    break;
            }

            if (isMultiExpression)
            {
                while (analyzer.hasNext())
                {
                    final ByteBuffer token = analyzer.next();
                    perColumn.add(new Expression(controller, columnIndex).add(e.operator(), token));
                }
            }
            else
            // "range" or not-equals operator, combines both bounds together into the single expression,
            // iff operation of the group is AND, otherwise we are forced to create separate expressions,
            // not-equals is combined with the range iff operator is AND.
            {
                Expression range;
                if (perColumn.size() == 0 || op != OperationType.AND)
                    perColumn.add((range = new Expression(controller, columnIndex)));
                else
                    range = Iterables.getLast(perColumn);

                while (analyzer.hasNext())
                    range.add(e.operator(), analyzer.next());
            }
        }

        return analyzed;
    }

    private static int getPriority(Operator op)
    {
        switch (op)
        {
            case EQ:
                return 5;

            case LIKE_PREFIX:
            case LIKE_SUFFIX:
            case LIKE_CONTAINS:
            case LIKE_MATCHES:
                return 4;

            case GTE:
            case GT:
                return 3;

            case LTE:
            case LT:
                return 2;

            case NEQ:
                return 1;

            default:
                return 0;
        }
    }

    protected Token computeNext()
    {
        return range != null && range.hasNext() ? range.next() : endOfData();
    }

    protected void performSkipTo(Long nextToken)
    {
        if (range != null)
            range.skipTo(nextToken);
    }

    public void close() throws IOException
    {
        controller.releaseIndexes(this);
    }

    public static class Builder
    {
        private final QueryController controller;

        protected final OperationType op;
        protected final List<RowFilter.Expression> expressions;

        protected Builder left, right;

        public Builder(OperationType operation, QueryController controller, RowFilter.Expression... columns)
        {
            this.op = operation;
            this.controller = controller;
            this.expressions = new ArrayList<>();
            Collections.addAll(expressions, columns);
        }

        public Builder setRight(Builder operation)
        {
            this.right = operation;
            return this;
        }

        public Builder setLeft(Builder operation)
        {
            this.left = operation;
            return this;
        }

        public void add(RowFilter.Expression e)
        {
            expressions.add(e);
        }

        public void add(Collection<RowFilter.Expression> newExpressions)
        {
            if (expressions != null)
                expressions.addAll(newExpressions);
        }

        public Operation complete()
        {
            if (!expressions.isEmpty())
            {
                ListMultimap<ColumnMetadata, Expression> analyzedExpressions = analyzeGroup(controller, op, expressions);
                RangeIterator.Builder<Long, Token> range = controller.getIndexes(op, analyzedExpressions.values());

                Operation rightOp = null;
                if (right != null)
                {
                    rightOp = right.complete();
                    range.add(rightOp);
                }

                return new Operation(op, controller, analyzedExpressions, range.build(), null, rightOp);
            }
            else
            {
                Operation leftOp = null, rightOp = null;
                boolean leftIndexes = false, rightIndexes = false;

                if (left != null)
                {
                    leftOp = left.complete();
                    leftIndexes = leftOp != null && leftOp.range != null;
                }

                if (right != null)
                {
                    rightOp = right.complete();
                    rightIndexes = rightOp != null && rightOp.range != null;
                }

                RangeIterator<Long, Token> join;
                /**
                 * Operation should allow one of it's sub-trees to wrap no indexes, that is related  to the fact that we
                 * have to accept defined-but-not-indexed columns as well as key range as IndexExpressions.
                 *
                 * Two cases are possible:
                 *
                 * only left child produced indexed iterators, that could happen when there are two columns
                 * or key range on the right:
                 *
                 *                AND
                 *              /     \
                 *            OR       \
                 *           /   \     AND
                 *          a     b   /   \
                 *                  key   key
                 *
                 * only right child produced indexed iterators:
                 *
                 *               AND
                 *              /    \
                 *            AND     a
                 *           /   \
                 *         key  key
                 */
                if (leftIndexes && !rightIndexes)
                    join = leftOp;
                else if (!leftIndexes && rightIndexes)
                    join = rightOp;
                else if (leftIndexes)
                {
                    RangeIterator.Builder<Long, Token> builder = op == OperationType.OR
                                                ? RangeUnionIterator.<Long, Token>builder()
                                                : RangeIntersectionIterator.<Long, Token>builder();

                    join = builder.add(leftOp).add(rightOp).build();
                }
                else
                    throw new AssertionError("both sub-trees have 0 indexes.");

                return new Operation(op, controller, null, join, leftOp, rightOp);
            }
        }
    }
}
