/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.utils.RangeIntersectionIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUnionIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;

public class Operation extends RangeIterator
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

    final FilterTree filterTree;
    final RangeIterator range;

    final QueryController controller;

    private Operation(RangeIterator range, FilterTree filterTree, QueryController controller)
    {
        super(range);
        this.filterTree = filterTree;
        this.range = range;
        this.controller = controller;
    }

    public boolean satisfiedBy(DecoratedKey key, Unfiltered currentCluster, Row staticRow)
    {
        return filterTree.satisfiedBy(key, currentCluster, staticRow);
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
        expressions.sort((a, b) -> {
            int cmp = a.column().compareTo(b.column());
            return cmp == 0 ? -Integer.compare(getPriority(a.operator()), getPriority(b.operator())) : cmp;
        });

        for (final RowFilter.Expression e : expressions)
        {
            ColumnContext columnContext = controller.getContext(e);
            List<Expression> perColumn = analyzed.get(e.column());

            AbstractAnalyzer analyzer = columnContext.getAnalyzer();
            analyzer.reset(e.getIndexValue().duplicate());

            // EQ/LIKE_*/NOT_EQ can have multiple expressions e.g. text = "Hello World",
            // becomes text = "Hello" OR text = "World" because "space" is always interpreted as a split point (by analyzer),
            // CONTAINS/CONTAINS_KEY are always treated as multiple expressions since they currently only targetting
            // collections, NOT_EQ is made an independent expression only in case of pre-existing multiple EQ expressions, or
            // if there is no EQ operations and NOT_EQ is met or a single NOT_EQ expression present,
            // in such case we know exactly that there would be no more EQ/RANGE expressions for given column
            // since NOT_EQ has the lowest priority.
            boolean isMultiExpression = false;
            switch (e.operator())
            {
                case EQ:
                    // EQ operator will always be a multiple expression because it is being used by
                    // map entries
                    isMultiExpression = columnContext.isNonFrozenCollection();
                    break;

                case CONTAINS:
                case CONTAINS_KEY:
                case LIKE_PREFIX:
                case LIKE_MATCHES:
                    isMultiExpression = true;
                    break;

                case NEQ:
                    isMultiExpression = (perColumn.size() == 0 || perColumn.size() > 1
                                     || (perColumn.size() == 1 && perColumn.get(0).getOp() == Expression.Op.NOT_EQ));
                    break;
            }
            if (isMultiExpression)
            {
                while (analyzer.hasNext())
                {
                    final ByteBuffer token = analyzer.next();
                    perColumn.add(new Expression(columnContext).add(e.operator(), token));
                }
            }
            else
            // "range" or not-equals operator, combines both bounds together into the single expression,
            // iff operation of the group is AND, otherwise we are forced to create separate expressions,
            // not-equals is combined with the range iff operator is AND.
            {
                Expression range;
                if (perColumn.size() == 0 || op != OperationType.AND)
                {
                    perColumn.add((range = new Expression(columnContext)));
                }
                else
                {
                    range = Iterables.getLast(perColumn);
                }

                if (!TypeUtil.isLiteral(columnContext.getValidator()))
                {
                    range.add(e.operator(), e.getIndexValue().duplicate());
                }
                else
                {
                    while (analyzer.hasNext())
                    {
                        range.add(e.operator(), analyzer.next());
                    }
                }
            }
        }

        return analyzed;
    }

    private static int getPriority(org.apache.cassandra.cql3.Operator op)
    {
        switch (op)
        {
            case EQ:
            case CONTAINS:
            case CONTAINS_KEY:
                return 5;

            case LIKE_PREFIX:
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

    @Override
    protected Token computeNext()
    {
        return range != null && range.hasNext() ? range.next() : endOfData();
    }

    @Override
    protected void performSkipTo(Long nextToken)
    {
        if (range != null)
            range.skipTo(nextToken);
    }

    @Override
    public void close() throws IOException
    {
        if (range != null)
            range.close();

        controller.releaseIndexes(filterTree.expressions);
    }

    /**
     * @param controller current query controller
     * @return tree builder with query expressions added from query controller.
     */
    static TreeBuilder initTreeBuilder(QueryController controller)
    {
        TreeBuilder tree = new TreeBuilder(controller);
        tree.add(controller.getExpressions());
        return tree;
    }

    /**
     * A builder on which like expressions are built as subtrees using {@link OperationType} OR to
     * keep their correct semantics. Remaining expressions are added into the root AND OperationType.
     *
     *  Example:
     *
     *   3 Like expressions:
     *
     *                    AND (expressions)
     *                  /   \
     *                AND   OR (like)
     *               /   \
     *      (like) OR   OR (like)
     *
     **/
    public static class TreeBuilder
    {
        private final QueryController controller;
        final Builder root;
        Builder subtree;

        TreeBuilder(QueryController controller)
        {
            this.controller = controller;
            this.root = new Builder(OperationType.AND, controller);
            this.subtree = root;
        }

        public TreeBuilder add(Collection<RowFilter.Expression> expressions)
        {
            if (expressions != null)
                expressions.forEach(this::add);
            return this;
        }

        public TreeBuilder add(RowFilter.Expression exp)
        {
            if (exp.operator().isLike())
                addToSubTree(exp);
            else
                root.add(exp);

            return this;
        }

        private void addToSubTree(RowFilter.Expression exp)
        {
            Builder likeOperation = new Builder(OperationType.OR, controller);
            likeOperation.add(exp);
            if (subtree.right == null)
            {
                subtree.setRight(likeOperation);
            }
            else if (subtree.left == null)
            {
                Builder newSubtree = new Builder(OperationType.AND, controller);
                subtree.setLeft(newSubtree);
                newSubtree.setRight(likeOperation);
                subtree = newSubtree;
            }
            else
            {
                throw new IllegalStateException("Both trees are full");
            }
        }

        public Operation complete()
        {
            return root.complete();
        }

        FilterTree completeFilter()
        {
            return root.completeFilter();
        }
    }

    public static class Builder
    {
        private final QueryController controller;

        protected final OperationType op;
        private final List<RowFilter.Expression> expressions;

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

        @SuppressWarnings("resource")
        public Operation complete()
        {
            if (!expressions.isEmpty())
            {
                ListMultimap<ColumnMetadata, Expression> analyzedExpressions = analyzeGroup(controller, op, expressions);
                RangeIterator.Builder range = controller.getIndexes(op, analyzedExpressions.values());

                Operation rightOp = null;
                if (right != null)
                {
                    rightOp = right.complete();
                    range.add(rightOp);
                }

                FilterTree filterTree  = new FilterTree(op, analyzedExpressions, null, rightOp != null ? rightOp.filterTree : null);
                return new Operation(range.build(), filterTree, controller);
            }
            else // when OR is used
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

                RangeIterator join;
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
                    RangeIterator.Builder builder = op == OperationType.OR
                                                                 ? RangeUnionIterator.builder()
                                                                 : RangeIntersectionIterator.selectiveBuilder();

                    join = builder.add(leftOp).add(rightOp).build();
                }
                else
                    throw new AssertionError("both sub-trees have 0 indexes.");

                return new Operation(join,
                                     new FilterTree(op, null,
                                                    leftOp == null ? null : leftOp.filterTree,
                                                    leftOp == null ? null : leftOp.filterTree),
                                     controller);
            }
        }

        /**
         * To build a filter tree used to filter data using indexed expressions and non-user-defined expressions.
         *
         * Similar to {@link #complete()}, except that this method won't reference {@link SSTableIndex} and avoids
         * complexity of RangeIterator.
         *
         * @return the filter tree
         */
        FilterTree completeFilter()
        {
            if (!expressions.isEmpty())
            {
                ListMultimap<ColumnMetadata, Expression> analyzedExpressions = analyzeGroup(controller, op, expressions);
                if (right != null)
                {
                    FilterTree ro = right.completeFilter();
                    return new FilterTree(op, analyzedExpressions, null, ro);
                }
                return new FilterTree(op, analyzedExpressions, null, null);
            }
            else
            {
                FilterTree leftOperation = left != null ? left.completeFilter() : null;
                FilterTree rightOperation = right != null ? right.completeFilter() : null;

                if (leftOperation == null && rightOperation == null)
                    throw new AssertionError("both sub-trees have 0 indexes.");

                return new FilterTree(op, null, leftOperation, rightOperation);
            }
        }
    }
}
