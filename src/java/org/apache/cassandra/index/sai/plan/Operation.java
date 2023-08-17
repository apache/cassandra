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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;

public class Operation
{
    public enum BooleanOperator
    {
        AND((a, b) -> a & b),
        AND_NOT((a, b) -> a & !b);

        private final BiFunction<Boolean, Boolean, Boolean> func;

        BooleanOperator(BiFunction<Boolean, Boolean, Boolean> func)
        {
            this.func = func;
        }

        public boolean apply(boolean a, boolean b)
        {
            return func.apply(a, b);
        }
    }

    @VisibleForTesting
    protected static ListMultimap<ColumnMetadata, Expression> buildIndexExpressions(QueryController controller,
                                                                                    BooleanOperator booleanOperator,
                                                                                    List<RowFilter.Expression> expressions)
    {
        ListMultimap<ColumnMetadata, Expression> analyzed = ArrayListMultimap.create();

        // sort all the expressions in the operation by name and priority of the logical operator
        // this gives us an efficient way to handle inequality and combining into ranges without extra processing
        // and converting expressions from one type to another.
        expressions.sort((a, b) -> {
            int cmp = a.column().compareTo(b.column());
            return cmp == 0 ? -Integer.compare(getPriority(a.operator()), getPriority(b.operator())) : cmp;
        });

        for (final RowFilter.Expression e : expressions)
        {
            IndexContext indexContext = controller.getContext(e);
            List<Expression> perColumn = analyzed.get(e.column());

            AbstractAnalyzer analyzer = indexContext.getAnalyzerFactory().create();
            try
            {
                analyzer.reset(e.getIndexValue().duplicate());

                // EQ can have multiple expressions e.g. text = "Hello World",
                // becomes text = "Hello" OR text = "World" because "space" is always interpreted as a split point (by analyzer),
                // CONTAINS/CONTAINS_KEY are always treated as multiple expressions since they currently only targetting
                // collections.
                boolean isMultiExpression = false;
                switch (e.operator())
                {
                    case EQ:
                    case NEQ:
                        // EQ and NEQ operator will always be a multiple expression because it is being used by
                        // map entries
                        isMultiExpression = indexContext.isNonFrozenCollection();
                        break;

                    case CONTAINS:
                    case CONTAINS_KEY:
                    case NOT_CONTAINS:
                    case NOT_CONTAINS_KEY:
                        isMultiExpression = true;
                        break;
                }
                if (isMultiExpression)
                {
                    while (analyzer.hasNext())
                    {
                        final ByteBuffer token = analyzer.next();
                        perColumn.add(new Expression(indexContext).add(e.operator(), token.duplicate()));
                    }
                }
                else
                // "range" or not-equals operator, combines both bounds together into the single expression,
                // if operation of the group is AND, otherwise we are forced to create separate expressions,
                // not-equals is combined with the range iff operator is AND.
                {
                    Expression range;
                    if (perColumn.size() == 0 || booleanOperator != BooleanOperator.AND)
                    {
                        range = new Expression(indexContext);
                        perColumn.add(range);
                    }
                    else
                    {
                        range = Iterables.getLast(perColumn);
                    }

                    if (!TypeUtil.isLiteral(indexContext.getValidator()))
                    {
                        range.add(e.operator(), e.getIndexValue().duplicate());
                    }
                    else
                    {
                        while (analyzer.hasNext())
                        {
                            ByteBuffer term = analyzer.next();
                            range.add(e.operator(), term.duplicate());
                        }
                    }
                }
            }
            finally
            {
                analyzer.end();
            }
        }

        return analyzed;
    }

    private static int getPriority(Operator op)
    {
        switch (op)
        {
            case EQ:
            case CONTAINS:
            case CONTAINS_KEY:
            case NOT_CONTAINS:
            case NOT_CONTAINS_KEY:
                return 5;

            case GTE:
            case GT:
                return 3;

            case LTE:
            case LT:
                return 2;

            default:
                return 0;
        }
    }

    /**
     * Converts expressions into filter tree for query.
     *
     * @return a KeyRangeIterator over the index query results
     */
    static KeyRangeIterator buildIterator(QueryController controller)
    {
        Node tree = Node.buildTree(controller.filterOperation());
        return tree.analyzeTree(controller).rangeIterator(controller);
    }

    /**
     * Converts expressions into filter tree (which is currently just a single AND).
     *
     * Filter tree allows us to do a couple of important optimizations
     * namely, group flattening for AND operations (query rewrite), expression bounds checks,
     * "satisfies by" checks for resulting rows with an early exit.
     *
     * @return root of the filter tree.
     */
    static FilterTree buildFilter(QueryController controller)
    {
        return Node.buildTree(controller.filterOperation()).buildFilter(controller);
    }

    static abstract class Node
    {
        ListMultimap<ColumnMetadata, Expression> expressionMap;

        boolean canFilter()
        {
            return (expressionMap != null && !expressionMap.isEmpty()) || !children().isEmpty();
        }

        List<Node> children()
        {
            return Collections.emptyList();
        }

        void add(Node child)
        {
            throw new UnsupportedOperationException();
        }

        RowFilter.Expression expression()
        {
            throw new UnsupportedOperationException();
        }

        abstract void analyze(List<RowFilter.Expression> expressionList, QueryController controller);

        abstract FilterTree filterTree();

        abstract KeyRangeIterator rangeIterator(QueryController controller);

        static Node buildTree(RowFilter filterOperation)
        {
            OperatorNode node = new AndNode();
            for (RowFilter.Expression expression : filterOperation.getExpressions())
                node.add(buildExpression(expression));
            return node;
        }

        static Node buildExpression(RowFilter.Expression expression)
        {
            return new ExpressionNode(expression);
        }

        Node analyzeTree(QueryController controller)
        {
            List<RowFilter.Expression> expressionList = new ArrayList<>();
            doTreeAnalysis(this, expressionList, controller);
            if (!expressionList.isEmpty())
                this.analyze(expressionList, controller);
            return this;
        }

        void doTreeAnalysis(Node node, List<RowFilter.Expression> expressions, QueryController controller)
        {
            if (node.children().isEmpty())
                expressions.add(node.expression());
            else
            {
                List<RowFilter.Expression> expressionList = new ArrayList<>();
                for (Node child : node.children())
                    doTreeAnalysis(child, expressionList, controller);
                node.analyze(expressionList, controller);
            }
        }

        FilterTree buildFilter(QueryController controller)
        {
            analyzeTree(controller);
            FilterTree tree = filterTree();
            for (Node child : children())
                if (child.canFilter())
                    tree.addChild(child.buildFilter(controller));
            return tree;
        }
    }

    static abstract class OperatorNode extends Node
    {
        final List<Node> children = new ArrayList<>();

        @Override
        public List<Node> children()
        {
            return children;
        }

        @Override
        public void add(Node child)
        {
            children.add(child);
        }
    }

    static class AndNode extends OperatorNode
    {
        @Override
        public void analyze(List<RowFilter.Expression> expressionList, QueryController controller)
        {
            expressionMap = buildIndexExpressions(controller, BooleanOperator.AND, expressionList);
        }

        @Override
        FilterTree filterTree()
        {
            return new FilterTree(BooleanOperator.AND, expressionMap);
        }

        @Override
        KeyRangeIterator rangeIterator(QueryController controller)
        {
            KeyRangeIterator.Builder builder = controller.getIndexQueryResults(expressionMap.values());
            for (Node child : children)
            {
                boolean canFilter = child.canFilter();
                if (canFilter)
                    builder.add(child.rangeIterator(controller));
            }
            return builder.build();
        }
    }

    static class ExpressionNode extends Node
    {
        final RowFilter.Expression expression;

        @Override
        public void analyze(List<RowFilter.Expression> expressionList, QueryController controller)
        {
            expressionMap = buildIndexExpressions(controller, BooleanOperator.AND, expressionList);
        }

        @Override
        FilterTree filterTree()
        {
            return new FilterTree(BooleanOperator.AND, expressionMap);
        }

        public ExpressionNode(RowFilter.Expression expression)
        {
            this.expression = expression;
        }

        @Override
        public RowFilter.Expression expression()
        {
            return expression;
        }

        @Override
        KeyRangeIterator rangeIterator(QueryController controller)
        {
            assert canFilter() : "Cannot process query with no expressions";

            return controller.getIndexQueryResults(expressionMap.values()).build();
        }
    }
}
