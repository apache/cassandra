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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.LikePatterns;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.ExternalRestriction;
import org.apache.cassandra.cql3.restrictions.Restrictions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.SelectOptions;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.Redaction;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.InvalidPatternException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.sai.utils.GeoUtil;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.lucene.util.SloppyMath;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkBindValueSet;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

/**
 * A filter on which rows a given query should include or exclude.
 * <p>
 * This corresponds to the restrictions on rows that are not handled by the query
 * {@link ClusteringIndexFilter}. Some of the expressions of this filter may
 * be handled by a 2ndary index, and the rest is simply filtered out from the
 * result set (the later can only happen if the query was using ALLOW FILTERING).
 */
public class RowFilter
{
    private static final Logger logger = LoggerFactory.getLogger(RowFilter.class);

    public static final Serializer serializer = new Serializer();

    /**
     * An empty row filter that filters no rows, so it is always satisfied.
     */
    public static final RowFilter REJECT_NONE = new RowFilter(FilterElement.REJECT_NONE, false, IndexHints.NONE, OptimizerOptions.NONE);

    /**
     * A row filter that filters all rows, so it can never be satisfied.
     * Used when the CQL restrictions are detected to be trivially unsatisfiable,
     * for example for {@code x < 1 AND x > 2}.
     */
    public static final RowFilter REJECT_ALL = new RowFilter(FilterElement.REJECT_ALL, false, IndexHints.NONE, OptimizerOptions.NONE)
    {
        @Override
        public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator partitions, long nowInSec)
        {
            partitions.close();
            return EmptyIterators.unfilteredPartition(partitions.metadata());
        }

        @Override
        public PartitionIterator filter(PartitionIterator partitions, TableMetadata metadata, long nowInSec)
        {
            partitions.close();
            return EmptyIterators.partition();
        }

        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            return false;
        }

        @Override
        public boolean isUnsatisfiable()
        {
            return true;
        }
    };

    public final FilterElement root;
    public final IndexHints indexHints;
    public final OptimizerOptions optimizerOptions;
    private final boolean needsReconciliation;

    protected RowFilter(FilterElement root, boolean needsReconciliation, IndexHints indexHints, OptimizerOptions optimizerOptions)
    {
        this.root = root;
        this.needsReconciliation = needsReconciliation;
        this.indexHints = indexHints;
        this.optimizerOptions = optimizerOptions;
    }

    public static RowFilter none()
    {
        return REJECT_NONE;
    }

    public FilterElement root()
    {
        return root;
    }

    /**
     * @return all the expressions in this filter expression tree by traversing it in pre-order
     */
    public List<Expression> expressions()
    {
        return root.traversedExpressions();
    }

    /**
     * @return {@code true} if this filter contains any expression with an ANN operator, {@code false} otherwise.
     */
    public boolean hasANN()
    {
        for (Expression expression : root.expressions()) // ANN expressions are always on the first tree level
        {
            if (expression.operator == Operator.ANN)
                return true;
        }
        return false;
    }

    /**
     * @return {@code true} if this filter contains any expression with an ordering expression, {@code false} otherwise.
     */
    public boolean hasOrdering()
    {
        for (Expression expression : root.expressions()) // ordering expressions are always on the first tree level
        {
            if (expression.isOrderingExpression())
                return true;
        }
        return false;
    }

    /**
     * Returns a copy of this {@link RowFilter} with ordering expressions removed
     */
    public RowFilter withoutOrderingExpressions()
    {
        return restrict(e -> !e.isOrderingExpression());
    }

    /**
     * @return the {@link ANNOptions} of the ANN expression in this filter, or {@link ANNOptions#NONE} if there is
     * no ANN expression.
     */
    public ANNOptions annOptions()
    {
        for (Expression expression : root.expressions()) // ANN expressions are always on the first tree level
        {
            if (expression.operator == Operator.ANN)
                return expression.annOptions();
        }
        return ANNOptions.NONE;
    }

    /**
     * @return {@code true} if this filter contains any disjunction, {@code false} otherwise.
     */
    public boolean containsDisjunctions()
    {
        return root.containsDisjunctions();
    }

    /**
     * @return true if this filter belongs to a read that requires reconciliation at the coordinator
     * @see StatementRestrictions#getRowFilter(IndexRegistry, QueryOptions, ClientState, SelectOptions)
     */
    public boolean needsReconciliation()
    {
        return needsReconciliation;
    }

    /**
     * If this filter belongs to a read that requires reconciliation at the coordinator, and it contains an intersection
     * on two or more non-key (and therefore mutable) columns, we cannot strictly apply it to local, unrepaired rows.
     * When this occurs, we must downgrade the intersection of expressions to a union and leave the coordinator to
     * filter strictly before sending results to the client.
     *
     * @return true if strict filtering is safe
     *
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-19018">CASSANDRA-19018</a>
     */
    public boolean isStrict()
    {
        return !needsReconciliation || !isMutableIntersection();
    }

    /**
     * @return true if this filter contains an intersection on either any static column or two regular mutable columns
     */
    public boolean isMutableIntersection()
    {
        List<Expression> exprs = expressions();
        Set<ColumnMetadata> columns = null;
        for (Expression e : exprs)
        {
            if (e.column.isStatic() && exprs.size() > 1)
                return true;

            if (!e.column.isPrimaryKeyColumn())
            {
                if (columns == null)
                    columns = new HashSet<>(exprs.size());

                columns.add(e.column);
                if (columns.size() > 1)
                    return true;
            }
        }
        return false;
    }

    /**
     * Checks if some of the expressions apply to clustering or regular columns.
     * @return {@code true} if some of the expressions apply to clustering or regular columns, {@code false} otherwise.
     */
    public boolean hasExpressionOnClusteringOrRegularColumns()
    {
        for (Expression expression : expressions())
        {
            ColumnMetadata column = expression.column();
            if (column.isClusteringColumn() || column.isRegular())
                return true;
        }
        return false;
    }

    protected Transformation<BaseRowIterator<?>> filter(TableMetadata metadata, long nowInSec)
    {
        FilterElement partitionLevelOperation = root.partitionLevelTree();
        FilterElement rowLevelOperation = root.rowLevelTree();

        final boolean filterNonStaticColumns = !rowLevelOperation.isEmpty();

        return new Transformation<>()
        {
            DecoratedKey pk;

            @Override
            protected BaseRowIterator<?> applyToPartition(BaseRowIterator<?> partition)
            {
                pk = partition.partitionKey();

                // Short-circuit all partitions that won't match based on static and partition keys
                if (!partitionLevelOperation.isSatisfiedBy(metadata, partition.partitionKey(), partition.staticRow(), nowInSec))
                {
                    partition.close();
                    return null;
                }

                BaseRowIterator<?> iterator = partition instanceof UnfilteredRowIterator
                                              ? Transformation.apply((UnfilteredRowIterator) partition, this)
                                              : Transformation.apply((RowIterator) partition, this);

                if (filterNonStaticColumns && !iterator.hasNext())
                {
                    iterator.close();
                    return null;
                }

                return iterator;
            }

            @Override
            public Row applyToRow(Row row)
            {
                // If we purge deletions when reconciliation is required, we hide information replica filtering
                // protection would require to filter rows that are no longer matches are the coordinator.
                Row purged = needsReconciliation() ? row : row.purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());

                if (purged == null)
                    return null;

                if (!rowLevelOperation.isSatisfiedBy(metadata, pk, purged, nowInSec))
                    return null;

                return row;
            }
        };
    }

    /**
     * Filters the provided iterator so that only the row satisfying the expression of this filter
     * are included in the resulting iterator.
     *
     * @param iter the iterator to filter
     * @param nowInSec the time of query in seconds.
     * @return the filtered iterator.
     */
    public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter, long nowInSec)
    {
        return root.isEmpty() ? iter : Transformation.apply(iter, filter(iter.metadata(), nowInSec));
    }

    /**
     * Filters the provided iterator so that only the row satisfying the expression of this filter
     * are included in the resulting iterator.
     *
     * @param iter the iterator to filter
     * @param nowInSec the time of query in seconds.
     * @return the filtered iterator.
     */
    public PartitionIterator filter(PartitionIterator iter, TableMetadata metadata, long nowInSec)
    {
        return root.isEmpty() ? iter : Transformation.apply(iter, filter(metadata, nowInSec));
    }

    /**
     * Whether the provided row in the provided partition satisfies this filter.
     *
     * @param metadata the table metadata.
     * @param partitionKey the partition key for partition to test.
     * @param row the row to test.
     * @param nowInSec the current time in seconds (to know what is live and what isn't).
     * @return {@code true} if {@code row} in partition {@code partitionKey} satisfies this row filter.
     */
    public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
    {
        // We purge all tombstones as the expressions isSatisfiedBy methods expects it
        Row purged = row.purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());
        if (purged == null)
            return root.isEmpty();

        return root.isSatisfiedBy(metadata, partitionKey, purged, nowInSec);
    }

    /**
     * Returns true if all the expressions within this filter that apply to the partition key are satisfied by
     * the given key, false otherwise.
     */
    public boolean partitionKeyRestrictionsAreSatisfiedBy(DecoratedKey key, AbstractType<?> keyValidator)
    {
        for (Expression e : expressions())
        {
            if (!e.column.isPartitionKey())
                continue;

            ByteBuffer value = keyValidator instanceof CompositeType
                             ? ((CompositeType) keyValidator).split(key.getKey())[e.column.position()]
                             : key.getKey();

            if (!e.isSatisfiedBy(e.column.type, value))
                return false;
        }
        return true;
    }

    /**
     * Returns true if all the expressions within this filter that apply to the clustering key are satisfied by
     * the given Clustering, false otherwise.
     */
    public boolean clusteringKeyRestrictionsAreSatisfiedBy(Clustering<?> clustering)
    {
        for (Expression e : expressions())
        {
            if (!e.column.isClusteringColumn())
                continue;

            ByteBuffer value = clustering.bufferAt(e.column.position());

            if (!e.isSatisfiedBy(e.column.type, value))
                return false;
        }
        return true;
    }

    /**
     * Returns this filter but without the provided expression. This method *assumes* that the filter contains the
     * provided expression, and it looks in all the levels of the filter tree.
     */
    public RowFilter without(Expression expression)
    {
        assert root.contains(expression);
        if (root.size() == 1)
            return RowFilter.none();

        return new RowFilter(root.filter(e -> !e.equals(expression)), needsReconciliation, indexHints, optimizerOptions);
    }

    /**
     * Returns a copy of this filter but without the provided expression. If this filter doesn't contain the specified
     * expression this method will just return an identical copy of this filter.
     */
    public RowFilter without(ColumnMetadata column, Operator op, ByteBuffer value)
    {
        if (isEmpty())
            return this;

        return new RowFilter(root.filter(e -> !e.column().equals(column) || e.operator() != op || !e.value.equals(value)), needsReconciliation, indexHints, optimizerOptions);
    }

    public boolean hasNonKeyExpression()
    {
        for (Expression e : expressions())
            if (!e.column().isPrimaryKeyColumn())
                return true;

        return false;
    }

    public boolean hasStaticExpression()
    {
        for (Expression e : expressions())
            if (e.column().isStatic())
                return true;

        return false;
    }

    /**
     * Returns a copy of this filter but without first level expressions with the provided column.
     * If this filter doesn't contain the specified expressions this method will just return an identical copy of this filter.
     */
    public RowFilter withoutFirstLevelExpression(ColumnMetadata column)
    {
        return restrictFirstLevel(e -> !e.column.equals(column));
    }

    /**
     * @return this filter pruning all its disjunction branches
     */
    public RowFilter withoutDisjunctions()
    {
        return new RowFilter(root.withoutDisjunctions(), needsReconciliation, indexHints, optimizerOptions);
    }

    public RowFilter restrict(Predicate<Expression> filter)
    {
        return new RowFilter(root.filter(filter), needsReconciliation, indexHints, optimizerOptions);
    }

    public RowFilter restrictFirstLevel(Predicate<Expression> filter)
    {
        return new RowFilter(root.filterFirstLevel(filter), needsReconciliation, indexHints, optimizerOptions);
    }

    public boolean isEmpty()
    {
        return root.isEmpty();
    }

    /**
     * @return {@code true} if this filter rejects all rows.
     */
    public boolean isUnsatisfiable()
    {
        return root.isUnsatisfiable();
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    /**
     * Returns a CQL representation of this row filter.
     *
     * @return a CQL representation of this row filter
     */
    public String toCQLString(Redaction redaction)
    {
        return root.toCQLString(redaction);
    }

    public String toString(boolean cql)
    {
        return root.toString(cql);
    }

    public static Builder builder(boolean needsReconciliation)
    {
        return new Builder(needsReconciliation, null, IndexHints.NONE);
    }

    public static Builder builder(boolean needsReconciliation, IndexRegistry indexRegistry)
    {
        return new Builder(needsReconciliation, indexRegistry, IndexHints.NONE);
    }

    public static Builder builder(IndexRegistry indexRegistry, IndexHints indexHints)
    {
        return new Builder(false, indexRegistry, indexHints, OptimizerOptions.NONE);
    }

    public static Builder builder(IndexRegistry indexRegistry, IndexHints indexHints, OptimizerOptions optimizerOptions)
    {
        return new Builder(false, indexRegistry, indexHints, optimizerOptions);
    }

    public static class Builder
    {
        private FilterElement.Builder current = new FilterElement.Builder(false);
        boolean needsReconciliation = false;
        private final IndexRegistry indexRegistry;
        private final IndexHints indexHints;
        private final OptimizerOptions optimizerOptions;

        public Builder(boolean needsReconciliation, IndexRegistry indexRegistry, IndexHints indexHints)
        {
            this(needsReconciliation, indexRegistry, indexHints, OptimizerOptions.NONE);
        }

        public Builder(boolean needsReconciliation, IndexRegistry indexRegistry, IndexHints indexHints, OptimizerOptions optimizerOptions)
        {
            this.needsReconciliation = needsReconciliation;
            this.indexRegistry = indexRegistry;
            this.indexHints = indexHints;
            this.optimizerOptions = optimizerOptions;
        }

        public RowFilter build()
        {
            return new RowFilter(current.build(), needsReconciliation, indexHints, optimizerOptions);
        }

        public RowFilter buildFromRestrictions(StatementRestrictions restrictions,
                                               TableMetadata table,
                                               QueryOptions options,
                                               ClientState state,
                                               ANNOptions annOptions)
        {
            FilterElement root = doBuild(restrictions, table, options, annOptions);

            if (root.isUnsatisfiable())
                return REJECT_ALL;

            if (Guardrails.queryFilters.enabled(state))
                Guardrails.queryFilters.guard(root.numFilteredValues(), "Select query", false, state);

            return new RowFilter(root, needsReconciliation, indexHints, optimizerOptions);
        }

        private FilterElement doBuild(StatementRestrictions restrictions,
                                      TableMetadata table,
                                      QueryOptions options,
                                      ANNOptions annOptions)
        {
            FilterElement.Builder element = new FilterElement.Builder(restrictions.isDisjunction());
            this.current = element;

            for (Restrictions restrictionSet : restrictions.filterRestrictions().getRestrictions())
                restrictionSet.addToRowFilter(this, indexRegistry, options, annOptions, indexHints);

            for (ExternalRestriction expression : restrictions.filterRestrictions().getExternalExpressions())
                addAllAsConjunction(b -> expression.addToRowFilter(b, table, options));

            // Add children, optimizing out any conjunctions/disjunctions with always-true or always-false restrictions.
            // Optimizing is not needed for correctness.
            List<StatementRestrictions> children = restrictions.children();
            return restrictions.isDisjunction()
                   ? buildDisjunction(element, children, table, options, annOptions)
                   : buildConjunction(element, children, table, options, annOptions);
        }

        /**
         * Processes the children of a disjunction (OR) node, pruning always-false branches and short-circuiting
         * on any always-true branch, then returns the resulting {@link FilterElement}.
         */
        private FilterElement buildDisjunction(FilterElement.Builder disjunction,
                                               List<StatementRestrictions> children,
                                               TableMetadata table,
                                               QueryOptions options,
                                               ANNOptions annOptions)
        {
            for (StatementRestrictions child : children)
            {
                FilterElement childElement = doBuild(child, table, options, annOptions);

                // `OR FALSE` elements can be skipped in disjunctions.
                if (childElement.isUnsatisfiable())
                    continue;

                // `OR TRUE` swallows all other restrictions in disjunctions.
                if (childElement.isAlwaysSatisfied())
                    return FilterElement.REJECT_NONE;

                disjunction.children.add(childElement);
            }

            // If every OR branch was dropped the whole disjunction can never be satisfied.
            if (disjunction.expressions.isEmpty() && disjunction.children.isEmpty())
                return FilterElement.REJECT_ALL;

            return flattenOrBuild(disjunction);
        }

        /**
         * Processes the children of a conjunction (AND) node, short-circuiting on any always-false branch and
         * dropping always-true branches, then returns the resulting {@link FilterElement}.
         */
        private FilterElement buildConjunction(FilterElement.Builder conjunction,
                                               List<StatementRestrictions> children,
                                               TableMetadata table,
                                               QueryOptions options,
                                               ANNOptions annOptions)
        {
            if (CellBounds.isUnsatisfiableConjunction(conjunction.expressions, table, indexHints))
                return FilterElement.REJECT_ALL;

            for (StatementRestrictions child : children)
            {
                FilterElement childElement = doBuild(child, table, options, annOptions);

                // `AND FALSE` swallows all other restrictions in conjunctions.
                if (childElement.isUnsatisfiable())
                    return FilterElement.REJECT_ALL;

                // `AND TRUE` does nothing in conjunctions, so don't add it.
                if (!childElement.isAlwaysSatisfied())
                    conjunction.children.add(childElement);
            }

            return flattenOrBuild(conjunction);
        }

        /**
         * Flattens a single-child wrapper node (redundant disjunction/conjunction) or builds the element normally.
         */
        private static FilterElement flattenOrBuild(FilterElement.Builder element)
        {
            // A disjunction/conjunction with no expressions and exactly one child is redundant, so flatten it.
            if (element.expressions.isEmpty() && element.children.size() == 1)
                return element.children.get(0);

            return element.build();
        }

        /**
         * Adds multiple filter expressions to this {@link RowFilter.Builder} and joins them with AND (conjunction),
         * regardless of the current mode (conjunction / disjunction) of the {@link RowFilter.Builder}.
         * <p>
         *
         * This wrapper method makes sure we pass a {@code RowFilter.Builder} that is always in conjunction mode to the
         * respective {@code addToRowFilterDelegate} method. If multiple expressions are added to the row filter, this
         * method makes sure they are joined with AND in their own {@link FilterElement}.
         *
         * @param addToRowFilterDelegate a function that adds expressions / child filter elements
         *                               to a provided {@link RowFilter.Builder}, and expects all
         *                               added expressions to be joined with AND operator
         */
        public void addAllAsConjunction(Consumer<Builder> addToRowFilterDelegate)
        {
            if (current.isDisjunction)
            {
                // If we're in disjunction mode, we must not pass the current builder to addToRowFilter.
                // We create a new conjunction sub-builder instead and add all expressions there.
                var builder = new Builder(needsReconciliation, indexRegistry, indexHints, optimizerOptions);
                addToRowFilterDelegate.accept(builder);

                if (builder.current.expressions.size() == 1 && builder.current.children.isEmpty())
                {
                    // Optimization:
                    // if there is one expression, we can just add it directly to the current FilterElement
                    // making the result tree flatter
                    current.expressions.add(builder.current.expressions.get(0));
                }
                else if (builder.current.children.size() == 1 && builder.current.expressions.isEmpty())
                {
                    // Optimization:
                    // if there is one child, we can just add it directly to the current FilterElement,
                    // making the result tree flatter
                    current.children.add(builder.current.children.get(0));
                }
                else
                {
                    // More expressions means we have to create a new child node (AND) for them.
                    // Also note that we use this for adding zero expressions/children as well.
                    // A conjunction with no restrictions means selecting everything, so if we didn't add an empty
                    // AND node in such case, we could end up with a filter that misses to match some rows.
                    current.children.add(builder.current.build());
                }
            }
            else
            {
                // Just an optimisation. If we're already in the conjunction mode, we don't need to create
                // a sub-builder; we can just use this one to collect the expressions.
                addToRowFilterDelegate.accept(this);
            }
        }

        /**
         * Adds the specified simple filter expression to this builder.
         *
         * @param def the filtered column
         * @param op the filtering operator, shouldn't be {@link Operator#ANN}.
         * @param value the filtered value
         * @return the added expression
         */
        public SimpleExpression add(ColumnMetadata def, Operator op, ByteBuffer value)
        {
            assert op != Operator.ANN : "ANN expressions should be added with the addANNExpression method";
            SimpleExpression expression = new SimpleExpression(def, op, value, analyzer(def, op, value), null);
            add(expression);
            return expression;
        }

        /**
         * Adds the specified ANN expression to this builder.
         *
         * @param def the column for ANN ordering
         * @param value the value for ANN ordering
         * @param annOptions the ANN options
         */
        public void addANNExpression(ColumnMetadata def, ByteBuffer value, ANNOptions annOptions)
        {
            add(new SimpleExpression(def, Operator.ANN, value, null, annOptions));
        }

        public void addMapComparison(ColumnMetadata def, ByteBuffer key, Operator op, ByteBuffer value)
        {
            add(new MapComparisonExpression(def, key, op, value));
        }

        @Nullable
        private Index.Analyzer analyzer(ColumnMetadata def, Operator op, ByteBuffer value)
        {
            return indexRegistry == null ? null : indexRegistry.getAnalyzerFor(def, op, value, indexHints).orElse(null);
        }

        public void addGeoDistanceExpression(ColumnMetadata def, ByteBuffer point, Operator op, ByteBuffer distance)
        {
            var primaryGeoDistanceExpression = new GeoDistanceExpression(def, point, op, distance);
            // The following logic optionally adds a second search expression in the event that the query area
            // crosses then antimeridian.
            if (primaryGeoDistanceExpression.crossesAntimeridian())
            {
                // The primry GeoDistanceExpression includes points on/over the antimeridian. Since we search
                // using the lat/lon coordinates, we must create a shifted expression that will collect
                // results on the other side of the antimeridian.
                var shiftedGeoDistanceExpression = primaryGeoDistanceExpression.buildShiftedExpression();
                if (current.isDisjunction)
                {
                    // We can add both expressions to this level of the tree because it is a disjunction.
                    add(primaryGeoDistanceExpression);
                    add(shiftedGeoDistanceExpression);
                }
                else
                {
                    // We need to add a new level to the tree so that we can get all results that match the primary
                    // or the shifted expressions.
                    var builder = new FilterElement.Builder(true);
                    primaryGeoDistanceExpression.validate();
                    shiftedGeoDistanceExpression.validate();
                    builder.expressions.add(primaryGeoDistanceExpression);
                    builder.expressions.add(shiftedGeoDistanceExpression);
                    current.children.add(builder.build());
                }
            }
            else
            {
                add(primaryGeoDistanceExpression);
            }
        }

        public void addCustomIndexExpression(TableMetadata metadata, IndexMetadata targetIndex, ByteBuffer value)
        {
            add(CustomExpression.build(metadata, targetIndex, value));
        }

        public Builder add(Expression expression)
        {
            expression.validate();
            current.expressions.add(expression);
            return this;
        }

        public void addUserExpression(UserExpression e)
        {
            current.expressions.add(e);
        }
    }

    public static class FilterElement
    {
        public static final Serializer serializer = new Serializer();

        /** A filter element that filters no rows, so it is always satisfied. */
        public static final FilterElement REJECT_NONE = new FilterElement(false, Collections.emptyList(), Collections.emptyList());

        /** A filter element that filters all rows, so it can never be satisfied. */
        public static final FilterElement REJECT_ALL = new FilterElement(true, Collections.emptyList(), Collections.emptyList());

        private final boolean isDisjunction;

        private final List<Expression> expressions;

        private final List<FilterElement> children;

        public FilterElement(boolean isDisjunction, List<Expression> expressions, List<FilterElement> children)
        {
            this.isDisjunction = isDisjunction;
            this.expressions = expressions;
            this.children = children;
        }

        public boolean isDisjunction()
        {
            return isDisjunction;
        }

        private boolean containsDisjunctions()
        {
            if (isDisjunction)
                return true;

            for (FilterElement child : children)
                if (child.containsDisjunctions())
                    return true;

            return false;
        }

        public List<Expression> expressions()
        {
            return expressions;
        }

        private List<Expression> traversedExpressions()
        {
            List<Expression> allExpressions = new ArrayList<>(expressions);
            for (FilterElement child : children)
                allExpressions.addAll(child.traversedExpressions());
            return allExpressions;
        }

        private FilterElement withoutDisjunctions()
        {
            if (isDisjunction)
                return REJECT_NONE;

            FilterElement.Builder builder = new Builder(false);
            builder.expressions.addAll(expressions);

            for (FilterElement child : children)
            {
                if (!child.isDisjunction)
                    builder.children.add(child);
            }

            return builder.build();
        }

        public FilterElement filter(Predicate<Expression> filter)
        {
            FilterElement.Builder builder = new Builder(isDisjunction);

            expressions.stream().filter(filter).forEach(builder.expressions::add);

            children.stream().map(c -> c.filter(filter)).forEach(builder.children::add);

            return builder.build();
        }

        public FilterElement filterFirstLevel(Predicate<Expression> filter)
        {
            FilterElement.Builder builder = new Builder(isDisjunction);
            expressions.stream().filter(filter).forEach(builder.expressions::add);
            builder.children.addAll(children);
            return builder.build();
        }

        public List<FilterElement> children()
        {
            return children;
        }

        public boolean isEmpty()
        {
            return expressions.isEmpty() && children.isEmpty();
        }

        /**
         * @return whether this filter element accepts any row in
         * {@link #isSatisfiedBy(TableMetadata, DecoratedKey, Row, long)}.
         */
        public boolean isAlwaysSatisfied()
        {
            return !isDisjunction && isEmpty();
        }

        /**
         * @return whether this filter element doesn't accept any row in
         * {@link #isSatisfiedBy(TableMetadata, DecoratedKey, Row, long)}.
         */
        public boolean isUnsatisfiable()
        {
            return this == REJECT_ALL;
        }

        public boolean contains(Expression expression)
        {
            return expressions.contains(expression) || children.stream().anyMatch(c -> contains(expression));
        }

        public FilterElement partitionLevelTree()
        {
            return new FilterElement(isDisjunction,
                                     expressions.stream()
                                                .filter(e -> e.column.isStatic() || e.column.isPartitionKey())
                                                .collect(Collectors.toList()),
                                     children.stream()
                                             .map(FilterElement::partitionLevelTree)
                                             .collect(Collectors.toList()));
        }

        public FilterElement rowLevelTree()
        {
            return new FilterElement(isDisjunction,
                                     expressions.stream()
                                                .filter(e -> !e.column.isStatic() && !e.column.isPartitionKey())
                                                .collect(Collectors.toList()),
                                     children.stream()
                                             .map(FilterElement::rowLevelTree)
                                             .collect(Collectors.toList()));
        }

        public int size()
        {
            return expressions.size() + children.stream().mapToInt(FilterElement::size).sum();
        }

        public boolean isSatisfiedBy(TableMetadata table, DecoratedKey key, Row row, long nowInSec)
        {
            if (this == REJECT_ALL)
                return false;

            if (isEmpty())
                return true;

            if (isDisjunction)
            {
                for (Expression e : expressions)
                    if (e.isSatisfiedBy(table, key, row, nowInSec))
                        return true;
                for (FilterElement child : children)
                    if (child.isSatisfiedBy(table, key, row, nowInSec))
                        return true;
                return false;
            }
            else
            {
                for (Expression e : expressions)
                    if (!e.isSatisfiedBy(table, key, row, nowInSec))
                        return false;
                for (FilterElement child : children)
                    if (!child.isSatisfiedBy(table, key, row, nowInSec))
                        return false;
                return true;
            }
        }

        /**
         * Returns the number of values that this filter will filter out after applying any index analyzers.
         */
        private int numFilteredValues()
        {
            int result = 0;

            for (Expression expression : expressions)
                result += expression.numFilteredValues();

            for (FilterElement child : children)
                result += child.numFilteredValues();

            return result;
        }

        public String toString(boolean cql)
        {
            return toCQLString(Redaction.NONE);
        }

        public String toCQLString(Redaction redaction)
        {
            StringBuilder sb = new StringBuilder();
            for (Expression expression : expressions)
            {
                if (expression.isOrderingExpression())
                    continue;
                if (sb.length() > 0)
                    sb.append(isDisjunction ? " OR " : " AND ");
                sb.append(expression.toCQLString(redaction));
            }
            for (FilterElement child : children)
            {
                if (sb.length() > 0)
                    sb.append(isDisjunction ? " OR " : " AND ");
                sb.append('(');
                sb.append(child.toCQLString(redaction));
                sb.append(')');
            }
            for (Expression expression : expressions)
            {
                if (!expression.isOrderingExpression())
                    continue;
                if (sb.length() > 0)
                    sb.append(' ');
                sb.append(expression.toCQLString(redaction));
            }
            return sb.toString();
        }

        public static class Builder
        {
            private final boolean isDisjunction;
            private final List<Expression> expressions = new ArrayList<>();
            private final List<FilterElement> children = new ArrayList<>();

            public Builder(boolean isDisjunction)
            {
                this.isDisjunction = isDisjunction;
            }

            public FilterElement build()
            {
                return new FilterElement(isDisjunction, expressions, children);
            }
        }

        public static class Serializer
        {
            public void serialize(FilterElement operation, DataOutputPlus out, int version) throws IOException
            {
                assert (!operation.isDisjunction && operation.children().isEmpty()) || version >= MessagingService.VERSION_DS_10 :
                "Attempting to serialize a disjunct row filter to a node that doesn't support disjunction";

                if (operation == FilterElement.REJECT_ALL)
                {
                    throw new AssertionError("An unsatisfiable row filter should either have produced an immediate " +
                                             "coordinator response or have been removed from the row filter.");
                }

                out.writeUnsignedVInt32(operation.expressions.size());
                for (Expression expr : operation.expressions)
                    Expression.serializer.serialize(expr, out, version);

                if (version < MessagingService.VERSION_DS_10)
                    return;

                out.writeBoolean(operation.isDisjunction);
                out.writeUnsignedVInt32(operation.children.size());
                for (FilterElement child : operation.children)
                    serialize(child, out, version);
            }

            public FilterElement deserialize(DataInputPlus in, int version, TableMetadata metadata, IndexHints indexHints) throws IOException
            {
                int size = in.readUnsignedVInt32();
                List<Expression> expressions = new ArrayList<>(size);
                for (int i = 0; i < size; i++)
                    expressions.add(Expression.serializer.deserialize(in, version, metadata, indexHints));

                if (version < MessagingService.VERSION_DS_10)
                    return new FilterElement(false, expressions, Collections.emptyList());

                boolean isDisjunction = in.readBoolean();
                size = in.readUnsignedVInt32();
                List<FilterElement> children = new ArrayList<>(size);
                for (int i  = 0; i < size; i++)
                    children.add(deserialize(in, version, metadata, indexHints));
                return new FilterElement(isDisjunction, expressions, children);
            }

            public long serializedSize(FilterElement operation, int version)
            {
                long size = TypeSizes.sizeofUnsignedVInt(operation.expressions.size());
                for (Expression expr : operation.expressions)
                    size += Expression.serializer.serializedSize(expr, version);

                if (version < MessagingService.VERSION_DS_10)
                    return size;

                size++; // isDisjunction boolean
                size += TypeSizes.sizeofUnsignedVInt(operation.children.size());
                for (FilterElement child : operation.children)
                    size += serializedSize(child, version);
                return size;
            }
        }
    }

    /**
     * Accumulates lower and upper bounds for the expressions for a single cell to detect sets of expressions that are
     * unsatisfiable when combined into a conjunction. For example, {@code x < 0 AND x > 1} is unsatisfiable.
     * </p>
     * CQL validation forbids more than one lower bound and more than one upper bound on the same column or
     * map element in a conjunction (e.g. x > 1 AND x >= 2), so there is no need to keep the tightest bound.
     */
    @VisibleForTesting
    static class CellBounds
    {
        static final String MULTIPLE_LOWER_BOUNDS_MESSAGE = "Unexpected multiple lower bounds on the same cell";
        static final String MULTIPLE_UPPER_BOUNDS_MESSAGE = "Unexpected multiple upper bounds on the same cell";
        static final String EQUALITY_WITH_BOUNDS_MESSAGE = "Unexpected equality combined with slice bounds on the same cell";
        static final String UNEXPECTED_OPERATOR_MESSAGE = "Unexpected operator: ";

        private final AbstractType<?> type;
        private ByteBuffer lowerBound = null;
        private ByteBuffer upperBound = null;
        private boolean includesLower = true;
        private boolean includesUpper = true;
        private ByteBuffer eqValue = null; // CQL forbids multiple equalities in a conjunction
        private List<ByteBuffer> neqValues = null;

        @VisibleForTesting
        CellBounds(AbstractType<?> type)
        {
            this.type = type;
        }

        @VisibleForTesting
        void add(Operator op, ByteBuffer value)
        {
            switch (op)
            {
                case GT:
                    assert lowerBound == null : MULTIPLE_LOWER_BOUNDS_MESSAGE;
                    lowerBound = value;
                    includesLower = false;
                    break;
                case GTE:
                    assert lowerBound == null : MULTIPLE_LOWER_BOUNDS_MESSAGE;
                    lowerBound = value;
                    includesLower = true;
                    break;
                case LT:
                    assert upperBound == null : MULTIPLE_UPPER_BOUNDS_MESSAGE;
                    upperBound = value;
                    includesUpper = false;
                    break;
                case LTE:
                    assert upperBound == null : MULTIPLE_UPPER_BOUNDS_MESSAGE;
                    upperBound = value;
                    includesUpper = true;
                    break;
                case EQ:
                    eqValue = value;
                    break;
                case NEQ:
                    if (neqValues == null)
                        neqValues = new ArrayList<>();
                    neqValues.add(value);
                    break;
                default:
                    throw new AssertionError(UNEXPECTED_OPERATOR_MESSAGE + op);
            }
        }

        /**
         * Returns {@code true} if the accumulated bounds form an empty interval.
         * Handles:
         * <ul>
         *   <li>lower > upper  (e.g. {@code x > 5 AND x < 3})</li>
         *   <li>lower == upper but at least one side is strict (e.g. {@code x >= 0 AND x < 0})</li>
         *   <li>NEQ eliminates a single-point interval (e.g. {@code x >= 0 AND x <= 0 AND x != 0})</li>
         * </ul>
         */
        private boolean isUnsatisfiable()
        {
            // CQL validation forbids combining an equality with any other relation on the same column or map element
            // (e.g. x = 2 AND x > 5), so there is no need to check an equality against the slice bounds.
            assert eqValue == null || (lowerBound == null && upperBound == null) : EQUALITY_WITH_BOUNDS_MESSAGE;

            if (eqValue != null || lowerBound == null || upperBound == null)
                return false;

            int cmp = type.compareForCQL(lowerBound, upperBound);
            if (cmp > 0)
                return true;  // lower > upper

            if (cmp < 0)
                return false;

            // lower == upper
            if (!includesLower || !includesUpper)
                return true;  // e.g. x > 0 AND x < 0  or  x >= 0 AND x < 0

            // The range has been pinned to a single point (both bounds inclusive),
            // so if one of the NEQ values equals that point the interval is empty.
            if (neqValues != null)
            {
                for (ByteBuffer neq : neqValues)
                {
                    if (type.compareForCQL(neq, lowerBound) == 0)
                        return true;
                }
            }

            return false;
        }

        /**
         * Checks whether the specified conjunction of expressions forms an empty interval.
         * For example, {@code x < 1 AND x > 2} is an unsatisfiable interval.
         */
        @VisibleForTesting
        static boolean isUnsatisfiableConjunction(List<Expression> expressions, TableMetadata table, IndexHints hints)
        {
            if (expressions.size() <= 1)
                return false;

            Map<Pair<ColumnMetadata, ByteBuffer>, CellBounds> perCellBounds = new HashMap<>();

            for (Expression expression : expressions)
            {
                AbstractType<?> valueType = boundValueType(expression, table, hints);
                if (valueType == null)
                    continue;

                ByteBuffer mapKey = expression instanceof MapComparisonExpression ? ((MapComparisonExpression) expression).key : null;
                CellBounds bounds = perCellBounds.computeIfAbsent(Pair.create(expression.column, mapKey), c -> new CellBounds(valueType));
                bounds.add(expression.operator, expression.value);
            }

            for (CellBounds bounds : perCellBounds.values())
            {
                if (bounds.isUnsatisfiable())
                    return true;
            }

            return false;
        }

        /**
         * Returns the type used to compare the bounds set by the specified expression,
         * or {@code null} if the expression should not be considered when looking for empty intervals.
         */
        @Nullable
        private static AbstractType<?> boundValueType(Expression expression, TableMetadata table, IndexHints hints)
        {
            Operator op = expression.operator;
            if (!op.isSlice() && op != Operator.EQ && op != Operator.NEQ)
                return null;

            ColumnMetadata column = expression.column;
            AbstractType<?> valueType;

            if (expression instanceof SimpleExpression)
                valueType = column.type.unwrap();
            else if (expression instanceof MapComparisonExpression) // e.g. map['key'] > 'value'
                valueType = ((MapType<?, ?>) column.type.unwrap()).valueComparator();
            else
                return null;

            // Skip types that don't support comparison (e.g. counter columns, whose values are
            // stored in an internal representation that is not comparable as plain CQL values).
            if (valueType.comparisonType == AbstractType.ComparisonType.NOT_COMPARABLE)
                return null;

            // Skip SAI-indexed inet columns, because SAI orders inet values differently from CQL (see CNDB-12978).
            // Using the CQL order here could wrongly discard ranges that are not empty for SAI,
            // so we preserve the original behavior and let SAI deal with the empty ranges.
            if (valueType instanceof InetAddressType && isSaiIndexed(expression, table, hints))
                return null;

            return valueType;
        }

        private static boolean isSaiIndexed(Expression expression, TableMetadata table, IndexHints hints)
        {
            return !table.isVirtual() &&
                   Keyspace.openAndGetStore(table)
                           .getIndexManager()
                           .isSaiIndexed(expression, hints);
        }
    }

    public static abstract class Expression
    {
        public static final Serializer serializer = new Serializer();

        // Note: the val of this enum is used for serialization,
        // and this is why we have some UNUSEDX for values we don't use anymore
        // (we could clean those on a major protocol update, but it's not worth
        // the trouble for now)
        // VECTOR
        protected enum Kind
        {
            SIMPLE(0), MAP_COMPARISON(1), UNUSED1(2), CUSTOM(3), USER(4), VECTOR_RADIUS(100);
            private final int val;
            Kind(int v) { val = v; }
            public int getVal() { return val; }
            public static Kind fromVal(int val)
            {
                switch (val)
                {
                    case 0: return SIMPLE;
                    case 1: return MAP_COMPARISON;
                    case 2: return UNUSED1;
                    case 3: return CUSTOM;
                    case 4: return USER;
                    case 100: return VECTOR_RADIUS;
                    default: throw new IllegalArgumentException("Unknown index expression kind: " + val);
                }
            }
        }

        protected abstract Kind kind();

        protected final ColumnMetadata column;
        protected final Operator operator;
        protected final ByteBuffer value;

        /** The LIKE_MATCHES pattern normalized by the analyzer of the index, computed on first use. */
        @Nullable
        private volatile ByteBuffer normalizedPattern;

        protected Expression(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            this.column = column;
            this.operator = operator;
            this.value = value;
        }

        public boolean isCustom()
        {
            return kind() == Kind.CUSTOM;
        }

        public boolean isUserDefined()
        {
            return kind() == Kind.USER;
        }

        public ColumnMetadata column()
        {
            return column;
        }

        public Operator operator()
        {
            return operator;
        }

        @Nullable
        public Index.Analyzer analyzer()
        {
            return null;
        }

        public boolean isOrderingExpression()
        {
            return operator == Operator.ANN || operator == Operator.BM25 || operator == Operator.ORDER_BY_ASC || operator == Operator.ORDER_BY_DESC;
        }

        protected boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer foundValue)
        {
            if (foundValue == null)
                return false;

            try
            {
                return isSatisfiedByNonNull(type, foundValue);
            }
            catch (InvalidPatternException e)
            {
                // a LIKE pattern that can't be compiled; the operator evaluating it doesn't know the column
                throw e.withColumn(column.name);
            }
        }

        /**
         * Returns the {@link Operator#LIKE_MATCHES} pattern of this expression, its literals normalized like the
         * values by the given analyzer (see {@link LikePatterns#normalizeLiterals}).
         */
        private ByteBuffer normalizedPattern(Index.Analyzer analyzer)
        {
            ByteBuffer pattern = normalizedPattern;
            if (pattern == null)
            {
                pattern = LikePatterns.normalizeLiterals(value, literal -> {
                    List<ByteBuffer> tokens = analyzer.indexedTokens(literal);
                    return tokens.isEmpty() ? null : tokens.get(0);
                });
                normalizedPattern = pattern;
            }
            return pattern;
        }

        private boolean isSatisfiedByNonNull(AbstractType<?> type, ByteBuffer foundValue)
        {
            Index.Analyzer analyzer = analyzer();

            // Note that CQL expression are always of the form 'x < 4', i.e. the tested value is on the left.
            if (analyzer == null)
                return operator.isSatisfiedBy(type, foundValue, value);

            // Tokenizing indexes don't serve LIKE, so the value and the pattern each analyze to a single
            // (normalized) token: match the normalized pattern against the normalized value, as the index does
            // against its normalized terms.
            if (operator.isLike())
            {
                List<ByteBuffer> indexedTokens = analyzer.indexedTokens(foundValue);
                List<ByteBuffer> queriedTokens = operator == Operator.LIKE_MATCHES
                                                 ? List.of(normalizedPattern(analyzer))
                                                 : analyzer.queriedTokens();
                assert indexedTokens.size() <= 1 && queriedTokens.size() <= 1
                    : "LIKE on " + column.name + " is served by a tokenizing index: " + indexedTokens.size() + " value tokens, " + queriedTokens.size() + " pattern tokens";
                return indexedTokens.size() == 1
                       && queriedTokens.size() == 1
                       && operator.isSatisfiedBy(type, indexedTokens.get(0), queriedTokens.get(0));
            }

            return operator.isSatisfiedByAnalyzed(type, analyzer.indexedTokens(foundValue), analyzer.queriedTokens());
        }

        @Nullable
        public ANNOptions annOptions()
        {
            return null;
        }

        /**
         * Checks if the operator of this <code>IndexExpression</code> is a <code>CONTAINS</code> operator.
         *
         * @return <code>true</code> if the operator of this <code>IndexExpression</code> is a <code>CONTAINS</code>
         * operator, <code>false</code> otherwise.
         */
        public boolean isContains()
        {
            return Operator.CONTAINS == operator;
        }

        /**
         * Checks if the operator of this <code>IndexExpression</code> is a <code>CONTAINS_KEY</code> operator.
         *
         * @return <code>true</code> if the operator of this <code>IndexExpression</code> is a <code>CONTAINS_KEY</code>
         * operator, <code>false</code> otherwise.
         */
        public boolean isContainsKey()
        {
            return Operator.CONTAINS_KEY == operator;
        }

        /**
         * Checks the operator of this {@code IndexExpression} is any of the variations of {@code [NOT] CONTAINS [KEY]}.
         *
         * @return {@code true} if this operator is any kind of contains operator, {@code false} otherwise.
         */
        public boolean isAnyContains()
        {
            return operator.isAnyContains();
        }

        /**
         * If this expression is used to query an index, the value to use as
         * partition key for that index query.
         */
        public ByteBuffer getIndexValue()
        {
            return value;
        }

        public void validate()
        {
            checkNotNull(value, "Unsupported null value for column %s", column.name);
            checkBindValueSet(value, "Unsupported unset value for column %s", column.name);
        }

        /** @deprecated See CASSANDRA-6377 */
        @Deprecated(since = "3.5")
        public void validateForIndexing()
        {
            checkFalse(value.remaining() > FBUtilities.MAX_UNSIGNED_SHORT,
                       "Index expression values may not be larger than 64K");
        }

        /**
         * Returns whether the provided row satisfied this expression or not.
         *
         * @param metadata the metadata of the queried table
         * @param partitionKey the partition key for row to check.
         * @param row the row to check. It should *not* contain deleted cells
         * (i.e. it should come from a RowIterator).
         * @return whether the row is satisfied by this expression.
         */
        public abstract boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec);

        /**
         * Returns the number of values that this expression will check after applying any index analyzers.
         */
        protected int numFilteredValues()
        {
            return 1;
        }

        protected ByteBuffer getValue(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            switch (column.kind)
            {
                case PARTITION_KEY:
                    return metadata.partitionKeyType instanceof CompositeType
                         ? CompositeType.extractComponent(partitionKey.getKey(), column.position())
                         : partitionKey.getKey();
                case CLUSTERING:
                    return row.clustering().bufferAt(column.position());
                default:
                    Cell<?> cell = row.getCell(column);
                    return cell == null || cell.isTombstone() || !cell.isLive(nowInSec) ? null : cell.buffer();
            }
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof Expression))
                return false;

            Expression that = (Expression)o;

            return this.kind() == that.kind()
                && this.operator == that.operator
                && Objects.equal(this.column.name, that.column.name)
                && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column.name, operator, value);
        }

        @Override
        public String toString()
        {
            return toCQLString(Redaction.NONE);
        }

        /**
         * Returns a CQL representation of this expression.
         *
         * @return a CQL representation of this expression
         */
        public String toCQLString(Redaction redaction)
        {
            return "";
        }

        public String toCQLString(boolean redact)
        {
            return toCQLString(redact ? Redaction.REDACT : Redaction.NONE);
        }

        public static class Serializer
        {
            public void serialize(Expression expression, DataOutputPlus out, int version) throws IOException
            {
                out.writeByte(expression.kind().getVal());

                // Custom expressions include neither a column or operator, but all
                // other expressions do.
                if (expression.kind() == Kind.CUSTOM)
                {
                    IndexMetadata.serializer.serialize(((CustomExpression)expression).targetIndex, out, version);
                    ByteBufferUtil.writeWithShortLength(expression.value, out);
                    return;
                }

                if (expression.kind() == Kind.USER)
                {
                    UserExpression.serialize((UserExpression)expression, out, version);
                    return;
                }

                ByteBufferUtil.writeWithShortLength(expression.column.name.bytes, out);
                expression.operator.writeTo(out);

                switch (expression.kind())
                {
                    case SIMPLE:
                        ByteBufferUtil.writeWithShortLength(expression.value, out);
                        if (expression.operator == Operator.ANN)
                            ANNOptions.serializer.serialize(expression.annOptions(), out, version);
                        break;
                    case MAP_COMPARISON:
                        MapComparisonExpression mexpr = (MapComparisonExpression)expression;
                        ByteBufferUtil.writeWithShortLength(mexpr.key, out);
                        ByteBufferUtil.writeWithShortLength(mexpr.value, out);
                        break;
                    case VECTOR_RADIUS:
                        GeoDistanceExpression gexpr = (GeoDistanceExpression) expression;
                        gexpr.distanceOperator.writeTo(out);
                        ByteBufferUtil.writeWithShortLength(gexpr.distance, out);
                        ByteBufferUtil.writeWithShortLength(gexpr.value, out);
                        break;
                }
            }

            public Expression deserialize(DataInputPlus in, int version, TableMetadata metadata, IndexHints indexHints) throws IOException
            {
                Kind kind = Kind.fromVal(in.readByte());

                // custom expressions (3.0+ only) do not contain a column or operator, only a value
                if (kind == Kind.CUSTOM)
                {
                    return new CustomExpression(metadata,
                            IndexMetadata.serializer.deserialize(in, version, metadata),
                            ByteBufferUtil.readWithShortLength(in));
                }

                if (kind == Kind.USER)
                    return UserExpression.deserialize(in, version, metadata);

                ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
                Operator operator = Operator.readFrom(in);
                ColumnMetadata column = metadata.getColumn(name);
                IndexRegistry indexRegistry = IndexRegistry.obtain(metadata);

                // Compact storage tables, when used with thrift, used to allow falling through this withouot throwing an
                // exception. However, since thrift was removed in 4.0, this behaviour was not restored in CASSANDRA-16217
                if (column == null)
                    throw new RuntimeException("Unknown (or dropped) column " + UTF8Type.instance.getString(name) + " during deserialization");

                switch (kind)
                {
                    case SIMPLE:
                        ByteBuffer value = ByteBufferUtil.readWithShortLength(in);
                        ANNOptions annOptions = operator == Operator.ANN ? ANNOptions.serializer.deserialize(in, version) : null;
                        Index.Analyzer analyzer = indexRegistry.getAnalyzerFor(column, operator, value, indexHints).orElse(null);
                        return new SimpleExpression(column, operator, value, analyzer, annOptions);
                    case MAP_COMPARISON:
                        ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                        ByteBuffer val = ByteBufferUtil.readWithShortLength(in);
                        return new MapComparisonExpression(column, key, operator, val);
                    case VECTOR_RADIUS:
                        Operator boundaryOperator = Operator.readFrom(in);
                        ByteBuffer distance = ByteBufferUtil.readWithShortLength(in);
                        ByteBuffer searchVector = ByteBufferUtil.readWithShortLength(in);
                        return new GeoDistanceExpression(column, searchVector, boundaryOperator, distance);
                }
                throw new AssertionError();
            }

            public long serializedSize(Expression expression, int version)
            {
                long size = 1; // kind byte

                // Custom expressions include neither a column or operator, but all
                // other expressions do.
                if (expression.kind() != Kind.CUSTOM && expression.kind() != Kind.USER)
                    size += ByteBufferUtil.serializedSizeWithShortLength(expression.column().name.bytes)
                            + expression.operator.serializedSize();

                switch (expression.kind())
                {
                    case SIMPLE:
                        size += ByteBufferUtil.serializedSizeWithShortLength((expression).value);
                        if (expression.operator == Operator.ANN)
                            size += ANNOptions.serializer.serializedSize(expression.annOptions(), version);
                        break;
                    case MAP_COMPARISON:
                        MapComparisonExpression mexpr = (MapComparisonExpression)expression;
                        size += ByteBufferUtil.serializedSizeWithShortLength(mexpr.key)
                              + ByteBufferUtil.serializedSizeWithShortLength(mexpr.value);
                        break;
                    case CUSTOM:
                        size += IndexMetadata.serializer.serializedSize(((CustomExpression)expression).targetIndex, version)
                               + ByteBufferUtil.serializedSizeWithShortLength(expression.value);
                        break;
                    case USER:
                        size += UserExpression.serializedSize((UserExpression)expression, version);
                        break;
                    case VECTOR_RADIUS:
                        GeoDistanceExpression geoDistanceRelation = (GeoDistanceExpression) expression;
                        size += ByteBufferUtil.serializedSizeWithShortLength(geoDistanceRelation.distance)
                                + ByteBufferUtil.serializedSizeWithShortLength(geoDistanceRelation.value)
                                + geoDistanceRelation.distanceOperator.serializedSize();
                        break;
                }
                return size;
            }
        }
    }

    /**
     * An expression of the form 'column' 'op' 'value'.
     */
    public static class SimpleExpression extends Expression
    {
        @Nullable
        protected final Index.Analyzer analyzer;

        @Nullable
        private final ANNOptions annOptions;

        public SimpleExpression(ColumnMetadata column,
                                Operator operator,
                                ByteBuffer value,
                                @Nullable Index.Analyzer analyzer,
                                @Nullable ANNOptions annOptions)
        {
            super(column, operator, value);
            this.analyzer = analyzer;
            this.annOptions = annOptions;
        }

        @Override
        @Nullable
        public Index.Analyzer analyzer()
        {
            return analyzer;
        }

        @Override
        public int numFilteredValues()
        {
            return analyzer == null
                    ? super.numFilteredValues()
                    : analyzer.queriedTokens().size();
        }

        @Nullable
        public ANNOptions annOptions()
        {
            return annOptions;
        }

        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            // We support null conditions for LWT (in ColumnCondition) but not for RowFilter.
            // TODO: we should try to merge both code someday.
            assert value != null;

            switch (operator)
            {
                case EQ:
                case IN:
                case NOT_IN:
                case LT:
                case LTE:
                case GTE:
                case GT:
                    {
                        assert !column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for collection types";

                        ByteBuffer foundValue = getValue(metadata, partitionKey, row, nowInSec);

                        if (foundValue == null)
                            return false;

                        // In order to support operators on Counter types, their value has to be extracted from internal
                        // representation. See CASSANDRA-11629
                        if (column.type.isCounter())
                        {
                            ByteBuffer counterValue = LongType.instance.decompose(CounterContext.instance().total(foundValue, ByteBufferAccessor.instance));
                            return isSatisfiedBy(LongType.instance, counterValue);
                        }
                        else
                        {
                            return isSatisfiedBy(column.type, foundValue);
                        }
                    }
                case NEQ:
                case LIKE_PREFIX:
                case LIKE_SUFFIX:
                case LIKE_CONTAINS:
                case LIKE_MATCHES:
                case ANALYZER_MATCHES:
                case ANN:
                case BM25:
                    {
                        assert !column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for collection types";
                        ByteBuffer foundValue = getValue(metadata, partitionKey, row, nowInSec);
                        // Note that CQL expression are always of the form 'x < 4', i.e. the tested value is on the left.
                        return isSatisfiedBy(column.type, foundValue);
                    }
                case CONTAINS:
                    return contains(metadata, partitionKey, row, nowInSec);
                case CONTAINS_KEY:
                    return containsKey(metadata, partitionKey, row, nowInSec);
                case NOT_CONTAINS:
                    return !contains(metadata, partitionKey, row, nowInSec);
                case NOT_CONTAINS_KEY:
                    return !containsKey(metadata, partitionKey, row, nowInSec);
            }
            throw new AssertionError("Unsupported operator: " + operator);
        }

        private boolean contains(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            assert column.type.isCollection();

            CollectionType<?> type = (CollectionType<?>) column.type;

            if (column.isComplex())
            {
                ComplexColumnData complexData = row.getComplexColumnData(column);
                if (complexData != null)
                {
                    AbstractType<?> elementType = type.kind == CollectionType.Kind.SET ? type.nameComparator() : type.valueComparator();
                    for (Cell<?> cell : complexData)
                    {
                        ByteBuffer elementValue = type.kind == CollectionType.Kind.SET ? cell.path().get(0) : cell.buffer();
                        if (analyzer != null)
                        {
                            List<ByteBuffer> elementTokens = analyzer.indexedTokens(elementValue);
                            List<ByteBuffer> queriedTokens = analyzer.queriedTokens();
                            if (Operator.ANALYZER_MATCHES.isSatisfiedByAnalyzed(elementType, elementTokens, queriedTokens))
                                return true;
                        }
                        else
                        {
                            if (Operator.EQ.isSatisfiedBy(elementType, elementValue, value))
                                return true;
                        }
                    }
                }
                return false;
            }
            else
            {
                assert analyzer == null : "Analyzers are not supported on frozen collections";
                ByteBuffer foundValue = getValue(metadata, partitionKey, row, nowInSec);
                return foundValue != null && Operator.CONTAINS.isSatisfiedBy(type, foundValue, value);
            }
        }

        private boolean containsKey(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            assert column.type.isCollection() && column.type instanceof MapType;
            MapType<?, ?> mapType = (MapType<?, ?>) column.type;
            if (column.isComplex())
            {
                if (analyzer != null)
                {
                    for (Cell<?> cell : row.getComplexColumnData(column))
                    {
                        AbstractType<?> elementType = mapType.nameComparator();
                        ByteBuffer elementValue = cell.path().get(0);
                        List<ByteBuffer> elementTokens = analyzer.indexedTokens(elementValue);
                        List<ByteBuffer> queriedTokens = analyzer.queriedTokens();
                        if (Operator.ANALYZER_MATCHES.isSatisfiedByAnalyzed(elementType, elementTokens, queriedTokens))
                            return true;
                    }
                    return false;
                }
                return row.getCell(column, CellPath.create(value)) != null;
            }
            else
            {
                assert analyzer == null : "Analyzers are not supported on frozen collections";
                ByteBuffer foundValue = getValue(metadata, partitionKey, row, nowInSec);
                return foundValue != null && Operator.CONTAINS_KEY.isSatisfiedBy(mapType, foundValue, value);
            }
        }

        @Override
        public String toCQLString(Redaction redaction)
        {
            AbstractType<?> type = column.type;
            switch (operator)
            {
                case CONTAINS:
                case NOT_CONTAINS:
                    assert type instanceof CollectionType;
                    CollectionType<?> ct = (CollectionType<?>)type;
                    type = ct.kind == CollectionType.Kind.SET ? ct.nameComparator() : ct.valueComparator();
                    break;
                case CONTAINS_KEY:
                case NOT_CONTAINS_KEY:
                    assert type instanceof MapType;
                    type = ((MapType<?, ?>)type).nameComparator();
                    break;
                case IN:
                case NOT_IN:
                    type = ListType.getInstance(type.freeze(), false);
                    break;
                case ORDER_BY_ASC:
                case ORDER_BY_DESC:
                    // These don't have a value, so we return here to prevent an error calling type.getString(value)
                    return String.format("ORDER BY %s %s", column.name.toCQLString(), operator);
                case ANN:
                    return String.format("ORDER BY %s ANN OF %s", column.name.toCQLString(), truncateValue(type.toCQLString(value, redaction)));
                case LIKE_PREFIX:
                    return likeToCQLString("'%s%%'", type, redaction, true);
                case LIKE_SUFFIX:
                    return likeToCQLString("'%%%s'", type, redaction, true);
                case LIKE_CONTAINS:
                    return likeToCQLString("'%%%s%%'", type, redaction, true);
                case LIKE_MATCHES:
                    // already a pattern, in the internal form of LikePatterns#ESCAPE
                    return likeToCQLString("'%s'", type, redaction, false);
                default:
                    break;
            }
            return String.format("%s %s %s", column.name.toCQLString(), operator, truncateValue(type.toCQLString(value, redaction)));
        }

        private String likeToCQLString(String pattern, AbstractType<?> type, Redaction redaction, boolean literal)
        {
            if (redaction == Redaction.REDACT)
                return String.format("%s LIKE ?", column.name.toCQLString());

            String term = type.getString(value);
            if (literal)
                term = LikePatterns.escape(term);
            String stringValue = String.format(pattern, term);
            String escape = term.indexOf(LikePatterns.ESCAPE) >= 0 ? " ESCAPE '" + LikePatterns.ESCAPE + '\'' : "";
            return String.format("%s LIKE %s%s", column.name.toCQLString(), truncateValue(stringValue), escape);
        }

        private static String truncateValue(String value)
        {
            return value.length() > 9 ? value.substring(0, 6) + "..." : value;
        }

        @Override
        protected Kind kind()
        {
            return Kind.SIMPLE;
        }
    }

    /**
     * An expression of the form 'column' ['key'] OPERATOR 'value' (which is only
     * supported when 'column' is a map) and where the operator can be {@link Operator#EQ}, {@link Operator#NEQ},
     * {@link Operator#LT}, {@link Operator#LTE}, {@link Operator#GT}, or {@link Operator#GTE}.
     */
    public static class MapComparisonExpression extends Expression
    {
        private final ByteBuffer key;
        private ByteBuffer indexValue = null;

        public MapComparisonExpression(ColumnMetadata column,
                                       ByteBuffer key,
                                       Operator operator,
                                       ByteBuffer value)
        {
            super(column, operator, value);
            assert column.type instanceof MapType && (operator == Operator.EQ || operator == Operator.NEQ || operator.isSlice());
            this.key = key;
        }

        @Override
        public void validate() throws InvalidRequestException
        {
            checkNotNull(key, "Unsupported null map key for column %s", column.name);
            checkBindValueSet(key, "Unsupported unset map key for column %s", column.name);
            checkNotNull(value, "Unsupported null map value for column %s", column.name);
            checkBindValueSet(value, "Unsupported unset map value for column %s", column.name);
        }

        @Override
        public ByteBuffer getIndexValue()
        {
            if (indexValue == null)
                indexValue = CompositeType.build(ByteBufferAccessor.instance, key, value);
            return indexValue;
        }

        /**
         * Returns whether the provided row satisfies this expression. For equality, it validates that the row contains
         * the exact key/value pair. For inequalities, it validates that the row contains the key, then that the value
         * satisfies the inequality.
         *
         * @param metadata the metadata of the queried table
         * @param partitionKey the partition key for row to check.
         * @param row the row to check. It should *not* contain deleted cells
         * @param nowInSec the current time in seconds (to know what is live and what isn't).
         * (i.e. it should come from a RowIterator).
         * @return whether the row is satisfied by this expression.
         */
        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            return isSatisfiedByEq(metadata, partitionKey, row, nowInSec) ^ (operator == Operator.NEQ);
        }

        private boolean isSatisfiedByEq(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            assert key != null;
            // We support null conditions for LWT (in ColumnCondition) but not for RowFilter.
            // TODO: we should try to merge both code someday.
            assert value != null;

            if (row.isStatic() != column.isStatic())
                return true;

            int comp;
            MapType<?, ?> mt = (MapType<?, ?>)column.type;
            if (column.isComplex())
            {
                Cell<?> cell = row.getCell(column, CellPath.create(key));
                if (cell == null)
                    return false;
                comp = mt.valueComparator().compare(cell.buffer(), value);
            }
            else
            {
                ByteBuffer serializedMap = getValue(metadata, partitionKey, row, nowInSec);
                if (serializedMap == null)
                    return false;

                ByteBuffer foundValue = mt.getSerializer().getSerializedValue(serializedMap, key, mt.getKeysType());
                if (foundValue == null)
                    return false;
                comp = mt.valueComparator().compare(foundValue, value);
            }
            switch (operator) {
                case EQ:
                case NEQ: // NEQ is inverted in calling method. We do this to simplify handling of null cells.
                    return comp == 0;
                case LT:
                    return comp < 0;
                case LTE:
                    return comp <= 0;
                case GT:
                    return comp > 0;
                case GTE:
                    return comp >= 0;
                default:
                    throw new AssertionError("Unsupported operator: " + operator);
            }
        }

        @Override
        public String toCQLString(Redaction redaction)
        {
            MapType<?, ?> mt = (MapType<?, ?>) column.type;
            return String.format("%s[%s] %s %s",
                                 column.name.toCQLString(),
                                 mt.nameComparator().toCQLString(key, redaction),
                                 operator,
                                 mt.valueComparator().toCQLString(value, redaction));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof MapComparisonExpression))
                return false;

            MapComparisonExpression that = (MapComparisonExpression)o;

            return Objects.equal(this.column.name, that.column.name)
                && this.operator == that.operator
                && Objects.equal(this.key, that.key)
                && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column.name, operator, key, value);
        }

        @Override
        protected Kind kind()
        {
            return Kind.MAP_COMPARISON;
        }

        /**
         * Get the lower bound for this expression. When the expression is EQ, GT, or GTE, the lower bound is the
         * expression itself. When the expression is LT or LTE, the lower bound is the map's key becuase
         * {@link ByteBuffer} comparisons will work correctly.
         * @return the lower bound for this expression.
         */
        public ByteBuffer getLowerBound()
        {
            switch (operator) {
                case EQ:
                case GT:
                case GTE:
                    return this.getIndexValue();
                case LT:
                case LTE:
                    return CompositeType.extractFirstComponentAsTrieSearchPrefix(getIndexValue(), true);
                default:
                    throw new AssertionError("Unsupported operator: " + operator);
            }
        }

        /**
         * Get the upper bound for this expression. When the expression is EQ, LT, or LTE, the upper bound is the
         * expression itself. When the expression is GT or GTE, the upper bound is the map's key with the last byte
         * set to 1 so that {@link ByteBuffer} comparisons will work correctly.
         * @return the upper bound for this express
         */
        public ByteBuffer getUpperBound()
        {
            switch (operator) {
                case GT:
                case GTE:
                    return CompositeType.extractFirstComponentAsTrieSearchPrefix(getIndexValue(), false);
                case EQ:
                case LT:
                case LTE:
                    return this.getIndexValue();
                default:
                    throw new AssertionError("Unsupported operator: " + operator);
            }
        }
    }

    public static class GeoDistanceExpression extends Expression
    {
        private final ByteBuffer distance;
        private final Operator distanceOperator;
        private final float searchRadiusMeters;
        private final float searchLat;
        private final float searchLon;
        // Whether this is a shifted expression, which is used to handle crossing the antimeridian
        private final boolean isShifted;

        public GeoDistanceExpression(ColumnMetadata column, ByteBuffer point, Operator operator, ByteBuffer distance)
        {
            this(column, point, operator, distance, false);
        }

        private GeoDistanceExpression(ColumnMetadata column, ByteBuffer point, Operator operator, ByteBuffer distance, boolean isShifted)
        {
            super(column, Operator.BOUNDED_ANN, point);
            assert column.type instanceof VectorType && (operator == Operator.LTE || operator == Operator.LT);
            this.isShifted = isShifted;
            this.distanceOperator = operator;
            this.distance = distance;
            searchRadiusMeters = FloatType.instance.compose(distance);
            float[] pointVector = TypeUtil.decomposeVector(column.type, point);
            // This is validated earlier in the parser because the column requires size 2, so only assert on it
            assert pointVector.length == 2 : "GEO_DISTANCE requires search vector to have 2 dimensions.";
            searchLat = pointVector[0];
            searchLon = pointVector[1];
        }

        public boolean crossesAntimeridian()
        {
            return GeoUtil.crossesAntimeridian(searchLat, searchLon, searchRadiusMeters);
        }

        /**
         * @return a new {@link GeoDistanceExpression} that is shifted by 360 degrees and can correctly search
         * on the opposite side of the antimeridian.
         */
        public GeoDistanceExpression buildShiftedExpression()
        {
            float shiftedLon = searchLon > 0 ? searchLon - 360 : searchLon + 360;
            var newPoint = VectorType.getInstance(FloatType.instance, 2)
                                     .decompose(List.of(searchLat, shiftedLon));
            return new GeoDistanceExpression(column, newPoint, distanceOperator, distance, true);
        }

        public Operator getDistanceOperator()
        {
            return distanceOperator;
        }

        public ByteBuffer getDistance()
        {
            return distance;
        }

        @Override
        public void validate() throws InvalidRequestException
        {
            checkBindValueSet(distance, "Unsupported unset distance for column %s", column.name);
            checkBindValueSet(value, "Unsupported unset vector value for column %s", column.name);

            if (searchRadiusMeters <= 0)
                throw new InvalidRequestException("GEO_DISTANCE radius must be positive, got " + searchRadiusMeters);

            if (searchLat < -90 || searchLat > 90)
                throw new InvalidRequestException("GEO_DISTANCE latitude must be between -90 and 90 degrees, got " + searchLat);
            if (!isShifted && (searchLon < -180 || searchLon > 180))
                throw new InvalidRequestException("GEO_DISTANCE longitude must be between -180 and 180 degrees, got " + searchLon);
        }

        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            ByteBuffer foundValue = getValue(metadata, partitionKey, row, nowInSec);
            if (foundValue == null)
                return false;
            float[] foundVector = TypeUtil.decomposeVector(column.type, foundValue);
            double haversineDistance = SloppyMath.haversinMeters(foundVector[0], foundVector[1], searchLat, searchLon);
            switch (distanceOperator)
            {
                case LTE:
                    return haversineDistance <= searchRadiusMeters;
                case LT:
                    return haversineDistance < searchRadiusMeters;
                default:
                    throw new AssertionError("Unsupported operator: " + operator);
            }
        }

        @Override
        public String toCQLString(Redaction redaction)
        {
            return String.format("GEO_DISTANCE(%s, %s) %s %s",
                                 column.name.toCQLString(),
                                 column.type.toCQLString(value, redaction),
                                 distanceOperator,
                                 FloatType.instance.toCQLString(distance, redaction));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof GeoDistanceExpression))
                return false;

            GeoDistanceExpression that = (GeoDistanceExpression)o;

            return Objects.equal(this.column.name, that.column.name)
                   && this.distanceOperator == that.distanceOperator
                   && Objects.equal(this.distance, that.distance)
                   && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column.name, distanceOperator, value, distance);
        }

        @Override
        protected Kind kind()
        {
            return Kind.VECTOR_RADIUS;
        }
    }

    /**
     * A custom index expression for use with 2i implementations which support custom syntax and which are not
     * necessarily linked to a single column in the base table.
     */
    public static class CustomExpression extends Expression
    {
        private final IndexMetadata targetIndex;
        private final TableMetadata table;

        public CustomExpression(TableMetadata table, IndexMetadata targetIndex, ByteBuffer value)
        {
            // The operator is not relevant, but Expression requires it so for now we just hardcode EQ
            super(makeDefinition(table, targetIndex), Operator.EQ, value);
            this.targetIndex = targetIndex;
            this.table = table;
        }

        public static CustomExpression build(TableMetadata metadata, IndexMetadata targetIndex, ByteBuffer value)
        {
            // delegate the expression creation to the target custom index
            return Keyspace.openAndGetStore(metadata).indexManager.getIndex(targetIndex).customExpressionFor(metadata, value);
        }

        private static ColumnMetadata makeDefinition(TableMetadata table, IndexMetadata index)
        {
            // Similarly to how we handle non-defined columns in thift, we create a fake column definition to
            // represent the target index. This is definitely something that can be improved though.
            return ColumnMetadata.regularColumn(table, ByteBuffer.wrap(index.name.getBytes()), BytesType.instance);
        }

        public IndexMetadata getTargetIndex()
        {
            return targetIndex;
        }

        public ByteBuffer getValue()
        {
            return value;
        }

        @Override
        public String toCQLString(Redaction redaction)
        {
            return String.format("expr(%s, %s)",
                                 ColumnIdentifier.maybeQuote(targetIndex.name),
                                 Keyspace.openAndGetStore(table)
                                         .indexManager
                                         .getIndex(targetIndex)
                                         .customExpressionValueType());
        }

        @Override
        protected Kind kind()
        {
            return Kind.CUSTOM;
        }

        // Filtering by custom expressions isn't supported yet, so just accept any row
        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            return true;
        }
    }

    /**
     * A user defined filtering expression. These may be added to RowFilter programmatically by a
     * QueryHandler implementation. No concrete implementations are provided and adding custom impls
     * to the classpath is a task for operators (needless to say, this is something of a power
     * user feature). Care must also be taken to register implementations, via the static register
     * method during system startup. An implementation and its corresponding Deserializer must be
     * registered before sending or receiving any messages containing expressions of that type.
     * Use of custom filtering expressions in a mixed version cluster should be handled with caution
     * as the order in which types are registered is significant: if continuity of use during upgrades
     * is important, new types should registered last and obsoleted types should still be registered (
     * or dummy implementations registered in their place) to preserve consistent identifiers across
     * the cluster).
     * </p>
     * During serialization, the identifier for the Deserializer implementation is prepended to the
     * implementation specific payload. To deserialize, the identifier is read first to obtain the
     * Deserializer, which then provides the concrete expression instance.
     */
    public static abstract class UserExpression extends Expression
    {
        private static final DeserializerRegistry deserializers = new DeserializerRegistry();

        @Override
        public String toCQLString(Redaction redaction)
        {
            return toCQLString(redaction == Redaction.REDACT);
        }
        private static final class DeserializerRegistry
        {
            private final AtomicInteger counter = new AtomicInteger(0);
            private final ConcurrentMap<Integer, Deserializer> deserializers = new ConcurrentHashMap<>();
            private final ConcurrentMap<Class<? extends UserExpression>, Integer> registeredClasses = new ConcurrentHashMap<>();

            public void registerUserExpressionClass(Class<? extends UserExpression> expressionClass,
                                                    UserExpression.Deserializer deserializer)
            {
                int id = registeredClasses.computeIfAbsent(expressionClass, (cls) -> counter.getAndIncrement());
                deserializers.put(id, deserializer);

                logger.debug("Registered user defined expression type {} and serializer {} with identifier {}",
                             expressionClass.getName(), deserializer.getClass().getName(), id);
            }

            public Integer getId(UserExpression expression)
            {
                return registeredClasses.get(expression.getClass());
            }

            public Deserializer getDeserializer(int id)
            {
                return deserializers.get(id);
            }
        }

        public static abstract class Deserializer
        {
            protected abstract UserExpression deserialize(DataInputPlus in,
                                                          int version,
                                                          TableMetadata metadata) throws IOException;
        }

        public static void register(Class<? extends UserExpression> expressionClass, Deserializer deserializer)
        {
            deserializers.registerUserExpressionClass(expressionClass, deserializer);
        }

        private static UserExpression deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            int id = in.readInt();
            Deserializer deserializer = deserializers.getDeserializer(id);
            assert deserializer != null : "No user defined expression type registered with id " + id;
            return deserializer.deserialize(in, version, metadata);
        }

        private static void serialize(UserExpression expression, DataOutputPlus out, int version) throws IOException
        {
            Integer id = deserializers.getId(expression);
            assert id != null : "User defined expression type " + expression.getClass().getName() + " is not registered";
            out.writeInt(id);
            expression.serialize(out, version);
        }

        private static long serializedSize(UserExpression expression, int version)
        {   // 4 bytes for the expression type id
            return 4 + expression.serializedSize(version);
        }

        protected UserExpression(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
        }

        @Override
        protected Kind kind()
        {
            return Kind.USER;
        }

        protected abstract void serialize(DataOutputPlus out, int version) throws IOException;
        protected abstract long serializedSize(int version);
    }

    public static class Serializer
    {
        public void serialize(RowFilter filter, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(false); // Old "is for thrift" boolean
            IndexHints.serializer.serialize(filter.indexHints, out, version); // hints first because the expressions might need them
            FilterElement.serializer.serialize(filter.root, out, version);
            OptimizerOptions.serializer.serialize(filter.optimizerOptions, out, version);
        }

        public RowFilter deserialize(DataInputPlus in, int version, TableMetadata metadata, boolean needsReconciliation) throws IOException
        {
            in.readBoolean(); // Unused
            IndexHints indexHints = IndexHints.serializer.deserialize(in, version, metadata);
            FilterElement operation = FilterElement.serializer.deserialize(in, version, metadata, indexHints);
            OptimizerOptions optimizerOptions = OptimizerOptions.serializer.deserialize(in, version);
            return new RowFilter(operation, needsReconciliation, indexHints, optimizerOptions);
        }

        public long serializedSize(RowFilter filter, int version)
        {
            return 1 // unused boolean
                   + IndexHints.serializer.serializedSize(filter.indexHints, version)
                   + FilterElement.serializer.serializedSize(filter.root, version)
                   + OptimizerOptions.serializer.serializedSize(filter.optimizerOptions, version);
        }
    }
}
