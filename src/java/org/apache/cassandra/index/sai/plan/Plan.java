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

import java.util.*;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Redaction;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.iterators.KeyRangeIntersectionIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TreeFormatter;
import org.apache.cassandra.io.util.FileUtils;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static org.apache.cassandra.index.sai.plan.Plan.CostCoefficients.*;

/**
 * The common base class for query execution plan nodes.
 * The top-level node is considered to be the execution plan of the query.
 *
 * <h1>Structure</h1>
 * A query plan is an immutable tree constisting of nodes representing physical data operations,
 * e.g. index scans, intersections, unions, filtering, limiting, sorting, etc.
 * Nodes of type {@link KeysIteration} operate on streams of keys, and nodes of type {@link RowsIteration} operate
 * on streams of rows. You should build a plan bottom-up by using static methods in {@link Plan.Factory}.
 * Nodes don't have pointers to parent nodes on purpose – this way multiple plans can share subtrees.
 *
 * <h1>Cost estimation</h1>
 * A plan can estimate its execution cost and result set size which is useful to select the best plan among
 * the semantically equivalent candidate plans. Operations represented by nodes may be pipelined, so their actual
 * runtime cost may depend on how many rows are read from the top level node. Upon construction, each plan node
 * gets an {@link Access} object which describes the way how the node results are going to be used by the parent nodes:
 * how many rows will be requested or what skip operations are going to be performed on the iterator.
 * The access objects get propagated down the tree to the leaves. This way we get an accurate cost of execution
 * at the leave nodes, taking into account any top-level limit or intersections.
 * <p>
 * Some nodes cannot be pipelined, e.g. nodes that represent sorting. To make cost estimation for such nodes possible,
 * each node maintains an initial cost (initCost) of the operation - that is the cost of preparation before the first
 * result row or key can be returned. Sorting nodes can have that cost very high.
 *
 * <h1>Optimization</h1>
 * This class also offers a few methods for modifying the plans (e.g. removing nodes) and a method allowing
 * to automatically improve the plan – see {@link #optimize()}. Whenever we talk about "modification" or "updates"
 * we always mean constructing a new plan. All updates are non-destructive. Each node has a unique numeric
 * identifier in the tree. Because a modification requires creating some new nodes, identifiers allow to find
 * corresponding nodes in the modified plan, even if they addresses changed (they are different java objects).
 *
 * <h1>Execution</h1>
 * The plan tree may store additional context information to be executable, i.e. to produce the iterator over the result
 * keys or rows - see {@link KeysIteration#execute}. However, the purpose of the plan nodes is not to perform
 * the actual computation of the result set. Instead, it should delegate the control to other modules responsible
 * for data retrieval. The plan only sets up the execution, but must not contain the execution logic.
 * For the sake of good testability, plan trees must be creatable, estimatable and optimizable also without
 * creating any of the objects used by the execution engine.
 *
 * <h1>Example</h1>
 * The CQL query
 * <pre>
 * SELECT * FROM table WHERE  a < 0.01 AND b < 0.2 LIMIT 10
 * </pre>
 *
 * can be represented by the following query execution plan:
 * <pre>
 * Limit 10 (rows: 10.0, cost/row: 265.2, cost: 80.0..2732.2)
 *  └─ Filter a < 0.2 AND b < 0.01 (sel: 1.000000000) (rows: 10.0, cost/row: 265.2, cost: 80.0..2732.2)
 *      └─ Fetch (rows: 10.0, cost/row: 265.2, cost: 80.0..2732.2)
 *          └─ Intersection (keys: 10.0, cost/key: 58.2, cost: 80.0..662.1)
 *              ├─ NumericIndexScan of vector_b_idx using Expression{ ... } (sel: 0.010010000, step: 1.0) (keys: 50.2, cost/key: 1.0, cost: 40.0..90.2)
 *              └─ NumericIndexScan of vector_a_idx using Expression{ ... } (sel: 0.199230000, step: 19.9) (keys: 50.2, cost/key: 10.6, cost: 40.0..571.9)
 * </pre>
 */
@NotThreadSafe
abstract public class Plan
{
    private static final Logger logger = LoggerFactory.getLogger(Plan.class);

    @VisibleForTesting
    static DoubleSupplier hitRateSupplier = () -> {
        // cache hit rate with reasonable defaults if we have no data
        double hitRate = ChunkCache.instance == null ? 1.0 : ChunkCache.instance.metrics.hitRate();
        return Double.isFinite(hitRate) ? hitRate : 1.0;
    };

    /**
     * Identifier of the plan tree node.
     * Used to identify the nodes of the plan.
     * Preserved during plan transformations.
     * <p>
     * Identifiers are more useful than object's identity (address) because plans can be transformed functionally
     * and as the result of that process we may get new node objects.
     * Identifiers allow us to match nodes in the transformed plan to the original.
     */
    final int id;

    /**
     * Reference to the factory gives access to common data shared among all nodes,
     * e.g. total number of keys in the table and the cost parameters.
     * It also allows to modify plan trees, e.g. create new nodes or recreate this node with different parameters.
     */
    final Factory factory;

    /**
     * Describes how this node is going to be used.
     * Very likely affects the cost.
     */
    final Access access;

    /**
     * Lazily caches the estimated fraction of the table data that the result of this plan is expected to match.
     */
    private double selectivity = -1;


    private Plan(Factory factory, int id, Access access)
    {
        this.id = id;
        this.factory = factory;
        this.access = access;
    }

    /**
     * Returns the order of the keys / rows returned by this plan.
     */
    protected abstract @Nullable Orderer ordering();

    /** selectivity comparisons to 0 will probably cause bugs, use this instead */
    protected static boolean isEffectivelyZero(double a) {
        assert a >= 0;
        return a < 1e-9;
    }

    /** dividing by extremely tiny numbers can cause overflow so clamp the minimum to 1e-9 */
    protected static double boundedSelectivity(double selectivity) {
        assert 0 <= selectivity && selectivity <= 1.0;
        return Math.max(1e-9, selectivity);
    }

    /**
     * Returns a new list containing subplans of this node.
     * The list can be later freely modified by the caller and does not affect the original plan.
     * <p>
     * Performance warning: This allocates a fresh list on the heap.
     * If you only want to iterate the subplan nodes, it is recommended to use {@link #forEachSubplan(Function)}
     * or {@link #withUpdatedSubplans(Function)} which offer better performance and less GC pressure.
     */
    @VisibleForTesting
    public final List<Plan> subplans()
    {
        List<Plan> result = new ArrayList<>();
        forEachSubplan(subplan -> {
            result.add(subplan);
            return ControlFlow.Continue;
        });
        return result;
    }

    /**
     * Returns a new list of nodes of given type.
     * The tree is traversed in depth-first order.
     * This node is included in the search.
     * The list can be later freely modified by the caller and does not affect the original plan.
     * <p>
     * Performance warning: This allocates a fresh list on the heap.
     * If you only want to iterate the subplan nodes, it is recommended to use {@link #forEachSubplan(Function)}
     * which should offer better performance and less GC pressure.
     */
    @SuppressWarnings("unchecked")
    final <T extends Plan> List<T> nodesOfType(Class<T> nodeType)
    {
        List<T> result = new ArrayList<>();
        forEach(node -> {
            if (nodeType.isAssignableFrom(node.getClass()))
                result.add((T) node);
            return ControlFlow.Continue;
        });
        return result;
    }

    /**
     * Returns the first node of the given type.
     * Searches the tree in depth-first order.
     * This node is included in the search.
     * If node of given type is not found, returns null.
     */
    @SuppressWarnings("unchecked")
    public final <T extends Plan> @Nullable T firstNodeOfType(Class<T> nodeType)
    {
        Plan[] result = new Plan[] { null };
        forEach(node -> {
            if (nodeType.isAssignableFrom(node.getClass()))
            {
                result[0] = node;
                return ControlFlow.Break;
            }
            return ControlFlow.Continue;
        });
        return (T) result[0];
    }

    /**
     * Calls a function recursively for each node of given type in the tree.
     * If the function returns {@link ControlFlow#Break} then the traversal is aborted.
     * @return {@link ControlFlow#Continue} if traversal hasn't been aborted, {@link ControlFlow#Break} otherwise.
     */
    final ControlFlow forEach(Function<Plan, ControlFlow> function)
    {
        return (function.apply(this) == ControlFlow.Continue)
               ? forEachSubplan(subplan -> subplan.forEach(function))
               : ControlFlow.Break;
    }

    /**
     * Calls a function for each child node of this plan.
     * The function should return {@link ControlFlow#Continue} to indicate the iteration should be continued
     * and {@link ControlFlow#Break} to abort it.
     *
     * @return the value returned by the last invocation of the function
     */
    abstract ControlFlow forEachSubplan(Function<Plan, ControlFlow> function);


    /** Controls tree traversals, see {@link #forEach(Function)} and {@link #forEachSubplan(Function)}  */
    enum ControlFlow { Continue, Break }

    /**
     * Runs the updater function on each subplan and if the updater returns a new subplan, then reconstructs this
     * plan from the modified subplans.
     * <p>
     * Accepting a list of sub-plans would be a valid alternative design of this API,
     * but that would require constructing a list on the heap by the caller for each updated node,
     * and that would be potentially wasteful as most of the node types have at most one subplan and don't use
     * lists internally.
     *
     * @param updater a function to be called on each subplan; if no update is needed, should return the argument
     * @return a new plan if any of the subplans has been replaced, this otherwise
     */
    protected abstract Plan withUpdatedSubplans(Function<Plan, Plan> updater);

    /**
     * Returns an object describing detailed cost information about running this plan.
     * The actual type of the Cost depends in practice on the type of the result set returned by the node.
     * The results of this method are supposed to be cached. The method is idempotent.
     * The cost usually depends on the Access value.
     */
    protected abstract Cost cost();

    /**
     * Estimates the probability of a random key or row of the table to be included in the result set
     * if the result was iterated fully with no skipping and if it did not have any limits.
     * This property is independent of the way how result set is used.
     */
    protected abstract double estimateSelectivity();

    /**
     * Formats the whole plan as a pretty tree.
     *
     * @param redaction whether to redact the queried column values.
     */
    public String toStringRecursive(Redaction redaction)
    {
        return toStringRecursive(redaction, null);
    }

    /**
     * Formats the whole plan as a pretty tree, with indentation
     *
     * @param redaction whether to redact the queried column values.
     * @param indent a string used for indentation
     */
    public String toStringRecursive(Redaction redaction, String indent)
    {
        TreeFormatter<Plan> formatter = new TreeFormatter<>(plan -> plan.toString(redaction), Plan::subplans, indent);
        return formatter.format(this);
    }

    /**
     * Returns the string representation of this node only, without redacting the queried column values.
     * @see #toString(Redaction)
     */
    @Override
    public final String toString()
    {
        return toString(Redaction.NONE);
    }

    /**
     * Returns the string representation of this node only
     *
     * @param redaction whether to redact the queried column values.
     */
    public final String toString(Redaction redaction)
    {
        String title = title(redaction);
        String description = description(redaction);
        return (title.isEmpty())
               ? String.format("%s (%s)\n%s", getClass().getSimpleName(), cost(), description).stripTrailing()
               : String.format("%s %s (%s)\n%s", getClass().getSimpleName(), title, cost(), description).stripTrailing();
    }

    /**
     * Returns additional information specific to the node displayed in the first line.
     * The information is included in the output of {@link #toString()} and {@link #toString(Redaction)}.
     * It is up to subclasses to implement it.
     */
    protected String title(Redaction redaction)
    {
        return "";
    }

    /**
     * Returns additional information specific to the node, displayed below the title.
     * The information is included in the output of {@link #toString()} and {@link #toString(Redaction)}.
     * It is up to subclasses to implement it.
     */
    protected String description(Redaction redaction)
    {
        return "";
    }

    /**
     * Traverses the tree recursively and calls the consumer for each index used in the plan.
     */
    public final void visitIndexesRecursive(Consumer<IndexContext> consumer)
    {
        forEach(node -> {
            node.visitIndexes(consumer);
            return Plan.ControlFlow.Continue;
        });
    }

    /**
     * Non-recursive auxiliary method for {@link #visitIndexesRecursive(Consumer)}
     * that calls the consumer for the index(es) in the current node.
     */
    protected void visitIndexes(Consumer<IndexContext> consumer)
    {
        // By default, a node does not contain an index.
    }

    /**
     * Returns an optimized plan.
     * <p>
     * The current optimization algorithm repeatedly cuts down one leaf of the plan tree
     * and recomputes the nodes above it. Then it returns the best plan from candidates obtained that way.
     * The expected running time is proportional to the height of the plan tree multiplied by the number of the leaves.
     */
    protected Plan optimize()
    {
        if (logger.isTraceEnabled())
            logger.trace("Optimizing plan:\n{}", toStringRecursive(Redaction.REDACT));

        Plan bestPlanSoFar = this;
        List<Leaf> leaves = nodesOfType(Leaf.class);

        // If there is a KeysSort on an index included by the hints, we should do sort-then-filter.
        // Replace the KeysSort (and its entire filter source) with a bare scored index scan, keeping
        // all nodes above it (Fetch, Filter, Limit) so the WHERE predicates are still applied as a
        // post-filter over the results that come back in sorted order.
        // Note that KeysSort represents a CQL ORDER BY clause, so there can only be one per query plan.
        for (KeysSort ks : nodesOfType(KeysSort.class))
        {
            if (factory.hints.includes(ks.ordering.getIndexName()))
            {
                return bestPlanSoFar.removeRestriction(ks.id);
            }
        }

        // Remove leaves one by one, starting from the ones with the worst selectivity
        leaves.sort(Comparator.comparingDouble(Plan::selectivity).reversed());
        for (Leaf leaf : leaves)
        {
            // We won't try to skip leaves with a preferred index
            if (leaf.usesIncludedIndex())
                continue;

            Plan candidate = bestPlanSoFar.removeRestriction(leaf.id);
            if (logger.isTraceEnabled())
                logger.trace("Candidate query plan:\n{}", candidate.toStringRecursive(Redaction.REDACT));

            if (candidate.fullCost() <= bestPlanSoFar.fullCost())
                bestPlanSoFar = candidate;
        }

        if (logger.isTraceEnabled())
            logger.trace("Optimized plan:\n{}", bestPlanSoFar.toStringRecursive(Redaction.REDACT));
        return bestPlanSoFar;
    }

    /**
     * Modifies all intersections to not intersect more clauses than the given limit.
     * Retains the most selective clauses.
     */
    protected Plan limitIntersectedClauses(int clauseLimit)
    {
        Plan result = this;
        if (result instanceof Intersection)
        {
            Plan.Intersection intersection = (Plan.Intersection) result;
            result = intersection.stripSubplans(clauseLimit);
        }
        return result.withUpdatedSubplans(p -> p.limitIntersectedClauses(clauseLimit));
    }

    /** Returns true if the plan contains a node matching the condition */
    public final boolean contains(Function<Plan, Boolean> condition)
    {
        ControlFlow res = forEach(node -> (condition.apply(node)) ? ControlFlow.Break : ControlFlow.Continue);
        return res == ControlFlow.Break;
    }

    /**
     * Returns true if the plan represents a hybrid query that first selects the matching
     * rows by index-based search and then orders the matching rows in memory. This order of execution
     * is best when the query contains a predicate that matches only a very small number of rows.
     * Returns false in other cases, including when the query is not a hybrid query.
     */
    public final boolean isSearchThenOrderHybrid()
    {
        return contains(node -> node instanceof KeysSort);
    }

    /**
     * Returns true if the plan represents a hybrid query that first scans the index in
     * the index term-order (sorted by the index terms) and then filters the matching rows in memory.
     * This order of execution is best when the query contains a predicate with a poor selectivity.
     * Returns false in other cases, including when the query is not a hybrid query.
     */
    public final boolean isOrderedScanThenFilterHybrid()
    {
        return (contains(node -> node instanceof Filter)
                && contains(node -> node instanceof IndexScan && ((IndexScan) node).ordering != null
                                      || node instanceof ScoredIndexScan));
    }

    /**
     * Returns a new plan with the given node filtering restriction removed.
     * Searches for the subplan to remove recursively down the tree.
     * If the new plan is different, its estimates are also recomputed.
     * If *this* plan matches the id, then the {@link Everything} node is returned.
     *
     * <p>
     * The purpose of this method is to optimise the plan.
     * Sometimes not doing an intersection and post-filtering instead can be faster, so by removing child nodes from
     * intersections we can potentially get a better plan.
     */
    final Plan removeRestriction(int id)
    {
        if (this.id != id)
            return withUpdatedSubplans(subplan -> subplan.removeRestriction(id));

        // If id is the same, replace this node with "everything"
        // because a query with no filter expression returns all rows
        // (removing restrictions should widen the result set).
        // Beware we must not remove ordering because that would change the semantics of the query.
        Orderer ordering = this.ordering();
        return (ordering != null)
               ? factory.sort(factory.everything, ordering)
               : factory.everything;
    }

    /**
     * Replaces the node with the given {@code id} anywhere in the plan tree with {@code replacement}.
     * If no node with that id is found the plan is returned unchanged.
     */
    final Plan replaceNode(int id, Plan replacement)
    {
        if (this.id == id)
            return replacement;
        return withUpdatedSubplans(subplan -> subplan.replaceNode(id, replacement));
    }

    /**
     * Returns the estimated cost of preparation steps
     * that must be done before returning the first row / key
     */
    public final double initCost()
    {
        return cost().initCost();
    }

    public final double iterCost()
    {
        return cost().iterCost();
    }

    /**
     * Returns the estimated cost of running the plan to completion, i.e. exhausting
     * the key or row iterator returned by it
     */
    public final double fullCost()
    {
        return cost().fullCost();
    }

    /**
     * Returns the estimated fraction of the table data that the result of this plan is expected to match
     */
    public final double selectivity()
    {
        if (selectivity == -1)
            selectivity = estimateSelectivity();
        assert 0.0 <= selectivity && selectivity <= 1.0 : "Invalid selectivity: " + selectivity;
        return selectivity;
    }

    /**
     * Returns the number of indexes referenced by this plan.
     * The same index referenced from unrelated query clauses,
     * leading to separate index searches, are counted separately.
     */
    public final int referencedIndexCount()
    {
        MutableInt count = new MutableInt(0);
        visitIndexesRecursive(index -> count.increment());
        return count.intValue();
    }

    /**
     * Returns the estimated number of rows to be fetched from storage.
     */
    public final double estimatedRowsToFetch()
    {
        Fetch fetch = firstNodeOfType(Plan.Fetch.class);
        return fetch != null ? fetch.expectedRows() : 0.0;
    }

    /**
     * Returns the estimated number of primary keys to be iterated by all index iterators.
     * This may be larger than the number of rows to fetch because of intersections.
     */
    public final double estimatedKeysToIterate()
    {
        MutableDouble total = new MutableDouble(0.0);
        forEach(node -> {
            if (node instanceof Leaf)
            {
                total.add(((Leaf) node).expectedKeys());
            }
            return ControlFlow.Continue;
        });
        return total.doubleValue();
    }

    protected interface Cost
    {
        /**
         * Initialization cost: cannot be reduced later.
         */
        double initCost();

        /**
         * Cost to iterate over all the expected keys or rows.  May be reduced by LIMIT.
         */
        double iterCost();

        default double fullCost()
        {
            return initCost() + iterCost();
        }
    }

    protected static final class KeysIterationCost implements Cost
    {
        final double expectedKeys;
        final double initCost;
        final double iterCost;

        /**
         * @param expectedKeys number of keys expected to be iterated over
         * @param initCost cost to set up the iteration
         * @param iterCost *total* cost of iterating over the expected number of keys
         */
        public KeysIterationCost(double expectedKeys, double initCost, double iterCost)
        {
            this.expectedKeys = expectedKeys;
            this.initCost = initCost;
            this.iterCost = iterCost;
        }

        @Override
        public double initCost()
        {
            return initCost;
        }

        @Override
        public double iterCost()
        {
            return iterCost;
        }

        public double costPerKey()
        {
            return expectedKeys == 0 ? 0.0 : iterCost / expectedKeys;
        }

        public String toString()
        {
            return String.format("keys: %.1f, cost/key: %.1f, cost: %.1f..%.1f",
                                 expectedKeys, costPerKey(), initCost, fullCost());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KeysIterationCost that = (KeysIterationCost) o;
            return Double.compare(expectedKeys, that.expectedKeys) == 0
                   && Double.compare(initCost, that.initCost) == 0
                   && Double.compare(iterCost, that.iterCost) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expectedKeys, initCost, iterCost);
        }
    }

    protected static final class RowsIterationCost implements Cost
    {
        final double expectedRows;
        final double initCost;
        final double iterCost;

        public RowsIterationCost(double expectedRows, double initCost, double iterCost)
        {
            this.expectedRows = expectedRows;
            this.initCost = initCost;
            this.iterCost = iterCost;
        }

        @Override
        public double initCost()
        {
            return initCost;
        }

        @Override
        public double iterCost()
        {
            return iterCost;
        }

        public double costPerRow()
        {
            return expectedRows == 0 ? 0.0 : iterCost / expectedRows;
        }

        public String toString()
        {
            return String.format("rows: %.1f, cost/row: %.1f, cost: %.1f..%.1f",
                                 expectedRows, costPerRow(), initCost, fullCost());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RowsIterationCost that = (RowsIterationCost) o;
            return Double.compare(expectedRows, that.expectedRows) == 0
                   && Double.compare(initCost, that.initCost) == 0
                   && Double.compare(iterCost, that.iterCost) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expectedRows, initCost, iterCost);
        }
    }

    /**
     * Common base class for all plan nodes that iterate over primary keys.
     */
    public abstract static class KeysIteration extends Plan
    {
        /**
         * Caches the estimated cost to avoid frequent recomputation
         */
        private KeysIterationCost cost;

        protected KeysIteration(Factory factory, int id, Access access)
        {
            super(factory, id, access);
        }

        @Override
        protected final KeysIterationCost cost()
        {
            if (cost == null)
                cost = estimateCost();
            return cost;
        }

        protected abstract KeysIterationCost estimateCost();

        /**
         * Executes the operation represented by this node.
         * The node itself isn't supposed for doing the actual work, but rather serves as a director which
         * delegates the work to the query controller through the passed Executor.
         *
         * @param executor does all the hard work like fetching keys from the indexes or ANN sort
         */
        protected abstract Iterator<? extends PrimaryKey> execute(Executor executor);

        protected abstract KeysIteration withAccess(Access patterns);

        final double expectedKeys()
        {
            return cost().expectedKeys;
        }

        final double costPerKey()
        {
            return cost().costPerKey();
        }

        @Override
        public final KeysIteration optimize()
        {
            return (KeysIteration) super.optimize();
        }

        @Override
        public final KeysIteration limitIntersectedClauses(int clauseLimit)
        {
            return (KeysIteration) super.limitIntersectedClauses(clauseLimit);
        }

        protected abstract boolean usesIncludedIndex();
    }

    /**
     * Leaves of the plan tree cannot have subplans.
     * This class exists purely for DRY purpose.
     */
    abstract static class Leaf extends KeysIteration
    {
        protected Leaf(Factory factory, int id, Access accesses)
        {
            super(factory, id, accesses);
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            return ControlFlow.Continue;
        }

        @Override
        protected final Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            // There are no subplans so it is a noop
            return this;
        }
    }

    /**
     * Represents an index scan that returns an empty range
     */
    static class Nothing extends Leaf
    {
        protected Nothing(int id, Factory factory)
        {
            super(factory, id, null);
        }

        @Nonnull
        @Override
        protected KeysIterationCost estimateCost()
        {
            return new KeysIterationCost(0, 0.0, 0.0);
        }

        @Override
        protected boolean usesIncludedIndex()
        {
            return false;
        }

        @Nullable
        @Override
        protected Orderer ordering()
        {
            return null;
        }

        @Override
        protected double estimateSelectivity()
        {
            return 0;
        }

        @Override
        protected KeyRangeIterator execute(Executor executor)
        {
            return KeyRangeIterator.empty();
        }

        @Override
        protected Nothing withAccess(Access patterns)
        {
            // limit does not matter for Nothing node because it always returns 0 keys
            return this;
        }
    }

    /**
     * Represents an index scan that returns all keys in the table.
     * This is a virtual node that has no real representation in the database system.
     * It is useful in query optimization.
     */
    static class Everything extends Leaf
    {
        protected Everything(int id, Factory factory, Access accesses)
        {
            super(factory, id, accesses);
        }

        @Nonnull
        @Override
        protected KeysIterationCost estimateCost()
        {
            // We set the cost to infinity so this node is never present in the optimized plan.
            // We don't want to have those nodes in the final plan,
            // because currently we have no way to execute it efficiently.
            // In the future we may want to change it, when we have a way to return all rows without using an index.
            return new KeysIterationCost(access.expectedAccessCount(factory.tableMetrics.rows),
                                         Double.POSITIVE_INFINITY,
                                         Double.POSITIVE_INFINITY);
        }

        @Override
        protected boolean usesIncludedIndex()
        {
            return false;
        }

        @Nullable
        @Override
        protected Orderer ordering()
        {
            return null;
        }

        @Override
        protected double estimateSelectivity()
        {
            return 1.0;
        }

        @Override
        protected KeyRangeIterator execute(Executor executor)
        {
            // Not supported because it doesn't make a lot of sense.
            // A direct scan of table data would be certainly faster.
            // Everything node is not supposed to be executed. However, it is useful for analyzing various plans,
            // e.g. we may get such node after removing some nodes from a valid, executable plan.
            throw new UnsupportedOperationException("Returning an iterator over all keys is not supported.");
        }

        @Override
        protected Everything withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                   ? this
                   : new Everything(id, factory, access);
        }
    }

    /**
     * Marker interface for all the plan nodes that scan an index, either {@link IndexScan} or {@link ScoredIndexScan}.
     */
    public interface AnyIndexScan
    {
    }

    abstract static class IndexScan extends Leaf implements AnyIndexScan
    {
        @Nullable
        protected final Expression predicate;
        @Nullable
        protected final Orderer ordering;

        protected final long matchingKeysCount;

        public IndexScan(Factory factory, int id, Expression predicate, long matchingKeysCount, Access access, Orderer ordering)
        {
            super(factory, id, access);
            Preconditions.checkArgument(predicate != null || ordering != null,
                                        "Either predicate or ordering must be set");
            Preconditions.checkArgument(predicate == null
                                        || ordering == null
                                        || predicate.getIndexName().equals(ordering.getIndexName()),
                                        "Ordering must use the same index as the predicate");
            this.predicate = predicate;
            // If we match by equality, ordering makes no sense because all term values would be the same.
            this.ordering = (predicate == null || predicate.getOp() != Expression.Op.EQ) ? ordering : null;
            this.matchingKeysCount = matchingKeysCount;
        }

        @Override
        protected final String title(Redaction redaction)
        {
            return String.format("of %s (sel: %.9f, step: %.1f)",
                                 getIndexName(), selectivity(), access.meanDistance());
        }

        @Override
        protected String description(Redaction redaction)
        {
            StringBuilder sb = new StringBuilder();
            if (predicate != null)
            {
                sb.append("predicate: ");
                sb.append(predicate.toString(redaction == Redaction.REDACT));
                sb.append('\n');
            }
            if (ordering != null)
            {
                sb.append("ordering: ");
                sb.append(ordering.toString(redaction));
                sb.append('\n');
            }
            return sb.toString();
        }

        @Nullable
        @Override
        protected final Orderer ordering()
        {
            return ordering;
        }

        @Override
        protected final KeysIterationCost estimateCost()
        {
            double expectedKeys = access.expectedAccessCount(matchingKeysCount);
            double costPerKey = access.unitCost(SAI_KEY_COST, this::estimateCostPerSkip);
            // we hit-rate-scale the open cost, but not the per-key cost, under the assumption that
            // readahead against the postings file will mostly amortize the penalty when hitting disk per key
            double initCost = hrs(SAI_OPEN_COST) * factory.tableMetrics.sstables;
            double iterCost = expectedKeys * costPerKey;
            return new KeysIterationCost(expectedKeys, initCost, iterCost);
        }

        @Override
        protected double estimateSelectivity()
        {
            return factory.tableMetrics.rows > 0
                   ? ((double) matchingKeysCount / factory.tableMetrics.rows)
                   : 0.0;
        }

        @Override
        protected boolean usesIncludedIndex()
        {
            return factory.hints.includes(getIndexName());
        }

        private double estimateCostPerSkip(double step)
        {
            // This is the first very rough approximation of the cost model for skipTo operation.
            // It is likely not a very accurate model.
            // We know for sure that the cost goes up the bigger the skip distance (= the more key we skip over)
            // and also the bigger the merged posting list is. A range scan of the numeric
            // index may require merging posting lists from many index nodes. Intuitively, the more keys we match,
            // the higher number of posting lists are merged. Also, the further we skip, the higher number
            // of posting lists must be advanced, and we're also more likely to hit a non-cached chunk.
            // From a few experiments I did, I conclude those costs grow sublinearly.
            // In the future we probably will need to take more index metrics into account
            // (e.g. number of distinct values).

            // there are no sstables when all the data is in memtables, avoid dividing by zero and getting NaN costs
            double keysPerSSTable = factory.tableMetrics.sstables == 0
                                    ? 0.0
                                    : (double) matchingKeysCount / factory.tableMetrics.sstables;

            double skipCostFactor;
            double postingsCountFactor;
            double postingsCountExponent;
            double skipDistanceFactor;
            double skipDistanceExponent;
            if (predicate == null
                || predicate.getOp() == Expression.Op.RANGE
                || predicate.getOp() == Expression.Op.PREFIX
                || predicate.getOp() == Expression.Op.AUTOMATON)
            {
                // prefix and pattern (automaton) predicates scan a range of terms and merge their posting lists,
                // like range predicates
                skipCostFactor = RANGE_SCAN_SKIP_COST;
                postingsCountFactor = RANGE_SCAN_SKIP_COST_POSTINGS_COUNT_FACTOR;
                postingsCountExponent = RANGE_SCAN_SKIP_COST_POSTINGS_COUNT_EXPONENT;
                skipDistanceFactor = RANGE_SCAN_SKIP_COST_DISTANCE_FACTOR;
                skipDistanceExponent = RANGE_SCAN_SKIP_COST_DISTANCE_EXPONENT;
            }
            else
            {
                skipCostFactor = POINT_LOOKUP_SKIP_COST;
                postingsCountFactor = 0.0;
                postingsCountExponent = 1.0;
                skipDistanceFactor = POINT_LOOKUP_SKIP_COST_DISTANCE_FACTOR;
                skipDistanceExponent = POINT_LOOKUP_SKIP_COST_DISTANCE_EXPONENT;
            }

            // divide by exponent so the derivative at 1.0 equals postingsCountFactor
            double dKeys = postingsCountFactor / postingsCountExponent;
            double postingsCountPenalty = dKeys * Math.pow(keysPerSSTable, postingsCountExponent);

            // divide by exponent so the derivative at 1.0 equals skipDistanceFactor
            double dPostings = skipDistanceFactor / skipDistanceExponent;
            double distancePenalty = dPostings * Math.pow(step, skipDistanceExponent);

            return skipCostFactor
                   * (1.0 + distancePenalty)
                   * (1.0 + postingsCountPenalty)
                   * factory.tableMetrics.sstables;
        }

        @Override
        protected Iterator<? extends PrimaryKey> execute(Executor executor)
        {
            return (ordering != null)
                ? executor.getTopKRows(predicate, max(1, round((float) access.expectedAccessCount(factory.tableMetrics.rows))))
                : executor.getKeysFromIndex(predicate);
        }

        public String getIndexName()
        {
            assert predicate != null || ordering != null;
            return predicate != null ? predicate.getIndexName() : ordering.getIndexName();
        }

        @Override
        final protected void visitIndexes(Consumer<IndexContext> consumer)
        {
            assert predicate != null || ordering != null;
            consumer.accept(predicate != null ? predicate.context : ordering.context);
        }
    }
    /**
     * Represents a scan over a numeric storage attached index.
     */
    @VisibleForTesting
    public static class NumericIndexScan extends IndexScan
    {
        public NumericIndexScan(Factory factory, int id, Expression predicate, long matchingKeysCount, Access access, Orderer ordering)
        {
            super(factory, id, predicate, matchingKeysCount, access, ordering);
        }

        @Override
        protected NumericIndexScan withAccess(Access access)
        {
            return Objects.equals(this.access, access)
                   ? this
                   : new NumericIndexScan(factory, id, predicate, matchingKeysCount, access, ordering);
        }
    }

    /**
     * Represents a scan over a literal storage attached index
     */
    @VisibleForTesting
    public static class LiteralIndexScan extends IndexScan
    {
        public LiteralIndexScan(Factory factory, int id, Expression predicate, long matchingKeysCount, Access access, Orderer ordering)
        {
            super(factory, id, predicate, matchingKeysCount, access, ordering);
        }

        @Override
        protected LiteralIndexScan withAccess(Access access)
        {
            return Objects.equals(this.access, access)
                   ? this
                   : new LiteralIndexScan(factory, id, predicate, matchingKeysCount, this.access, ordering);
        }
    }

    /**
     * Union of multiple primary key streams.
     * This is a fairly cheap operation - its cost is basically a sum of costs of the subplans.
     */
    static final class Union extends KeysIteration
    {
        private final LazyTransform<List<KeysIteration>> subplansSupplier;
        private final boolean disjoint;

        Union(Factory factory, int id, List<KeysIteration> subplans, boolean disjoint, Access access)
        {
            super(factory, id, access);
            Preconditions.checkArgument(!subplans.isEmpty(), "Subplans must not be empty");

            this.disjoint = disjoint;

            // We propagate Access lazily just before we need the subplans.
            // This is because there may be several requests to change the access pattern from the top,
            // and we don't want to reconstruct the whole subtree each time
            this.subplansSupplier = new LazyTransform<>(subplans, this::propagateAccess);
        }

        /**
         * Adjusts the counts for each subplan to account for the other subplans.
         * As explained in `estimateSelectivity`, the union of (for instance) two subplans
         * that each select 50% of the keys is 75%, not 100%.  Thus, we need to reduce the counts
         * to remove estimated overlapping keys.
         */
        private List<KeysIteration> propagateAccess(List<KeysIteration> subplans)
        {
            if (isEffectivelyZero(selectivity()))
            {
                // all subplan selectivity should also be ~0
                for (var subplan: subplans)
                    assert isEffectivelyZero(subplan.selectivity());
                return subplans;
            }

            ArrayList<KeysIteration> newSubplans = new ArrayList<>(subplans.size());
            for (KeysIteration subplan : subplans)
            {
                Access access = this.access.scaleCount(subplan.selectivity() / selectivity());
                newSubplans.add(subplan.withAccess(access));
            }
            return newSubplans;
        }

        @Override
        protected double estimateSelectivity()
        {
            if (disjoint)
            {
                // If we know the subplans are disjoint, we can just sum their selectivities.
                double selectivity = 0.0;
                for (KeysIteration plan : subplansSupplier.orig)
                    selectivity += plan.selectivity();
                return Math.min(1.0, selectivity);
            }
            else
            {
                // Assume independence (lack of correlation) of subplans.
                // We multiply the probabilities of *not* selecting a key.
                // Because selectivity is usage-independent, we can use the original subplans,
                // to avoid forcing pushdown of Access information down.
                double inverseSelectivity = 1.0;
                for (KeysIteration plan : subplansSupplier.orig)
                    inverseSelectivity *= (1.0 - plan.selectivity());
                return 1.0 - inverseSelectivity;
            }
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            for (Plan s : subplansSupplier.get())
            {
                if (function.apply(s) == ControlFlow.Break)
                    return ControlFlow.Break;
            }
            return ControlFlow.Continue;
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            List<KeysIteration> subplans = subplansSupplier.get();
            ArrayList<KeysIteration> newSubplans = new ArrayList<>(subplans.size());
            for (Plan subplan : subplans)
                newSubplans.add((KeysIteration) updater.apply(subplan));

            return newSubplans.equals(subplans)
                   ? this
                   : factory.union(newSubplans, disjoint, id).withAccess(access);
        }

        @Override
        protected Union withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                   ? this
                   : new Union(factory, id, subplansSupplier.orig, disjoint, access);
        }

        @Override
        protected KeysIterationCost estimateCost()
        {
            double initCost = 0.0;
            double iterCost = 0.0;
            List<KeysIteration> subplans = subplansSupplier.get();
            for (int i = 0; i < subplans.size(); i++)
            {
                KeysIteration subplan = subplans.get(i);
                // Initialization must be done all branches before we can start iterating
                initCost += subplan.initCost();
                iterCost += subplan.iterCost();
            }
            double expectedKeys = access.expectedAccessCount(factory.tableMetrics.rows * selectivity());
            return new KeysIterationCost(expectedKeys, initCost, iterCost);
        }

        @Nullable
        @Override
        protected Orderer ordering()
        {
            return subplansSupplier.get().get(0).ordering();
        }

        @Override
        protected KeyRangeIterator execute(Executor executor)
        {
            KeyRangeIterator.Builder builder = KeyRangeUnionIterator.builder();
            try
            {
                for (KeysIteration plan : subplansSupplier.get())
                    builder.add((KeyRangeIterator) plan.execute(executor));
                return builder.build();
            }
            catch (Throwable t)
            {
                FileUtils.closeQuietly(builder.ranges());
                throw t;
            }
        }

        @Override
        protected boolean usesIncludedIndex()
        {
            return subplansSupplier.get().stream().anyMatch(KeysIteration::usesIncludedIndex);
        }
    }

    /**
     * Intersection of multiple primary key streams.
     * This is quite complex operation where many keys from all underlying streams must be read in order
     * to return one matching key. Therefore, expect the cost of this operation to be significantly higher than
     * the costs of the subplans.
     */
    static final class Intersection extends KeysIteration
    {
        private final LazyTransform<List<KeysIteration>> subplansSupplier;

        private Intersection(Factory factory, int id, List<KeysIteration> subplans, Access access)
        {
            super(factory, id, access);
            Preconditions.checkArgument(!subplans.isEmpty(), "Subplans must not be empty");

            // We propagate Access lazily just before we need the subplans.
            // This is because there may be several requests to change the access pattern from the top,
            // and we don't want to reconstruct the whole subtree each time
            this.subplansSupplier = new LazyTransform<>(subplans, this::propagateAccess);
        }

        /**
         * In an intersection operation, the goal is to find the common elements between the results
         * of multiple subplans.  This requires taking into account not only the selectivity but also
         * the match probabilities between subplans.
         * <p>
         * VSTODO explain what's going on in more detail.
         */
        private ArrayList<KeysIteration> propagateAccess(List<KeysIteration> subplans)
        {
            double loops = subplans.get(0).selectivity() / boundedSelectivity(selectivity());
            ArrayList<KeysIteration> newSubplans = new ArrayList<>(subplans.size());
            KeysIteration s0 = subplans.get(0).withAccess(access.scaleDistance(loops).convolute(loops, 1.0));
            newSubplans.add(s0);

            // We may run out of keys while iterating the first iterator, and then we just break the loop early
            loops = Math.min(s0.expectedKeys(), loops);

            double matchProbability = 1.0;
            for (int i = 1; i < subplans.size(); i++)
            {
                KeysIteration subplan = subplans.get(i);
                double cumulativeSelectivity = subplans.get(0).selectivity() * matchProbability;
                double skipDistance = subplan.selectivity() / boundedSelectivity(cumulativeSelectivity);
                Access subAccess = access.scaleDistance(subplan.selectivity() / boundedSelectivity(selectivity()))
                                         .convolute(loops * matchProbability, skipDistance)
                                         .forceSkip();
                newSubplans.add(subplan.withAccess(subAccess));
                matchProbability *= subplan.selectivity();
            }
            return newSubplans;
        }

        @Override
        protected double estimateSelectivity()
        {
            double selectivity = 1.0;
            for (KeysIteration plan : subplansSupplier.orig)
                selectivity *= plan.selectivity();
            return selectivity;
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            for (Plan s : subplansSupplier.get())
            {
                if (function.apply(s) == ControlFlow.Break)
                    return ControlFlow.Break;
            }
            return ControlFlow.Continue;
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            List<KeysIteration> subplans = subplansSupplier.get();
            ArrayList<KeysIteration> newSubplans = new ArrayList<>(subplans.size());
            for (Plan subplan : subplans)
                newSubplans.add((KeysIteration) updater.apply(subplan));

            return newSubplans.equals(subplans)
                   ? this
                   : factory.intersection(newSubplans, id).withAccess(access);
        }

        @Override
        protected Intersection withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                   ? this
                   : new Intersection(factory, id, subplansSupplier.orig, access);
        }

        @Nullable
        @Override
        protected Orderer ordering()
        {
            return subplansSupplier.get().get(0).ordering();
        }

        @Override
        protected KeysIterationCost estimateCost()
        {
            List<KeysIteration> subplans = subplansSupplier.get();
            assert !subplans.isEmpty() : "Expected at least one subplan here. An intersection of 0 plans should have been optimized out.";

            double initCost = 0.0;
            double iterCost = 0.0;
            for (KeysIteration subplan : subplans)
            {
                initCost += subplan.initCost();
                iterCost += subplan.iterCost();
            }
            double expectedKeyCount = access.expectedAccessCount(factory.tableMetrics.rows * selectivity());
            return new KeysIterationCost(expectedKeyCount, initCost, iterCost);
        }

        @Override
        protected KeyRangeIterator execute(Executor executor)
        {
            KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder();
            try
            {
                for (KeysIteration plan : subplansSupplier.get())
                    builder.add((KeyRangeIterator) plan.execute(executor));

                return builder.build();
            }
            catch (Throwable t)
            {
                FileUtils.closeQuietly(builder.ranges());
                throw t;
            }
        }

        @Override
        protected boolean usesIncludedIndex()
        {
            return subplansSupplier.get().stream().anyMatch(KeysIteration::usesIncludedIndex);
        }

        /**
         * Limits the number of intersected subplans
         */
        public Plan stripSubplans(int clauseLimit)
        {
            if (subplansSupplier.orig.size() <= clauseLimit)
                return this;

            if (factory.hints.included.isEmpty())
                return withNewSubplans(new ArrayList<>(subplansSupplier.orig.subList(0, clauseLimit)));

            List<Plan.KeysIteration> newSubplans = new ArrayList<>(clauseLimit);
            List<Plan.KeysIteration> optionalNewSubplans = new ArrayList<>(clauseLimit);
            for (KeysIteration keysIteration : subplansSupplier.orig)
            {
                if (keysIteration.usesIncludedIndex())
                    newSubplans.add(keysIteration);
                else
                    optionalNewSubplans.add(keysIteration);
            }
            for (KeysIteration keysIteration : optionalNewSubplans)
            {
                if (newSubplans.size() >= clauseLimit)
                    break;
                newSubplans.add(keysIteration);
            }

            return withNewSubplans(newSubplans);
        }

        private Plan withNewSubplans(List<Plan.KeysIteration> newSubplans)
        {
            return factory.intersection(newSubplans, id).withAccess(access);
        }
    }

    /**
     * Sorts keys in ANN order.
     * Must fetch all keys from the source before sorting, so it has a high initial cost.
     */
    static final class KeysSort extends KeysIteration
    {
        private final KeysIteration source;
        final Orderer ordering;

        KeysSort(Factory factory, int id, KeysIteration source, Access access, Orderer ordering)
        {
            super(factory, id, access);
            this.source = source;
            this.ordering = ordering;
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            return function.apply(source);
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            return factory.sort((KeysIteration) updater.apply(source), ordering, id).withAccess(access);
        }

        @Override
        protected double estimateSelectivity()
        {
            return source.selectivity();
        }

        @Override
        protected KeysIterationCost estimateCost()
        {
            if (ordering.isANN())
                return estimateAnnSortCost();
            else if (ordering.isBM25())
                return estimateBm25SortCost();
            else
                return estimateGlobalSortCost();
        }

        private KeysIterationCost estimateAnnSortCost()
        {
            double expectedKeys = access.expectedAccessCount(source.expectedKeys());
            int expectedKeysInt = max(1, (int) Math.ceil(expectedKeys));
            int expectedSourceKeysInt = max(1, (int) Math.ceil(source.expectedKeys()));
            double initCost = annSortOpenCost(factory.keyspace) * factory.tableMetrics.sstables
                              + source.fullCost()
                              + source.expectedKeys() * CostCoefficients.annSortKeyCost(factory.keyspace);
            double searchCost = factory.costEstimator.estimateAnnSearchCost(ordering,
                                                                            expectedKeysInt,
                                                                            expectedSourceKeysInt);
            return new KeysIterationCost(expectedKeys, initCost, searchCost);
        }

        private KeysIterationCost estimateBm25SortCost()
        {
            double expectedKeys = access.expectedAccessCount(source.expectedKeys());

            int termCount = ordering.getQueryTerms().size();
            // all of the cost for BM25 is up front since the index doesn't give us the information we need
            // to return results in order, in isolation.  The big cost is reading the indexed cells out of
            // the sstables.
            // VSTODO if we had stats on cell size _per column_ we could usefully include ROW_BYTE_COST
            double initCost = source.fullCost()
                              + source.expectedKeys() * (hrs(ROW_CELL_COST) + ROW_CELL_COST)
                              + termCount * BM25_SCORE_COST;
            return new KeysIterationCost(expectedKeys, initCost, 0);
        }

        private KeysIterationCost estimateGlobalSortCost()
        {
            return new KeysIterationCost(source.expectedKeys(),
                                         source.fullCost() + source.expectedKeys() * hrs(ROW_COST),
                                         source.expectedKeys() * SAI_KEY_COST);

        }

        @Nullable
        @Override
        protected Orderer ordering()
        {
            return ordering;
        }

        @Override
        protected Iterator<? extends PrimaryKey> execute(Executor executor)
        {
            KeyRangeIterator sourceIterator = (KeyRangeIterator) source.execute(executor);
            int softLimit = max(1, round((float) access.expectedAccessCount(factory.tableMetrics.rows)));
            return executor.getTopKRows(sourceIterator, softLimit);
        }

        @Override
        protected KeysSort withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                   ? this
                   : new KeysSort(factory, id, source, access, ordering);
        }

        @Override
        protected boolean usesIncludedIndex()
        {
            return source.usesIncludedIndex();
        }

        @Override
        protected String description(Redaction redaction)
        {
            return ordering.toString(redaction);
        }
    }

    /**
     * Base class for index scans that return results in a computed order (ANN, BM25)
     * rather than the natural index order.
     */
    public abstract static class ScoredIndexScan extends Leaf implements AnyIndexScan
    {
        final Orderer ordering;

        protected ScoredIndexScan(Factory factory, int id, Access access, Orderer ordering)
        {
            super(factory, id, access);
            this.ordering = ordering;
        }

        @Nullable
        @Override
        protected Orderer ordering()
        {
            return ordering;
        }

        @Override
        protected double estimateSelectivity()
        {
            return 1.0;
        }

        @Override
        protected boolean usesIncludedIndex()
        {
            return factory.hints.includes(getIndexName());
        }

        @Override
        protected Iterator<? extends PrimaryKey> execute(Executor executor)
        {
            int softLimit = max(1, round((float) access.expectedAccessCount(factory.tableMetrics.rows)));
            return executor.getTopKRows((Expression) null, softLimit);
        }

        public String getIndexName()
        {
            return ordering.getIndexName();
        }
    }

    /**
     * Returns all keys in ANN order.
     * Contrary to {@link KeysSort}, there is no input node here and the output is generated lazily.
     */
    @VisibleForTesting
    public static final class AnnIndexScan extends ScoredIndexScan
    {
        private AnnIndexScan(Factory factory, int id, Access access, Orderer ordering)
        {
            super(factory, id, access, ordering);
        }

        @Override
        protected KeysIterationCost estimateCost()
        {
            double expectedKeys = access.expectedAccessCount(factory.tableMetrics.rows);
            int expectedKeysInt = Math.max(1, (int) Math.ceil(expectedKeys));
            double searchCost = factory.costEstimator.estimateAnnSearchCost(ordering,
                                                                            expectedKeysInt,
                                                                            factory.tableMetrics.rows);
            double initCost = 0; // negligible
            return new KeysIterationCost(expectedKeys, initCost, searchCost);
        }

        @Override
        protected KeysIteration withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                   ? this
                   : new AnnIndexScan(factory, id, access, ordering);
        }

        @Override
        protected void visitIndexes(Consumer<IndexContext> consumer)
        {
            consumer.accept(ordering.context);
        }

        @Override
        protected String description(Redaction redaction)
        {
            return ordering.toString(redaction);
        }
    }

    /**
     * Returns all keys in BM25 order.
     * Like AnnIndexScan, this generates results lazily without an input node.
     */
    @VisibleForTesting
    public static final class Bm25IndexScan extends ScoredIndexScan
    {
        private Bm25IndexScan(Factory factory, int id, Access access, Orderer ordering)
        {
            super(factory, id, access, ordering);
        }

        @Override
        protected double estimateSelectivity()
        {
            return factory.costEstimator.estimateBM25Selectivity(ordering);
        }

        @Nonnull
        @Override
        protected KeysIterationCost estimateCost()
        {
            double expectedKeys = access.expectedAccessCount(factory.tableMetrics.rows * selectivity());
            int expectedKeysInt = Math.max(1, (int) Math.ceil(expectedKeys));

            int termCount = ordering.getQueryTerms().size();
            double initCost = expectedKeysInt * (hrs(ROW_CELL_COST) + ROW_CELL_COST)
                              + termCount * BM25_SCORE_COST;

            return new KeysIterationCost(expectedKeys, initCost, 0);
        }

        @Override
        protected KeysIteration withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                   ? this
                   : new Bm25IndexScan(factory, id, access, ordering);
        }

        @Override
        protected void visitIndexes(Consumer<IndexContext> consumer)
        {
            consumer.accept(ordering.context);
        }

        @Override
        protected String description(Redaction redaction)
        {
            return ordering.toString(redaction);
        }
    }

    abstract public static class RowsIteration extends Plan
    {
        private RowsIterationCost cost;

        private RowsIteration(Factory factory, int id, Access access)
        {
            super(factory, id, access);
        }

        @Override
        protected RowsIterationCost cost()
        {
            if (cost == null)
                cost = estimateCost();
            return cost;
        }

        protected abstract RowsIterationCost estimateCost();

        protected abstract RowsIteration withAccess(Access patterns);

        final double costPerRow()
        {
            return cost().costPerRow();
        }

        @VisibleForTesting
        public final double expectedRows()
        {
            return cost().expectedRows;
        }

        @Override
        public final RowsIteration optimize()
        {
            return (RowsIteration) super.optimize();
        }

        @Override
        public final RowsIteration limitIntersectedClauses(int clauseLimit)
        {
            return (RowsIteration) super.limitIntersectedClauses(clauseLimit);
        }
    }

    /**
     * Retrieves rows from storage based on the stream of primary keys
     */
    static final class Fetch extends RowsIteration
    {
        private final LazyTransform<KeysIteration> source;

        private Fetch(Factory factory, int id, KeysIteration keysIteration, Access access)
        {
            super(factory, id, access);
            this.source = new LazyTransform<>(keysIteration, k -> k.withAccess(access));
        }

        @Nullable
        @Override
        protected Orderer ordering()
        {
            return source.get().ordering();
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            return function.apply(source.get());
        }

        @Override
        protected Fetch withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            Plan.KeysIteration updatedSource = (KeysIteration) updater.apply(source.get());
            return updatedSource == source.get() ? this : new Fetch(factory, id, updatedSource, access);
        }

        @Override
        protected double estimateSelectivity()
        {
            return source.orig.selectivity();
        }

        @Override
        protected RowsIterationCost estimateCost()
        {
            // VSTODO this assumes we will need to deserialize the entire row for any fetch.
            // For vector rows where we need to check a non-vector field for a predicate,
            // this is a very pessimistic assumption since the vectors (that we don't read)
            // are by far the majority of the row size.
            double rowFetchCost = hrs(CostCoefficients.ROW_COST)
                                  + CostCoefficients.ROW_CELL_COST * factory.tableMetrics.avgCellsPerRow
                                  + CostCoefficients.ROW_BYTE_COST * factory.tableMetrics.avgBytesPerRow;

            KeysIteration src = source.get();
            double expectedKeys = access.expectedAccessCount(src.expectedKeys());
            return new RowsIterationCost(expectedKeys,
                                         src.initCost(),
                                         src.iterCost() + expectedKeys * rowFetchCost);
        }

        @Override
        protected Fetch withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                   ? this
                   : new Fetch(factory, id, source.orig, access);
        }
    }

    /**
     * Filters rows.
     * In order to return one row in the result set it may need to retrieve many rows from the source node.
     * Hence, it will typically have higher cost-per-row than the source node, and will return fewer rows.
     */
    static class Filter extends RowsIteration
    {
        private final RowFilter filter;
        private final LazyTransform<RowsIteration> source;
        private final double targetSelectivity;

        Filter(Factory factory, int id, RowFilter filter, RowsIteration source, double targetSelectivity, Access access)
        {
            super(factory, id, access);
            this.filter = filter;
            this.source = new LazyTransform<>(source, this::propagateAccess);
            this.targetSelectivity = targetSelectivity;
        }

        @Nullable
        @Override
        protected Orderer ordering()
        {
            return source.get().ordering();
        }

        /**
         * Scale the access pattern of the source to reflect that we will need
         * to keep pulling rows from it until the Filter is satisfied.
         */
        private RowsIteration propagateAccess(RowsIteration source)
        {
            Access scaledAccess = access.scaleCount(source.selectivity() / boundedSelectivity(targetSelectivity));
            return source.withAccess(scaledAccess);
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            return function.apply(source.get());
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            Plan.RowsIteration updatedSource = (RowsIteration) updater.apply(source.get());
            return updatedSource == source.get()
                   ? this
                   : new Filter(factory, id, filter, updatedSource, targetSelectivity, access);
        }

        @Override
        protected double estimateSelectivity()
        {
            return targetSelectivity;
        }

        @Override
        protected RowsIterationCost estimateCost()
        {
            double expectedRows = access.expectedAccessCount(factory.tableMetrics.rows * targetSelectivity);
            return new RowsIterationCost(expectedRows,
                                         source.get().initCost(),
                                         source.get().iterCost());
        }

        @Override
        protected Filter withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                   ? this
                   : new Filter(factory, id, filter, source.orig, targetSelectivity, access);
        }

        @Override
        protected String title(Redaction redaction)
        {
            return String.format("%s (sel: %.9f)", filter.toCQLString(redaction), selectivity() / source.get().selectivity());
        }
    }

    /**
     * Limits the number of returned rows to a fixed number.
     * Unlike {@link Filter} it does not affect the cost-per-row.
     */
    static class Limit extends RowsIteration
    {
        private final LazyTransform<RowsIteration> source;
        final int limit;

        private Limit(Factory factory, int id, RowsIteration source, int limit, Access access)
        {
            super(factory, id, access);
            this.limit = limit;
            this.source = new LazyTransform<>(source, s -> s.withAccess(access.limit(limit)));
        }

        @Nullable
        @Override
        protected Orderer ordering()
        {
            return source.get().ordering();
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            return function.apply(source.get());
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            Plan.RowsIteration updatedSource = (RowsIteration) updater.apply(source.get());
            return updatedSource == source.get() ? this : new Limit(factory, id, updatedSource, limit, access);
        }

        @Override
        protected double estimateSelectivity()
        {
            return source.orig.selectivity();
        }

        @Override
        protected RowsIterationCost estimateCost()
        {
            RowsIteration src = source.get();
            double expectedRows = access.expectedAccessCount(src.expectedRows());
            double iterCost = (limit >= src.expectedRows())
                              ? src.iterCost()
                              : src.iterCost() * limit / src.expectedRows();
            return new RowsIterationCost(expectedRows, src.initCost(), iterCost);
        }

        @Override
        protected RowsIteration withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                   ? this
                   : new Limit(factory, id, source.orig, limit, access);
        }

        @Override
        protected String title(Redaction redaction)
        {
            return "" + limit;
        }
    }

    /**
     * Constructs plan nodes.
     * Contains data common for all plan nodes.
     * Performs very lightweight local optimizations.
     * E.g. requesting an intersection/union of only one subplan will result in returning the subplan directly
     * and no intersection/union will be created.
     */
    @NotThreadSafe
    public static final class Factory
    {
        public final String keyspace;

        /** Table metrics that affect cost estimates, e.g. row count, sstable count etc */
        public final TableMetrics tableMetrics;

        public final CostEstimator costEstimator;

        public final IndexHints hints;

        /** A plan returning no keys */
        public final KeysIteration nothing;

        /** A plan returning all keys in the table */
        public final KeysIteration everything;

        /** Default access pattern is to read all rows/keys without skipping until the end of the iterator */
        private final Access defaultAccess;

        /** Id of the next new node created by this factory */
        private int nextId = 0;

        /**
         * Creates a factory that produces Plan nodes.
         *
         * @param tableMetrics allows the planner to adapt the cost estimates to the actual amount of data stored in the table
         * @param costEstimator a cost estimator
         * @param hints the user-provided index hints, the plan should try to respect them
         */
        public Factory(String keyspace, TableMetrics tableMetrics, CostEstimator costEstimator, IndexHints hints)
        {
            this.keyspace = keyspace;
            this.tableMetrics = tableMetrics;
            this.costEstimator = costEstimator;
            this.hints = hints;
            this.nothing = new Nothing(-1, this);
            this.defaultAccess = Access.sequential(tableMetrics.rows);
            this.everything = new Everything(-1, this, defaultAccess);
        }

        /**
         * Constructs a plan node representing a direct scan of an index.
         *
         * @param predicate         the expression matching the rows that we want to search in the index;
         *                          this is needed for identifying this node, it doesn't affect the cost
         * @param matchingKeysCount the number of row keys expected to be returned by the index scan,
         *                          i.e. keys of rows that match the search predicate
         */
        public KeysIteration indexScan(@Nullable Expression predicate, long matchingKeysCount)
        {
            Preconditions.checkArgument(matchingKeysCount >= 0, "matchingKeyCount must not be negative");
            Preconditions.checkArgument(matchingKeysCount <= tableMetrics.rows, "matchingKeyCount must not exceed totalKeyCount");
            return indexScan(predicate, matchingKeysCount, null, nextId++);
        }

        private KeysIteration indexScan(Expression predicate, long matchingKeysCount, Orderer ordering, int id)
        {
            if (predicate == null && ordering == null)
            {
                assert matchingKeysCount == tableMetrics.rows;
                return everything;
            }

            if (ordering != null)
                if (ordering.isANN())
                    return new AnnIndexScan(this, id, defaultAccess, ordering);
                else if (ordering.isBM25())
                    return new Bm25IndexScan(this, id, defaultAccess, ordering);
                else if (ordering.isLiteral())
                    return new LiteralIndexScan(this, id, predicate, matchingKeysCount, defaultAccess, ordering);
                else
                    return new NumericIndexScan(this, id, predicate, matchingKeysCount, defaultAccess, ordering);

            Preconditions.checkNotNull(predicate, "predicate must not be null");
            Preconditions.checkArgument(matchingKeysCount >= 0, "matchingKeyCount must not be negative");
            Preconditions.checkArgument(matchingKeysCount <= tableMetrics.rows, "matchingKeyCount must not exceed totalKeyCount");
            return predicate.isLiteral()
                   ? new LiteralIndexScan(this, id, predicate, matchingKeysCount, defaultAccess, null)
                   : new NumericIndexScan(this, id, predicate, matchingKeysCount, defaultAccess, null);
        }

        public KeysIteration fullIndexScan(IndexContext context)
        {
            Expression everythingExpression = new Expression(context);
            everythingExpression.operation = Expression.Op.RANGE;
            return indexScan(everythingExpression, tableMetrics.rows);
        }

        /**
         * Constructs a plan node representing a union of two key sets.
         * Key sets are assumed to be non-correlated and may overlap.
         * @param subplans a list of subplans for unioned key sets
         */
        public KeysIteration union(List<KeysIteration> subplans)
        {
            return union(subplans, false, nextId++);
        }

        /**
         * Constructs a plan node representing a union of two key sets.
         *
         * @param subplans a list of subplans for unioned key sets
         * @param disjoint hint to the planner that the subplans are disjoint; used for better row count estimation
         */
        public KeysIteration union(List<KeysIteration> subplans, boolean disjoint)
        {
            return union(subplans, disjoint, nextId++);
        }


        private KeysIteration union(List<KeysIteration> subplans, boolean disjoint, int id)
        {
            if (subplans.contains(everything))
                return everything;
            if (subplans.contains(nothing))
                subplans.removeIf(s -> s == nothing);
            if (subplans.size() == 1)
                return subplans.get(0);
            if (subplans.isEmpty())
                return nothing;

            return new Union(this, id, subplans, disjoint, defaultAccess);
        }

        /**
         * Constructs a plan node representing an intersection of key sets.
         * The subplans will be sorted by selectivity from the most selective to the least selective ones.
         * @param subplans a list of subplans for intersected key sets
         */
        public KeysIteration intersection(List<KeysIteration> subplans)
        {
            return intersection(subplans, nextId++);
        }

        /**
         * Constructs a plan node representing an intersection of the given plan with some additional subplans.
         * If the given plan is already an intersection, the additional subplans are added to it.
         *
         * @param source a plan, possibly an intersection, to intersect with the additional subplans
         * @param additional the subplans to be added to the intersection
         */
        public KeysIteration intersection(KeysIteration source, List<KeysIteration> additional)
        {
            List<KeysIteration> subplans = new ArrayList<>();
            if (source instanceof Intersection)
                subplans.addAll(((Intersection) source).subplansSupplier.orig);
            else
                subplans.add(source);
            subplans.addAll(additional);
            return intersection(subplans);
        }

        private KeysIteration intersection(List<KeysIteration> subplans, int id)
        {
            if (subplans.contains(nothing))
                return nothing;
            if (subplans.contains(everything))
                subplans.removeIf(c -> c == everything);
            if (subplans.size() == 1)
                return subplans.get(0);
            if (subplans.isEmpty())
                return everything;

            subplans.sort(Comparator.comparing(KeysIteration::selectivity));
            return new Intersection(this, id, subplans, defaultAccess);
        }

        public Builder unionBuilder()
        {
            return new Builder(this, Operation.OperationType.OR);
        }

        public Builder intersectionBuilder()
        {
            return new Builder(this, Operation.OperationType.AND);
        }

        /**
         * Constructs a node that sorts keys using an index
         */
        public KeysIteration sort(@Nonnull KeysIteration source, @Nonnull Orderer ordering)
        {
            return sort(source, ordering, nextId++);
        }

        /**
         * Constructs a {@link KeysSort} node unconditionally, without applying the collapsing optimisation
         * that {@link #sort} uses. Use this when a {@code FILTER_THEN_SORT} override must be enforced even
         * when the optimiser would otherwise produce a {@link ScoredIndexScan}.
         */
        public KeysSort sortForced(@Nonnull KeysIteration source, @Nonnull Orderer ordering)
        {
            return new KeysSort(this, nextId++, source, defaultAccess, ordering);
        }

        private KeysIteration sort(@Nonnull KeysIteration source, @Nonnull Orderer ordering, int id)
        {
            if (source instanceof IndexScan)
            {
                // Optimization
                // If we want to sort on the same column as the index scan we already have,
                // then we collapse sorting with filtering in a single plan node as the index
                // is already sorted.
                IndexScan indexScan = (IndexScan) source;
                if (indexScan.getIndexName().equals(ordering.getIndexName()))
                    return indexScan(indexScan.predicate, indexScan.matchingKeysCount, ordering, id);
            }

            return (source instanceof Everything)
                   ? indexScan(null, tableMetrics.rows, ordering, id)
                   : new KeysSort(this, id, source, defaultAccess, ordering);
        }

        /**
         * Constructs a node that lazily fetches the rows from storage, based on the primary key iterator.
         */
        public RowsIteration fetch(@Nonnull KeysIteration keysIterationPlan)
        {
            return new Fetch(this, nextId++, keysIterationPlan, defaultAccess);
        }

        /**
         * Constructs a filter node with fixed target selectivity set to the selectivity of the source node.
         * @see Plan.Factory#filter
         */
        public RowsIteration recheckFilter(@Nonnull RowFilter filter, @Nonnull RowsIteration source)
        {
            return filter.isEmpty()
                   ? source
                   : new Filter(this, nextId++, filter, source, source.selectivity(), defaultAccess);
        }

        /**
         * Constructs a filter node with fixed target selectivity.
         * <p>
         * Fixed target selectivity means that the expected number of rows returned by this node is always
         * targetSelectivity/totalRows, regardless of the number of the input rows.
         * Changing the number of the input rows by replacing the subplan
         * with a subplan of different selectivity does not cause this node to return a different number
         * of rows (however, it may change the cost per row estimate).
         * </p><p>
         * This property is useful for constructing so-called "recheck filters" – filters that
         * are not any weaker than the filters in the subplan. If a recheck filter is present, we can freely reduce
         * selectivity of the subplan by e.g. removing intersection nodes, and we still get exactly same number of rows
         * in the result set.
         * </p>
         * @param filter defines which rows are accepted
         * @param source source plan providing the input rows
         * @param targetSelectivity a value in range [0.0, 1.0], but not greater than the selectivity of source
         */
        public RowsIteration filter(@Nonnull RowFilter filter, @Nonnull RowsIteration source, double targetSelectivity)
        {
            Preconditions.checkArgument(targetSelectivity >= 0.0, "selectivity must not be negative");
            Preconditions.checkArgument(targetSelectivity <= source.selectivity(), "selectivity must not exceed source selectivity of " + source.selectivity());
            return new Filter(this, nextId++, filter, source, targetSelectivity, defaultAccess);
        }

        /**
         * Constructs a plan node that fetches only a limited number of rows.
         * It is likely going to have lower fullCost than the fullCost of its input.
         */
        public RowsIteration limit(@Nonnull RowsIteration source, int limit)
        {
            return new Limit(this, nextId++, source, limit, defaultAccess);
        }
    }

    public static class TableMetrics
    {
        public final long rows;
        public final double avgCellsPerRow;
        public final double avgBytesPerRow;
        public final int sstables;

        public TableMetrics(long rows, double avgCellsPerRow, double avgBytesPerRow, int sstables)
        {
            this.rows = rows;
            this.avgCellsPerRow = avgCellsPerRow;
            this.avgBytesPerRow = avgBytesPerRow;
            this.sstables = sstables;
        }
    }

    /**
     * Executes the plan
     */
    public interface Executor
    {
        Iterator<? extends PrimaryKey> getKeysFromIndex(Expression predicate);
        Iterator<? extends PrimaryKey> getTopKRows(Expression predicate, int softLimit);
        Iterator<? extends PrimaryKey> getTopKRows(KeyRangeIterator keys, int softLimit);
    }

    /**
     * Outsources more complex cost estimates to external components.
     * Some components may collect stats on previous data execution and deliver more accurate estimates based
     * on that state.
     */
    public interface CostEstimator
    {
        /**
         * Returns the expected number of ANN index nodes that must be visited to get the list of candidates for top K.
         *
         * @param ordering   allows to identify the proper index
         * @param limit      number of rows to fetch; must be > 0
         * @param candidates number of candidate rows that satisfy the expression predicates
         */
        double estimateAnnSearchCost(Orderer ordering, int limit, long candidates);

        /**
         * Returns the estimated selectivity of a BM25 ordering clause, i.e. the estimated fraction of rows in the
         * table that contain all query terms. BM25 uses intersection semantics: only rows containing every query
         * term are returned. The estimate assumes independence of term occurrences across rows.
         *
         * @param ordering the BM25 orderer whose query terms are used for the estimate
         * @return a value in [0.0, 1.0] representing the estimated fraction of matching rows
         */
        double estimateBM25Selectivity(Orderer ordering);
    }

    /**
     * Data-independent cost coefficients.
     * They are likely going to change whenever storage engine algorithms change.
     */
    public static class CostCoefficients
    {
        /** The constant cost of performing skipTo on posting lists returned from range scans */
        public final static double RANGE_SCAN_SKIP_COST = 0.2;

        /** The coefficient controlling the increase of the skip cost with the distance of the skip. */
        public final static double RANGE_SCAN_SKIP_COST_DISTANCE_FACTOR = 0.1;
        public final static double RANGE_SCAN_SKIP_COST_DISTANCE_EXPONENT = 0.5;

        /** The coefficient controlling the increase of the skip cost with the total size of the posting list. */
        public final static double RANGE_SCAN_SKIP_COST_POSTINGS_COUNT_FACTOR = 0.03;
        public final static double RANGE_SCAN_SKIP_COST_POSTINGS_COUNT_EXPONENT = 0.33;

        /** The constant cost of performing skipTo on literal indexes */
        public final static double POINT_LOOKUP_SKIP_COST = 0.5;

        /** The coefficient controlling the increase of the skip cost with the total size of the posting list for point lookup queries. */
        public final static double POINT_LOOKUP_SKIP_COST_DISTANCE_FACTOR = 0.1;
        public final static double POINT_LOOKUP_SKIP_COST_DISTANCE_EXPONENT = 0.5;

        /** Cost to open the per-sstable index, read metadata and obtain the iterators.  Affected by cache hit rate. */
        public final static double SAI_OPEN_COST = 1500.0;

        /** Cost to advance the index iterator to the next key and load the key. Common for literal and numeric indexes. */
        public final static double SAI_KEY_COST = 0.1;

        /** Cost to get a scored key from DiskANN (~rerank cost). Affected by cache hit rate */
        public final static double ANN_SCORED_KEY_COST = 15;

        /** Cost to perform a coarse (PQ) in-memory similarity computation */
        public final static double ANN_SIMILARITY_COST = 0.5;

        /** Cost to load the neighbor list for a DiskANN node. Affected by cache hit rate */
        public final static double ANN_EDGELIST_COST = 20.0;

        /** Cost to fetch one row from storage. Affected by cache hit rate */
        public final static double ROW_COST = 100.0;

        /** Additional cost added to row fetch cost per each row cell */
        public final static double ROW_CELL_COST = 0.4;

        /** Additional cost added to row fetch cost per each serialized byte of the row */
        public final static double ROW_BYTE_COST = 0.005;

        /** Cost to perform BM25 scoring, per query term */
        public final static double BM25_SCORE_COST = 0.5;

        /** Cost to begin processing PKs into index ordinals for estimateAnnSortCost */
        // DC introduced the one-to-many ordinal mapping optimization
        public static double annSortOpenCost(String keyspace)
        {
            return Version.current(keyspace).onOrAfter(Version.DC) ? 370 : 4200;
        }

        /** Additional overhead needed to process each input key fed to the ANN index searcher */
        // DC introduced the one-to-many ordinal mapping optimization
        public static double annSortKeyCost(String keyspace)
        {
            return Version.current(keyspace).onOrAfter(Version.DC) ? 0.03 : 0.2;
        }

    }

    /** Convenience builder for building intersection and union nodes */
    public static class Builder
    {
        final Factory factory;
        final Operation.OperationType type;
        final List<KeysIteration> subplans;

        Builder(Factory context, Operation.OperationType type)
        {
            this.factory = context;
            this.type = type;
            this.subplans = new ArrayList<>(4);
        }

        public Builder add(KeysIteration subplan)
        {
            subplans.add(subplan);
            return this;
        }

        public KeysIteration build()
        {
            if (type == Operation.OperationType.AND)
                return factory.intersection(subplans);
            if (type == Operation.OperationType.OR)
                return factory.union(subplans);

            // Should never hit this
            throw new AssertionError("Unexpected builder type: " + type);
        }
    }

    /** hit-rate-scale the raw cost */
    public static double hrs(double raw)
    {
        double multiplier = min(1000.0, 1 / hitRateSupplier.getAsDouble());
        return raw * multiplier;
    }

    /**
     * Describes the expected data access patterns for a plan node.
     * <br>
     * Each access pattern is assumed to follow uniform distribution and
     * is represented by a pair of values:
     * - a count (number of expected occurrences) and
     * - an average distance to next occurrence (skip distance).
     * For performance, these are split into arrays of the primitives.
     * <br>
     * For example, given:
     *     counts = [100, 50, 10]
     *     distances = [1.0, 2.0, 5.0]
     * This represents:
     *     - 100 sequential accesses (distance 1.0)
     *     - 50 accesses with a skip distance of 2.0
     *     - 10 accesses with a skip distance of 5.0
     * <br>
     * This information is used to optimize query execution plans by predicting
     * how data will be accessed.
     */
    protected static final class Access
    {
        /** Represents an empty access pattern. */
        final static Access EMPTY = Access.sequential(0);

        /**
         * Array of expected occurrence counts for each access pattern.
         * Each element represents the number of times a particular access pattern
         * is expected to occur.
         */
        final double[] counts;

        /**
         * Array of skip distances for each access pattern.
         * Each element represents the skip distance for a particular access pattern.
         * A distance of 1.0 indicates sequential access.  Smaller distances than 1.0 do not make sense.
         */
        final double[] distances;

        /**
         * The total count of expected accesses across all patterns.
         * This is the sum of all elements in the counts array.
         */
        final double totalCount;

        /**
         * The total weighted distance of all access patterns.
         * Calculated as the sum of (count * distance) for all patterns.
         */
        final double totalDistance;

        /**
         * Flag indicating whether to force the use of skip operations.
         * When true, skip operations are used even for small distances (1.0).
         */
        final boolean forceSkip;

        private Access(double[] count, double[] distance, boolean forceSkip)
        {
            assert count.length == distance.length;
            this.counts = count;
            this.distances = distance;
            this.forceSkip = forceSkip;

            double totalDistance = 0.0;
            double totalCount = 0.0;
            for (int i = 0; i < counts.length; i++)
            {
                totalCount += counts[i];
                totalDistance += counts[i] * distances[i];
            }

            this.totalDistance = totalDistance;
            this.totalCount = totalCount;
        }

        static Access sequential(double count)
        {
            return new Access(new double[] { count }, new double[] { 1.0 }, false);
        }

        /** Scales the counts so that the total count does not exceed given limit */
        Access limit(long limit)
        {
            double totalCount = 0.0;
            for (int i = 0; i < counts.length; i++)
                totalCount += counts[i];

            return limit > totalCount
                   ? this
                   : this.scaleCount(limit / totalCount);
        }

        /** Multiplies all counts by a constant without changing the distribution */
        Access scaleCount(double factor)
        {
            assert Double.isFinite(factor) : "Count multiplier must not be finite; got " + factor;

            double[] counts = Arrays.copyOf(this.counts, this.counts.length);
            double[] skipDistances = Arrays.copyOf(this.distances, this.distances.length);
            for (int i = 0; i < counts.length; i++)
                counts[i] *= factor;
            return new Access(counts, skipDistances, forceSkip);
        }

        /**
         * Multiplies all skip distances by a constant
         * (if constant is > 1, it spreads accesses further away from each other)
         */
        Access scaleDistance(double factor)
        {
            assert Double.isFinite(factor) : "Distance multiplier must not be finite; got " + factor;

            double[] counts = Arrays.copyOf(this.counts, this.counts.length);
            double[] skipDistances = Arrays.copyOf(this.distances, this.distances.length);
            for (int i = 0; i < counts.length; i++)
                skipDistances[i] *= factor;
            return new Access(counts, skipDistances, forceSkip);
        }

        /**
         * Returns a new Access pattern derived by applying a repeated access pattern to the current one,
         * to represent the effect of intersecting with another predicate.  That is, given "x intersect y,"
         * we apply `convolute` to y's Access pattern to account for the skips introduced by x.
         * <p>
         * Example (a star denotes a single access):
         * <pre>
         * Access.sequential(4).scaleDistance(6):
         * *     *     *     *
         * Access.sequential(4).scaleDistance(6).convolute(3, 1):
         * ***   ***   ***   ***
         * </pre>
         * */
        Access convolute(double count, double skipDistance)
        {
            assert !Double.isNaN(count) : "Count must not be NaN";
            assert !Double.isNaN(skipDistance) : "Skip distance must not be NaN";

            if (count <= 1.0)
                return scaleCount(count);

            double[] counts = Arrays.copyOf(this.counts, this.counts.length + 1);
            double[] skipDistances = Arrays.copyOf(this.distances, this.distances.length + 1);

            counts[counts.length - 1] = (count - 1) * totalCount;
            skipDistances[skipDistances.length - 1] = skipDistance;

            // Because we added new accesses, we need to adjust the distance of the remaining points
            // in a way that the total distance stays the same:
            for (int i = 0; i < skipDistances.length - 1; i++)
                skipDistances[i] -= (count - 1) * skipDistance;

            return new Access(counts, skipDistances, forceSkip);
        }

        /** Forces using skipTo cost even if skipping distance is not greater than 1 item */
        Access forceSkip()
        {
            return new Access(counts, distances, true);
        }

        /** Returns the total expected number of items (rows or keys) to be retrieved from the node */
        double expectedAccessCount(double availableCount)
        {
            return totalCount == 0 || totalDistance <= availableCount
                   ? totalCount
                   : availableCount / totalDistance * totalCount;
        }

        /**
         * Computes the expected cost of fetching one item (row or key).
         * This is computed as an arithmetic mean of costs of skipping by each distance, weighted by counts.
         * @param nextCost the cost of fetching one item from the plan node as a function of the skip distance
         *                 (measured in rows or keys)
         */
        double unitCost(double nextCost, Function<Double, Double> skipCostFn)
        {
            if (totalCount == 0)
                return 0.0;  // we don't want NaNs ;)

            double totalCost = 0.0;
            double totalWeight = 0.0;
            for (int i = 0; i < counts.length; i++)
            {
                double skipCost = (distances[i] > 1.0 || forceSkip) ? skipCostFn.apply(distances[i]) : 0.0;
                totalCost += counts[i] * (nextCost + skipCost);
                totalWeight += counts[i];
            }
            return totalCost / totalWeight;
        }

        public double meanDistance()
        {
            return totalCount > 0.0 ? totalDistance / totalCount : 0.0;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Access that = (Access) o;
            return Arrays.equals(counts, that.counts) && Arrays.equals(distances, that.distances);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(Arrays.hashCode(counts), Arrays.hashCode(distances));
        }
    }

    /**
     * Applies given function to given object lazily, only when the result is needed.
     * Caches the result for subsequent executions.
     */
    static class LazyTransform<T>
    {
        final T orig;
        final Function<T, T> transform;
        private T result;

        LazyTransform(T orig, Function<T, T> transform)
        {
            this.orig = orig;
            this.transform = transform;
        }

        public T get()
        {
            if (result == null)
                result = transform.apply(orig);
            return result;
        }
    }
}
