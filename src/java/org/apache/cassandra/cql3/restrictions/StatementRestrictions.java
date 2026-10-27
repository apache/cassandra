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
package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.Ordering;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.SelectOptions;
import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ANNOptions;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.OptimizerOptions;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.NoopIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_ENABLE_GENERAL_ORDER_BY;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * The restrictions corresponding to the relations specified on the where-clause of CQL query.
 */
public class StatementRestrictions
{
    public static final boolean ENABLE_SAI_GENERAL_ORDER_BY = SAI_ENABLE_GENERAL_ORDER_BY.getBoolean();

    private static final String ALLOW_FILTERING_MESSAGE =
            "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. ";

    public static final String REQUIRES_ALLOW_FILTERING_MESSAGE = ALLOW_FILTERING_MESSAGE +
            "If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING";

    public static final String CANNOT_USE_ALLOW_FILTERING_MESSAGE = ALLOW_FILTERING_MESSAGE +
            "Executing this query despite the performance unpredictability with ALLOW FILTERING has been disabled " +
            "by the allow_filtering_enabled property in cassandra.yaml";

    public static final String HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE =
    "Column '%s' has an index but does not support the operators specified in the query. " +
    "If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING";

    public static final String HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_MULTI =
    "Columns %s have indexes but do not support the operators specified in the query. " +
    "If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING";

    public static final String INDEX_DOES_NOT_SUPPORT_LIKE_MESSAGE = "Index on column %s does not support LIKE restrictions.";

    public static final String INDEX_DOES_NOT_SUPPORT_ANALYZER_MATCHES_MESSAGE = "Index on column %s does not support ':' restrictions.";

    public static final String INDEX_DOES_NOT_SUPPORT_DISJUNCTION =
    "An index involved in this query does not support disjunctive queries using the OR operator";

    public static final String RESTRICTION_REQUIRES_INDEX_MESSAGE = "%s restriction is only supported on properly indexed columns. %s is not valid.";

    public static final String PARTITION_KEY_RESTRICTION_MUST_BE_TOP_LEVEL =
    "Restriction on partition key column %s must not be nested under OR operator";

    public static final String GEO_DISTANCE_REQUIRES_INDEX_MESSAGE = "GEO_DISTANCE requires the vector column to be indexed";
    public static final String BM25_ORDERING_REQUIRES_ANALYZED_INDEX_MESSAGE = "BM25 ordering on column %s requires an analyzed index";
    public static final String BM25_ORDERING_REQUIRES_REGULAR_COLUMN_MESSAGE = "BM25 ordering on %s column %s is not supported. " +
                                                                               "Only regular columns are supported.";
    public static final String NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE =
    "Ordering on non-clustering column %s requires the column to be indexed with a non-analyzed index.";
    public static final String NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE =
    "Ordering on non-clustering column requires each restricted column to be indexed except for fully-specified partition keys";

    public static final String VECTOR_INDEX_PRESENT_NOT_SUPPORT_GEO_DISTANCE_MESSAGE =
    "Vector index present, but configuration does not support GEO_DISTANCE queries. GEO_DISTANCE requires similarity_function 'euclidean'";
    public static final String VECTOR_INDEXES_UNSUPPORTED_OP_MESSAGE = "Vector indexes only support ANN and GEO_DISTANCE queries";
    public static final String ANN_OPTIONS_WITHOUT_ORDER_BY_ANN = "ANN options specified without ORDER BY ... ANN OF ...";

    public static final String INDEX_WITH_IN_ON_PK_MESSAGE = "Select on indexed columns and with IN clause for the PRIMARY KEY are not supported";

    /**
     * The Column Family meta data
     */
    public final TableMetadata table;

    /**
     * The index hints, needed to validate {@code ALLOW FILTERING}.
     */
    protected final IndexHints indexHints;

    /**
     * Restrictions on partitioning columns
     */
    protected final PartitionKeyRestrictions partitionKeyRestrictions;

    /**
     * Restrictions on clustering columns
     */
    private final ClusteringColumnRestrictions clusteringColumnsRestrictions;

    /**
     * Restriction on non-primary key columns (i.e. secondary index restrictions)
     */
    private final RestrictionSet nonPrimaryKeyRestrictions;

    private final ImmutableSet<ColumnMetadata> notNullColumns;

    /**
     * The restrictions used to build the row filter
     */
    private final IndexRestrictions filterRestrictions;

    /**
     * <code>true</code> if these restrictions form part of an OR query, <code>false</code> otherwise
     */
    private boolean isDisjunction;

    /**
     * <code>true</code> if the secondary index need to be queried, <code>false</code> otherwise
     */
    protected boolean usesSecondaryIndexing;

    /**
     * Specify if the query will return a range of partition keys.
     */
    protected boolean isKeyRange;

    /**
     * <code>true</code> if nonPrimaryKeyRestrictions contains restriction on a regular column,
     * <code>false</code> otherwise.
     */
    private boolean hasRegularColumnsRestrictions;

    private final List<StatementRestrictions> children;

    /**
     * Creates a new empty <code>StatementRestrictions</code>.
     *
     * @param table the column family meta data
     * @return a new empty <code>StatementRestrictions</code>.
     */
    public static StatementRestrictions empty(TableMetadata table)
    {
        return new StatementRestrictions(table, IndexHints.NONE, false);
    }

    private StatementRestrictions(TableMetadata table, IndexHints indexHints, boolean allowFiltering)
    {
        this.table = table;
        this.indexHints = indexHints;
        this.partitionKeyRestrictions = PartitionKeySingleRestrictionSet.builder(table.partitionKeyAsClusteringComparator())
                                                                        .build(IndexRegistry.obtain(table));
        this.clusteringColumnsRestrictions = ClusteringColumnRestrictions.builder(table, allowFiltering).build();
        this.nonPrimaryKeyRestrictions = RestrictionSet.builder().build();
        this.notNullColumns = ImmutableSet.of();
        this.filterRestrictions = IndexRestrictions.of();
        this.children = Collections.emptyList();
    }

    /**
     * Adds the following restrictions to the index restrictions.
     *
     * @param restrictions the restrictions to add to the index restrictions
     * @return a new {@code StatementRestrictions} with the new index restrictions
     */
    public StatementRestrictions addIndexRestrictions(Restrictions restrictions)
    {
        IndexRestrictions newIndexRestrictions = IndexRestrictions.builder()
                                                                  .add(filterRestrictions)
                                                                  .add(restrictions)
                                                                  .build();

        return new StatementRestrictions(table,
                                         indexHints,
                                         partitionKeyRestrictions,
                                         clusteringColumnsRestrictions,
                                         nonPrimaryKeyRestrictions,
                                         notNullColumns,
                                         isDisjunction,
                                         usesSecondaryIndexing,
                                         isKeyRange,
                                         hasRegularColumnsRestrictions,
                                         newIndexRestrictions,
                                         children);
    }

    /**
     * Adds the following external restrictions (mostly custom and user index expressions) to the index restrictions.
     *
     * @param restrictions the restrictions to add to the index restrictions
     * @return a new {@code StatementRestrictions} with the new index restrictions
     */
    public StatementRestrictions addExternalRestrictions(Iterable<ExternalRestriction> restrictions)
    {
        IndexRestrictions.Builder newIndexRestrictions = IndexRestrictions.builder().add(filterRestrictions);

        for (ExternalRestriction restriction : restrictions)
            newIndexRestrictions.add(restriction);

        return new StatementRestrictions(table,
                                         indexHints,
                                         partitionKeyRestrictions,
                                         clusteringColumnsRestrictions,
                                         nonPrimaryKeyRestrictions,
                                         notNullColumns,
                                         isDisjunction,
                                         usesSecondaryIndexing,
                                         isKeyRange,
                                         hasRegularColumnsRestrictions,
                                         newIndexRestrictions.build(),
                                         children);
    }

    private StatementRestrictions(TableMetadata table,
                                  IndexHints indexHints,
                                  PartitionKeyRestrictions partitionKeyRestrictions,
                                  ClusteringColumnRestrictions clusteringColumnsRestrictions,
                                  RestrictionSet nonPrimaryKeyRestrictions,
                                  ImmutableSet<ColumnMetadata> notNullColumns,
                                  boolean isDisjunction,
                                  boolean usesSecondaryIndexing,
                                  boolean isKeyRange,
                                  boolean hasRegularColumnsRestrictions,
                                  IndexRestrictions filterRestrictions,
                                  List<StatementRestrictions> children)
    {
        this.table = table;
        this.indexHints = indexHints;
        this.partitionKeyRestrictions = partitionKeyRestrictions;
        this.clusteringColumnsRestrictions = clusteringColumnsRestrictions;
        this.nonPrimaryKeyRestrictions = nonPrimaryKeyRestrictions;
        this.notNullColumns = notNullColumns;
        this.filterRestrictions = filterRestrictions;
        this.isDisjunction = isDisjunction;
        this.usesSecondaryIndexing = usesSecondaryIndexing;
        this.isKeyRange = isKeyRange;
        this.hasRegularColumnsRestrictions = hasRegularColumnsRestrictions;
        this.children = children;
    }

    public static StatementRestrictions create(ClientState state,
                                               StatementType type,
                                               TableMetadata table,
                                               WhereClause whereClause,
                                               VariableSpecifications boundNames,
                                               List<Ordering> orderings,
                                               IndexHints indexHints,
                                               boolean selectsOnlyStaticColumns,
                                               boolean allowFiltering,
                                               boolean forView)
    {
        return new Builder(state,
                           type,
                           table,
                           whereClause,
                           boundNames,
                           orderings,
                           indexHints,
                           selectsOnlyStaticColumns,
                           type.allowUseOfSecondaryIndices(),
                           allowFiltering,
                           forView).build();
    }

    public static StatementRestrictions create(ClientState state,
                                               StatementType type,
                                               TableMetadata table,
                                               WhereClause whereClause,
                                               VariableSpecifications boundNames,
                                               List<Ordering> orderings,
                                               IndexHints indexHints,
                                               boolean selectsOnlyStaticColumns,
                                               boolean allowUseOfSecondaryIndices,
                                               boolean allowFiltering,
                                               boolean forView)
    {
        return new Builder(state,
                           type,
                           table,
                           whereClause,
                           boundNames,
                           orderings,
                           indexHints,
                           selectsOnlyStaticColumns,
                           allowUseOfSecondaryIndices,
                           allowFiltering,
                           forView).build();
    }

    /**
     * Build a <code>StatementRestrictions</code> from a <code>WhereClause</code> for a given
     * <code>StatementType</code>, <code>TableMetadata</code> and <code>VariableSpecifications</code>
     * </p>
     * The validation rules for whether the <code>StatementRestrictions</code> are valid depend on a
     * number of considerations, including whether indexes are being used and whether filtering is being
     * used.
     */
    public static class Builder
    {
        private final ClientState state;
        private final StatementType type;
        private final TableMetadata table;
        private final WhereClause whereClause;
        private final VariableSpecifications boundNames;

        private final List<Ordering> orderings;
        private final IndexHints indexHints;
        private final boolean selectsOnlyStaticColumns;
        private final boolean allowUseOfSecondaryIndices;
        private final boolean allowFiltering;
        private final boolean forView;

        public Builder(ClientState state,
                       StatementType type,
                       TableMetadata table,
                       WhereClause whereClause,
                       VariableSpecifications boundNames,
                       List<Ordering> orderings,
                       IndexHints indexHints,
                       boolean selectsOnlyStaticColumns,
                       boolean allowUseOfSecondaryIndices,
                       boolean allowFiltering,
                       boolean forView)
        {
            this.state = state;
            this.type = type;
            this.table = table;
            this.whereClause = whereClause;
            this.boundNames = boundNames;
            this.orderings = orderings;
            this.indexHints = indexHints;
            this.selectsOnlyStaticColumns = selectsOnlyStaticColumns;
            this.allowUseOfSecondaryIndices = allowUseOfSecondaryIndices;
            this.allowFiltering = allowFiltering;
            this.forView = forView;
        }

        public StatementRestrictions build()
        {
            IndexRegistry indexRegistry = null;

            // We want to avoid opening the keyspace during view construction
            // since we're parsing these for restore and the base table or keyspace might not exist in the current schema.
            if (allowUseOfSecondaryIndices && type.allowUseOfSecondaryIndices())
                indexRegistry = IndexRegistry.obtain(table);

            WhereClause.AndElement root = whereClause.root().conjunctiveForm();
            return doBuild(root, indexRegistry, 0);
        }

        /**
         * Processes the WHERE clause expression tree recursively and assigns the restrictions to different sets
         * based on the columns they are applied to.
         *
         * @param element root of the tree
         * @param indexRegistry the index registry for the queried table
         * @param nestingLevel recursion depth needed to reject the restrictions that
         *                     are not allowed to be nested (e.g. partition key restrictions)
         */
        StatementRestrictions doBuild(WhereClause.ExpressionElement element,
                                      IndexRegistry indexRegistry,
                                      int nestingLevel)
        {
            assert element instanceof WhereClause.AndElement || nestingLevel > 0:
                    "Root of the WHERE clause expression tree must be a conjunction";

            PartitionKeySingleRestrictionSet.Builder partitionKeyRestrictionSet = PartitionKeySingleRestrictionSet.builder(table.partitionKeyAsClusteringComparator());
            ClusteringColumnRestrictions.Builder clusteringColumnsRestrictionSet = ClusteringColumnRestrictions.builder(table, allowFiltering, indexRegistry, indexHints);
            RestrictionSet.Builder nonPrimaryKeyRestrictionSet = RestrictionSet.builder();
            ImmutableSet.Builder<ColumnMetadata> notNullColumnsBuilder = ImmutableSet.builder();


            // ORDER BY clause. We add it first because orderings are not really restrictions
            // and by adding first, we ensure that merging restrictions works as expected.
            // The long term solution will break ordering out into its own abstraction.
            if (nestingLevel == 0)
                addOrderingRestrictions(orderings, indexRegistry, nonPrimaryKeyRestrictionSet);

            /*
             * WHERE clause. For a given entity, rules are:
             *   - EQ relation conflicts with anything else (including a 2nd EQ)
             *   - Can't have more than one LT(E) relation (resp. GT(E) relation)
             *   - IN relation are restricted to row keys (for now) and conflicts with anything else (we could
             *     allow two IN for the same entity but that doesn't seem very useful)
             *   - The value_alias cannot be restricted in any way (we don't support wide rows with indexed value
             *     in CQL so far)
             *   - CONTAINS and CONTAINS_KEY cannot be used with UPDATE or DELETE
             */
            for (Relation relation : element.relations())
            {
                if ((relation.isContains() || relation.isContainsKey() || relation.isNotContains() || relation.isNotContainsKey())
                        && (type.isUpdate() || type.isDelete()))
                {
                    throw invalidRequest("Cannot use %s with %s", type, relation.operator());
                }

                if (relation.operator() == Operator.IS_NOT)
                {
                    if (!forView)
                        throw invalidRequest("Unsupported restriction: %s", relation);

                    notNullColumnsBuilder.addAll(relation.toRestriction(table, boundNames, indexHints).getColumnDefs());
                }
                else
                {
                    Restriction restriction = relation.toRestriction(table, boundNames, indexHints);

                    if (relation.isLIKE() && (!type.allowUseOfSecondaryIndices() || !restriction.hasSupportingIndex(indexRegistry, indexHints)))
                    {
                        if (getColumnsWithUnsupportedIndexRestrictions(table, indexHints, ImmutableList.of(restriction)).isEmpty())
                        {
                            throw invalidRequest(RESTRICTION_REQUIRES_INDEX_MESSAGE, relation.operator(), relation.toString());
                        }
                        else
                        {
                            throw invalidRequest(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_LIKE_MESSAGE, restriction.getFirstColumn());
                        }
                    }
                    if (relation.operator() == Operator.ANALYZER_MATCHES)
                    {
                        if (!type.allowUseOfSecondaryIndices())
                        {
                            throw invalidRequest("Invalid query. %s does not support use of secondary indices, but %s restriction requires a secondary index.", type.name(), relation.toString());
                        }
                        if (!restriction.hasSupportingIndex(indexRegistry, indexHints))
                        {
                            if (getColumnsWithUnsupportedIndexRestrictions(table, indexHints, ImmutableList.of(restriction)).isEmpty())
                            {
                                throw invalidRequest(RESTRICTION_REQUIRES_INDEX_MESSAGE, relation.operator(), relation.toString());
                            }
                            else
                            {
                                throw invalidRequest(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_ANALYZER_MATCHES_MESSAGE, restriction.getFirstColumn());
                            }
                        }
                    }
    
                    ColumnMetadata def = restriction.getFirstColumn();
                    if (def.isPartitionKey())
                    {
                        // All partition key restrictions must be a part of the top-level AND operation.
                        // The read path filtering logic is currently unable to filter rows based on
                        // partition key restriction that is a part of a complex expression involving disjunctions.
                        // ALLOW FILTERING does not cut it, as RowFilter can't handle ORed partition
                        // key restrictions properly.
                        if (nestingLevel > 0)
                            throw invalidRequest(StatementRestrictions.PARTITION_KEY_RESTRICTION_MUST_BE_TOP_LEVEL, def);

                        partitionKeyRestrictionSet.addRestriction(restriction);
                    }
                    // If a clustering column restriction is nested (under OR operator),
                    // we can't treat it as a real clustering column,
                    // but instead we treat it as a regular column and use
                    // index (if we have one) or use row filtering on it; hence we require nestingLevel == 0 check here
                    else if (def.isClusteringColumn() && nestingLevel == 0)
                    {
                        // If a clustering column restriction is nested (under OR operator),
                        // we can't treat it as a real clustering column,
                        // but instead we treat it as a regular column and use
                        // index (if we have one) or use row filtering on it; hence we require nestingLevel == 0 check here
                        clusteringColumnsRestrictionSet.addRestriction(restriction);
                    }
                    else
                    {
                        nonPrimaryKeyRestrictionSet.addRestriction((SingleRestriction) restriction, element.isDisjunction());
                    }
                }
            }

            PartitionKeyRestrictions partitionKeyRestrictions = partitionKeyRestrictionSet.build(indexRegistry);
            ClusteringColumnRestrictions clusteringColumnsRestrictions = clusteringColumnsRestrictionSet.build();
            RestrictionSet nonPrimaryKeyRestrictions = nonPrimaryKeyRestrictionSet.build();
            ImmutableSet<ColumnMetadata> notNullColumns = notNullColumnsBuilder.build();
            boolean hasRegularColumnsRestrictions = nonPrimaryKeyRestrictions.hasRestrictionFor(ColumnMetadata.Kind.REGULAR);
            boolean usesSecondaryIndexing = false;
            boolean isKeyRange = false;

            boolean hasQueryableClusteringColumnIndex = false;
            boolean hasQueryableIndex = false;

            IndexRestrictions.Builder filterRestrictionsBuilder = IndexRestrictions.builder();

            if (allowUseOfSecondaryIndices)
            {
                if (element.containsCustomExpressions())
                {
                    CustomIndexExpression customExpression = prepareCustomIndexExpression(element.expressions(),
                                                                                          boundNames,
                                                                                          indexRegistry);
                    filterRestrictionsBuilder.add(customExpression);
                }

                hasQueryableClusteringColumnIndex = clusteringColumnsRestrictions.hasSupportingIndex(indexRegistry, indexHints);
                hasQueryableIndex = element.containsCustomExpressions()
                                    || hasQueryableClusteringColumnIndex
                                    || partitionKeyRestrictions.hasSupportingIndex(indexRegistry, indexHints)
                                    || nonPrimaryKeyRestrictions.hasSupportingIndex(indexRegistry, indexHints);
            }

            // At this point, the select statement if fully constructed, but we still have a few things to validate
            if (!type.allowPartitionKeyRanges())
            {
                checkFalse(partitionKeyRestrictions.isOnToken(),
                           "The token function cannot be used in WHERE clauses for %s statements", type);
    
                if (partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(table))
                    throw invalidRequest("Some partition key parts are missing: %s",
                                         Joiner.on(", ").join(getPartitionKeyUnrestrictedComponents(partitionKeyRestrictions)));

                // slice query
                checkFalse(partitionKeyRestrictions.hasSlice(),
                           "Only EQ and IN relation are supported on the partition key (unless you use the token() function)"
                           + " for %s statements", type);
            }
            else
            {
                // If there are no partition restrictions or there's only token restriction, we have to set a key range
                if (partitionKeyRestrictions.isOnToken())
                    isKeyRange = true;
    
                if (partitionKeyRestrictions.isEmpty() && partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(table))
                {
                    isKeyRange = true;
                    usesSecondaryIndexing = hasQueryableIndex;
                }

                // If there is a queryable index, no special condition is required on the other restrictions.
                // But we still need to know 2 things:
                // - If we don't have a queryable index, is the query ok
                // - Is it queryable without 2ndary index, which is always more efficient
                // If a component of the partition key is restricted by a relation, all preceding
                // components must have a EQ. Only the last partition key component can be in IN relation.
                // If partition key restrictions exist and this is a disjunction then we may need filtering
                if (partitionKeyRestrictions.needFiltering(table) || (!partitionKeyRestrictions.isEmpty() && element.isDisjunction()))
                {
                    if (!allowFiltering && !forView && !hasQueryableIndex && requiresAllowFilteringIfNotSpecified(table))
                        throw new InvalidRequestException(allowFilteringMessage(state));
    
                    isKeyRange = true;
                    usesSecondaryIndexing = hasQueryableIndex;
                }
            }

            // Some but not all of the partition key columns have been specified or they form part of a disjunction;
            // hence we need turn these restrictions into a row filter.
            if (usesSecondaryIndexing || partitionKeyRestrictions.needFiltering(table) || element.isDisjunction())
                filterRestrictionsBuilder.add(partitionKeyRestrictions);

            if (selectsOnlyStaticColumns && !clusteringColumnsRestrictions.isEmpty())
            {
                // If the only updated/deleted columns are static, then we don't need clustering columns.
                // And in fact, unless it is an INSERT, we reject if clustering colums are provided as that
                // suggest something unintended. For instance, given:
                //   CREATE TABLE t (k int, v int, s int static, PRIMARY KEY (k, v))
                // it can make sense to do:
                //   INSERT INTO t(k, v, s) VALUES (0, 1, 2)
                // but both
                //   UPDATE t SET s = 3 WHERE k = 0 AND v = 1
                //   DELETE v FROM t WHERE k = 0 AND v = 1
                // sounds like you don't really understand what you are doing.
                if (type.isDelete() || type.isUpdate())
                    throw invalidRequest("Invalid restrictions on clustering columns since the %s statement modifies only static columns",
                                         type);
            }

            // Now process and validate the clustering column restrictions
            checkFalse(!type.allowClusteringColumnSlices() && clusteringColumnsRestrictions.hasSlice(),
                       "Slice restrictions are not supported on the clustering columns in %s statements", type);

            if (!type.allowClusteringColumnSlices()
                && (!table.isCompactTable() || (table.isCompactTable() && clusteringColumnsRestrictions.isEmpty())))
            {
                if (!selectsOnlyStaticColumns && (table.clusteringColumns().size() != clusteringColumnsRestrictions.size()))
                    throw invalidRequest("Some clustering keys are missing: %s",
                                         Joiner.on(", ").join(getUnrestrictedClusteringColumns(clusteringColumnsRestrictions)));
            }
            else
            {
                checkFalse(clusteringColumnsRestrictions.hasContains() && !hasQueryableIndex && !allowFiltering,
                           "Clustering columns can only be restricted with CONTAINS with a secondary index or filtering");
    
                if (!clusteringColumnsRestrictions.isEmpty() && clusteringColumnsRestrictions.needFiltering())
                {
                    if (hasQueryableIndex || forView)
                    {
                        usesSecondaryIndexing = true;
                    }
                    else if (!allowFiltering)
                    {
                        List<ColumnMetadata> clusteringColumns = table.clusteringColumns();
                        List<ColumnMetadata> restrictedColumns = clusteringColumnsRestrictions.getColumnDefs();
    
                        for (int i = 0, m = restrictedColumns.size(); i < m; i++)
                        {
                            ColumnMetadata clusteringColumn = clusteringColumns.get(i);
                            ColumnMetadata restrictedColumn = restrictedColumns.get(i);
    
                            if (!clusteringColumn.equals(restrictedColumn))
                            {
                                throw invalidRequest("PRIMARY KEY column \"%s\" cannot be restricted as preceding column \"%s\" is not restricted",
                                                     restrictedColumn.name,
                                                     clusteringColumn.name);
                            }
                        }
                    }
                }
            }

            // Covers indexes on the first clustering column (among others).
            if (isKeyRange && hasQueryableClusteringColumnIndex)
                usesSecondaryIndexing = true;

            // Because an ANN queries limit the result set based within the SAI, clustering column restrictions
            // must be added to the filter restrictions.
            if (orderings.stream().anyMatch(o -> o.expression.hasNonClusteredOrdering()))
                usesSecondaryIndexing = true;

            if (usesSecondaryIndexing || clusteringColumnsRestrictions.needFiltering())
                filterRestrictionsBuilder.add(clusteringColumnsRestrictions);

            // Even if usesSecondaryIndexing is false at this point, we'll still have to use one if
            // there is restrictions not covered by the PK.
            if (!nonPrimaryKeyRestrictions.isEmpty())
            {
                Iterable<Restriction> columnRestrictions = allColumnRestrictions(clusteringColumnsRestrictions, nonPrimaryKeyRestrictions);

                if (!type.allowNonPrimaryKeyInWhereClause())
                {
                    Collection<ColumnIdentifier> nonPrimaryKeyColumns =
                            ColumnMetadata.toIdentifiers(nonPrimaryKeyRestrictions.getColumnDefs());
    
                    throw invalidRequest("Non PRIMARY KEY columns found in where clause: %s ",
                                         Joiner.on(", ").join(nonPrimaryKeyColumns));
                }
                if (hasQueryableIndex)
                    usesSecondaryIndexing = true;
                else
                {
                    Optional<ColumnMetadata> vectorColumn = nonPrimaryKeyRestrictions.getColumnDefs().stream().filter(c -> c.type.isVector()).findFirst();
                    if (vectorColumn.isPresent())
                    {
                        var vc = vectorColumn.get();
                        var hasIndex = indexRegistry.listNotExcludedIndexes(indexHints).stream().anyMatch(i -> i.dependsOn(vc));
                        var isBoundedANN = nonPrimaryKeyRestrictions.restrictions().stream().anyMatch(SingleRestriction::isBoundedAnn);
                        var isIndexBasedOrdering = nonPrimaryKeyRestrictions.restrictions().stream().anyMatch(SingleRestriction::isIndexBasedOrdering);
                        if (hasIndex)
                        {
                            if (isBoundedANN)
                                throw invalidRequest(StatementRestrictions.VECTOR_INDEX_PRESENT_NOT_SUPPORT_GEO_DISTANCE_MESSAGE);
                            else
                                throw invalidRequest(StatementRestrictions.VECTOR_INDEXES_UNSUPPORTED_OP_MESSAGE, vc);
                        }
                        else
                        {
                            // We check if ANN vector column has index earlier, so we only need to for bounded ann here
                            if (isBoundedANN)
                                throw invalidRequest(StatementRestrictions.GEO_DISTANCE_REQUIRES_INDEX_MESSAGE);
                            else if (isIndexBasedOrdering)
                                throw invalidRequest(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE);
                        }
                    }

                    if (!allowFiltering)
                        throwRequiresAllowFilteringError(table, indexHints, columnRestrictions, state);
                }
                filterRestrictionsBuilder.add(nonPrimaryKeyRestrictions);
            }

            if (usesSecondaryIndexing)
                checkFalse(partitionKeyRestrictions.hasIN(), INDEX_WITH_IN_ON_PK_MESSAGE);

            ImmutableList.Builder<StatementRestrictions> children = ImmutableList.builder();

            for (WhereClause.ContainerElement container : element.operations())
                children.add(doBuild(container, indexRegistry, nestingLevel + 1));

            return new StatementRestrictions(table,
                                             indexHints,
                                             partitionKeyRestrictions,
                                             clusteringColumnsRestrictions,
                                             nonPrimaryKeyRestrictions,
                                             notNullColumns,
                                             element.isDisjunction(),
                                             usesSecondaryIndexing,
                                             isKeyRange,
                                             hasRegularColumnsRestrictions,
                                             filterRestrictionsBuilder.build(),
                                             children.build());
        }

        /**
         * This is a hack to push ordering down to indexes.
         * Indexes are selected based on RowFilter only, so we need to turn orderings into restrictions
         * so they end up in the row filter.
         *
         * @param orderings orderings from the select statement
         * @param indexRegistry used to check if the ordering is supported by an index
         * @param receiver target restriction builder to receive the additional restrictions
         */
        private void addOrderingRestrictions(List<Ordering> orderings, IndexRegistry indexRegistry, RestrictionSet.Builder receiver)
        {
            List<Ordering> indexOrderings = orderings.stream().filter(o -> o.expression.hasNonClusteredOrdering()).collect(Collectors.toList());

            if (indexOrderings.size() > 1)
                throw new InvalidRequestException("Cannot specify more than one ordering column when using SAI indexes");
            else if (indexOrderings.size() == 1)
            {
                if (orderings.size() > 1)
                    throw new InvalidRequestException("Cannot combine clustering column ordering with non-clustering column ordering");
                Ordering ordering = indexOrderings.get(0);
                // TODO remove the instanceof with Ordering.Ann.USE_SYNTHETIC_SCORE.
                if (ordering.direction != Ordering.Direction.ASC && (ordering.expression.isScored() || ordering.expression instanceof Ordering.Ann))
                    throw new InvalidRequestException("Descending ANN ordering is not supported");
                if (!ENABLE_SAI_GENERAL_ORDER_BY && ordering.expression instanceof Ordering.SingleColumn)
                    throw new InvalidRequestException("SAI based ORDER BY on non-vector column is not supported");

                SingleRestriction restriction = ordering.expression.toRestriction();

                ColumnMetadata column = restriction.getFirstColumn();

                if (!restriction.hasSupportingIndex(indexRegistry, indexHints))
                {
                    var type = column.type.asCQL3Type().getType();
                    // This is a slight hack, but once we support a way to order these types, we can remove it.
                    if (type instanceof IntegerType || type instanceof DecimalType)
                        throw new InvalidRequestException(String.format("SAI based ordering on column %s of type %s is not supported",
                                                                        column,
                                                                        column.type.asCQL3Type()));
                    if (ordering.expression instanceof Ordering.Bm25)
                        throw new InvalidRequestException(String.format(BM25_ORDERING_REQUIRES_ANALYZED_INDEX_MESSAGE, column));
                    else
                        throw new InvalidRequestException(String.format(NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, column));
                }

                if (ordering.expression instanceof Ordering.Bm25 && !column.isRegular())
                    throw new InvalidRequestException(String.format(BM25_ORDERING_REQUIRES_REGULAR_COLUMN_MESSAGE,
                                                                    column.kind.name().toLowerCase().replace("_", " "),
                                                                    column.name));

                receiver.addRestriction(restriction, false);
            }
        }

        private CustomIndexExpression prepareCustomIndexExpression(List<CustomIndexExpression> expressions,
                                                                   VariableSpecifications boundNames,
                                                                   IndexRegistry indexRegistry)
        {
            if (expressions.size() > 1)
                throw new InvalidRequestException(IndexRestrictions.MULTIPLE_EXPRESSIONS);

            CustomIndexExpression expression = expressions.get(0);

            QualifiedName name = expression.targetIndex;

            if (name.hasKeyspace() && !name.getKeyspace().equals(table.keyspace))
                throw IndexRestrictions.invalidIndex(expression.targetIndex, table);

            if (!table.indexes.has(expression.targetIndex.getName()))
                throw IndexRestrictions.indexNotFound(expression.targetIndex, table);

            Index index = indexRegistry.getIndex(table.indexes.get(expression.targetIndex.getName()).orElseThrow());
            if (!index.getIndexMetadata().isCustom())
                throw IndexRestrictions.nonCustomIndexInExpression(expression.targetIndex);

            AbstractType<?> expressionType = index.customExpressionValueType();
            if (expressionType == null)
                throw IndexRestrictions.customExpressionNotSupported(expression.targetIndex);

            expression.prepareValue(table, expressionType, boundNames);
            return expression;
        }

        /**
         * Returns the partition key components that are not restricted.
         * @return the partition key components that are not restricted.
         */
        private Collection<ColumnIdentifier> getPartitionKeyUnrestrictedComponents(PartitionKeyRestrictions partitionKeyRestrictions)
        {
            List<ColumnMetadata> list = new ArrayList<>(table.partitionKeyColumns());
            list.removeAll(partitionKeyRestrictions.getColumnDefs());
            return ColumnMetadata.toIdentifiers(list);
        }

        /**
         * Returns the clustering columns that are not restricted.
         * @return the clustering columns that are not restricted.
         */
        private Collection<ColumnIdentifier> getUnrestrictedClusteringColumns(ClusteringColumnRestrictions clusteringColumnsRestrictions)
        {
            List<ColumnMetadata> missingClusteringColumns = new ArrayList<>(table.clusteringColumns());
            missingClusteringColumns.removeAll(clusteringColumnsRestrictions.getColumnDefs());
            return ColumnMetadata.toIdentifiers(missingClusteringColumns);
        }
    }

    public IndexRestrictions filterRestrictions()
    {
        return filterRestrictions;
    }

    public List<StatementRestrictions> children()
    {
        return children;
    }

    public static boolean requiresAllowFilteringIfNotSpecified(TableMetadata table)
    {
        if (!table.isVirtual())
            return true;

        VirtualTable tableNullable = VirtualKeyspaceRegistry.instance.getTableNullable(table.id);
        assert tableNullable != null;
        return !tableNullable.allowFilteringImplicitly();
    }

    private boolean hasAnnOrdering()
    {
        return nonPrimaryKeyRestrictions.restrictions().stream().anyMatch(r -> r instanceof SingleColumnRestriction.AnnRestriction);
    }

    public boolean hasIndexBasedOrdering()
    {
        return nonPrimaryKeyRestrictions.restrictions().stream().anyMatch(SingleRestriction::isIndexBasedOrdering);
    }

    public void throwRequiresAllowFilteringError(TableMetadata table, ClientState state)
    {
        if (hasIndexBasedOrdering())
            throw invalidRequest(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE);

        throwRequiresAllowFilteringError(table, indexHints, allColumnRestrictions(clusteringColumnsRestrictions, nonPrimaryKeyRestrictions), state);
    }

    private static void throwRequiresAllowFilteringError(TableMetadata table, IndexHints indexHints, Iterable<Restriction> columnRestrictions, ClientState state)
    {
        Set<ColumnMetadata> unsupported = getColumnsWithUnsupportedIndexRestrictions(table, indexHints, columnRestrictions);
        if (unsupported.isEmpty())
        {
            if (requiresAllowFilteringIfNotSpecified(table))
                throw invalidRequest(allowFilteringMessage(state));
        }
        else
        {
            // If there's an index on these columns but the restriction is not supported on this index, throw a more specific error message
            if (unsupported.size() == 1)
                throw invalidRequest(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, unsupported.iterator().next()));
            else
                throw invalidRequest(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_MULTI, unsupported));
        }
    }

    public void throwsRequiresIndexSupportingDisjunctionError()
    {
        throw invalidRequest(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_DISJUNCTION);
    }

    public void addFunctionsTo(List<Function> functions)
    {
        partitionKeyRestrictions.addFunctionsTo(functions);
        clusteringColumnsRestrictions.addFunctionsTo(functions);
        nonPrimaryKeyRestrictions.addFunctionsTo(functions);

        for (StatementRestrictions child : children)
            child.addFunctionsTo(functions);
    }

    // may be used by QueryHandler implementations
    public IndexRestrictions getIndexRestrictions()
    {
        return filterRestrictions;
    }

    /**
     * Returns the non-PK column that are restricted.  If includeNotNullRestrictions is true, columns that are restricted
     * by an IS NOT NULL restriction will be included, otherwise they will not be included (unless another restriction
     * applies to them).
     */
    public Set<ColumnMetadata> nonPKRestrictedColumns(boolean includeNotNullRestrictions)
    {
        Set<ColumnMetadata> columns = new HashSet<>();
        for (Restrictions r : filterRestrictions.getRestrictions())
        {
            for (ColumnMetadata def : r.getColumnDefs())
                if (!def.isPrimaryKeyColumn())
                    columns.add(def);
        }

        if (includeNotNullRestrictions)
        {
            for (ColumnMetadata def : notNullColumns())
            {
                if (!def.isPrimaryKeyColumn())
                    columns.add(def);
            }
        }

        for (StatementRestrictions child : children)
            columns.addAll(child.nonPKRestrictedColumns(includeNotNullRestrictions));

        return columns;
    }

    /**
     * @return the set of columns that have an IS NOT NULL restriction on them
     */
    public ImmutableSet<ColumnMetadata> notNullColumns()
    {
        return notNullColumns;
    }

    /**
     * @return true if column is restricted by some restriction, false otherwise
     */
    public boolean isRestricted(ColumnMetadata column)
    {
        if (notNullColumns().contains(column))
            return true;

        if (getRestrictions(column.kind).getColumnDefs().contains(column))
            return true;

        for (StatementRestrictions child : children)
            if (child.isRestricted(column))
                return true;

        return false;
    }

    /**
     * Checks if the restrictions on the partition key has IN restrictions.
     *
     * @return <code>true</code> the restrictions on the partition key has an IN restriction, <code>false</code>
     * otherwise.
     */
    public boolean keyIsInRelation()
    {
        return partitionKeyRestrictions.hasIN();
    }

    /**
     * Checks if the query request a range of partition keys.
     *
     * @return <code>true</code> if the query request a range of partition keys, <code>false</code> otherwise.
     */
    public boolean isKeyRange()
    {
        return isKeyRange;
    }

    /**
     * Checks if the specified column is restricted by an EQ restriction.
     *
     * @param columnDef the column definition
     * @return <code>true</code> if the specified column is restricted by an EQ restiction, <code>false</code>
     * otherwise.
     */
    public boolean isColumnRestrictedByEq(ColumnMetadata columnDef)
    {
        Set<Restriction> restrictions = getRestrictions(columnDef.kind).getRestrictions(columnDef);
        return restrictions.stream()
                           .filter(SingleRestriction.class::isInstance)
                           .anyMatch(p -> ((SingleRestriction) p).isEQ());
    }

    /**
     * This method determines whether a specified column is restricted on equality or something equivalent, like IN.
     * It can be used in conjunction with the columns selected by a query to determine which of those columns is
     * already bound by the client (and from its perspective, not retrieved by the database).
     *
     * @param column a column from the same table these restrictions are against
     *
     * @return <code>true</code> if the given column is restricted on equality
     */
    public boolean isEqualityRestricted(ColumnMetadata column)
    {
        if (column.kind == ColumnMetadata.Kind.PARTITION_KEY)
        {
            if (partitionKeyRestrictions.hasOnlyEqualityRestrictions())
                for (ColumnMetadata restricted : partitionKeyRestrictions.getColumnDefinitions())
                    if (restricted.name.equals(column.name))
                        return true;
        }
        else if (column.kind == ColumnMetadata.Kind.CLUSTERING)
        {
            if (hasClusteringColumnsRestrictions())
            {
                for (SingleRestriction restriction : clusteringColumnsRestrictions.restrictions())
                {
                    if (restriction.isEqualityBased())
                    {
                        if (restriction.isMultiColumn())
                        {
                            for (ColumnMetadata restricted : restriction.getColumnDefs())
                                if (restricted.name.equals(column.name))
                                    return true;
                        }
                        else if (restriction.getFirstColumn().name.equals(column.name))
                            return true;
                    }
                }
            }
        }
        else if (hasNonPrimaryKeyRestrictions())
        {
            for (SingleRestriction restriction : nonPrimaryKeyRestrictions.restrictions())
                if (restriction.getFirstColumn().name.equals(column.name) && restriction.isEqualityBased())
                    return true;
        }

        return false;
    }

    /**
     * Returns the <code>Restrictions</code> for the specified type of columns.
     *
     * @param kind the column type
     * @return the <code>Restrictions</code> for the specified type of columns
     */
    protected Restrictions getRestrictions(ColumnMetadata.Kind kind)
    {
        switch (kind)
        {
            case PARTITION_KEY: return partitionKeyRestrictions;
            case CLUSTERING: return clusteringColumnsRestrictions;
            default: return nonPrimaryKeyRestrictions;
        }
    }

    /**
     * Checks if the secondary index need to be queried.
     *
     * @return <code>true</code> if the secondary index need to be queried, <code>false</code> otherwise.
     */
    public boolean usesSecondaryIndexing()
    {
        if (usesSecondaryIndexing)
            return true;

        for (StatementRestrictions child: children)
            if (child.usesSecondaryIndexing)
                return true;

        return false;
    }

    public boolean hasPartitionKeyRestrictions()
    {
        return !partitionKeyRestrictions.isEmpty();
    }

    /**
     * Checks if the restrictions contain any non-primary key restrictions
     * @return <code>true</code> if the restrictions contain any non-primary key restrictions, <code>false</code> otherwise.
     */
    public boolean hasNonPrimaryKeyRestrictions()
    {
        return !nonPrimaryKeyRestrictions.isEmpty();
    }

    /**
     * Checks if the restrictions on the partition key are token restrictions.
     *
     * @return <code>true</code> if the restrictions on the partition key are token restrictions,
     * <code>false</code> otherwise.
     */
    public boolean isPartitionKeyRestrictionsOnToken()
    {
        return partitionKeyRestrictions.isOnToken();
    }

    /**
     * Checks if restrictions on the clustering key have IN restrictions.
     *
     * @return <code>true</code> if the restrictions on the clustering key have IN restrictions,
     * <code>false</code> otherwise.
     */
    public boolean clusteringKeyRestrictionsHasIN()
    {
        return clusteringColumnsRestrictions.hasIN();
    }

    /**
     * Checks if some clustering columns are not restricted.
     * @return <code>true</code> if some clustering columns are not restricted, <code>false</code> otherwise.
     */
    private boolean hasUnrestrictedClusteringColumns()
    {
        return table.clusteringColumns().size() != clusteringColumnsRestrictions.size();
    }

    public RowFilter getRowFilter(IndexRegistry indexRegistry, QueryOptions options, ClientState clientState, SelectOptions selectOptions)
    {
        boolean hasAnnOptions = selectOptions.hasANNOptions();

        if (filterRestrictions.isEmpty() && children.isEmpty())
        {
            if (hasAnnOptions)
                throw new InvalidRequestException(ANN_OPTIONS_WITHOUT_ORDER_BY_ANN);

            return RowFilter.none();
        }

        if (hasAnnOptions && !hasAnnOrdering())
            throw new InvalidRequestException(ANN_OPTIONS_WITHOUT_ORDER_BY_ANN);

        // If there is only one replica, we don't need reconciliation at any consistency level.
        boolean needsReconciliation = !table.isVirtual()
                                      && options.getConsistency().needsReconciliation()
                                      && Keyspace.open(table.keyspace).getReplicationStrategy().getReplicationFactor().allReplicas > 1;

        ANNOptions annOptions = selectOptions.parseANNOptions();
        OptimizerOptions optimizerOptions = selectOptions.parseOptimizerOptions();

        RowFilter.Builder filterBuilder = new RowFilter.Builder(needsReconciliation, indexRegistry, indexHints, optimizerOptions);
        return filterBuilder.buildFromRestrictions(this, table, options, clientState, annOptions);
    }

    /**
     * Returns the partition keys for which the data is requested.
     *
     * @param options the query options
     * @param state the client state
     * @return the partition keys for which the data is requested.
     */
    public List<ByteBuffer> getPartitionKeys(final QueryOptions options, ClientState state)
    {
        return partitionKeyRestrictions.values(options, state);
    }

    /**
     * Returns the specified bound of the partition key.
     *
     * @param b the boundary type
     * @param options the query options
     * @return the specified bound of the partition key
     */
    private ByteBuffer getPartitionKeyBound(Bound b, QueryOptions options)
    {
        // We deal with IN queries for keys in other places, so we know buildBound will return only one result
        return partitionKeyRestrictions.bounds(b, options).get(0);
    }

    /**
     * Returns the partition key bounds.
     *
     * @param options the query options
     * @return the partition key bounds
     */
    public AbstractBounds<PartitionPosition> getPartitionKeyBounds(QueryOptions options)
    {
        IPartitioner p = table.partitioner;

        if (partitionKeyRestrictions.isOnToken())
        {
            return getPartitionKeyBoundsForTokenRestrictions(p, options);
        }

        return getPartitionKeyBounds(p, options);
    }

    private AbstractBounds<PartitionPosition> getPartitionKeyBounds(IPartitioner p,
                                                                    QueryOptions options)
    {
        // Deal with unrestricted partition key components (special-casing is required to deal with 2i queries on the
        // first component of a composite partition key) queries that filter on the partition key.
        if (partitionKeyRestrictions.needFiltering(table) || isDisjunction)
            return new Range<>(p.getMinimumToken().minKeyBound(), p.getMinimumToken().maxKeyBound());

        ByteBuffer startKeyBytes = getPartitionKeyBound(Bound.START, options);
        ByteBuffer finishKeyBytes = getPartitionKeyBound(Bound.END, options);

        PartitionPosition startKey = PartitionPosition.ForKey.get(startKeyBytes, p);
        PartitionPosition finishKey = PartitionPosition.ForKey.get(finishKeyBytes, p);

        if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum())
            return null;

        if (partitionKeyRestrictions.isInclusive(Bound.START))
        {
            return partitionKeyRestrictions.isInclusive(Bound.END)
                   ? new Bounds<>(startKey, finishKey)
                   : new IncludingExcludingBounds<>(startKey, finishKey);
        }

        return partitionKeyRestrictions.isInclusive(Bound.END)
               ? new Range<>(startKey, finishKey)
               : new ExcludingBounds<>(startKey, finishKey);
    }

    private AbstractBounds<PartitionPosition> getPartitionKeyBoundsForTokenRestrictions(IPartitioner p,
                                                                                        QueryOptions options)
    {
        Token startToken = getTokenBound(Bound.START, options, p);
        Token endToken = getTokenBound(Bound.END, options, p);

        boolean includeStart = partitionKeyRestrictions.isInclusive(Bound.START);
        boolean includeEnd = partitionKeyRestrictions.isInclusive(Bound.END);

        /*
         * If we ask SP.getRangeSlice() for (token(200), token(200)], it will happily return the whole ring.
         * However, wrapping range doesn't really make sense for CQL, and we want to return an empty result in that
         * case (CASSANDRA-5573). So special case to create a range that is guaranteed to be empty.
         *
         * In practice, we want to return an empty result set if either startToken > endToken, or both are equal but
         * one of the bound is excluded (since [a, a] can contains something, but not (a, a], [a, a) or (a, a)).
         * Note though that in the case where startToken or endToken is the minimum token, then this special case
         * rule should not apply.
         */
        int cmp = startToken.compareTo(endToken);
        if (!startToken.isMinimum() && !endToken.isMinimum()
                && (cmp > 0 || (cmp == 0 && (!includeStart || !includeEnd))))
            return null;

        PartitionPosition start = includeStart ? startToken.minKeyBound() : startToken.maxKeyBound();
        PartitionPosition end = includeEnd ? endToken.maxKeyBound() : endToken.minKeyBound();

        return new Range<>(start, end);
    }

    private Token getTokenBound(Bound b, QueryOptions options, IPartitioner p)
    {
        if (!partitionKeyRestrictions.hasBound(b))
            return p.getMinimumToken();

        ByteBuffer value = partitionKeyRestrictions.bounds(b, options).get(0);
        checkNotNull(value, "Invalid null token value");
        return p.getTokenFactory().fromByteArray(value);
    }

    /**
     * Checks if the query has some restrictions on the clustering columns.
     *
     * @return <code>true</code> if the query has some restrictions on the clustering columns,
     * <code>false</code> otherwise.
     */
    public boolean hasClusteringColumnsRestrictions()
    {
        return !clusteringColumnsRestrictions.isEmpty();
    }

    /**
     * Checks if the query has any cluster column restrictions that do not also have a supporting index.
     * @param table the table metadata
     * @return <code>true</code> if the query has any cluster column restrictions that do not also have a supporting index,
     * <code>false</code> otherwise.
     */
    public boolean hasClusterColumnRestrictionWithoutSupportingIndex(TableMetadata table)
    {
        IndexRegistry registry = IndexRegistry.obtain(table);
        for (Restriction restriction : clusteringColumnsRestrictions.restrictions())
            if (!restriction.hasSupportingIndex(registry, indexHints))
                return true;
        return false;
    }

    /**
     * Returns the requested clustering columns.
     *
     * @param options the query options
     * @param state the client state
     * @return the requested clustering columns
     */
    public NavigableSet<Clustering<?>> getClusteringColumns(QueryOptions options, ClientState state)
    {
        // If this is a names command and the table is a static compact one, then as far as CQL is concerned we have
        // only a single row which internally correspond to the static parts. In which case we want to return an empty
        // set (since that's what ClusteringIndexNamesFilter expects).
        if (table.isStaticCompactTable())
            return BTreeSet.empty(table.comparator);

        return clusteringColumnsRestrictions.valuesAsClustering(options, state);
    }

    /**
     * Returns the bounds (start or end) of the clustering columns.
     *
     * @param b the bound type
     * @param options the query options
     * @return the bounds (start or end) of the clustering columns
     */
    public NavigableSet<ClusteringBound<?>> getClusteringColumnsBounds(Bound b, QueryOptions options)
    {
        return clusteringColumnsRestrictions.boundsAsClustering(b, options);
    }

    public boolean isDisjunction()
    {
        return isDisjunction;
    }

    /**
     * Checks if the query returns a range of columns.
     *
     * @return <code>true</code> if the query returns a range of columns, <code>false</code> otherwise.
     */
    public boolean isColumnRange()
    {
        // For static compact tables we want to ignore the fake clustering column (note that if we weren't special casing,
        // this would mean a 'SELECT *' on a static compact table would query whole partitions, even though we'll only return
        // the static part as far as CQL is concerned. This is thus mostly an optimization to use the query-by-name path).
        int numberOfClusteringColumns = table.isStaticCompactTable() ? 0 : table.clusteringColumns().size();
        // it is a range query if it has at least one the column alias for which no relation is defined or is not EQ or IN.
        return clusteringColumnsRestrictions.size() < numberOfClusteringColumns
            || !clusteringColumnsRestrictions.hasOnlyEqualityRestrictions();
    }

    /**
     * Checks if the query needs to use filtering.
     *
     * @return <code>true</code> if the query need to use filtering, <code>false</code> otherwise.
     */
    public boolean needFiltering(TableMetadata table)
    {
        IndexRegistry indexRegistry = IndexRegistry.obtain(table);
        boolean hasClusteringColumnRestrictions = !clusteringColumnsRestrictions.isEmpty();
        boolean hasMultipleContains = nonPrimaryKeyRestrictions.hasMultipleContains();
        if (filterRestrictions.needFiltering(indexRegistry, indexHints, hasClusteringColumnRestrictions, hasMultipleContains))
            return true;

        for (StatementRestrictions child : children)
            if (child.needFiltering(table))
                return true;

        return false;
    }

    public boolean needsDisjunctionSupport(TableMetadata table)
    {
        boolean containsDisjunction = isDisjunction || !children.isEmpty();

        if (!containsDisjunction)
            return false;

        IndexRegistry indexRegistry = IndexRegistry.obtain(table);

        // Check if there's at least one index group that can be used AND supports disjunctions
        boolean hasIndexSupportingDisjunction = false;
        boolean hasIndexBeingUsed = false;

        for (Index.Group group : indexRegistry.listIndexGroups())
        {
            if (filterRestrictions.indexBeingUsed(group, indexHints))
            {
                hasIndexBeingUsed = true;
                if (group.supportsDisjunction())
                {
                    hasIndexSupportingDisjunction = true;
                    break;
                }
            }
        }

        // If we have at least one index that can be used and supports disjunctions, we're good
        if (hasIndexBeingUsed && !hasIndexSupportingDisjunction)
            return true;

        for (StatementRestrictions child : children)
            if (child.needsDisjunctionSupport(table))
                return true;

        return false;
    }

    private static Iterable<Restriction> allColumnRestrictions(ClusteringColumnRestrictions clusteringColumnsRestrictions, RestrictionSet nonPrimaryKeyRestrictions)
    {
        return Iterables.concat(clusteringColumnsRestrictions.restrictions(), nonPrimaryKeyRestrictions.restrictions());
    }

    private static Set<ColumnMetadata> getColumnsWithUnsupportedIndexRestrictions(TableMetadata table, IndexHints indexHints, Iterable<Restriction> restrictions)
    {
        IndexRegistry indexRegistry = IndexRegistry.obtain(table);
        if (indexRegistry.listIndexes().isEmpty())
            return Collections.emptySet();

        ImmutableSet.Builder<ColumnMetadata> builder = ImmutableSet.builder();

        for (Restriction restriction : restrictions)
        {
            if (!restriction.hasSupportingIndex(indexRegistry, indexHints))
            {
                for (Index index : indexRegistry.listNotExcludedIndexes(indexHints))
                {
                    // If a column restriction has an index which was not picked up by hasSupportingIndex, it means it's an unsupported restriction
                    for (ColumnMetadata column : restriction.getColumnDefs())
                    {
                        if (index.dependsOn(column))
                        {
                            if (index instanceof NoopIndex)
                                throw invalidRequest(((NoopIndex) index).getUnsupportedMessage());
                            builder.add(column);
                        }
                    }
                }
            }
        }

        return builder.build();
    }

    /**
     * Checks that all the primary key columns (partition key and clustering columns) are restricted by an equality
     * relation ('=' or 'IN').
     *
     * @return <code>true</code> if all the primary key columns are restricted by an equality relation.
     */
    public boolean hasAllPKColumnsRestrictedByEqualities()
    {
        return !isPartitionKeyRestrictionsOnToken()
                && !partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(table)
                && (partitionKeyRestrictions.hasOnlyEqualityRestrictions())
                && !hasUnrestrictedClusteringColumns()
                && (clusteringColumnsRestrictions.hasOnlyEqualityRestrictions());
    }

    /**
     * Checks if one of the restrictions applies to a regular column.
     * @return {@code true} if one of the restrictions applies to a regular column, {@code false} otherwise.
     */
    public boolean hasRegularColumnsRestrictions()
    {
        return hasRegularColumnsRestrictions;
    }

    /**
     * Checks if the query is a full partitions selection.
     * @return {@code true} if the query is a full partitions selection, {@code false} otherwise.
     */
    private boolean queriesFullPartitions()
    {
        return !hasClusteringColumnsRestrictions() && !hasRegularColumnsRestrictions();
    }

    /**
     * Determines if the query should return the static content when a partition without rows is returned (as a
     * result set row with null for all other regular columns.)
     *
     * @return {@code true} if the query should return the static content when a partition without rows is returned,
     * {@code false} otherwise.
     */
    public boolean returnStaticContentOnPartitionWithNoRows()
    {
        if (table.isStaticCompactTable())
            return true;

        // The general rationale is that if some rows are specifically selected by the query (have clustering or
        // regular columns restrictions), we ignore partitions that are empty outside of static content, but if it's
        // a full partition query, then we include that content.
        return queriesFullPartitions();
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    private static String allowFilteringMessage(ClientState state)
    {
        return Guardrails.allowFilteringEnabled.isEnabled(state)
               ? REQUIRES_ALLOW_FILTERING_MESSAGE
               : CANNOT_USE_ALLOW_FILTERING_MESSAGE;
    }
}
