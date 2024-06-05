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
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Joiner;
import com.google.common.collect.Streams;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.btree.BTreeSet;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * The restrictions corresponding to the relations specified on the where-clause of CQL query.
 */
public final class StatementRestrictions
{
    private static final String ALLOW_FILTERING_MESSAGE =
            "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. ";

    public static final String REQUIRES_ALLOW_FILTERING_MESSAGE = ALLOW_FILTERING_MESSAGE +
            "If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING";

    public static final String CANNOT_USE_ALLOW_FILTERING_MESSAGE = ALLOW_FILTERING_MESSAGE +
            "Executing this query despite the performance unpredictability with ALLOW FILTERING has been disabled " +
            "by the allow_filtering_enabled property in cassandra.yaml";

    public static final String ANN_REQUIRES_INDEX_MESSAGE = "ANN ordering by vector requires the column to be indexed";

    public static final String VECTOR_INDEXES_ANN_ONLY_MESSAGE = "Vector indexes only support ANN queries";

    public static final String ANN_ONLY_SUPPORTED_ON_VECTOR_MESSAGE = "ANN ordering is only supported on float vector indexes";

    public static final String ANN_REQUIRES_INDEXED_FILTERING_MESSAGE = "ANN ordering by vector requires all restricted column(s) to be indexed";

    /**
     * The type of statement
     */
    private final StatementType type;

    /**
     * The Column Family meta data
     */
    public final TableMetadata table;

    /**
     * Restrictions on partitioning columns
     */
    private PartitionKeyRestrictions partitionKeyRestrictions;

    /**
     * Restrictions on clustering columns
     */
    private ClusteringColumnRestrictions clusteringColumnsRestrictions;

    /**
     * Restriction on non-primary key columns (i.e. secondary index restrictions)
     */
    private RestrictionSet nonPrimaryKeyRestrictions;

    private Set<ColumnMetadata> notNullColumns;

    /**
     * The restrictions used to build the row filter
     */
    private final IndexRestrictions filterRestrictions = new IndexRestrictions();

    /**
     * <code>true</code> if the secondary index need to be queried, <code>false</code> otherwise
     */
    private boolean usesSecondaryIndexing;

    /**
     * Specify if the query will return a range of partition keys.
     */
    private boolean isKeyRange;

    /**
     * <code>true</code> if nonPrimaryKeyRestrictions contains restriction on a regular column,
     * <code>false</code> otherwise.
     */
    private boolean hasRegularColumnsRestrictions;

    /**
     * Creates a new empty <code>StatementRestrictions</code>.
     *
     * @param type the type of statement
     * @param table the column family meta data
     * @return a new empty <code>StatementRestrictions</code>.
     */
    public static StatementRestrictions empty(StatementType type, TableMetadata table)
    {
        return new StatementRestrictions(type, table, false);
    }

    private StatementRestrictions(StatementType type, TableMetadata table, boolean allowFiltering)
    {
        this.type = type;
        this.table = table;
        this.partitionKeyRestrictions = new PartitionKeyRestrictions(table.partitionKeyAsClusteringComparator());
        this.clusteringColumnsRestrictions = new ClusteringColumnRestrictions(table, allowFiltering);
        this.nonPrimaryKeyRestrictions = RestrictionSet.empty();
        this.notNullColumns = new HashSet<>();
    }

    public StatementRestrictions(ClientState state,
                                 StatementType type,
                                 TableMetadata table,
                                 WhereClause whereClause,
                                 VariableSpecifications boundNames,
                                 List<Ordering> orderings,
                                 boolean selectsOnlyStaticColumns,
                                 boolean allowFiltering,
                                 boolean forView)
    {
        this(state, type, table, whereClause, boundNames, orderings, selectsOnlyStaticColumns, type.allowUseOfSecondaryIndices(), allowFiltering, forView);
    }

    /*
     * We want to override allowUseOfSecondaryIndices flag from the StatementType for MV statements
     * to avoid initing the Keyspace and SecondaryIndexManager.
     */
    public StatementRestrictions(ClientState state,
                                 StatementType type,
                                 TableMetadata table,
                                 WhereClause whereClause,
                                 VariableSpecifications boundNames,
                                 List<Ordering> orderings,
                                 boolean selectsOnlyStaticColumns,
                                 boolean allowUseOfSecondaryIndices,
                                 boolean allowFiltering,
                                 boolean forView)
    {
        this(type, table, allowFiltering);

        final IndexRegistry indexRegistry = type.allowUseOfSecondaryIndices() && allowUseOfSecondaryIndices
                                            ? IndexRegistry.obtain(table)
                                            : null;
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
        for (Relation relation : whereClause.relations)
        {

            Operator operator = relation.operator();
            if (operator.requiresFilteringOrIndexingFor(ColumnMetadata.Kind.CLUSTERING) && (type.isUpdate() || type.isDelete()))
            {
                throw invalidRequest("Cannot use %s with %s", type, operator);
            }

            if (operator == Operator.IS_NOT)
            {
                if (!forView)
                    throw new InvalidRequestException("Unsupported restriction: " + relation);

                this.notNullColumns.addAll(relation.toRestriction(table, boundNames).columns());
            }
            else if (operator.requiresIndexing())
            {
                Restriction restriction = relation.toRestriction(table, boundNames);

                if (!type.allowUseOfSecondaryIndices() || !restriction.hasSupportingIndex(indexRegistry))
                    throw invalidRequest("%s restriction is only supported on properly " +
                                                        "indexed columns. %s is not valid.", operator, relation);

                addRestriction(restriction, indexRegistry);
            }
            else
            {
                addRestriction(relation.toRestriction(table, boundNames), indexRegistry);
            }
        }

        // ORDER BY clause.
        // Some indexes can be used for ordering.
        nonPrimaryKeyRestrictions = addOrderingRestrictions(orderings, nonPrimaryKeyRestrictions);

        hasRegularColumnsRestrictions = nonPrimaryKeyRestrictions.hasRestrictionFor(ColumnMetadata.Kind.REGULAR);

        boolean hasQueriableClusteringColumnIndex = false;
        boolean hasQueriableIndex = false;

        if (allowUseOfSecondaryIndices)
        {
            if (whereClause.containsCustomExpressions())
                processCustomIndexExpressions(whereClause.expressions, boundNames, indexRegistry);

            hasQueriableClusteringColumnIndex = clusteringColumnsRestrictions.hasSupportingIndex(indexRegistry);
            hasQueriableIndex = !filterRestrictions.getCustomIndexExpressions().isEmpty()
                    || hasQueriableClusteringColumnIndex
                    || partitionKeyRestrictions.hasSupportingIndex(indexRegistry)
                    || nonPrimaryKeyRestrictions.hasSupportingIndex(indexRegistry);
        }

        // At this point, the select statement if fully constructed, but we still have a few things to validate
        processPartitionKeyRestrictions(state, hasQueriableIndex, allowFiltering, forView);

        // Some but not all of the partition key columns have been specified;
        // hence we need turn these restrictions into a row filter.
        if (usesSecondaryIndexing || partitionKeyRestrictions.needFiltering())
            filterRestrictions.add(partitionKeyRestrictions);

        if (selectsOnlyStaticColumns && hasClusteringColumnsRestrictions())
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
            // sounds like you don't really understand what your are doing.
            if (type.isDelete() || type.isUpdate())
                throw invalidRequest("Invalid restrictions on clustering columns since the %s statement modifies only static columns",
                                     type);
            if (type.isSelect())
                throw invalidRequest("Cannot restrict clustering columns when selecting only static columns");
        }

        processClusteringColumnsRestrictions(hasQueriableIndex,
                                             selectsOnlyStaticColumns,
                                             forView,
                                             allowFiltering);

        // Covers indexes on the first clustering column (among others).
        if (isKeyRange && hasQueriableClusteringColumnIndex)
            usesSecondaryIndexing = true;

        if (usesSecondaryIndexing || clusteringColumnsRestrictions.needFiltering())
            filterRestrictions.add(clusteringColumnsRestrictions);

        // Even if usesSecondaryIndexing is false at this point, we'll still have to use one if
        // there is restrictions not covered by the PK.
        if (!nonPrimaryKeyRestrictions.isEmpty())
        {
            if (!type.allowNonPrimaryKeyInWhereClause())
            {
                Collection<ColumnIdentifier> nonPrimaryKeyColumns =
                        ColumnMetadata.toIdentifiers(nonPrimaryKeyRestrictions.columns());

                throw invalidRequest("Non PRIMARY KEY columns found in where clause: %s ",
                                     Joiner.on(", ").join(nonPrimaryKeyColumns));
            }

            Optional<SingleRestriction> annRestriction = Streams.stream(nonPrimaryKeyRestrictions)
                                                                .filter(SingleRestriction::isANN)
                                                                .findFirst();
            if (annRestriction.isPresent())
            {
                // If there is an ANN restriction then it must be for a vector<float, n> column, and it must have an index
                ColumnMetadata annColumn = annRestriction.get().firstColumn();

                if (!annColumn.type.isVector() || !(((VectorType<?>)annColumn.type).elementType instanceof FloatType))
                    throw invalidRequest(ANN_ONLY_SUPPORTED_ON_VECTOR_MESSAGE);
                if (indexRegistry == null || indexRegistry.listIndexes().stream().noneMatch(i -> i.dependsOn(annColumn)))
                    throw invalidRequest(ANN_REQUIRES_INDEX_MESSAGE);
                // We do not allow ANN queries using partition key restrictions that need filtering
                if (partitionKeyRestrictions.needFiltering())
                    throw invalidRequest(ANN_REQUIRES_INDEXED_FILTERING_MESSAGE);
                // We do not allow ANN query filtering using non-indexed columns
                List<ColumnMetadata> nonAnnColumns = Streams.stream(nonPrimaryKeyRestrictions)
                                                            .filter(r -> !r.isANN())
                                                            .map(SingleRestriction::firstColumn)
                                                            .collect(Collectors.toList());
                List<ColumnMetadata> clusteringColumns = clusteringColumnsRestrictions.columns();
                if (!nonAnnColumns.isEmpty() || !clusteringColumns.isEmpty())
                {
                    List<ColumnMetadata> nonIndexedColumns = Stream.concat(nonAnnColumns.stream(), clusteringColumns.stream())
                                                                   .filter(c -> indexRegistry.listIndexes().stream().noneMatch(i -> i.dependsOn(c)))
                                                                   .collect(Collectors.toList());
                    if (!nonIndexedColumns.isEmpty())
                    {
                        // restrictions on non-clustering columns, or clusterings that still need filtering, are invalid
                        if (!clusteringColumns.containsAll(nonIndexedColumns)
                                || partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents()
                                || clusteringColumnsRestrictions.needFiltering())
                            throw invalidRequest(StatementRestrictions.ANN_REQUIRES_INDEXED_FILTERING_MESSAGE);
                    }
                }
            }
            else
            {
                // We do not support indexed vector restrictions that are not part of an ANN ordering
                Optional<ColumnMetadata> vectorColumn = nonPrimaryKeyRestrictions.columns()
                                                                                 .stream()
                                                                                 .filter(c -> c.type.isVector())
                                                                                 .findFirst();
                if (vectorColumn.isPresent() && indexRegistry.listIndexes().stream().anyMatch(i -> i.dependsOn(vectorColumn.get())))
                    throw invalidRequest(StatementRestrictions.VECTOR_INDEXES_ANN_ONLY_MESSAGE);
            }

            if (hasQueriableIndex)
            {
                usesSecondaryIndexing = true;
            }
            else
            {
                if (!allowFiltering && requiresAllowFilteringIfNotSpecified())
                    throw invalidRequest(allowFilteringMessage(state));
            }

            filterRestrictions.add(nonPrimaryKeyRestrictions);
        }

        if (usesSecondaryIndexing)
            validateSecondaryIndexSelections();
    }

    public boolean requiresAllowFilteringIfNotSpecified()
    {
        if (!table.isVirtual())
            return true;

        VirtualTable tableNullable = VirtualKeyspaceRegistry.instance.getTableNullable(table.id);
        assert tableNullable != null;
        return !tableNullable.allowFilteringImplicitly();
    }

    private void addRestriction(Restriction restriction, IndexRegistry indexRegistry)
    {
        ColumnMetadata def = restriction.firstColumn();
        if (def.isPartitionKey())
            partitionKeyRestrictions = partitionKeyRestrictions.mergeWith(restriction);
        else if (def.isClusteringColumn())
            clusteringColumnsRestrictions = clusteringColumnsRestrictions.mergeWith(restriction, indexRegistry);
        else
            nonPrimaryKeyRestrictions = nonPrimaryKeyRestrictions.addRestriction((SingleRestriction) restriction);
    }

    public void addFunctionsTo(List<Function> functions)
    {
        partitionKeyRestrictions.addFunctionsTo(functions);
        clusteringColumnsRestrictions.addFunctionsTo(functions);
        nonPrimaryKeyRestrictions.addFunctionsTo(functions);
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
            for (ColumnMetadata def : r.columns())
                if (!def.isPrimaryKeyColumn())
                    columns.add(def);
        }

        if (includeNotNullRestrictions)
        {
            for (ColumnMetadata def : notNullColumns)
            {
                if (!def.isPrimaryKeyColumn())
                    columns.add(def);
            }
        }

        return columns;
    }

    /**
     * @return true if column is restricted by some restriction, false otherwise
     */
    public boolean isRestricted(ColumnMetadata column)
    {
        if (notNullColumns.contains(column))
            return true;

        return getRestrictions(column.kind).columns().contains(column);
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
        return this.isKeyRange;
    }

    /**
     * Checks if the specified column is restricted by an EQ restriction.
     *
     * @param column the column definition
     * @return <code>true</code> if the specified column is restricted by an EQ restiction, <code>false</code>
     * otherwise.
     */
    public boolean isColumnRestrictedByEq(ColumnMetadata column)
    {
        return getRestrictions(column.kind).isRestrictedByEquals(column);
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
        return getRestrictions(column.kind).isRestrictedByEqualsOrIN(column);
    }

    public boolean isTopK()
    {
        return nonPrimaryKeyRestrictions.hasAnn();
    }
    /**
     * Returns the <code>Restrictions</code> for the specified type of columns.
     *
     * @param kind the column type
     * @return the <code>Restrictions</code> for the specified type of columns
     */
    private Restrictions getRestrictions(ColumnMetadata.Kind kind)
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
        return this.usesSecondaryIndexing;
    }

    /**
     * This is a hack to push ordering down to indexes.
     * Indexes are selected based on RowFilter only, so we need to turn orderings into restrictions
     * so they end up in the row filter.
     *
     * @param orderings orderings from the select statement
     * @return the {@link RestrictionSet} with the added orderings
     */
    private RestrictionSet addOrderingRestrictions(List<Ordering> orderings, RestrictionSet restrictionSet)
    {
        List<Ordering> annOrderings = orderings.stream().filter(o -> o.expression.hasNonClusteredOrdering()).collect(Collectors.toList());

        if (annOrderings.size() > 1)
            throw new InvalidRequestException("Cannot specify more than one ANN ordering");
        else if (annOrderings.size() == 1)
        {
            if (orderings.size() > 1)
                throw new InvalidRequestException("ANN ordering does not support any other ordering");
            Ordering annOrdering = annOrderings.get(0);
            if (annOrdering.direction != Ordering.Direction.ASC)
                throw new InvalidRequestException("Descending ANN ordering is not supported");
            SingleRestriction restriction = annOrdering.expression.toRestriction();
            return restrictionSet.addRestriction(restriction);
        }
        return restrictionSet;
    }

    private void processPartitionKeyRestrictions(ClientState state, boolean hasQueriableIndex, boolean allowFiltering, boolean forView)
    {
        if (!type.allowPartitionKeyRanges())
        {
            checkFalse(partitionKeyRestrictions.isOnToken(),
                       "The token function cannot be used in WHERE clauses for %s statements", type);

            if (partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents())
                throw invalidRequest("Some partition key parts are missing: %s",
                                     Joiner.on(", ").join(getPartitionKeyUnrestrictedComponents()));

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

            if (partitionKeyRestrictions.isEmpty() && partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents())
            {
                isKeyRange = true;
                usesSecondaryIndexing = hasQueriableIndex;
            }

            // If there is a queriable index, no special condition is required on the other restrictions.
            // But we still need to know 2 things:
            // - If we don't have a queriable index, is the query ok
            // - Is it queriable without 2ndary index, which is always more efficient
            // If a component of the partition key is restricted by a relation, all preceding
            // components must have a EQ. Only the last partition key component can be in IN relation.
            if (partitionKeyRestrictions.needFiltering())
            {
                if (!allowFiltering && !forView && !hasQueriableIndex && requiresAllowFilteringIfNotSpecified())
                    throw new InvalidRequestException(allowFilteringMessage(state));

                isKeyRange = true;
                usesSecondaryIndexing = hasQueriableIndex;
            }
        }
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
     * Returns the partition key components that are not restricted.
     * @return the partition key components that are not restricted.
     */
    private Collection<ColumnIdentifier> getPartitionKeyUnrestrictedComponents()
    {
        List<ColumnMetadata> list = new ArrayList<>(table.partitionKeyColumns());
        list.removeAll(partitionKeyRestrictions.columns());
        return ColumnMetadata.toIdentifiers(list);
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
     * Processes the clustering column restrictions.
     *
     * @param hasQueriableIndex <code>true</code> if some of the queried data are indexed, <code>false</code> otherwise
     * @param selectsOnlyStaticColumns <code>true</code> if the selected or modified columns are all statics,
     * <code>false</code> otherwise.
     */
    private void processClusteringColumnsRestrictions(boolean hasQueriableIndex,
                                                      boolean selectsOnlyStaticColumns,
                                                      boolean forView,
                                                      boolean allowFiltering)
    {
        checkFalse(!type.allowClusteringColumnSlices() && clusteringColumnsRestrictions.hasSlice(),
                   "Slice restrictions are not supported on the clustering columns in %s statements", type);

        if (!type.allowClusteringColumnSlices()
            && (!table.isCompactTable() || (table.isCompactTable() && !hasClusteringColumnsRestrictions())))
        {
            if (!selectsOnlyStaticColumns && hasUnrestrictedClusteringColumns())
                throw invalidRequest("Some clustering keys are missing: %s",
                                     Joiner.on(", ").join(getUnrestrictedClusteringColumns()));
        }
        else
        {
            if (clusteringColumnsRestrictions.needsFilteringOrIndexing() && !hasQueriableIndex && !allowFiltering)
                throw invalidRequest("Clustering column restrictions require the use of secondary indices" +
                                     " or filtering for map-element restrictions and for the following operators: %s",
                                     Operator.operatorsRequiringFilteringOrIndexingFor(ColumnMetadata.Kind.CLUSTERING)
                                             .stream()
                                             .map(Operator::toString)
                                             .collect(Collectors.joining(", ")));

            if (hasClusteringColumnsRestrictions() && clusteringColumnsRestrictions.needFiltering())
            {
                if (hasQueriableIndex || forView)
                {
                    usesSecondaryIndexing = true;
                }
                else if (!allowFiltering)
                {
                    List<ColumnMetadata> clusteringColumns = table.clusteringColumns();
                    List<ColumnMetadata> restrictedColumns = new ArrayList<>(clusteringColumnsRestrictions.columns());

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
    }

    /**
     * Returns the clustering columns that are not restricted.
     * @return the clustering columns that are not restricted.
     */
    private Collection<ColumnIdentifier> getUnrestrictedClusteringColumns()
    {
        List<ColumnMetadata> missingClusteringColumns = new ArrayList<>(table.clusteringColumns());
        missingClusteringColumns.removeAll(new LinkedList<>(clusteringColumnsRestrictions.columns()));
        return ColumnMetadata.toIdentifiers(missingClusteringColumns);
    }

    /**
     * Checks if some clustering columns are not restricted.
     * @return <code>true</code> if some clustering columns are not restricted, <code>false</code> otherwise.
     */
    private boolean hasUnrestrictedClusteringColumns()
    {
        return table.clusteringColumns().size() != clusteringColumnsRestrictions.size();
    }

    private void processCustomIndexExpressions(List<CustomIndexExpression> expressions,
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

        Index index = indexRegistry.getIndex(table.indexes.get(expression.targetIndex.getName()).get());
        if (!index.getIndexMetadata().isCustom())
            throw IndexRestrictions.nonCustomIndexInExpression(expression.targetIndex);

        AbstractType<?> expressionType = index.customExpressionValueType();
        if (expressionType == null)
            throw IndexRestrictions.customExpressionNotSupported(expression.targetIndex);

        expression.prepareValue(table, expressionType, boundNames);

        filterRestrictions.add(expression);
    }

    public RowFilter getRowFilter(IndexRegistry indexRegistry, QueryOptions options)
    {
        if (filterRestrictions.isEmpty())
            return RowFilter.none();

        // If there is only one replica, we don't need reconciliation at any consistency level.
        boolean needsReconciliation = !table.isVirtual()
                                      && options.getConsistency().needsReconciliation()
                                      && Keyspace.open(table.keyspace).getReplicationStrategy().getReplicationFactor().allReplicas > 1;

        RowFilter filter = RowFilter.create(needsReconciliation);
        for (Restrictions restrictions : filterRestrictions.getRestrictions())
            restrictions.addToRowFilter(filter, indexRegistry, options);

        for (CustomIndexExpression expression : filterRestrictions.getCustomIndexExpressions())
            expression.addToRowFilter(filter, table, options);

        return filter;
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
        return partitionKeyRestrictions.values(table.partitioner, options, state);
    }

    /**
     * Returns the partition key bounds.
     *
     * @param options the query options
     * @return the partition key bounds
     */
    public AbstractBounds<PartitionPosition> getPartitionKeyBounds(QueryOptions options)
    {
        return partitionKeyRestrictions.bounds(table.partitioner, options);
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
     * Returns the clustering columns slices.
     *
     * @param options the query options
     * @return the clustering columns slices
     */
    public Slices getSlices(QueryOptions options)
    {
        return clusteringColumnsRestrictions.slices(options);
    }

    /**
     * Checks if the query returns a range of columns.
     *
     * @return <code>true</code> if the query returns a range of columns, <code>false</code> otherwise.
     */
    public boolean isColumnRange()
    {
        int numberOfClusteringColumns = table.clusteringColumns().size();
        if (table.isStaticCompactTable())
        {
            // For static compact tables we want to ignore the fake clustering column (note that if we weren't special casing,
            // this would mean a 'SELECT *' on a static compact table would query whole partitions, even though we'll only return
            // the static part as far as CQL is concerned. This is thus mostly an optimization to use the query-by-name path).
            numberOfClusteringColumns = 0;
        }

        // it is a range query if it has at least one the column alias for which no relation is defined or is not EQ or IN.
        return clusteringColumnsRestrictions.size() < numberOfClusteringColumns
            || !clusteringColumnsRestrictions.hasOnlyEqualityRestrictions();
    }

    /**
     * Checks if the query need to use filtering.
     * @return <code>true</code> if the query need to use filtering, <code>false</code> otherwise.
     */
    public boolean needFiltering(TableMetadata table)
    {
        IndexRegistry indexRegistry = IndexRegistry.obtain(table);
        if (filterRestrictions.needsFiltering(indexRegistry))
            return true;

        int numberOfRestrictions = filterRestrictions.getCustomIndexExpressions().size();
        for (Restrictions restrictions : filterRestrictions.getRestrictions())
            numberOfRestrictions += restrictions.size();

        return numberOfRestrictions == 0 && !clusteringColumnsRestrictions.isEmpty();
    }

    private void validateSecondaryIndexSelections()
    {
        checkFalse(keyIsInRelation(),
                   "Select on indexed columns and with IN clause for the PRIMARY KEY are not supported");
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
                && !partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents()
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
