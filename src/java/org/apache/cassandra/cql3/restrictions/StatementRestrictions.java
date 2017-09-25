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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ColumnDefinition.Kind;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * The restrictions corresponding to the relations specified on the where-clause of CQL query.
 */
public final class StatementRestrictions
{
    public static final String REQUIRES_ALLOW_FILTERING_MESSAGE =
            "Cannot execute this query as it might involve data filtering and " +
            "thus may have unpredictable performance. If you want to execute " +
            "this query despite the performance unpredictability, use ALLOW FILTERING";

    /**
     * The type of statement
     */
    private final StatementType type;

    /**
     * The Column Family meta data
     */
    public final CFMetaData cfm;

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

    private Set<ColumnDefinition> notNullColumns;

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
     * @param cfm the column family meta data
     * @return a new empty <code>StatementRestrictions</code>.
     */
    public static StatementRestrictions empty(StatementType type, CFMetaData cfm)
    {
        return new StatementRestrictions(type, cfm, false);
    }

    private StatementRestrictions(StatementType type, CFMetaData cfm, boolean allowFiltering)
    {
        this.type = type;
        this.cfm = cfm;
        this.partitionKeyRestrictions = new PartitionKeySingleRestrictionSet(cfm.getKeyValidatorAsClusteringComparator());
        this.clusteringColumnsRestrictions = new ClusteringColumnRestrictions(cfm, allowFiltering);
        this.nonPrimaryKeyRestrictions = new RestrictionSet();
        this.notNullColumns = new HashSet<>();
    }

    public StatementRestrictions(StatementType type,
                                 CFMetaData cfm,
                                 WhereClause whereClause,
                                 VariableSpecifications boundNames,
                                 boolean selectsOnlyStaticColumns,
                                 boolean selectsComplexColumn,
                                 boolean allowFiltering,
                                 boolean forView)
    {
        this(type, cfm, allowFiltering);

        ColumnFamilyStore cfs;
        SecondaryIndexManager secondaryIndexManager = null;

        if (type.allowUseOfSecondaryIndices())
        {
            cfs = Keyspace.open(cfm.ksName).getColumnFamilyStore(cfm.cfName);
            secondaryIndexManager = cfs.indexManager;
        }

        /*
         * WHERE clause. For a given entity, rules are:
         *   - EQ relation conflicts with anything else (including a 2nd EQ)
         *   - Can't have more than one LT(E) relation (resp. GT(E) relation)
         *   - IN relation are restricted to row keys (for now) and conflicts with anything else (we could
         *     allow two IN for the same entity but that doesn't seem very useful)
         *   - The value_alias cannot be restricted in any way (we don't support wide rows with indexed value
         *     in CQL so far)
         */
        for (Relation relation : whereClause.relations)
        {
            if (relation.operator() == Operator.IS_NOT)
            {
                if (!forView)
                    throw new InvalidRequestException("Unsupported restriction: " + relation);

                for (ColumnDefinition def : relation.toRestriction(cfm, boundNames).getColumnDefs())
                    this.notNullColumns.add(def);
            }
            else if (relation.isLIKE())
            {
                Restriction restriction = relation.toRestriction(cfm, boundNames);

                if (!type.allowUseOfSecondaryIndices() || !restriction.hasSupportingIndex(secondaryIndexManager))
                    throw new InvalidRequestException(String.format("LIKE restriction is only supported on properly " +
                                                                    "indexed columns. %s is not valid.",
                                                                    relation.toString()));

                addRestriction(restriction);
            }
            else
            {
                if (cfm.isSuper() && cfm.isDense() && !relation.onToken())
                    addRestriction(relation.toSuperColumnAdapter().toRestriction(cfm, boundNames));
                else
                    addRestriction(relation.toRestriction(cfm, boundNames));
            }
        }

        hasRegularColumnsRestrictions = nonPrimaryKeyRestrictions.hasRestrictionFor(Kind.REGULAR);

        boolean hasQueriableClusteringColumnIndex = false;
        boolean hasQueriableIndex = false;

        if (type.allowUseOfSecondaryIndices())
        {
            if (whereClause.containsCustomExpressions())
                processCustomIndexExpressions(whereClause.expressions, boundNames, secondaryIndexManager);

            hasQueriableClusteringColumnIndex = clusteringColumnsRestrictions.hasSupportingIndex(secondaryIndexManager);
            hasQueriableIndex = !filterRestrictions.getCustomIndexExpressions().isEmpty()
                    || hasQueriableClusteringColumnIndex
                    || partitionKeyRestrictions.hasSupportingIndex(secondaryIndexManager)
                    || nonPrimaryKeyRestrictions.hasSupportingIndex(secondaryIndexManager);
        }

        // At this point, the select statement if fully constructed, but we still have a few things to validate
        processPartitionKeyRestrictions(hasQueriableIndex, allowFiltering, forView);

        // Some but not all of the partition key columns have been specified;
        // hence we need turn these restrictions into a row filter.
        if (usesSecondaryIndexing || partitionKeyRestrictions.needFiltering(cfm))
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
                                             selectsComplexColumn,
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
                        ColumnDefinition.toIdentifiers(nonPrimaryKeyRestrictions.getColumnDefs());

                throw invalidRequest("Non PRIMARY KEY columns found in where clause: %s ",
                                     Joiner.on(", ").join(nonPrimaryKeyColumns));
            }
            if (hasQueriableIndex)
            {
                usesSecondaryIndexing = true;
            }
            else if (!allowFiltering && !cfm.isSuper())
            {
                throw invalidRequest(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
            }

            checkFalse(clusteringColumnsRestrictions.isEmpty() && cfm.isSuper(),
                       "Filtering is not supported on SuperColumn tables");

            filterRestrictions.add(nonPrimaryKeyRestrictions);
        }

        if (usesSecondaryIndexing)
            validateSecondaryIndexSelections(selectsOnlyStaticColumns);
    }

    private void addRestriction(Restriction restriction)
    {
        ColumnDefinition def = restriction.getFirstColumn();
        if (def.isPartitionKey())
            partitionKeyRestrictions = partitionKeyRestrictions.mergeWith(restriction);
        else if (def.isClusteringColumn())
            clusteringColumnsRestrictions = clusteringColumnsRestrictions.mergeWith(restriction);
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
    public Set<ColumnDefinition> nonPKRestrictedColumns(boolean includeNotNullRestrictions)
    {
        Set<ColumnDefinition> columns = new HashSet<>();
        for (Restrictions r : filterRestrictions.getRestrictions())
        {
            for (ColumnDefinition def : r.getColumnDefs())
                if (!def.isPrimaryKeyColumn())
                    columns.add(def);
        }

        if (includeNotNullRestrictions)
        {
            for (ColumnDefinition def : notNullColumns)
            {
                if (!def.isPrimaryKeyColumn())
                    columns.add(def);
            }
        }

        return columns;
    }

    /**
     * @return the set of columns that have an IS NOT NULL restriction on them
     */
    public Set<ColumnDefinition> notNullColumns()
    {
        return notNullColumns;
    }

    /**
     * @return true if column is restricted by some restriction, false otherwise
     */
    public boolean isRestricted(ColumnDefinition column)
    {
        if (notNullColumns.contains(column))
            return true;

        return getRestrictions(column.kind).getColumnDefs().contains(column);
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
     * @param columnDef the column definition
     * @return <code>true</code> if the specified column is restricted by an EQ restiction, <code>false</code>
     * otherwise.
     */
    public boolean isColumnRestrictedByEq(ColumnDefinition columnDef)
    {
        Set<Restriction> restrictions = getRestrictions(columnDef.kind).getRestrictions(columnDef);
        return restrictions.stream()
                           .filter(SingleRestriction.class::isInstance)
                           .anyMatch(p -> ((SingleRestriction) p).isEQ());
    }

    /**
     * Returns the <code>Restrictions</code> for the specified type of columns.
     *
     * @param kind the column type
     * @return the <code>Restrictions</code> for the specified type of columns
     */
    private Restrictions getRestrictions(ColumnDefinition.Kind kind)
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

    private void processPartitionKeyRestrictions(boolean hasQueriableIndex, boolean allowFiltering, boolean forView)
    {
        if (!type.allowPartitionKeyRanges())
        {
            checkFalse(partitionKeyRestrictions.isOnToken(),
                       "The token function cannot be used in WHERE clauses for %s statements", type);

            if (partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(cfm))
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

            if (partitionKeyRestrictions.isEmpty() && partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(cfm))
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
            if (partitionKeyRestrictions.needFiltering(cfm))
            {
                if (!allowFiltering && !forView && !hasQueriableIndex)
                    throw new InvalidRequestException(REQUIRES_ALLOW_FILTERING_MESSAGE);

                if (partitionKeyRestrictions.hasIN())
                    throw new InvalidRequestException("IN restrictions are not supported when the query involves filtering");

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
        List<ColumnDefinition> list = new ArrayList<>(cfm.partitionKeyColumns());
        list.removeAll(partitionKeyRestrictions.getColumnDefs());
        return ColumnDefinition.toIdentifiers(list);
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
     * @param selectsComplexColumn <code>true</code> if the query should return a collection column
     */
    private void processClusteringColumnsRestrictions(boolean hasQueriableIndex,
                                                      boolean selectsOnlyStaticColumns,
                                                      boolean selectsComplexColumn,
                                                      boolean forView,
                                                      boolean allowFiltering)
    {
        checkFalse(!type.allowClusteringColumnSlices() && clusteringColumnsRestrictions.hasSlice(),
                   "Slice restrictions are not supported on the clustering columns in %s statements", type);

        if (!type.allowClusteringColumnSlices()
               && (!cfm.isCompactTable() || (cfm.isCompactTable() && !hasClusteringColumnsRestrictions())))
        {
            if (!selectsOnlyStaticColumns && hasUnrestrictedClusteringColumns())
                throw invalidRequest("Some clustering keys are missing: %s",
                                     Joiner.on(", ").join(getUnrestrictedClusteringColumns()));
        }
        else
        {
            checkFalse(clusteringColumnsRestrictions.hasIN() && selectsComplexColumn,
                       "Cannot restrict clustering columns by IN relations when a collection is selected by the query");
            checkFalse(clusteringColumnsRestrictions.hasContains() && !hasQueriableIndex && !allowFiltering,
                       "Clustering columns can only be restricted with CONTAINS with a secondary index or filtering");

            if (hasClusteringColumnsRestrictions() && clusteringColumnsRestrictions.needFiltering())
            {
                if (hasQueriableIndex || forView)
                {
                    usesSecondaryIndexing = true;
                }
                else if (!allowFiltering)
                {
                    List<ColumnDefinition> clusteringColumns = cfm.clusteringColumns();
                    List<ColumnDefinition> restrictedColumns = new LinkedList<>(clusteringColumnsRestrictions.getColumnDefs());

                    for (int i = 0, m = restrictedColumns.size(); i < m; i++)
                    {
                        ColumnDefinition clusteringColumn = clusteringColumns.get(i);
                        ColumnDefinition restrictedColumn = restrictedColumns.get(i);

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
        List<ColumnDefinition> missingClusteringColumns = new ArrayList<>(cfm.clusteringColumns());
        missingClusteringColumns.removeAll(new LinkedList<>(clusteringColumnsRestrictions.getColumnDefs()));
        return ColumnDefinition.toIdentifiers(missingClusteringColumns);
    }

    /**
     * Checks if some clustering columns are not restricted.
     * @return <code>true</code> if some clustering columns are not restricted, <code>false</code> otherwise.
     */
    private boolean hasUnrestrictedClusteringColumns()
    {
        return cfm.clusteringColumns().size() != clusteringColumnsRestrictions.size();
    }

    private void processCustomIndexExpressions(List<CustomIndexExpression> expressions,
                                               VariableSpecifications boundNames,
                                               SecondaryIndexManager indexManager)
    {
        if (!MessagingService.instance().areAllNodesAtLeast30())
            throw new InvalidRequestException("Please upgrade all nodes to at least 3.0 before using custom index expressions");

        if (expressions.size() > 1)
            throw new InvalidRequestException(IndexRestrictions.MULTIPLE_EXPRESSIONS);

        CustomIndexExpression expression = expressions.get(0);

        CFName cfName = expression.targetIndex.getCfName();
        if (cfName.hasKeyspace()
            && !expression.targetIndex.getKeyspace().equals(cfm.ksName))
            throw IndexRestrictions.invalidIndex(expression.targetIndex, cfm);

        if (cfName.getColumnFamily() != null && !cfName.getColumnFamily().equals(cfm.cfName))
            throw IndexRestrictions.invalidIndex(expression.targetIndex, cfm);

        if (!cfm.getIndexes().has(expression.targetIndex.getIdx()))
            throw IndexRestrictions.indexNotFound(expression.targetIndex, cfm);

        Index index = indexManager.getIndex(cfm.getIndexes().get(expression.targetIndex.getIdx()).get());

        if (!index.getIndexMetadata().isCustom())
            throw IndexRestrictions.nonCustomIndexInExpression(expression.targetIndex);

        AbstractType<?> expressionType = index.customExpressionValueType();
        if (expressionType == null)
            throw IndexRestrictions.customExpressionNotSupported(expression.targetIndex);

        expression.prepareValue(cfm, expressionType, boundNames);

        filterRestrictions.add(expression);
    }

    public RowFilter getRowFilter(SecondaryIndexManager indexManager, QueryOptions options)
    {
        if (filterRestrictions.isEmpty())
            return RowFilter.NONE;

        RowFilter filter = RowFilter.create();
        for (Restrictions restrictions : filterRestrictions.getRestrictions())
            restrictions.addRowFilterTo(filter, indexManager, options);

        for (CustomIndexExpression expression : filterRestrictions.getCustomIndexExpressions())
            expression.addToRowFilter(filter, cfm, options);

        return filter;
    }

    /**
     * Returns the partition keys for which the data is requested.
     *
     * @param options the query options
     * @return the partition keys for which the data is requested.
     */
    public List<ByteBuffer> getPartitionKeys(final QueryOptions options)
    {
        return partitionKeyRestrictions.values(options);
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
        IPartitioner p = cfm.partitioner;

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
        if (partitionKeyRestrictions.needFiltering(cfm))
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
     * Returns the requested clustering columns.
     *
     * @param options the query options
     * @return the requested clustering columns
     */
    public NavigableSet<Clustering> getClusteringColumns(QueryOptions options)
    {
        // If this is a names command and the table is a static compact one, then as far as CQL is concerned we have
        // only a single row which internally correspond to the static parts. In which case we want to return an empty
        // set (since that's what ClusteringIndexNamesFilter expects).
        if (cfm.isStaticCompactTable())
            return BTreeSet.empty(cfm.comparator);

        return clusteringColumnsRestrictions.valuesAsClustering(options);
    }

    /**
     * Returns the bounds (start or end) of the clustering columns.
     *
     * @param b the bound type
     * @param options the query options
     * @return the bounds (start or end) of the clustering columns
     */
    public NavigableSet<ClusteringBound> getClusteringColumnsBounds(Bound b, QueryOptions options)
    {
        return clusteringColumnsRestrictions.boundsAsClustering(b, options);
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
        int numberOfClusteringColumns = cfm.isStaticCompactTable() ? 0 : cfm.clusteringColumns().size();
        // it is a range query if it has at least one the column alias for which no relation is defined or is not EQ or IN.
        return clusteringColumnsRestrictions.size() < numberOfClusteringColumns
            || !clusteringColumnsRestrictions.hasOnlyEqualityRestrictions();
    }

    /**
     * Checks if the query need to use filtering.
     * @return <code>true</code> if the query need to use filtering, <code>false</code> otherwise.
     */
    public boolean needFiltering()
    {
        int numberOfRestrictions = filterRestrictions.getCustomIndexExpressions().size();
        for (Restrictions restrictions : filterRestrictions.getRestrictions())
            numberOfRestrictions += restrictions.size();

        return numberOfRestrictions > 1
                || (numberOfRestrictions == 0 && !clusteringColumnsRestrictions.isEmpty())
                || (numberOfRestrictions != 0
                        && nonPrimaryKeyRestrictions.hasMultipleContains());
    }

    private void validateSecondaryIndexSelections(boolean selectsOnlyStaticColumns)
    {
        checkFalse(keyIsInRelation(),
                   "Select on indexed columns and with IN clause for the PRIMARY KEY are not supported");
        // When the user only select static columns, the intent is that we don't query the whole partition but just
        // the static parts. But 1) we don't have an easy way to do that with 2i and 2) since we don't support index on
        // static columns
        // so far, 2i means that you've restricted a non static column, so the query is somewhat non-sensical.
        checkFalse(selectsOnlyStaticColumns, "Queries using 2ndary indexes don't support selecting only static columns");
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
                && !partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(cfm)
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

    private SuperColumnCompatibility.SuperColumnRestrictions cached;
    public SuperColumnCompatibility.SuperColumnRestrictions getSuperColumnRestrictions()
    {
        assert cfm.isSuper() && cfm.isDense();

        if (cached == null)
            cached = new SuperColumnCompatibility.SuperColumnRestrictions(Iterators.concat(clusteringColumnsRestrictions.iterator(),
                                                                                           nonPrimaryKeyRestrictions.iterator()));
        return cached;
    }
}
