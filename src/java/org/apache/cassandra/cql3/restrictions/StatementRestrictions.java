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
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * The restrictions corresponding to the relations specified on the where-clause of CQL query.
 */
public final class StatementRestrictions
{
    public static final String NO_INDEX_FOUND_MESSAGE =
        "No supported secondary index found for the non primary key columns restrictions";
    /**
     * The Column Family meta data
     */
    public final CFMetaData cfm;

    /**
     * Restrictions on partitioning columns
     */
    private PrimaryKeyRestrictions partitionKeyRestrictions;

    /**
     * Restrictions on clustering columns
     */
    private PrimaryKeyRestrictions clusteringColumnsRestrictions;

    /**
     * Restriction on non-primary key columns (i.e. secondary index restrictions)
     */
    private RestrictionSet nonPrimaryKeyRestrictions;

    /**
     * The restrictions used to build the row filter
     */
    private final List<Restrictions> indexRestrictions = new ArrayList<>();

    /**
     * <code>true</code> if the secondary index need to be queried, <code>false</code> otherwise
     */
    private boolean usesSecondaryIndexing;

    /**
     * Specify if the query will return a range of partition keys.
     */
    private boolean isKeyRange;

    /**
     * Creates a new empty <code>StatementRestrictions</code>.
     *
     * @param cfm the column family meta data
     * @return a new empty <code>StatementRestrictions</code>.
     */
    public static StatementRestrictions empty(CFMetaData cfm)
    {
        return new StatementRestrictions(cfm);
    }

    private StatementRestrictions(CFMetaData cfm)
    {
        this.cfm = cfm;
        this.partitionKeyRestrictions = new PrimaryKeyRestrictionSet(cfm.getKeyValidatorAsClusteringComparator(), true);
        this.clusteringColumnsRestrictions = new PrimaryKeyRestrictionSet(cfm.comparator, false);
        this.nonPrimaryKeyRestrictions = new RestrictionSet();
    }

    public StatementRestrictions(CFMetaData cfm,
                                 List<Relation> whereClause,
                                 VariableSpecifications boundNames,
                                 boolean selectsOnlyStaticColumns,
                                 boolean selectACollection,
                                 boolean useFiltering) throws InvalidRequestException
    {
        this(cfm);

        /*
         * WHERE clause. For a given entity, rules are:
         *   - EQ relation conflicts with anything else (including a 2nd EQ)
         *   - Can't have more than one LT(E) relation (resp. GT(E) relation)
         *   - IN relation are restricted to row keys (for now) and conflicts with anything else (we could
         *     allow two IN for the same entity but that doesn't seem very useful)
         *   - The value_alias cannot be restricted in any way (we don't support wide rows with indexed value
         *     in CQL so far)
         */
        for (Relation relation : whereClause)
            addRestriction(relation.toRestriction(cfm, boundNames));

        ColumnFamilyStore cfs = Keyspace.open(cfm.ksName).getColumnFamilyStore(cfm.cfName);
        SecondaryIndexManager secondaryIndexManager = cfs.indexManager;

        boolean hasQueriableClusteringColumnIndex = clusteringColumnsRestrictions.hasSupportingIndex(secondaryIndexManager);
        boolean hasQueriableIndex = hasQueriableClusteringColumnIndex
                || partitionKeyRestrictions.hasSupportingIndex(secondaryIndexManager)
                || nonPrimaryKeyRestrictions.hasSupportingIndex(secondaryIndexManager);

        // At this point, the select statement if fully constructed, but we still have a few things to validate
        processPartitionKeyRestrictions(hasQueriableIndex);

        // Some but not all of the partition key columns have been specified;
        // hence we need turn these restrictions into a row filter.
        if (usesSecondaryIndexing)
            indexRestrictions.add(partitionKeyRestrictions);

        checkFalse(selectsOnlyStaticColumns && hasClusteringColumnsRestriction(),
                   "Cannot restrict clustering columns when selecting only static columns");

        processClusteringColumnsRestrictions(hasQueriableIndex, selectACollection);

        // Covers indexes on the first clustering column (among others).
        if (isKeyRange && hasQueriableClusteringColumnIndex)
            usesSecondaryIndexing = true;

        usesSecondaryIndexing = usesSecondaryIndexing || clusteringColumnsRestrictions.isContains();

        if (usesSecondaryIndexing)
            indexRestrictions.add(clusteringColumnsRestrictions);

        // Even if usesSecondaryIndexing is false at this point, we'll still have to use one if
        // there is restrictions not covered by the PK.
        if (!nonPrimaryKeyRestrictions.isEmpty())
        {
            if (hasQueriableIndex)
                usesSecondaryIndexing = true;
            else if (!useFiltering)
                throw new InvalidRequestException(NO_INDEX_FOUND_MESSAGE);

            indexRestrictions.add(nonPrimaryKeyRestrictions);
        }

        if (usesSecondaryIndexing)
            validateSecondaryIndexSelections(selectsOnlyStaticColumns);
    }

    private void addRestriction(Restriction restriction) throws InvalidRequestException
    {
        if (restriction.isMultiColumn())
            clusteringColumnsRestrictions = clusteringColumnsRestrictions.mergeWith(restriction);
        else if (restriction.isOnToken())
            partitionKeyRestrictions = partitionKeyRestrictions.mergeWith(restriction);
        else
            addSingleColumnRestriction((SingleColumnRestriction) restriction);
    }

    public Iterable<Function> getFunctions()
    {
        return Iterables.concat(partitionKeyRestrictions.getFunctions(),
                                clusteringColumnsRestrictions.getFunctions(),
                                nonPrimaryKeyRestrictions.getFunctions());
    }

    private void addSingleColumnRestriction(SingleColumnRestriction restriction) throws InvalidRequestException
    {
        ColumnDefinition def = restriction.columnDef;
        if (def.isPartitionKey())
            partitionKeyRestrictions = partitionKeyRestrictions.mergeWith(restriction);
        else if (def.isClusteringColumn())
            clusteringColumnsRestrictions = clusteringColumnsRestrictions.mergeWith(restriction);
        else
            nonPrimaryKeyRestrictions = nonPrimaryKeyRestrictions.addRestriction(restriction);
    }

    /**
     * Returns the non-PK column that are restricted.
     */
    public Set<ColumnDefinition> nonPKRestrictedColumns()
    {
        Set<ColumnDefinition> columns = new HashSet<>();
        for (Restrictions r : indexRestrictions)
            for (ColumnDefinition def : r.getColumnDefs())
                if (!def.isPrimaryKeyColumn())
                    columns.add(def);
        return columns;
    }

    /**
     * Checks if the restrictions on the partition key is an IN restriction.
     *
     * @return <code>true</code> the restrictions on the partition key is an IN restriction, <code>false</code>
     * otherwise.
     */
    public boolean keyIsInRelation()
    {
        return partitionKeyRestrictions.isIN();
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
     * Checks if the secondary index need to be queried.
     *
     * @return <code>true</code> if the secondary index need to be queried, <code>false</code> otherwise.
     */
    public boolean usesSecondaryIndexing()
    {
        return this.usesSecondaryIndexing;
    }

    private void processPartitionKeyRestrictions(boolean hasQueriableIndex) throws InvalidRequestException
    {
        // If there is a queriable index, no special condition are required on the other restrictions.
        // But we still need to know 2 things:
        // - If we don't have a queriable index, is the query ok
        // - Is it queriable without 2ndary index, which is always more efficient
        // If a component of the partition key is restricted by a relation, all preceding
        // components must have a EQ. Only the last partition key component can be in IN relation.
        if (partitionKeyRestrictions.isOnToken())
            isKeyRange = true;

        if (hasPartitionKeyUnrestrictedComponents())
        {
            if (!partitionKeyRestrictions.isEmpty())
            {
                if (!hasQueriableIndex)
                    throw invalidRequest("Partition key parts: %s must be restricted as other parts are",
                                         Joiner.on(", ").join(getPartitionKeyUnrestrictedComponents()));
            }

            isKeyRange = true;
            usesSecondaryIndexing = hasQueriableIndex;
        }
    }

    /**
     * Checks if the partition key has some unrestricted components.
     * @return <code>true</code> if the partition key has some unrestricted components, <code>false</code> otherwise.
     */
    private boolean hasPartitionKeyUnrestrictedComponents()
    {
        return partitionKeyRestrictions.size() <  cfm.partitionKeyColumns().size();
    }

    public boolean hasPartitionKeyRestrictions()
    {
        return !partitionKeyRestrictions.isEmpty();
    }

    /**
     * Returns the partition key components that are not restricted.
     * @return the partition key components that are not restricted.
     */
    private List<ColumnIdentifier> getPartitionKeyUnrestrictedComponents()
    {
        List<ColumnDefinition> list = new ArrayList<>(cfm.partitionKeyColumns());
        list.removeAll(partitionKeyRestrictions.getColumnDefs());
        return ColumnDefinition.toIdentifiers(list);
    }

    /**
     * Processes the clustering column restrictions.
     *
     * @param hasQueriableIndex <code>true</code> if some of the queried data are indexed, <code>false</code> otherwise
     * @param selectACollection <code>true</code> if the query should return a collection column
     * @throws InvalidRequestException if the request is invalid
     */
    private void processClusteringColumnsRestrictions(boolean hasQueriableIndex,
                                                      boolean selectACollection) throws InvalidRequestException
    {
        checkFalse(clusteringColumnsRestrictions.isIN() && selectACollection,
                   "Cannot restrict clustering columns by IN relations when a collection is selected by the query");
        checkFalse(clusteringColumnsRestrictions.isContains() && !hasQueriableIndex,
                   "Cannot restrict clustering columns by a CONTAINS relation without a secondary index");

        if (hasClusteringColumnsRestriction())
        {
            List<ColumnDefinition> clusteringColumns = cfm.clusteringColumns();
            List<ColumnDefinition> restrictedColumns = new LinkedList<>(clusteringColumnsRestrictions.getColumnDefs());

            for (int i = 0, m = restrictedColumns.size(); i < m; i++)
            {
                ColumnDefinition clusteringColumn = clusteringColumns.get(i);
                ColumnDefinition restrictedColumn = restrictedColumns.get(i);

                if (!clusteringColumn.equals(restrictedColumn))
                {
                    checkTrue(hasQueriableIndex,
                              "PRIMARY KEY column \"%s\" cannot be restricted as preceding column \"%s\" is not restricted",
                              restrictedColumn.name,
                              clusteringColumn.name);

                    usesSecondaryIndexing = true; // handle gaps and non-keyrange cases.
                    break;
                }
            }
        }

        if (clusteringColumnsRestrictions.isContains())
            usesSecondaryIndexing = true;
    }

    public RowFilter getRowFilter(SecondaryIndexManager indexManager, QueryOptions options) throws InvalidRequestException
    {
        if (indexRestrictions.isEmpty())
            return RowFilter.NONE;

        RowFilter filter = RowFilter.create();
        for (Restrictions restrictions : indexRestrictions)
            restrictions.addRowFilterTo(filter, indexManager, options);

        return filter;
    }

    /**
     * Returns the partition keys for which the data is requested.
     *
     * @param options the query options
     * @return the partition keys for which the data is requested.
     * @throws InvalidRequestException if the partition keys cannot be retrieved
     */
    public Collection<ByteBuffer> getPartitionKeys(final QueryOptions options) throws InvalidRequestException
    {
        return partitionKeyRestrictions.values(options);
    }

    /**
     * Returns the specified bound of the partition key.
     *
     * @param b the boundary type
     * @param options the query options
     * @return the specified bound of the partition key
     * @throws InvalidRequestException if the boundary cannot be retrieved
     */
    private ByteBuffer getPartitionKeyBound(Bound b, QueryOptions options) throws InvalidRequestException
    {
        // Deal with unrestricted partition key components (special-casing is required to deal with 2i queries on the
        // first component of a composite partition key).
        if (hasPartitionKeyUnrestrictedComponents())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        // We deal with IN queries for keys in other places, so we know buildBound will return only one result
        return partitionKeyRestrictions.bounds(b, options).get(0);
    }

    /**
     * Returns the partition key bounds.
     *
     * @param options the query options
     * @return the partition key bounds
     * @throws InvalidRequestException if the query is invalid
     */
    public AbstractBounds<PartitionPosition> getPartitionKeyBounds(QueryOptions options) throws InvalidRequestException
    {
        IPartitioner p = cfm.partitioner;

        if (partitionKeyRestrictions.isOnToken())
        {
            return getPartitionKeyBoundsForTokenRestrictions(p, options);
        }

        return getPartitionKeyBounds(p, options);
    }

    private AbstractBounds<PartitionPosition> getPartitionKeyBounds(IPartitioner p,
                                                              QueryOptions options) throws InvalidRequestException
    {
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
                                                                                          throws InvalidRequestException
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

    private Token getTokenBound(Bound b, QueryOptions options, IPartitioner p) throws InvalidRequestException
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
    public boolean hasClusteringColumnsRestriction()
    {
        return !clusteringColumnsRestrictions.isEmpty();
    }

    /**
     * Returns the requested clustering columns.
     *
     * @param options the query options
     * @return the requested clustering columns
     * @throws InvalidRequestException if the query is not valid
     */
    public NavigableSet<Clustering> getClusteringColumns(QueryOptions options) throws InvalidRequestException
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
     * @throws InvalidRequestException if the request is not valid
     */
    public NavigableSet<Slice.Bound> getClusteringColumnsBounds(Bound b, QueryOptions options) throws InvalidRequestException
    {
        return clusteringColumnsRestrictions.boundsAsClustering(b, options);
    }

    /**
     * Checks if the bounds (start or end) of the clustering columns are inclusive.
     *
     * @param bound the bound type
     * @return <code>true</code> if the bounds (start or end) of the clustering columns are inclusive,
     * <code>false</code> otherwise
     */
    public boolean areRequestedBoundsInclusive(Bound bound)
    {
        return clusteringColumnsRestrictions.isInclusive(bound);
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
        // it is a range query if it has at least one the column alias for which no relation is defined or is not EQ.
        return clusteringColumnsRestrictions.size() < numberOfClusteringColumns
            || (!clusteringColumnsRestrictions.isEQ() && !clusteringColumnsRestrictions.isIN());
    }

    /**
     * Checks if the query need to use filtering.
     * @return <code>true</code> if the query need to use filtering, <code>false</code> otherwise.
     */
    public boolean needFiltering()
    {
        int numberOfRestrictedColumns = 0;
        for (Restrictions restrictions : indexRestrictions)
            numberOfRestrictedColumns += restrictions.size();

        return numberOfRestrictedColumns > 1
                || (numberOfRestrictedColumns == 0 && !clusteringColumnsRestrictions.isEmpty())
                || (numberOfRestrictedColumns != 0
                        && nonPrimaryKeyRestrictions.hasMultipleContains());
    }

    private void validateSecondaryIndexSelections(boolean selectsOnlyStaticColumns) throws InvalidRequestException
    {
        checkFalse(keyIsInRelation(),
                   "Select on indexed columns and with IN clause for the PRIMARY KEY are not supported");
        // When the user only select static columns, the intent is that we don't query the whole partition but just
        // the static parts. But 1) we don't have an easy way to do that with 2i and 2) since we don't support index on
        // static columns
        // so far, 2i means that you've restricted a non static column, so the query is somewhat non-sensical.
        checkFalse(selectsOnlyStaticColumns, "Queries using 2ndary indexes don't support selecting only static columns");
    }
}
