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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.SingleColumnRestriction.Contains;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 */
public class SelectStatement implements CQLStatement
{
    private static final Logger logger = LoggerFactory.getLogger(SelectStatement.class);

    private static final int DEFAULT_COUNT_PAGE_SIZE = 10000;

    /**
     * In the current version a query containing duplicate values in an IN restriction on the partition key will
     * cause the same record to be returned multiple time. This behavior will be changed in 3.0 but until then
     * we will log a warning the first time this problem occurs.
     */
    private static volatile boolean HAS_LOGGED_WARNING_FOR_IN_RESTRICTION_WITH_DUPLICATES;

    private final int boundTerms;
    public final CFMetaData cfm;
    public final Parameters parameters;
    private final Selection selection;
    private final Term limit;

    /** Restrictions on partitioning columns */
    private final Restriction[] keyRestrictions;

    /** Restrictions on clustering columns */
    private final Restriction[] columnRestrictions;

    /** Restrictions on non-primary key columns (i.e. secondary index restrictions) */
    private final Map<ColumnIdentifier, Restriction> metadataRestrictions = new HashMap<ColumnIdentifier, Restriction>();

    // The map keys are the name of the columns that must be converted into IndexExpressions if a secondary index need
    // to be used. The value specify if the column has an index that can be used to for the relation in which the column
    // is specified.
    private final Map<ColumnDefinition, Boolean> restrictedColumns = new HashMap<ColumnDefinition, Boolean>();
    private Restriction.Slice sliceRestriction;

    private boolean isReversed;
    private boolean onToken;
    private boolean isKeyRange;
    private boolean keyIsInRelation;
    private boolean usesSecondaryIndexing;

    private Map<ColumnIdentifier, Integer> orderingIndexes;

    private boolean selectsStaticColumns;
    private boolean selectsOnlyStaticColumns;

    // Used by forSelection below
    private static final Parameters defaultParameters = new Parameters(Collections.<ColumnIdentifier.Raw, Boolean>emptyMap(), false, false, null, false);

    private static final Predicate<ColumnDefinition> isStaticFilter = new Predicate<ColumnDefinition>()
    {
        public boolean apply(ColumnDefinition def)
        {
            return def.isStatic();
        }
    };

    public SelectStatement(CFMetaData cfm, int boundTerms, Parameters parameters, Selection selection, Term limit)
    {
        this.cfm = cfm;
        this.boundTerms = boundTerms;
        this.selection = selection;
        this.keyRestrictions = new Restriction[cfm.partitionKeyColumns().size()];
        this.columnRestrictions = new Restriction[cfm.clusteringColumns().size()];
        this.parameters = parameters;
        this.limit = limit;

        // Now gather a few info on whether we should bother with static columns or not for this statement
        initStaticColumnsInfo();
    }

    private void initStaticColumnsInfo()
    {
        if (!cfm.hasStaticColumns())
            return;

        // If it's a wildcard, we do select static but not only them
        if (selection.isWildcard())
        {
            selectsStaticColumns = true;
            return;
        }

        // Otherwise, check the selected columns
        selectsStaticColumns = !Iterables.isEmpty(Iterables.filter(selection.getColumns(), isStaticFilter));
        selectsOnlyStaticColumns = true;
        for (ColumnDefinition def : selection.getColumns())
        {
            if (def.kind != ColumnDefinition.Kind.PARTITION_KEY && def.kind != ColumnDefinition.Kind.STATIC)
            {
                selectsOnlyStaticColumns = false;
                break;
            }
        }
    }

    // Creates a simple select based on the given selection.
    // Note that the results select statement should not be used for actual queries, but only for processing already
    // queried data through processColumnFamily.
    static SelectStatement forSelection(CFMetaData cfm, Selection selection)
    {
        return new SelectStatement(cfm, 0, defaultParameters, selection, null);
    }

    public ResultSet.Metadata getResultMetadata()
    {
        return parameters.isCount
             ? ResultSet.makeCountMetadata(keyspace(), columnFamily(), parameters.countAlias)
             : selection.getResultMetadata();
    }

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.SELECT);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        // Nothing to do, all validation has been done by RawStatement.prepare()
    }

    public ResultMessage.Rows execute(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        ConsistencyLevel cl = options.getConsistency();
        if (cl == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        cl.validateForRead(keyspace());

        int limit = getLimit(options);
        long now = System.currentTimeMillis();
        Pageable command = getPageableCommand(options, limit, now);

        int pageSize = options.getPageSize();
        // A count query will never be paged for the user, but we always page it internally to avoid OOM.
        // If we user provided a pageSize we'll use that to page internally (because why not), otherwise we use our default
        // Note that if there are some nodes in the cluster with a version less than 2.0, we can't use paging (CASSANDRA-6707).
        if (parameters.isCount && pageSize <= 0)
            pageSize = DEFAULT_COUNT_PAGE_SIZE;

        if (pageSize <= 0 || command == null || !QueryPagers.mayNeedPaging(command, pageSize))
        {
            return execute(command, options, limit, now, state);
        }
        else
        {
            QueryPager pager = QueryPagers.pager(command, cl, state.getClientState(), options.getPagingState());
            if (parameters.isCount)
                return pageCountQuery(pager, options, pageSize, now, limit);

            // We can't properly do post-query ordering if we page (see #6722)
            if (needsPostQueryOrdering())
                throw new InvalidRequestException("Cannot page queries with both ORDER BY and a IN restriction on the partition key; you must either remove the "
                                                + "ORDER BY or the IN and sort client side, or disable paging for this query");

            List<Row> page = pager.fetchPage(pageSize);
            ResultMessage.Rows msg = processResults(page, options, limit, now);

            if (!pager.isExhausted())
                msg.result.metadata.setHasMorePages(pager.state());

            return msg;
        }
    }

    private Pageable getPageableCommand(QueryOptions options, int limit, long now) throws RequestValidationException
    {
        int limitForQuery = updateLimitForQuery(limit);
        if (isKeyRange || usesSecondaryIndexing)
            return getRangeCommand(options, limitForQuery, now);

        List<ReadCommand> commands = getSliceCommands(options, limitForQuery, now);
        return commands == null ? null : new Pageable.ReadCommands(commands, limitForQuery);
    }

    public Pageable getPageableCommand(QueryOptions options) throws RequestValidationException
    {
        return getPageableCommand(options, getLimit(options), System.currentTimeMillis());
    }

    private ResultMessage.Rows execute(Pageable command, QueryOptions options, int limit, long now, QueryState state) throws RequestValidationException, RequestExecutionException
    {
        List<Row> rows;
        if (command == null)
        {
            rows = Collections.<Row>emptyList();
        }
        else
        {
            rows = command instanceof Pageable.ReadCommands
                 ? StorageProxy.read(((Pageable.ReadCommands)command).commands, options.getConsistency(), state.getClientState())
                 : StorageProxy.getRangeSlice((RangeSliceCommand)command, options.getConsistency());
        }

        return processResults(rows, options, limit, now);
    }

    private ResultMessage.Rows pageCountQuery(QueryPager pager, QueryOptions options, int pageSize, long now, int limit) throws RequestValidationException, RequestExecutionException
    {
        int count = 0;
        while (!pager.isExhausted())
        {
            int maxLimit = pager.maxRemaining();
            logger.debug("New maxLimit for paged count query is {}", maxLimit);
            ResultSet rset = process(pager.fetchPage(pageSize), options, maxLimit, now);
            count += rset.rows.size();
        }

        // We sometimes query one more result than the user limit asks to handle exclusive bounds with compact tables (see updateLimitForQuery).
        // So do make sure the count is not greater than what the user asked for.
        ResultSet result = ResultSet.makeCountResult(keyspace(), columnFamily(), Math.min(count, limit), parameters.countAlias);
        return new ResultMessage.Rows(result);
    }

    public ResultMessage.Rows processResults(List<Row> rows, QueryOptions options, int limit, long now) throws RequestValidationException
    {
        // Even for count, we need to process the result as it'll group some column together in sparse column families
        ResultSet rset = process(rows, options, limit, now);
        rset = parameters.isCount ? rset.makeCountResult(parameters.countAlias) : rset;
        return new ResultMessage.Rows(rset);
    }

    static List<Row> readLocally(String keyspaceName, List<ReadCommand> cmds)
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);
        List<Row> rows = new ArrayList<Row>(cmds.size());
        for (ReadCommand cmd : cmds)
            rows.add(cmd.getRow(keyspace));
        return rows;
    }

    public ResultMessage.Rows executeInternal(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        int limit = getLimit(options);
        long now = System.currentTimeMillis();
        Pageable command = getPageableCommand(options, limit, now);
        List<Row> rows = command == null
                       ? Collections.<Row>emptyList()
                       : (command instanceof Pageable.ReadCommands
                          ? readLocally(keyspace(), ((Pageable.ReadCommands)command).commands)
                          : ((RangeSliceCommand)command).executeLocally());

        return processResults(rows, options, limit, now);
    }

    public ResultSet process(List<Row> rows) throws InvalidRequestException
    {
        assert !parameters.isCount; // not yet needed
        QueryOptions options = QueryOptions.DEFAULT;
        return process(rows, options, getLimit(options), System.currentTimeMillis());
    }

    public String keyspace()
    {
        return cfm.ksName;
    }

    public String columnFamily()
    {
        return cfm.cfName;
    }

    private List<ReadCommand> getSliceCommands(QueryOptions options, int limit, long now) throws RequestValidationException
    {
        Collection<ByteBuffer> keys = getKeys(options);
        if (keys.isEmpty()) // in case of IN () for (the last column of) the partition key.
            return null;

        List<ReadCommand> commands = new ArrayList<>(keys.size());

        IDiskAtomFilter filter = makeFilter(options, limit);
        if (filter == null)
            return null;

        // Note that we use the total limit for every key, which is potentially inefficient.
        // However, IN + LIMIT is not a very sensible choice.
        for (ByteBuffer key : keys)
        {
            QueryProcessor.validateKey(key);
            // We should not share the slice filter amongst the commands (hence the cloneShallow), due to
            // SliceQueryFilter not being immutable due to its columnCounter used by the lastCounted() method
            // (this is fairly ugly and we should change that but that's probably not a tiny refactor to do that cleanly)
            commands.add(ReadCommand.create(keyspace(), ByteBufferUtil.clone(key), columnFamily(), now, filter.cloneShallow()));
        }

        return commands;
    }

    private RangeSliceCommand getRangeCommand(QueryOptions options, int limit, long now) throws RequestValidationException
    {
        IDiskAtomFilter filter = makeFilter(options, limit);
        if (filter == null)
            return null;

        List<IndexExpression> expressions = getValidatedIndexExpressions(options);
        // The LIMIT provided by the user is the number of CQL row he wants returned.
        // We want to have getRangeSlice to count the number of columns, not the number of keys.
        AbstractBounds<RowPosition> keyBounds = getKeyBounds(options);
        return keyBounds == null
             ? null
             : new RangeSliceCommand(keyspace(), columnFamily(), now,  filter, keyBounds, expressions, limit, !parameters.isDistinct, false);
    }

    private AbstractBounds<RowPosition> getKeyBounds(QueryOptions options) throws InvalidRequestException
    {
        IPartitioner p = StorageService.getPartitioner();

        if (onToken)
        {
            Token startToken = getTokenBound(Bound.START, options, p);
            Token endToken = getTokenBound(Bound.END, options, p);

            boolean includeStart = includeKeyBound(Bound.START);
            boolean includeEnd = includeKeyBound(Bound.END);

            /*
             * If we ask SP.getRangeSlice() for (token(200), token(200)], it will happily return the whole ring.
             * However, wrapping range doesn't really make sense for CQL, and we want to return an empty result
             * in that case (CASSANDRA-5573). So special case to create a range that is guaranteed to be empty.
             *
             * In practice, we want to return an empty result set if either startToken > endToken, or both are
             * equal but one of the bound is excluded (since [a, a] can contains something, but not (a, a], [a, a)
             * or (a, a)). Note though that in the case where startToken or endToken is the minimum token, then
             * this special case rule should not apply.
             */
            int cmp = startToken.compareTo(endToken);
            if (!startToken.isMinimum() && !endToken.isMinimum() && (cmp > 0 || (cmp == 0 && (!includeStart || !includeEnd))))
                return null;

            RowPosition start = includeStart ? startToken.minKeyBound() : startToken.maxKeyBound();
            RowPosition end = includeEnd ? endToken.maxKeyBound() : endToken.minKeyBound();

            return new Range<RowPosition>(start, end);
        }
        else
        {
            ByteBuffer startKeyBytes = getKeyBound(Bound.START, options);
            ByteBuffer finishKeyBytes = getKeyBound(Bound.END, options);

            RowPosition startKey = RowPosition.ForKey.get(startKeyBytes, p);
            RowPosition finishKey = RowPosition.ForKey.get(finishKeyBytes, p);

            if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
                return null;

            if (includeKeyBound(Bound.START))
            {
                return includeKeyBound(Bound.END)
                     ? new Bounds<RowPosition>(startKey, finishKey)
                     : new IncludingExcludingBounds<RowPosition>(startKey, finishKey);
            }
            else
            {
                return includeKeyBound(Bound.END)
                     ? new Range<RowPosition>(startKey, finishKey)
                     : new ExcludingBounds<RowPosition>(startKey, finishKey);
            }
        }
    }

    private ColumnSlice makeStaticSlice()
    {
        // Note: we could use staticPrefix.start() for the start bound, but EMPTY gives us the
        // same effect while saving a few CPU cycles.
        return isReversed
             ? new ColumnSlice(cfm.comparator.staticPrefix().end(), Composites.EMPTY)
             : new ColumnSlice(Composites.EMPTY, cfm.comparator.staticPrefix().end());
    }

    private IDiskAtomFilter makeFilter(QueryOptions options, int limit)
    throws InvalidRequestException
    {
        int toGroup = cfm.comparator.isDense() ? -1 : cfm.clusteringColumns().size();
        if (parameters.isDistinct)
        {
            // For distinct, we only care about fetching the beginning of each partition. If we don't have
            // static columns, we in fact only care about the first cell, so we query only that (we don't "group").
            // If we do have static columns, we do need to fetch the first full group (to have the static columns values).

            // See the comments on IGNORE_TOMBSTONED_PARTITIONS and CASSANDRA-8490 for why we use a special value for
            // DISTINCT queries on the partition key only.
            toGroup = selectsStaticColumns ? toGroup : SliceQueryFilter.IGNORE_TOMBSTONED_PARTITIONS;
            return new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 1, toGroup);
        }
        else if (isColumnRange())
        {
            List<Composite> startBounds = getRequestedBound(Bound.START, options);
            List<Composite> endBounds = getRequestedBound(Bound.END, options);
            assert startBounds.size() == endBounds.size();

            // Handles fetching static columns. Note that for 2i, the filter is just used to restrict
            // the part of the index to query so adding the static slice would be useless and confusing.
            // For 2i, static columns are retrieve in CompositesSearcher with each index hit.
            ColumnSlice staticSlice = selectsStaticColumns && !usesSecondaryIndexing
                                    ? makeStaticSlice()
                                    : null;

            // The case where startBounds == 1 is common enough that it's worth optimizing
            if (startBounds.size() == 1)
            {
                ColumnSlice slice = new ColumnSlice(startBounds.get(0), endBounds.get(0));
                if (slice.isAlwaysEmpty(cfm.comparator, isReversed))
                    return staticSlice == null ? null : sliceFilter(staticSlice, limit, toGroup);

                if (staticSlice == null)
                    return sliceFilter(slice, limit, toGroup);

                if (isReversed)
                    return slice.includes(cfm.comparator.reverseComparator(), staticSlice.start)
                            ? sliceFilter(new ColumnSlice(slice.start, staticSlice.finish), limit, toGroup)
                            : sliceFilter(new ColumnSlice[]{ slice, staticSlice }, limit, toGroup);
                else
                    return slice.includes(cfm.comparator, staticSlice.finish)
                            ? sliceFilter(new ColumnSlice(staticSlice.start, slice.finish), limit, toGroup)
                            : sliceFilter(new ColumnSlice[]{ staticSlice, slice }, limit, toGroup);
            }

            List<ColumnSlice> l = new ArrayList<ColumnSlice>(startBounds.size());
            for (int i = 0; i < startBounds.size(); i++)
            {
                ColumnSlice slice = new ColumnSlice(startBounds.get(i), endBounds.get(i));
                if (!slice.isAlwaysEmpty(cfm.comparator, isReversed))
                    l.add(slice);
            }

            if (l.isEmpty())
                return staticSlice == null ? null : sliceFilter(staticSlice, limit, toGroup);
            if (staticSlice == null)
                return sliceFilter(l.toArray(new ColumnSlice[l.size()]), limit, toGroup);

            // The slices should not overlap. We know the slices built from startBounds/endBounds don't, but if there is
            // a static slice, it could overlap with the 2nd slice. Check for it and correct if that's the case
            ColumnSlice[] slices;
            if (isReversed)
            {
                if (l.get(l.size() - 1).includes(cfm.comparator.reverseComparator(), staticSlice.start))
                {
                    slices = l.toArray(new ColumnSlice[l.size()]);
                    slices[slices.length-1] = new ColumnSlice(slices[slices.length-1].start, Composites.EMPTY);
                }
                else
                {
                    slices = l.toArray(new ColumnSlice[l.size()+1]);
                    slices[slices.length-1] = staticSlice;
                }
            }
            else
            {
                if (l.get(0).includes(cfm.comparator, staticSlice.finish))
                {
                    slices = new ColumnSlice[l.size()];
                    slices[0] = new ColumnSlice(Composites.EMPTY, l.get(0).finish);
                    for (int i = 1; i < l.size(); i++)
                        slices[i] = l.get(i);
                }
                else
                {
                    slices = new ColumnSlice[l.size()+1];
                    slices[0] = staticSlice;
                    for (int i = 0; i < l.size(); i++)
                        slices[i+1] = l.get(i);
                }
            }
            return sliceFilter(slices, limit, toGroup);
        }
        else
        {
            SortedSet<CellName> cellNames = getRequestedColumns(options);
            if (cellNames == null) // in case of IN () for the last column of the key
                return null;
            QueryProcessor.validateCellNames(cellNames, cfm.comparator);
            return new NamesQueryFilter(cellNames, true);
        }
    }

    private SliceQueryFilter sliceFilter(ColumnSlice slice, int limit, int toGroup)
    {
        return sliceFilter(new ColumnSlice[]{ slice }, limit, toGroup);
    }

    private SliceQueryFilter sliceFilter(ColumnSlice[] slices, int limit, int toGroup)
    {
        assert ColumnSlice.validateSlices(slices, cfm.comparator, isReversed) : String.format("Invalid slices: " + Arrays.toString(slices) + (isReversed ? " (reversed)" : ""));
        return new SliceQueryFilter(slices, isReversed, limit, toGroup);
    }

    private int getLimit(QueryOptions options) throws InvalidRequestException
    {
        int l = Integer.MAX_VALUE;
        if (limit != null)
        {
            ByteBuffer b = limit.bindAndGet(options);
            if (b == null)
                throw new InvalidRequestException("Invalid null value of limit");

            try
            {
                Int32Type.instance.validate(b);
                l = Int32Type.instance.compose(b);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Invalid limit value");
            }
        }

        if (l <= 0)
            throw new InvalidRequestException("LIMIT must be strictly positive");

        return l;
    }

    private int updateLimitForQuery(int limit)
    {
        // Internally, we don't support exclusive bounds for slices. Instead, we query one more element if necessary
        // and exclude it later (in processColumnFamily)
        return sliceRestriction != null && (!sliceRestriction.isInclusive(Bound.START) || !sliceRestriction.isInclusive(Bound.END)) && limit != Integer.MAX_VALUE
             ? limit + 1
             : limit;
    }

    private Collection<ByteBuffer> getKeys(final QueryOptions options) throws InvalidRequestException
    {
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        CBuilder builder = cfm.getKeyValidatorAsCType().builder();
        for (ColumnDefinition def : cfm.partitionKeyColumns())
        {
            Restriction r = keyRestrictions[def.position()];
            assert r != null && !r.isSlice();

            List<ByteBuffer> values = r.values(options);

            if (builder.remainingCount() == 1)
            {
                if (values.size() > 1 && !HAS_LOGGED_WARNING_FOR_IN_RESTRICTION_WITH_DUPLICATES  && containsDuplicates(values))
                {
                    // This approach does not fully prevent race conditions but it is not a big deal.
                    HAS_LOGGED_WARNING_FOR_IN_RESTRICTION_WITH_DUPLICATES = true;
                    logger.warn("SELECT queries with IN restrictions on the partition key containing duplicate values will return duplicate rows.");
                }

                for (ByteBuffer val : values)
                {
                    if (val == null)
                        throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", def.name));
                    keys.add(builder.buildWith(val).toByteBuffer());
                }
            }
            else
            {
                // Note: for backward compatibility reasons, we let INs with 1 value slide
                if (values.size() != 1)
                    throw new InvalidRequestException("IN is only supported on the last column of the partition key");
                ByteBuffer val = values.get(0);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", def.name));
                builder.add(val);
            }
        }
        return keys;
    }

    /**
     * Checks if the specified list contains duplicate values.
     *
     * @param values the values to check
     * @return <code>true</code> if the specified list contains duplicate values, <code>false</code> otherwise.
     */
    private static boolean containsDuplicates(List<ByteBuffer> values)
    {
        return new HashSet<>(values).size() < values.size();
    }

    private ByteBuffer getKeyBound(Bound b, QueryOptions options) throws InvalidRequestException
    {
        // Deal with unrestricted partition key components (special-casing is required to deal with 2i queries on the first
        // component of a composite partition key).
        for (int i = 0; i < keyRestrictions.length; i++)
            if (keyRestrictions[i] == null)
                return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        // We deal with IN queries for keys in other places, so we know buildBound will return only one result
        return buildBound(b, cfm.partitionKeyColumns(), keyRestrictions, false, cfm.getKeyValidatorAsCType(), options).get(0).toByteBuffer();
    }

    private Token getTokenBound(Bound b, QueryOptions options, IPartitioner p) throws InvalidRequestException
    {
        assert onToken;

        Restriction restriction = keyRestrictions[0];

        assert !restriction.isMultiColumn() : "Unexpectedly got a multi-column restriction on a partition key for a range query";
        SingleColumnRestriction keyRestriction = (SingleColumnRestriction)restriction;

        ByteBuffer value;
        if (keyRestriction.isEQ())
        {
            value = keyRestriction.values(options).get(0);
        }
        else
        {
            SingleColumnRestriction.Slice slice = (SingleColumnRestriction.Slice)keyRestriction;
            if (!slice.hasBound(b))
                return p.getMinimumToken();

            value = slice.bound(b, options);
        }

        if (value == null)
            throw new InvalidRequestException("Invalid null token value");
        return p.getTokenFactory().fromByteArray(value);
    }

    private boolean includeKeyBound(Bound b)
    {
        for (Restriction r : keyRestrictions)
        {
            if (r == null)
                return true;
            else if (r.isSlice())
            {
                assert !r.isMultiColumn() : "Unexpectedly got multi-column restriction on partition key";
                return ((SingleColumnRestriction.Slice)r).isInclusive(b);
            }
        }
        // All equality
        return true;
    }

    private boolean isColumnRange()
    {
        // Due to CASSANDRA-5762, we always do a slice for CQL3 tables (not dense, composite).
        // Static CF (non dense but non composite) never entails a column slice however
        if (!cfm.comparator.isDense())
            return cfm.comparator.isCompound();

        // Otherwise (i.e. for compact table where we don't have a row marker anyway and thus don't care about CASSANDRA-5762),
        // it is a range query if it has at least one the column alias for which no relation is defined or is not EQ.
        for (Restriction r : columnRestrictions)
        {
            if (r == null || r.isSlice())
                return true;
        }
        return false;
    }

    private SortedSet<CellName> getRequestedColumns(QueryOptions options) throws InvalidRequestException
    {
        // Note: getRequestedColumns don't handle static columns, but due to CASSANDRA-5762
        // we always do a slice for CQL3 tables, so it's ok to ignore them here
        assert !isColumnRange();

        CBuilder builder = cfm.comparator.prefixBuilder();

        Iterator<ColumnDefinition> columnIter = cfm.clusteringColumns().iterator();
        while (columnIter.hasNext())
        {
            ColumnDefinition def = columnIter.next();
            Restriction r = columnRestrictions[def.position()];
            assert r != null && !r.isSlice();

            if (r.isEQ())
            {
                List<ByteBuffer> values = r.values(options);
                if (r.isMultiColumn())
                {
                    for (int i = 0, m = values.size(); i < m; i++)
                    {
                        ByteBuffer val = values.get(i);

                        if (i != 0)
                            columnIter.next();

                        if (val == null)
                            throw new InvalidRequestException(String.format("Invalid null value for clustering key part %s", def.name));
                        builder.add(val);
                    }
                }
                else
                {
                    ByteBuffer val = r.values(options).get(0);
                    if (val == null)
                        throw new InvalidRequestException(String.format("Invalid null value for clustering key part %s", def.name));
                    builder.add(val);
                }
            }
            else
            {
                if (!r.isMultiColumn())
                {
                    List<ByteBuffer> values = r.values(options);
                    // We have a IN, which we only support for the last column.
                    // If compact, just add all values and we're done. Otherwise,
                    // for each value of the IN, creates all the columns corresponding to the selection.
                    if (values.isEmpty())
                        return null;
                    SortedSet<CellName> columns = new TreeSet<CellName>(cfm.comparator);
                    Iterator<ByteBuffer> iter = values.iterator();
                    while (iter.hasNext())
                    {
                        ByteBuffer val = iter.next();
                        if (val == null)
                            throw new InvalidRequestException(String.format("Invalid null value for clustering key part %s", def.name));

                        Composite prefix = builder.buildWith(val);
                        columns.addAll(addSelectedColumns(prefix));
                    }
                    return columns;
                }

                // we have a multi-column IN restriction
                List<List<ByteBuffer>> values = ((MultiColumnRestriction.IN) r).splitValues(options);
                TreeSet<CellName> inValues = new TreeSet<>(cfm.comparator);
                for (List<ByteBuffer> components : values)
                {
                    for (int i = 0; i < components.size(); i++)
                        if (components.get(i) == null)
                            throw new InvalidRequestException("Invalid null value in condition for column "
                                    + cfm.clusteringColumns().get(i + def.position()).name);

                    Composite prefix = builder.buildWith(components);
                    inValues.addAll(addSelectedColumns(prefix));
                }
                return inValues;
            }
        }

        return addSelectedColumns(builder.build());
    }

    private SortedSet<CellName> addSelectedColumns(Composite prefix)
    {
        if (cfm.comparator.isDense())
        {
            return FBUtilities.singleton(cfm.comparator.create(prefix, null), cfm.comparator);
        }
        else
        {
            // Collections require doing a slice query because a given collection is a
            // non-know set of columns, so we shouldn't get there
            assert !selectACollection();

            SortedSet<CellName> columns = new TreeSet<CellName>(cfm.comparator);

            // We need to query the selected column as well as the marker
            // column (for the case where the row exists but has no columns outside the PK)
            // Two exceptions are "static CF" (non-composite non-compact CF) and "super CF"
            // that don't have marker and for which we must query all columns instead
            if (cfm.comparator.isCompound() && !cfm.isSuper())
            {
                // marker
                columns.add(cfm.comparator.rowMarker(prefix));

                // selected columns
                for (ColumnDefinition def : selection.getColumns())
                    if (def.kind == ColumnDefinition.Kind.REGULAR || def.kind == ColumnDefinition.Kind.STATIC)
                        columns.add(cfm.comparator.create(prefix, def));
            }
            else
            {
                // We now that we're not composite so we can ignore static columns
                for (ColumnDefinition def : cfm.regularColumns())
                    columns.add(cfm.comparator.create(prefix, def));
            }
            return columns;
        }
    }

    /** Returns true if a non-frozen collection is selected, false otherwise. */
    private boolean selectACollection()
    {
        if (!cfm.comparator.hasCollections())
            return false;

        for (ColumnDefinition def : selection.getColumns())
        {
            if (def.type.isCollection() && def.type.isMultiCell())
                return true;
        }

        return false;
    }

    @VisibleForTesting
    static List<Composite> buildBound(Bound bound,
                                      List<ColumnDefinition> defs,
                                      Restriction[] restrictions,
                                      boolean isReversed,
                                      CType type,
                                      QueryOptions options) throws InvalidRequestException
    {
        CBuilder builder = type.builder();

        // The end-of-component of composite doesn't depend on whether the
        // component type is reversed or not (i.e. the ReversedType is applied
        // to the component comparator but not to the end-of-component itself),
        // it only depends on whether the slice is reversed
        Bound eocBound = isReversed ? Bound.reverse(bound) : bound;
        for (int i = 0, m = defs.size(); i < m; i++)
        {
            ColumnDefinition def = defs.get(i);

            // In a restriction, we always have Bound.START < Bound.END for the "base" comparator.
            // So if we're doing a reverse slice, we must inverse the bounds when giving them as start and end of the slice filter.
            // But if the actual comparator itself is reversed, we must inversed the bounds too.
            Bound b = isReversed == isReversedType(def) ? bound : Bound.reverse(bound);
            Restriction r = restrictions[def.position()];
            if (isNullRestriction(r, b) || !r.canEvaluateWithSlices())
            {
                // There wasn't any non EQ relation on that key, we select all records having the preceding component as prefix.
                // For composites, if there was preceding component and we're computing the end, we must change the last component
                // End-Of-Component, otherwise we would be selecting only one record.
                Composite prefix = builder.build();
                return Collections.singletonList(eocBound == Bound.END ? prefix.end() : prefix.start());
            }
            if (r.isSlice())
            {
                if (r.isMultiColumn())
                {
                    MultiColumnRestriction.Slice slice = (MultiColumnRestriction.Slice) r;

                    if (!slice.hasBound(b))
                    {
                        Composite prefix = builder.build();
                        return Collections.singletonList(builder.remainingCount() > 0 && eocBound == Bound.END
                                ? prefix.end()
                                : prefix);
                    }

                    List<ByteBuffer> vals = slice.componentBounds(b, options);

                    for (int j = 0, n = vals.size(); j < n; j++)
                        addValue(builder, defs.get(i + j), vals.get(j)) ;
                }
                else
                {
                    builder.add(getSliceValue(r, b, options));
                }
                Operator relType = ((Restriction.Slice)r).getRelation(eocBound, b);
                return Collections.singletonList(builder.build().withEOC(eocForRelation(relType)));
            }

            if (r.isIN())
            {
                // The IN query might not have listed the values in comparator order, so we need to re-sort
                // the bounds lists to make sure the slices works correctly (also, to avoid duplicates).
                TreeSet<Composite> inValues = new TreeSet<>(isReversed ? type.reverseComparator() : type);

                if (r.isMultiColumn())
                {
                    List<List<ByteBuffer>> splitInValues = ((MultiColumnRestriction.IN) r).splitValues(options);

                    for (List<ByteBuffer> components : splitInValues)
                    {
                        for (int j = 0; j < components.size(); j++)
                            if (components.get(j) == null)
                                throw new InvalidRequestException("Invalid null value in condition for column " + defs.get(i + j).name);

                        Composite prefix = builder.buildWith(components);
                        inValues.add(builder.remainingCount() == 0 ? prefix : addEOC(prefix, eocBound));
                    }
                    return new ArrayList<>(inValues);
                }

                List<ByteBuffer> values = r.values(options);
                if (values.size() != 1)
                {
                    // IN query, we only support it on the clustering columns
                    assert def.position() == defs.size() - 1;
                    for (ByteBuffer val : values)
                    {
                        if (val == null)
                            throw new InvalidRequestException(String.format("Invalid null value in condition for column %s",
                                                                            def.name));
                        Composite prefix = builder.buildWith(val);
                        // See below for why this
                        inValues.add(builder.remainingCount() == 0 ? prefix : addEOC(prefix, eocBound));
                    }
                    return new ArrayList<>(inValues);
                }
            }

            List<ByteBuffer> values = r.values(options);

            if (r.isMultiColumn())
            {
                for (int j = 0; j < values.size(); j++)
                    addValue(builder, defs.get(i + j), values.get(j));
                i += values.size() - 1; // skips the processed columns
            }
            else
            {
                addValue(builder, def, values.get(0));
            }
        }
        // Means no relation at all or everything was an equal
        // Note: if the builder is "full", there is no need to use the end-of-component bit. For columns selection,
        // it would be harmless to do it. However, we use this method got the partition key too. And when a query
        // with 2ndary index is done, and with the the partition provided with an EQ, we'll end up here, and in that
        // case using the eoc would be bad, since for the random partitioner we have no guarantee that
        // prefix.end() will sort after prefix (see #5240).
        Composite prefix = builder.build();
        return Collections.singletonList(builder.remainingCount() == 0 ? prefix : addEOC(prefix, eocBound));
    }

    /**
     * Adds an EOC to the specified Composite.
     *
     * @param composite the composite
     * @param eocBound the EOC bound
     * @return a new <code>Composite</code> with the EOC corresponding to the eocBound
     */
    private static Composite addEOC(Composite composite, Bound eocBound)
    {
        return eocBound == Bound.END ? composite.end() : composite.start();
    }

    /**
     * Adds the specified value to the specified builder
     *
     * @param builder the CBuilder to which the value must be added
     * @param def the column associated to the value
     * @param value the value to add
     * @throws InvalidRequestException if the value is null
     */
    private static void addValue(CBuilder builder, ColumnDefinition def, ByteBuffer value) throws InvalidRequestException
    {
        if (value == null)
            throw new InvalidRequestException(String.format("Invalid null value in condition for column %s", def.name));
        builder.add(value);
    }

    private static Composite.EOC eocForRelation(Operator op)
    {
        switch (op)
        {
            case LT:
                // < X => using startOf(X) as finish bound
                return Composite.EOC.START;
            case GT:
            case LTE:
                // > X => using endOf(X) as start bound
                // <= X => using endOf(X) as finish bound
                return Composite.EOC.END;
            default:
                // >= X => using X as start bound (could use START_OF too)
                // = X => using X
                return Composite.EOC.NONE;
        }
    }

    private static boolean isNullRestriction(Restriction r, Bound b)
    {
        return r == null || (r.isSlice() && !((Restriction.Slice)r).hasBound(b));
    }

    private static ByteBuffer getSliceValue(Restriction r, Bound b, QueryOptions options) throws InvalidRequestException
    {
        Restriction.Slice slice = (Restriction.Slice)r;
        assert slice.hasBound(b);
        ByteBuffer val = slice.bound(b, options);
        if (val == null)
            throw new InvalidRequestException(String.format("Invalid null clustering key part %s", r));
        return val;
    }

    private List<Composite> getRequestedBound(Bound b, QueryOptions options) throws InvalidRequestException
    {
        assert isColumnRange();
        return buildBound(b, cfm.clusteringColumns(), columnRestrictions, isReversed, cfm.comparator, options);
    }

    public List<IndexExpression> getValidatedIndexExpressions(QueryOptions options) throws InvalidRequestException
    {
        if (!usesSecondaryIndexing || restrictedColumns.isEmpty())
            return Collections.emptyList();

        List<IndexExpression> expressions = new ArrayList<IndexExpression>();
        for (ColumnDefinition def : restrictedColumns.keySet())
        {
            Restriction restriction;
            switch (def.kind)
            {
                case PARTITION_KEY:
                    restriction = keyRestrictions[def.position()];
                    break;
                case CLUSTERING_COLUMN:
                    restriction = columnRestrictions[def.position()];
                    break;
                case REGULAR:
                case STATIC:
                    restriction = metadataRestrictions.get(def.name);
                    break;
                default:
                    // We don't allow restricting a COMPACT_VALUE for now in prepare.
                    throw new AssertionError();
            }

            if (restriction.isSlice())
            {
                Restriction.Slice slice = (Restriction.Slice)restriction;
                for (Bound b : Bound.values())
                {
                    if (slice.hasBound(b))
                    {
                        ByteBuffer value = validateIndexedValue(def, slice.bound(b, options));
                        Operator op = slice.getIndexOperator(b);
                        // If the underlying comparator for name is reversed, we need to reverse the IndexOperator: user operation
                        // always refer to the "forward" sorting even if the clustering order is reversed, but the 2ndary code does
                        // use the underlying comparator as is.
                        if (def.type instanceof ReversedType)
                            op = reverse(op);
                        expressions.add(new IndexExpression(def.name.bytes, op, value));
                    }
                }
            }
            else if (restriction.isContains())
            {
                SingleColumnRestriction.Contains contains = (SingleColumnRestriction.Contains)restriction;
                for (ByteBuffer value : contains.values(options))
                {
                    validateIndexedValue(def, value);
                    expressions.add(new IndexExpression(def.name.bytes, Operator.CONTAINS, value));
                }
                for (ByteBuffer key : contains.keys(options))
                {
                    validateIndexedValue(def, key);
                    expressions.add(new IndexExpression(def.name.bytes, Operator.CONTAINS_KEY, key));
                }
            }
            else
            {
                ByteBuffer value;
                if (restriction.isMultiColumn())
                {
                    List<ByteBuffer> values = restriction.values(options);
                    value = values.get(def.position());
                }
                else
                {
                    List<ByteBuffer> values = restriction.values(options);
                    if (values.size() != 1)
                        throw new InvalidRequestException("IN restrictions are not supported on indexed columns");

                    value = values.get(0);
                }

                validateIndexedValue(def, value);
                expressions.add(new IndexExpression(def.name.bytes, Operator.EQ, value));
            }
        }

        if (usesSecondaryIndexing)
        {
            ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(columnFamily());
            SecondaryIndexManager secondaryIndexManager = cfs.indexManager;
            secondaryIndexManager.validateIndexSearchersForQuery(expressions);
        }
        
        return expressions;
    }

    private static ByteBuffer validateIndexedValue(ColumnDefinition def, ByteBuffer value) throws InvalidRequestException
    {
        if (value == null)
            throw new InvalidRequestException(String.format("Unsupported null value for indexed column %s", def.name));
        if (value.remaining() > 0xFFFF)
            throw new InvalidRequestException("Index expression values may not be larger than 64K");
        return value;
    }

    private CellName makeExclusiveSliceBound(Bound bound, CellNameType type, QueryOptions options) throws InvalidRequestException
    {
        if (sliceRestriction.isInclusive(bound))
            return null;

        if (sliceRestriction.isMultiColumn())
            return type.makeCellName(((MultiColumnRestriction.Slice) sliceRestriction).componentBounds(bound, options).toArray());
        else
            return type.makeCellName(sliceRestriction.bound(bound, options));
    }

    private Iterator<Cell> applySliceRestriction(final Iterator<Cell> cells, final QueryOptions options) throws InvalidRequestException
    {
        assert sliceRestriction != null;

        final CellNameType type = cfm.comparator;
        final CellName excludedStart = makeExclusiveSliceBound(Bound.START, type, options);
        final CellName excludedEnd = makeExclusiveSliceBound(Bound.END, type, options);

        return new AbstractIterator<Cell>()
        {
            protected Cell computeNext()
            {
                while (cells.hasNext())
                {
                    Cell c = cells.next();

                    // For dynamic CF, the column could be out of the requested bounds (because we don't support strict bounds internally (unless
                    // the comparator is composite that is)), filter here
                    if ( (excludedStart != null && type.compare(c.name(), excludedStart) == 0)
                      || (excludedEnd != null && type.compare(c.name(), excludedEnd) == 0) )
                        continue;

                    return c;
                }
                return endOfData();
            }
        };
    }

    private static Operator reverse(Operator op)
    {
        switch (op)
        {
            case LT:  return Operator.GT;
            case LTE: return Operator.GTE;
            case GT:  return Operator.LT;
            case GTE: return Operator.LTE;
            default: return op;
        }
    }

    private ResultSet process(List<Row> rows, QueryOptions options, int limit, long now) throws InvalidRequestException
    {
        Selection.ResultSetBuilder result = selection.resultSetBuilder(now);
        for (org.apache.cassandra.db.Row row : rows)
        {
            // Not columns match the query, skip
            if (row.cf == null)
                continue;

            processColumnFamily(row.key.getKey(), row.cf, options, now, result);
        }

        ResultSet cqlRows = result.build();

        orderResults(cqlRows);

        // Internal calls always return columns in the comparator order, even when reverse was set
        if (isReversed)
            cqlRows.reverse();

        // Trim result if needed to respect the user limit
        cqlRows.trim(limit);
        return cqlRows;
    }

    // Used by ModificationStatement for CAS operations
    void processColumnFamily(ByteBuffer key, ColumnFamily cf, QueryOptions options, long now, Selection.ResultSetBuilder result)
    throws InvalidRequestException
    {
        CFMetaData cfm = cf.metadata();
        ByteBuffer[] keyComponents = null;
        if (cfm.getKeyValidator() instanceof CompositeType)
        {
            keyComponents = ((CompositeType)cfm.getKeyValidator()).split(key);
        }
        else
        {
            keyComponents = new ByteBuffer[]{ key };
        }

        Iterator<Cell> cells = cf.getSortedColumns().iterator();
        if (sliceRestriction != null)
            cells = applySliceRestriction(cells, options);

        CQL3Row.RowIterator iter = cfm.comparator.CQL3RowBuilder(cfm, now).group(cells);

        // If there is static columns but there is no non-static row, then provided the select was a full
        // partition selection (i.e. not a 2ndary index search and there was no condition on clustering columns)
        // then we want to include the static columns in the result set (and we're done).
        CQL3Row staticRow = iter.getStaticRow();
        if (staticRow != null && !iter.hasNext() && !usesSecondaryIndexing && hasNoClusteringColumnsRestriction())
        {
            result.newRow();
            for (ColumnDefinition def : selection.getColumns())
            {
                switch (def.kind)
                {
                    case PARTITION_KEY:
                        result.add(keyComponents[def.position()]);
                        break;
                    case STATIC:
                        addValue(result, def, staticRow, options);
                        break;
                    default:
                        result.add((ByteBuffer)null);
                }
            }
            return;
        }

        while (iter.hasNext())
        {
            CQL3Row cql3Row = iter.next();

            // Respect requested order
            result.newRow();
            // Respect selection order
            for (ColumnDefinition def : selection.getColumns())
            {
                switch (def.kind)
                {
                    case PARTITION_KEY:
                        result.add(keyComponents[def.position()]);
                        break;
                    case CLUSTERING_COLUMN:
                        result.add(cql3Row.getClusteringColumn(def.position()));
                        break;
                    case COMPACT_VALUE:
                        result.add(cql3Row.getColumn(null));
                        break;
                    case REGULAR:
                        addValue(result, def, cql3Row, options);
                        break;
                    case STATIC:
                        addValue(result, def, staticRow, options);
                        break;
                }
            }
        }
    }

    private static void addValue(Selection.ResultSetBuilder result, ColumnDefinition def, CQL3Row row, QueryOptions options)
    {
        if (row == null)
        {
            result.add((ByteBuffer)null);
            return;
        }

        if (def.type.isMultiCell())
        {
            List<Cell> cells = row.getMultiCellColumn(def.name);
            ByteBuffer buffer = cells == null
                             ? null
                             : ((CollectionType)def.type).serializeForNativeProtocol(def, cells, options.getProtocolVersion());
            result.add(buffer);
            return;
        }

        result.add(row.getColumn(def.name));
    }

    private boolean hasNoClusteringColumnsRestriction()
    {
        for (int i = 0; i < columnRestrictions.length; i++)
            if (columnRestrictions[i] != null)
                return false;
        return true;
    }

    private boolean needsPostQueryOrdering()
    {
        // We need post-query ordering only for queries with IN on the partition key and an ORDER BY.
        return keyIsInRelation && !parameters.orderings.isEmpty();
    }

    /**
     * Orders results when multiple keys are selected (using IN)
     */
    private void orderResults(ResultSet cqlRows) throws InvalidRequestException
    {
        if (cqlRows.size() == 0 || !needsPostQueryOrdering())
            return;

        assert orderingIndexes != null;

        List<Integer> idToSort = new ArrayList<Integer>();
        List<Comparator<ByteBuffer>> sorters = new ArrayList<Comparator<ByteBuffer>>();

        for (ColumnIdentifier.Raw identifier : parameters.orderings.keySet())
        {
            ColumnDefinition orderingColumn = cfm.getColumnDefinition(identifier.prepare(cfm));
            idToSort.add(orderingIndexes.get(orderingColumn.name));
            sorters.add(orderingColumn.type);
        }

        Comparator<List<ByteBuffer>> comparator = idToSort.size() == 1
                                                ? new SingleColumnComparator(idToSort.get(0), sorters.get(0))
                                                : new CompositeComparator(sorters, idToSort);
        Collections.sort(cqlRows.rows, comparator);
    }

    private static boolean isReversedType(ColumnDefinition def)
    {
        return def.type instanceof ReversedType;
    }

    private boolean columnFilterIsIdentity()
    {
        for (Restriction r : columnRestrictions)
        {
            if (r != null)
                return false;
        }
        return true;
    }

    private boolean hasClusteringColumnsRestriction()
    {
        for (int i = 0; i < columnRestrictions.length; i++)
            if (columnRestrictions[i] != null)
                return true;
        return false;
    }

    private void validateDistinctSelection()
    throws InvalidRequestException
    {
        Collection<ColumnDefinition> requestedColumns = selection.getColumns();
        for (ColumnDefinition def : requestedColumns)
            if (def.kind != ColumnDefinition.Kind.PARTITION_KEY && def.kind != ColumnDefinition.Kind.STATIC)
                throw new InvalidRequestException(String.format("SELECT DISTINCT queries must only request partition key columns and/or static columns (not %s)", def.name));

        // If it's a key range, we require that all partition key columns are selected so we don't have to bother with post-query grouping.
        if (!isKeyRange)
            return;

        for (ColumnDefinition def : cfm.partitionKeyColumns())
            if (!requestedColumns.contains(def))
                throw new InvalidRequestException(String.format("SELECT DISTINCT queries must request all the partition key columns (missing %s)", def.name));
    }

    /**
     * Checks if the specified column is restricted by multiple contains or contains key.
     *
     * @param columnDef the definition of the column to check
     * @return <code>true</code> the specified column is restricted by multiple contains or contains key,
     * <code>false</code> otherwise
     */
    private boolean isRestrictedByMultipleContains(ColumnDefinition columnDef)
    {
        if (!columnDef.type.isCollection())
            return false;

        Restriction restriction = metadataRestrictions.get(columnDef.name);

        if (!(restriction instanceof Contains))
            return false;

        Contains contains = (Contains) restriction;
        return (contains.numberOfValues() + contains.numberOfKeys()) > 1;
    }

    public static class RawStatement extends CFStatement
    {
        /**
         * Checks to ensure that the warning for missing allow filtering is only logged once.
         */
        private static volatile boolean hasLoggedMissingAllowFilteringWarning = false;

        private final Parameters parameters;
        private final List<RawSelector> selectClause;
        private final List<Relation> whereClause;
        private final Term.Raw limit;

        public RawStatement(CFName cfName, Parameters parameters, List<RawSelector> selectClause, List<Relation> whereClause, Term.Raw limit)
        {
            super(cfName);
            this.parameters = parameters;
            this.selectClause = selectClause;
            this.whereClause = whereClause == null ? Collections.<Relation>emptyList() : whereClause;
            this.limit = limit;
        }

        public ParsedStatement.Prepared prepare() throws InvalidRequestException
        {
            CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
            VariableSpecifications boundNames = getBoundVariables();

            // Select clause
            if (parameters.isCount && !selectClause.isEmpty())
                throw new InvalidRequestException("Only COUNT(*) and COUNT(1) operations are currently supported.");

            Selection selection = selectClause.isEmpty()
                                ? Selection.wildcard(cfm)
                                : Selection.fromSelectors(cfm, selectClause);

            SelectStatement stmt = new SelectStatement(cfm, boundNames.size(), parameters, selection, prepareLimit(boundNames));

            /*
             * WHERE clause. For a given entity, rules are:
             *   - EQ relation conflicts with anything else (including a 2nd EQ)
             *   - Can't have more than one LT(E) relation (resp. GT(E) relation)
             *   - IN relation are restricted to row keys (for now) and conflicts with anything else
             *     (we could allow two IN for the same entity but that doesn't seem very useful)
             *   - The value_alias cannot be restricted in any way (we don't support wide rows with indexed value in CQL so far)
             */
            boolean hasQueriableIndex = false;
            boolean hasQueriableClusteringColumnIndex = false;

            ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(columnFamily());
            SecondaryIndexManager indexManager = cfs.indexManager;

            for (Relation relation : whereClause)
            {
                if (relation.isMultiColumn())
                {
                    MultiColumnRelation rel = (MultiColumnRelation) relation;
                    List<ColumnDefinition> names = new ArrayList<>(rel.getEntities().size());
                    for (ColumnIdentifier.Raw rawEntity : rel.getEntities())
                    {
                        ColumnIdentifier entity = rawEntity.prepare(cfm);
                        ColumnDefinition def = cfm.getColumnDefinition(entity);
                        boolean[] queriable = processRelationEntity(stmt, indexManager, relation, entity, def);
                        hasQueriableIndex |= queriable[0];
                        hasQueriableClusteringColumnIndex |= queriable[1];
                        names.add(def);
                    }
                    updateRestrictionsForRelation(stmt, names, rel, boundNames);
                }
                else
                {
                    SingleColumnRelation rel = (SingleColumnRelation) relation;
                    ColumnIdentifier entity = rel.getEntity().prepare(cfm);
                    ColumnDefinition def = cfm.getColumnDefinition(entity);
                    boolean[] queriable = processRelationEntity(stmt, indexManager, relation, entity, def);
                    hasQueriableIndex |= queriable[0];
                    hasQueriableClusteringColumnIndex |= queriable[1];
                    updateRestrictionsForRelation(stmt, def, rel, boundNames);
                }
            }

             // At this point, the select statement if fully constructed, but we still have a few things to validate
            processPartitionKeyRestrictions(stmt, hasQueriableIndex, cfm);

            // All (or none) of the partition key columns have been specified;
            // hence there is no need to turn these restrictions into index expressions.
            if (!stmt.usesSecondaryIndexing)
                stmt.restrictedColumns.keySet().removeAll(cfm.partitionKeyColumns());

            if (stmt.selectsOnlyStaticColumns && stmt.hasClusteringColumnsRestriction())
                throw new InvalidRequestException("Cannot restrict clustering columns when selecting only static columns");

            processColumnRestrictions(stmt, hasQueriableIndex, cfm);

            // Covers indexes on the first clustering column (among others).
            if (stmt.isKeyRange && hasQueriableClusteringColumnIndex)
                stmt.usesSecondaryIndexing = true;

            int numberOfRestrictionsEvaluatedWithSlices = 0;

            for (ColumnDefinition def : cfm.clusteringColumns())
            {
                // Remove clustering column restrictions that can be handled by slices; the remainder will be
                // handled by filters (which may require a secondary index).
                Boolean indexed = stmt.restrictedColumns.get(def);
                if (indexed == null)
                    break;
                if (!indexed && stmt.columnRestrictions[def.position()].canEvaluateWithSlices())
                {
                    stmt.restrictedColumns.remove(def);
                    numberOfRestrictionsEvaluatedWithSlices++;
                }
            }

            // Even if usesSecondaryIndexing is false at this point, we'll still have to use one if
            // there are restrictions not covered by the PK.
            if (!stmt.metadataRestrictions.isEmpty())
                stmt.usesSecondaryIndexing = true;

            if (stmt.usesSecondaryIndexing)
                validateSecondaryIndexSelections(stmt);

            if (!stmt.parameters.orderings.isEmpty())
                processOrderingClause(stmt, cfm);

            checkNeedsFiltering(stmt, numberOfRestrictionsEvaluatedWithSlices);

            if (parameters.isDistinct)
                stmt.validateDistinctSelection();

            return new ParsedStatement.Prepared(stmt, boundNames);
        }

        /** Returns a pair of (hasQueriableIndex, hasQueriableClusteringColumnIndex) */
        private boolean[] processRelationEntity(SelectStatement stmt,
                                                SecondaryIndexManager indexManager,
                                                Relation relation,
                                                ColumnIdentifier entity,
                                                ColumnDefinition def) throws InvalidRequestException
        {
            if (def == null)
                handleUnrecognizedEntity(entity, relation);

            SecondaryIndex index = indexManager.getIndexForColumn(def.name.bytes);
            if (index != null && index.supportsOperator(relation.operator()))
            {
                stmt.restrictedColumns.put(def, Boolean.TRUE);
                return new boolean[]{true, def.kind == ColumnDefinition.Kind.CLUSTERING_COLUMN};
            }

            stmt.restrictedColumns.put(def, Boolean.FALSE);
            return new boolean[]{false, false};
        }

        /** Throws an InvalidRequestException for an unrecognized identifier in the WHERE clause */
        private void handleUnrecognizedEntity(ColumnIdentifier entity, Relation relation) throws InvalidRequestException
        {
            if (containsAlias(entity))
                throw new InvalidRequestException(String.format("Aliases aren't allowed in the where clause ('%s')", relation));
            else
                throw new InvalidRequestException(String.format("Undefined name %s in where clause ('%s')", entity, relation));
        }

        /** Returns a Term for the limit or null if no limit is set */
        private Term prepareLimit(VariableSpecifications boundNames) throws InvalidRequestException
        {
            if (limit == null)
                return null;

            Term prepLimit = limit.prepare(keyspace(), limitReceiver());
            prepLimit.collectMarkerSpecification(boundNames);
            return prepLimit;
        }

        private void updateRestrictionsForRelation(SelectStatement stmt, List<ColumnDefinition> defs, MultiColumnRelation relation, VariableSpecifications boundNames) throws InvalidRequestException
        {
            List<ColumnDefinition> restrictedColumns = new ArrayList<>();
            Set<ColumnDefinition> seen = new HashSet<>();
            Restriction existing = null;

            int previousPosition = defs.get(0).position() - 1;
            for (int i = 0, m = defs.size(); i < m; i++)
            {
                ColumnDefinition def = defs.get(i);

                // ensure multi-column restriction only applies to clustering columns
                if (def.kind != ColumnDefinition.Kind.CLUSTERING_COLUMN)
                    throw new InvalidRequestException(String.format("Multi-column relations can only be applied to clustering columns: %s", def.name));

                if (seen.contains(def))
                    throw new InvalidRequestException(String.format("Column \"%s\" appeared twice in a relation: %s", def.name, relation));
                seen.add(def);

                // check that no clustering columns were skipped
                if (def.position() != previousPosition + 1)
                {
                    if (previousPosition == -1)
                        throw new InvalidRequestException(String.format(
                                "Clustering columns may not be skipped in multi-column relations. " +
                                "They should appear in the PRIMARY KEY order. Got %s", relation));

                    throw new InvalidRequestException(String.format(
                                "Clustering columns must appear in the PRIMARY KEY order in multi-column relations: %s",
                                 relation));
                }
                previousPosition++;

                Restriction previous = existing;
                existing = getExistingRestriction(stmt, def);
                Operator operator = relation.operator();
                if (existing != null)
                {
                    if (operator == Operator.EQ || operator == Operator.IN)
                    {
                        throw new InvalidRequestException(String.format(
                                "Column \"%s\" cannot be restricted by more than one relation if it is in an %s relation",
                                def.name, operator));
                    }
                    else if (!existing.isSlice())
                    {
                        throw new InvalidRequestException(String.format(
                                "Column \"%s\" cannot be restricted by an equality relation and an inequality relation",
                                def.name));
                    }
                    else
                    {
                        if (!existing.isMultiColumn())
                        {
                            throw new InvalidRequestException(String.format(
                                    "Column \"%s\" cannot have both tuple-notation inequalities and single-column inequalities: %s",
                                    def.name, relation));
                        }

                        boolean existingRestrictionStartBefore =
                            (i == 0 && def.position() != 0 && stmt.columnRestrictions[def.position() - 1] == existing);

                        boolean existingRestrictionStartAfter = (i != 0 && previous != existing);

                        if (existingRestrictionStartBefore || existingRestrictionStartAfter)
                        {
                            throw new InvalidRequestException(String.format(
                                    "Column \"%s\" cannot be restricted by two tuple-notation inequalities not starting with the same column: %s",
                                    def.name, relation));
                        }

                        checkBound(existing, def, operator);
                    }
                }
                restrictedColumns.add(def);
            }

            switch (relation.operator())
            {
                case EQ:
                {
                    Term t = relation.getValue().prepare(keyspace(), defs);
                    t.collectMarkerSpecification(boundNames);
                    Restriction restriction = new MultiColumnRestriction.EQ(t, false);
                    for (ColumnDefinition def : restrictedColumns)
                        stmt.columnRestrictions[def.position()] = restriction;
                    break;
                }
                case IN:
                {
                    Restriction restriction;
                    List<? extends Term.MultiColumnRaw> inValues = relation.getInValues();
                    if (inValues != null)
                    {
                        // we have something like "(a, b, c) IN ((1, 2, 3), (4, 5, 6), ...) or
                        // "(a, b, c) IN (?, ?, ?)
                        List<Term> terms = new ArrayList<>(inValues.size());
                        for (Term.MultiColumnRaw tuple : inValues)
                        {
                            Term t = tuple.prepare(keyspace(), defs);
                            t.collectMarkerSpecification(boundNames);
                            terms.add(t);
                        }
                         restriction = new MultiColumnRestriction.InWithValues(terms);
                    }
                    else
                    {
                        Tuples.INRaw rawMarker = relation.getInMarker();
                        AbstractMarker t = rawMarker.prepare(keyspace(), defs);
                        t.collectMarkerSpecification(boundNames);
                        restriction = new MultiColumnRestriction.InWithMarker(t);
                    }
                    for (ColumnDefinition def : restrictedColumns)
                        stmt.columnRestrictions[def.position()] = restriction;

                    break;
                }
                case LT:
                case LTE:
                case GT:
                case GTE:
                {
                    Term t = relation.getValue().prepare(keyspace(), defs);
                    t.collectMarkerSpecification(boundNames);
                    Restriction.Slice restriction = (Restriction.Slice)getExistingRestriction(stmt, defs.get(0));
                    if (restriction == null)
                        restriction = new MultiColumnRestriction.Slice(false);
                    restriction.setBound(relation.operator(), t);

                    for (ColumnDefinition def : defs)
                    {
                        stmt.columnRestrictions[def.position()] = restriction;
                    }
                    break;
                }
                case NEQ:
                    throw new InvalidRequestException(String.format("Unsupported \"!=\" relation: %s", relation));
            }
        }

        /**
         * Checks that the operator for the specified column is compatible with the bounds of the existing restriction.
         *
         * @param existing the existing restriction
         * @param def the column definition
         * @param operator the operator
         * @throws InvalidRequestException if the operator is not compatible with the bounds of the existing restriction
         */
        private static void checkBound(Restriction existing, ColumnDefinition def, Operator operator) throws InvalidRequestException
        {
            Restriction.Slice existingSlice = (Restriction.Slice) existing;

            if (existingSlice.hasBound(Bound.START) && (operator == Operator.GT || operator == Operator.GTE))
                throw new InvalidRequestException(String.format(
                            "More than one restriction was found for the start bound on %s", def.name));

            if (existingSlice.hasBound(Bound.END) && (operator == Operator.LT || operator == Operator.LTE))
                throw new InvalidRequestException(String.format(
                            "More than one restriction was found for the end bound on %s", def.name));
        }

        private static Restriction getExistingRestriction(SelectStatement stmt, ColumnDefinition def)
        {
            switch (def.kind)
            {
                case PARTITION_KEY:
                    return stmt.keyRestrictions[def.position()];
                case CLUSTERING_COLUMN:
                    return stmt.columnRestrictions[def.position()];
                case REGULAR:
                case STATIC:
                    return stmt.metadataRestrictions.get(def.name);
                default:
                    throw new AssertionError();
            }
        }

        private void updateRestrictionsForRelation(SelectStatement stmt, ColumnDefinition def, SingleColumnRelation relation, VariableSpecifications names) throws InvalidRequestException
        {
            switch (def.kind)
            {
                case PARTITION_KEY:
                    stmt.keyRestrictions[def.position()] = updateSingleColumnRestriction(def, stmt.keyRestrictions[def.position()], relation, names);
                    break;
                case CLUSTERING_COLUMN:
                    stmt.columnRestrictions[def.position()] = updateSingleColumnRestriction(def, stmt.columnRestrictions[def.position()], relation, names);
                    break;
                case COMPACT_VALUE:
                    throw new InvalidRequestException(String.format("Predicates on the non-primary-key column (%s) of a COMPACT table are not yet supported", def.name));
                case REGULAR:
                case STATIC:
                    // We only all IN on the row key and last clustering key so far, never on non-PK columns, and this even if there's an index
                    Restriction r = updateSingleColumnRestriction(def, stmt.metadataRestrictions.get(def.name), relation, names);
                    if (r.isIN() && !((Restriction.IN)r).canHaveOnlyOneValue())
                        // Note: for backward compatibility reason, we conside a IN of 1 value the same as a EQ, so we let that slide.
                        throw new InvalidRequestException(String.format("IN predicates on non-primary-key columns (%s) is not yet supported", def.name));
                    stmt.metadataRestrictions.put(def.name, r);
                    break;
            }
        }

        Restriction updateSingleColumnRestriction(ColumnDefinition def, Restriction existingRestriction, SingleColumnRelation newRel, VariableSpecifications boundNames) throws InvalidRequestException
        {
            ColumnSpecification receiver = def;
            if (newRel.onToken)
            {
                if (def.kind != ColumnDefinition.Kind.PARTITION_KEY)
                    throw new InvalidRequestException(String.format("The token() function is only supported on the partition key, found on %s", def.name));

                receiver = new ColumnSpecification(def.ksName,
                                                   def.cfName,
                                                   new ColumnIdentifier("partition key token", true),
                                                   StorageService.getPartitioner().getTokenValidator());
            }

            // We don't support relations against entire collections (unless they're frozen), like "numbers = {1, 2, 3}"
            if (receiver.type.isCollection() && receiver.type.isMultiCell() && !(newRel.operator() == Operator.CONTAINS_KEY || newRel.operator() == Operator.CONTAINS))
            {
                throw new InvalidRequestException(String.format("Collection column '%s' (%s) cannot be restricted by a '%s' relation",
                                                                def.name, receiver.type.asCQL3Type(), newRel.operator()));
            }

            switch (newRel.operator())
            {
                case EQ:
                {
                    if (existingRestriction != null)
                        throw new InvalidRequestException(String.format("%s cannot be restricted by more than one relation if it includes an Equal", def.name));
                    Term t = newRel.getValue().prepare(keyspace(), receiver);
                    t.collectMarkerSpecification(boundNames);
                    existingRestriction = new SingleColumnRestriction.EQ(t, newRel.onToken);
                }
                break;
                case IN:
                    if (existingRestriction != null)
                        throw new InvalidRequestException(String.format("%s cannot be restricted by more than one relation if it includes a IN", def.name));

                    if (newRel.getInValues() == null)
                    {
                        // Means we have a "SELECT ... IN ?"
                        assert newRel.getValue() != null;
                        Term t = newRel.getValue().prepare(keyspace(), receiver);
                        t.collectMarkerSpecification(boundNames);
                        existingRestriction = new SingleColumnRestriction.InWithMarker((Lists.Marker)t);
                    }
                    else
                    {
                        List<Term> inValues = new ArrayList<>(newRel.getInValues().size());
                        for (Term.Raw raw : newRel.getInValues())
                        {
                            Term t = raw.prepare(keyspace(), receiver);
                            t.collectMarkerSpecification(boundNames);
                            inValues.add(t);
                        }
                        existingRestriction = new SingleColumnRestriction.InWithValues(inValues);
                    }
                    break;
                case NEQ:
                    throw new InvalidRequestException(String.format("Unsupported \"!=\" relation on column \"%s\"", def.name));
                case GT:
                case GTE:
                case LT:
                case LTE:
                    {
                        if (existingRestriction == null)
                            existingRestriction = new SingleColumnRestriction.Slice(newRel.onToken);
                        else if (!existingRestriction.isSlice())
                            throw new InvalidRequestException(String.format("Column \"%s\" cannot be restricted by both an equality and an inequality relation", def.name));
                        else if (existingRestriction.isMultiColumn())
                            throw new InvalidRequestException(String.format("Column \"%s\" cannot be restricted by both a tuple notation inequality and a single column inequality (%s)", def.name, newRel));
                        else if (existingRestriction.isOnToken() != newRel.onToken)
                            // For partition keys, we shouldn't have slice restrictions without token(). And while this is rejected later by
                            // processPartitionKeysRestrictions, we shouldn't update the existing restriction by the new one if the old one was using token()
                            // and the new one isn't since that would bypass that later test.
                            throw new InvalidRequestException("Only EQ and IN relation are supported on the partition key (unless you use the token() function)");

                        checkBound(existingRestriction, def, newRel.operator());

                        Term t = newRel.getValue().prepare(keyspace(), receiver);
                        t.collectMarkerSpecification(boundNames);
                        ((SingleColumnRestriction.Slice)existingRestriction).setBound(newRel.operator(), t);
                    }
                    break;
                case CONTAINS_KEY:
                    if (!(receiver.type instanceof MapType))
                        throw new InvalidRequestException(String.format("Cannot use CONTAINS KEY on non-map column %s", def.name));
                    // Fallthrough on purpose
                case CONTAINS:
                {
                    if (!receiver.type.isCollection())
                        throw new InvalidRequestException(String.format("Cannot use %s relation on non collection column %s", newRel.operator(), def.name));

                    if (existingRestriction == null)
                        existingRestriction = new SingleColumnRestriction.Contains();
                    else if (!existingRestriction.isContains())
                        throw new InvalidRequestException(String.format("Collection column %s can only be restricted by CONTAINS or CONTAINS KEY", def.name));

                    boolean isKey = newRel.operator() == Operator.CONTAINS_KEY;
                    receiver = makeCollectionReceiver(receiver, isKey);
                    Term t = newRel.getValue().prepare(keyspace(), receiver);
                    t.collectMarkerSpecification(boundNames);
                    ((SingleColumnRestriction.Contains)existingRestriction).add(t, isKey);
                    break;
                }
            }
            return existingRestriction;
        }

        private void processPartitionKeyRestrictions(SelectStatement stmt, boolean hasQueriableIndex, CFMetaData cfm) throws InvalidRequestException
        {
            // If there is a queriable index, no special condition are required on the other restrictions.
            // But we still need to know 2 things:
            //   - If we don't have a queriable index, is the query ok
            //   - Is it queriable without 2ndary index, which is always more efficient
            // If a component of the partition key is restricted by a relation, all preceding
            // components must have a EQ. Only the last partition key component can be in IN relation.
            boolean canRestrictFurtherComponents = true;
            ColumnDefinition previous = null;
            stmt.keyIsInRelation = false;
            Iterator<ColumnDefinition> iter = cfm.partitionKeyColumns().iterator();
            for (int i = 0; i < stmt.keyRestrictions.length; i++)
            {
                ColumnDefinition cdef = iter.next();
                Restriction restriction = stmt.keyRestrictions[i];

                if (restriction == null)
                {
                    if (stmt.onToken)
                        throw new InvalidRequestException("The token() function must be applied to all partition key components or none of them");

                    // The only time not restricting a key part is allowed is if none are restricted or an index is used.
                    if (i > 0 && stmt.keyRestrictions[i - 1] != null)
                    {
                        if (hasQueriableIndex)
                        {
                            stmt.usesSecondaryIndexing = true;
                            stmt.isKeyRange = true;
                            break;
                        }
                        throw new InvalidRequestException(String.format("Partition key part %s must be restricted since preceding part is", cdef.name));
                    }

                    stmt.isKeyRange = true;
                    canRestrictFurtherComponents = false;
                }
                else if (!canRestrictFurtherComponents)
                {
                    if (hasQueriableIndex)
                    {
                        stmt.usesSecondaryIndexing = true;
                        break;
                    }
                    throw new InvalidRequestException(String.format(
                            "Partitioning column \"%s\" cannot be restricted because the preceding column (\"%s\") is " +
                            "either not restricted or is restricted by a non-EQ relation", cdef.name, previous));
                }
                else if (restriction.isOnToken())
                {
                    // If this is a query on tokens, it's necessarily a range query (there can be more than one key per token).
                    stmt.isKeyRange = true;
                    stmt.onToken = true;
                }
                else if (stmt.onToken)
                {
                    throw new InvalidRequestException(String.format("The token() function must be applied to all partition key components or none of them"));
                }
                else if (!restriction.isSlice())
                {
                    if (restriction.isIN())
                    {
                        // We only support IN for the last name so far
                        if (i != stmt.keyRestrictions.length - 1)
                            throw new InvalidRequestException(String.format("Partition KEY part %s cannot be restricted by IN relation (only the last part of the partition key can)", cdef.name));
                        stmt.keyIsInRelation = true;
                    }
                }
                else
                {
                    // Non EQ relation is not supported without token(), even if we have a 2ndary index (since even those are ordered by partitioner).
                    // Note: In theory we could allow it for 2ndary index queries with ALLOW FILTERING, but that would probably require some special casing
                    // Note bis: This is also why we don't bother handling the 'tuple' notation of #4851 for keys. If we lift the limitation for 2ndary
                    // index with filtering, we'll need to handle it though.
                    throw new InvalidRequestException("Only EQ and IN relation are supported on the partition key (unless you use the token() function)");
                }
                previous = cdef;
            }

            if (stmt.onToken)
                checkTokenFunctionArgumentsOrder(cfm);
        }

        /**
         * Checks that the column identifiers used as argument for the token function have been specified in the
         * partition key order.
         * @param cfm the Column Family MetaData
         * @throws InvalidRequestException if the arguments have not been provided in the proper order.
         */
        private void checkTokenFunctionArgumentsOrder(CFMetaData cfm) throws InvalidRequestException
        {
            Iterator<ColumnDefinition> iter = Iterators.cycle(cfm.partitionKeyColumns());
            for (Relation relation : whereClause)
            {
                if (!relation.isOnToken())
                    continue;

                assert !relation.isMultiColumn() : "Unexpectedly got multi-column token relation";
                SingleColumnRelation singleColumnRelation = (SingleColumnRelation) relation;
                if (!cfm.getColumnDefinition(singleColumnRelation.getEntity().prepare(cfm)).equals(iter.next()))
                    throw new InvalidRequestException(String.format("The token function arguments must be in the partition key order: %s",
                                                                    Joiner.on(',').join(cfm.partitionKeyColumns())));
            }
        }

        private void processColumnRestrictions(SelectStatement stmt, boolean hasQueriableIndex, CFMetaData cfm) throws InvalidRequestException
        {
            // If a clustering key column is restricted by a non-EQ relation, all preceding
            // columns must have a EQ, and all following must have no restriction. Unless
            // the column is indexed that is.
            boolean canRestrictFurtherComponents = true;
            ColumnDefinition previous = null;
            Restriction previousRestriction = null;
            Iterator<ColumnDefinition> iter = cfm.clusteringColumns().iterator();
            for (int i = 0; i < stmt.columnRestrictions.length; i++)
            {
                ColumnDefinition cdef = iter.next();
                Restriction restriction = stmt.columnRestrictions[i];

                if (restriction == null)
                {
                    canRestrictFurtherComponents = false;
                }
                else if (!canRestrictFurtherComponents)
                {
                    // We're here if the previous clustering column was either not restricted, was a slice or an IN tulpe-notation.

                    // we can continue if we are in the special case of a slice 'tuple' notation from #4851
                    if (restriction != previousRestriction)
                    {
                        // if we have a 2ndary index, we need to use it
                        if (hasQueriableIndex)
                        {
                            stmt.usesSecondaryIndexing = true;
                            break;
                        }

                        if (previousRestriction == null)
                            throw new InvalidRequestException(String.format(
                                "PRIMARY KEY column \"%s\" cannot be restricted (preceding column \"%s\" is not restricted)", cdef.name, previous.name));

                        if (previousRestriction.isMultiColumn() && previousRestriction.isIN())
                            throw new InvalidRequestException(String.format(
                                     "PRIMARY KEY column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by an IN tuple notation)", cdef.name, previous.name));

                        throw new InvalidRequestException(String.format(
                                "PRIMARY KEY column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)", cdef.name, previous.name));
                    }
                }
                else if (restriction.isSlice())
                {
                    canRestrictFurtherComponents = false;
                    Restriction.Slice slice = (Restriction.Slice)restriction;
                    // For non-composite slices, we don't support internally the difference between exclusive and
                    // inclusive bounds, so we deal with it manually.
                    if (!cfm.comparator.isCompound() && (!slice.isInclusive(Bound.START) || !slice.isInclusive(Bound.END)))
                        stmt.sliceRestriction = slice;
                }
                else if (restriction.isIN())
                {
                    if (!restriction.isMultiColumn() && i != stmt.columnRestrictions.length - 1)
                        throw new InvalidRequestException(String.format("Clustering column \"%s\" cannot be restricted by an IN relation", cdef.name));

                    if (stmt.selectACollection())
                        throw new InvalidRequestException(String.format("Cannot restrict column \"%s\" by IN relation as a collection is selected by the query", cdef.name));

                    if (restriction.isMultiColumn())
                        canRestrictFurtherComponents = false;
                }
                else if (restriction.isContains())
                {
                    if (!hasQueriableIndex)
                        throw new InvalidRequestException(String.format("Cannot restrict column \"%s\" by a CONTAINS relation without a secondary index", cdef.name));
                    stmt.usesSecondaryIndexing = true;
                }

                previous = cdef;
                previousRestriction = restriction;
            }
        }

        private void validateSecondaryIndexSelections(SelectStatement stmt) throws InvalidRequestException
        {
            if (stmt.keyIsInRelation)
                throw new InvalidRequestException("Select on indexed columns and with IN clause for the PRIMARY KEY are not supported");
            // When the user only select static columns, the intent is that we don't query the whole partition but just
            // the static parts. But 1) we don't have an easy way to do that with 2i and 2) since we don't support index on static columns
            // so far, 2i means that you've restricted a non static column, so the query is somewhat non-sensical.
            if (stmt.selectsOnlyStaticColumns)
                throw new InvalidRequestException("Queries using 2ndary indexes don't support selecting only static columns");            
        }

        private void verifyOrderingIsAllowed(SelectStatement stmt) throws InvalidRequestException
        {
            if (stmt.usesSecondaryIndexing)
                throw new InvalidRequestException("ORDER BY with 2ndary indexes is not supported.");

            if (stmt.isKeyRange)
                throw new InvalidRequestException("ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
        }

        private void handleUnrecognizedOrderingColumn(ColumnIdentifier column) throws InvalidRequestException
        {
            if (containsAlias(column))
                throw new InvalidRequestException(String.format("Aliases are not allowed in order by clause ('%s')", column));
            else
                throw new InvalidRequestException(String.format("Order by on unknown column %s", column));
        }

        private void processOrderingClause(SelectStatement stmt, CFMetaData cfm) throws InvalidRequestException
        {
            verifyOrderingIsAllowed(stmt);

            // If we order post-query (see orderResults), the sorted column needs to be in the ResultSet for sorting, even if we don't
            // ultimately ship them to the client (CASSANDRA-4911).
            if (stmt.keyIsInRelation)
            {
                stmt.orderingIndexes = new HashMap<>();
                for (ColumnIdentifier.Raw rawColumn : stmt.parameters.orderings.keySet())
                {
                    ColumnIdentifier column = rawColumn.prepare(cfm);
                    final ColumnDefinition def = cfm.getColumnDefinition(column);
                    if (def == null)
                        handleUnrecognizedOrderingColumn(column);

                    int index = indexOf(def, stmt.selection);
                    if (index < 0)
                        index = stmt.selection.addColumnForOrdering(def);
                    stmt.orderingIndexes.put(def.name, index);
                }
            }
            stmt.isReversed = isReversed(stmt, cfm);
        }

        private boolean isReversed(SelectStatement stmt, CFMetaData cfm) throws InvalidRequestException
        {
            Boolean[] reversedMap = new Boolean[cfm.clusteringColumns().size()];
            int i = 0;
            for (Map.Entry<ColumnIdentifier.Raw, Boolean> entry : stmt.parameters.orderings.entrySet())
            {
                ColumnIdentifier column = entry.getKey().prepare(cfm);
                boolean reversed = entry.getValue();

                ColumnDefinition def = cfm.getColumnDefinition(column);
                if (def == null)
                    handleUnrecognizedOrderingColumn(column);

                if (def.kind != ColumnDefinition.Kind.CLUSTERING_COLUMN)
                    throw new InvalidRequestException(String.format("Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", column));

                if (i++ != def.position())
                    throw new InvalidRequestException(String.format("Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY"));

                reversedMap[def.position()] = (reversed != isReversedType(def));
            }

            // Check that all boolean in reversedMap, if set, agrees
            Boolean isReversed = null;
            for (Boolean b : reversedMap)
            {
                // Column on which order is specified can be in any order
                if (b == null)
                    continue;

                if (isReversed == null)
                {
                    isReversed = b;
                    continue;
                }
                if (!isReversed.equals(b))
                    throw new InvalidRequestException(String.format("Unsupported order by relation"));
            }
            assert isReversed != null;
            return isReversed;
        }

        /** If ALLOW FILTERING was not specified, this verifies that it is not needed */
        private void checkNeedsFiltering(SelectStatement stmt, int numberOfRestrictionsEvaluatedWithSlices) throws InvalidRequestException
        {
            // non-key-range non-indexed queries cannot involve filtering underneath
            if (!parameters.allowFiltering && (stmt.isKeyRange || stmt.usesSecondaryIndexing))
            {
                // We will potentially filter data if either:
                //  - Have more than one IndexExpression
                //  - Have no index expression and the column filter is not the identity
                if (needFiltering(stmt, numberOfRestrictionsEvaluatedWithSlices))
                    throw new InvalidRequestException("Cannot execute this query as it might involve data filtering and " +
                                                      "thus may have unpredictable performance. If you want to execute " +
                                                      "this query despite the performance unpredictability, use ALLOW FILTERING");
            }

            // We don't internally support exclusive slice bounds on non-composite tables. To deal with it we do an
            // inclusive slice and remove post-query the value that shouldn't be returned. One problem however is that
            // if there is a user limit, that limit may make the query return before the end of the slice is reached,
            // in which case, once we'll have removed bound post-query, we might end up with less results than
            // requested which would be incorrect. For single-partition query, this is not a problem, we just ask for
            // one more result (see updateLimitForQuery()) since that's enough to compensate for that problem. For key
            // range however, each returned row may include one result that will have to be trimmed, so we would have
            // to bump the query limit by N where N is the number of rows we will return, but we don't know that in
            // advance. So, since we currently don't have a good way to handle such query, we refuse it (#7059) rather
            // than answering with something that is wrong.
            if (stmt.sliceRestriction != null && stmt.isKeyRange && limit != null)
            {
                SingleColumnRelation rel = findInclusiveClusteringRelationForCompact(stmt.cfm);
                throw new InvalidRequestException(String.format("The query requests a restriction of rows with a strict bound (%s) over a range of partitions. "
                                                              + "This is not supported by the underlying storage engine for COMPACT tables if a LIMIT is provided. "
                                                              + "Please either make the condition non strict (%s) or remove the user LIMIT", rel, rel.withNonStrictOperator()));
            }
        }

        /**
         * Checks if the specified statement will need to filter the data.
         *
         * @param stmt the statement to test.
         * @param numberOfRestrictionsEvaluatedWithSlices the number of restrictions that can be evaluated with slices
         * @return <code>true</code> if the specified statement will need to filter the data, <code>false</code>
         * otherwise.
         */
        private static boolean needFiltering(SelectStatement stmt, int numberOfRestrictionsEvaluatedWithSlices)
        {
            boolean needFiltering = stmt.restrictedColumns.size() > 1
                    || (stmt.restrictedColumns.isEmpty() && !stmt.columnFilterIsIdentity())
                    || (!stmt.restrictedColumns.isEmpty()
                            && stmt.isRestrictedByMultipleContains(Iterables.getOnlyElement(stmt.restrictedColumns.keySet())));

            // For some secondary index queries, that were having some restrictions on non-indexed clustering columns,
            // were not requiring ALLOW FILTERING as we should. The first time such a query is executed we will log a
            // warning to notify the user (CASSANDRA-8418)
            if (!needFiltering
                    && !hasLoggedMissingAllowFilteringWarning
                    && (stmt.restrictedColumns.size() + numberOfRestrictionsEvaluatedWithSlices) > 1)
            {
                hasLoggedMissingAllowFilteringWarning = true;

                String msg = "Some secondary index queries with restrictions on non-indexed clustering columns "
                           + "were executed without ALLOW FILTERING. In Cassandra 3.0, these queries will require "
                           + "ALLOW FILTERING (see CASSANDRA-8418 for details).";

                logger.warn(msg);
            }

            return needFiltering;
        }

        private int indexOf(ColumnDefinition def, Selection selection)
        {
            return indexOf(def, selection.getColumns().iterator());
        }

        private int indexOf(final ColumnDefinition def, Iterator<ColumnDefinition> defs)
        {
            return Iterators.indexOf(defs, new Predicate<ColumnDefinition>()
                                           {
                                               public boolean apply(ColumnDefinition n)
                                               {
                                                   return def.name.equals(n.name);
                                               }
                                           });
        }

        private SingleColumnRelation findInclusiveClusteringRelationForCompact(CFMetaData cfm)
        {
            for (Relation r : whereClause)
            {
                // We only call this when sliceRestriction != null, i.e. for compact table with non composite comparator,
                // so it can't be a MultiColumnRelation.
                SingleColumnRelation rel = (SingleColumnRelation)r;
                if (cfm.getColumnDefinition(rel.getEntity().prepare(cfm)).kind == ColumnDefinition.Kind.CLUSTERING_COLUMN
                    && (rel.operator() == Operator.GT || rel.operator() == Operator.LT))
                    return rel;
            }

            // We're not supposed to call this method unless we know this can't happen
            throw new AssertionError();
        }

        private boolean containsAlias(final ColumnIdentifier name)
        {
            return Iterables.any(selectClause, new Predicate<RawSelector>()
                                               {
                                                   public boolean apply(RawSelector raw)
                                                   {
                                                       return name.equals(raw.alias);
                                                   }
                                               });
        }

        private ColumnSpecification limitReceiver()
        {
            return new ColumnSpecification(keyspace(), columnFamily(), new ColumnIdentifier("[limit]", true), Int32Type.instance);
        }

        private static ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey)
        {
            assert collection.type.isCollection();
            switch (((CollectionType)collection.type).kind)
            {
                case LIST:
                    assert !isKey;
                    return Lists.valueSpecOf(collection);
                case SET:
                    assert !isKey;
                    return Sets.valueSpecOf(collection);
                case MAP:
                    return isKey ? Maps.keySpecOf(collection) : Maps.valueSpecOf(collection);
            }
            throw new AssertionError();
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                          .add("name", cfName)
                          .add("selectClause", selectClause)
                          .add("whereClause", whereClause)
                          .add("isDistinct", parameters.isDistinct)
                          .add("isCount", parameters.isCount)
                          .toString();
        }
    }

    public static class Parameters
    {
        private final Map<ColumnIdentifier.Raw, Boolean> orderings;
        private final boolean isDistinct;
        private final boolean isCount;
        private final ColumnIdentifier countAlias;
        private final boolean allowFiltering;

        public Parameters(Map<ColumnIdentifier.Raw, Boolean> orderings,
                          boolean isDistinct,
                          boolean isCount,
                          ColumnIdentifier countAlias,
                          boolean allowFiltering)
        {
            this.orderings = orderings;
            this.isDistinct = isDistinct;
            this.isCount = isCount;
            this.countAlias = countAlias;
            this.allowFiltering = allowFiltering;
        }
    }

    /**
     * Used in orderResults(...) method when single 'ORDER BY' condition where given
     */
    private static class SingleColumnComparator implements Comparator<List<ByteBuffer>>
    {
        private final int index;
        private final Comparator<ByteBuffer> comparator;

        public SingleColumnComparator(int columnIndex, Comparator<ByteBuffer> orderer)
        {
            index = columnIndex;
            comparator = orderer;
        }

        public int compare(List<ByteBuffer> a, List<ByteBuffer> b)
        {
            return comparator.compare(a.get(index), b.get(index));
        }
    }

    /**
     * Used in orderResults(...) method when multiple 'ORDER BY' conditions where given
     */
    private static class CompositeComparator implements Comparator<List<ByteBuffer>>
    {
        private final List<Comparator<ByteBuffer>> orderTypes;
        private final List<Integer> positions;

        private CompositeComparator(List<Comparator<ByteBuffer>> orderTypes, List<Integer> positions)
        {
            this.orderTypes = orderTypes;
            this.positions = positions;
        }

        public int compare(List<ByteBuffer> a, List<ByteBuffer> b)
        {
            for (int i = 0; i < positions.size(); i++)
            {
                Comparator<ByteBuffer> type = orderTypes.get(i);
                int columnPos = positions.get(i);

                ByteBuffer aValue = a.get(columnPos);
                ByteBuffer bValue = b.get(columnPos);

                int comparison = type.compare(aValue, bValue);

                if (comparison != 0)
                    return comparison;
            }

            return 0;
        }
    }
}
