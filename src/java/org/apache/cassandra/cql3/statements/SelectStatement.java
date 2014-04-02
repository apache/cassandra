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

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import org.github.jamm.MemoryMeter;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 */
public class SelectStatement implements CQLStatement, MeasurableForPreparedCache
{
    private static final Logger logger = LoggerFactory.getLogger(SelectStatement.class);

    private static final int DEFAULT_COUNT_PAGE_SIZE = 10000;

    private final int boundTerms;
    public final CFDefinition cfDef;
    public final Parameters parameters;
    private final Selection selection;
    private final Term limit;

    private final Restriction[] keyRestrictions;
    private final Restriction[] columnRestrictions;
    private final Map<CFDefinition.Name, Restriction> metadataRestrictions = new HashMap<CFDefinition.Name, Restriction>();

    // The name of all restricted names not covered by the key or index filter
    private final Set<CFDefinition.Name> restrictedNames = new HashSet<CFDefinition.Name>();
    private Restriction.Slice sliceRestriction;

    private boolean isReversed;
    private boolean onToken;
    private boolean isKeyRange;
    private boolean keyIsInRelation;
    private boolean usesSecondaryIndexing;

    private Map<CFDefinition.Name, Integer> orderingIndexes;

    private boolean selectsStaticColumns;
    private boolean selectsOnlyStaticColumns;

    // Used by forSelection below
    private static final Parameters defaultParameters = new Parameters(Collections.<ColumnIdentifier, Boolean>emptyMap(), false, false, null, false);

    private static final Predicate<CFDefinition.Name> isStaticFilter = new Predicate<CFDefinition.Name>()
    {
        public boolean apply(CFDefinition.Name name)
        {
            return name.kind == CFDefinition.Name.Kind.STATIC;
        }
    };

    public SelectStatement(CFDefinition cfDef, int boundTerms, Parameters parameters, Selection selection, Term limit)
    {
        this.cfDef = cfDef;
        this.boundTerms = boundTerms;
        this.selection = selection;
        this.keyRestrictions = new Restriction[cfDef.partitionKeyCount()];
        this.columnRestrictions = new Restriction[cfDef.clusteringColumnsCount()];
        this.parameters = parameters;
        this.limit = limit;

        // Now gather a few info on whether we should bother with static columns or not for this statement
        initStaticColumnsInfo();
    }

    private void initStaticColumnsInfo()
    {
        if (!cfDef.cfm.hasStaticColumns())
            return;

        // If it's a wildcard, we do select static but not only them
        if (selection.isWildcard())
        {
            selectsStaticColumns = true;
            return;
        }

        // Otherwise, check the selected columns
        selectsStaticColumns = !Iterables.isEmpty(Iterables.filter(selection.getColumnsList(), isStaticFilter));
        selectsOnlyStaticColumns = true;
        for (CFDefinition.Name name : selection.getColumnsList())
        {
            if (name.kind != CFDefinition.Name.Kind.KEY_ALIAS && name.kind != CFDefinition.Name.Kind.STATIC)
            {
                selectsOnlyStaticColumns = false;
                break;
            }
        }
    }

    // Creates a simple select based on the given selection.
    // Note that the results select statement should not be used for actual queries, but only for processing already
    // queried data through processColumnFamily.
    static SelectStatement forSelection(CFDefinition cfDef, Selection selection)
    {
        return new SelectStatement(cfDef, 0, defaultParameters, selection, null);
    }

    public ResultSet.Metadata getResultMetadata()
    {
        return parameters.isCount
             ? ResultSet.makeCountMetadata(keyspace(), columnFamily(), parameters.countAlias)
             : selection.getResultMetadata();
    }

    public long measureForPreparedCache(MemoryMeter meter)
    {
        return meter.measure(this)
             + meter.measureDeep(parameters)
             + meter.measureDeep(selection)
             + (limit == null ? 0 : meter.measureDeep(limit))
             + meter.measureDeep(keyRestrictions)
             + meter.measureDeep(columnRestrictions)
             + meter.measureDeep(metadataRestrictions)
             + meter.measureDeep(restrictedNames)
             + (sliceRestriction == null ? 0 : meter.measureDeep(sliceRestriction))
             + (orderingIndexes == null ? 0 : meter.measureDeep(orderingIndexes));
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
        List<ByteBuffer> variables = options.getValues();
        if (cl == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        cl.validateForRead(keyspace());

        int limit = getLimit(variables);
        long now = System.currentTimeMillis();
        Pageable command;
        if (isKeyRange || usesSecondaryIndexing)
        {
            command = getRangeCommand(variables, limit, now);
        }
        else
        {
            List<ReadCommand> commands = getSliceCommands(variables, limit, now);
            command = commands == null ? null : new Pageable.ReadCommands(commands);
        }

        int pageSize = options.getPageSize();
        // A count query will never be paged for the user, but we always page it internally to avoid OOM.
        // If we user provided a pageSize we'll use that to page internally (because why not), otherwise we use our default
        // Note that if there are some nodes in the cluster with a version less than 2.0, we can't use paging (CASSANDRA-6707).
        if (parameters.isCount && pageSize <= 0 && MessagingService.instance().allNodesAtLeast20)
            pageSize = DEFAULT_COUNT_PAGE_SIZE;

        if (pageSize <= 0 || command == null || !QueryPagers.mayNeedPaging(command, pageSize))
        {
            return execute(command, cl, variables, limit, now);
        }
        else
        {
            QueryPager pager = QueryPagers.pager(command, cl, options.getPagingState());
            if (parameters.isCount)
                return pageCountQuery(pager, variables, pageSize, now);

            // We can't properly do post-query ordering if we page (see #6722)
            if (needsPostQueryOrdering())
                throw new InvalidRequestException("Cannot page queries with both ORDER BY and a IN restriction on the partition key; you must either remove the "
                                                + "ORDER BY or the IN and sort client side, or disable paging for this query");

            List<Row> page = pager.fetchPage(pageSize);
            ResultMessage.Rows msg = processResults(page, variables, limit, now);
            if (!pager.isExhausted())
                msg.result.metadata.setHasMorePages(pager.state());
            return msg;
        }
    }

    private ResultMessage.Rows execute(Pageable command, ConsistencyLevel cl, List<ByteBuffer> variables, int limit, long now) throws RequestValidationException, RequestExecutionException
    {
        List<Row> rows;
        if (command == null)
        {
            rows = Collections.<Row>emptyList();
        }
        else
        {
            rows = command instanceof Pageable.ReadCommands
                 ? StorageProxy.read(((Pageable.ReadCommands)command).commands, cl)
                 : StorageProxy.getRangeSlice((RangeSliceCommand)command, cl);
        }

        return processResults(rows, variables, limit, now);
    }

    private ResultMessage.Rows pageCountQuery(QueryPager pager, List<ByteBuffer> variables, int pageSize, long now) throws RequestValidationException, RequestExecutionException
    {
        int count = 0;
        while (!pager.isExhausted())
        {
            int maxLimit = pager.maxRemaining();
            logger.debug("New maxLimit for paged count query is {}", maxLimit);
            ResultSet rset = process(pager.fetchPage(pageSize), variables, maxLimit, now);
            count += rset.rows.size();
        }

        ResultSet result = ResultSet.makeCountResult(keyspace(), columnFamily(), count, parameters.countAlias);
        return new ResultMessage.Rows(result);
    }

    public ResultMessage.Rows processResults(List<Row> rows, List<ByteBuffer> variables, int limit, long now) throws RequestValidationException
    {
        // Even for count, we need to process the result as it'll group some column together in sparse column families
        ResultSet rset = process(rows, variables, limit, now);
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

    public ResultMessage.Rows executeInternal(QueryState state) throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> variables = Collections.emptyList();
        int limit = getLimit(variables);
        long now = System.currentTimeMillis();
        List<Row> rows;
        if (isKeyRange || usesSecondaryIndexing)
        {
            RangeSliceCommand command = getRangeCommand(variables, limit, now);
            rows = command == null ? Collections.<Row>emptyList() : command.executeLocally();
        }
        else
        {
            List<ReadCommand> commands = getSliceCommands(variables, limit, now);
            rows = commands == null ? Collections.<Row>emptyList() : readLocally(keyspace(), commands);
        }

        return processResults(rows, variables, limit, now);
    }

    public ResultSet process(List<Row> rows) throws InvalidRequestException
    {
        assert !parameters.isCount; // not yet needed
        return process(rows, Collections.<ByteBuffer>emptyList(), getLimit(Collections.<ByteBuffer>emptyList()), System.currentTimeMillis());
    }

    public String keyspace()
    {
        return cfDef.cfm.ksName;
    }

    public String columnFamily()
    {
        return cfDef.cfm.cfName;
    }

    private List<ReadCommand> getSliceCommands(List<ByteBuffer> variables, int limit, long now) throws RequestValidationException
    {
        Collection<ByteBuffer> keys = getKeys(variables);
        if (keys.isEmpty()) // in case of IN () for (the last column of) the partition key.
            return null;

        List<ReadCommand> commands = new ArrayList<ReadCommand>(keys.size());

        IDiskAtomFilter filter = makeFilter(variables, limit);
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
            commands.add(ReadCommand.create(keyspace(), key, columnFamily(), now, filter.cloneShallow()));
        }

        return commands;
    }

    private RangeSliceCommand getRangeCommand(List<ByteBuffer> variables, int limit, long now) throws RequestValidationException
    {
        IDiskAtomFilter filter = makeFilter(variables, limit);
        if (filter == null)
            return null;

        List<IndexExpression> expressions = getIndexExpressions(variables);
        // The LIMIT provided by the user is the number of CQL row he wants returned.
        // We want to have getRangeSlice to count the number of columns, not the number of keys.
        AbstractBounds<RowPosition> keyBounds = getKeyBounds(variables);
        return keyBounds == null
             ? null
             : new RangeSliceCommand(keyspace(), columnFamily(), now,  filter, keyBounds, expressions, limit, !parameters.isDistinct, false);
    }

    private AbstractBounds<RowPosition> getKeyBounds(List<ByteBuffer> variables) throws InvalidRequestException
    {
        IPartitioner<?> p = StorageService.getPartitioner();

        if (onToken)
        {
            Token startToken = getTokenBound(Bound.START, variables, p);
            Token endToken = getTokenBound(Bound.END, variables, p);

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
            ByteBuffer startKeyBytes = getKeyBound(Bound.START, variables);
            ByteBuffer finishKeyBytes = getKeyBound(Bound.END, variables);

            RowPosition startKey = RowPosition.forKey(startKeyBytes, p);
            RowPosition finishKey = RowPosition.forKey(finishKeyBytes, p);

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

    private IDiskAtomFilter makeFilter(List<ByteBuffer> variables, int limit)
    throws InvalidRequestException
    {
        if (parameters.isDistinct)
        {
            return new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 1, -1);
        }
        else if (isColumnRange())
        {
            // For sparse, we used to ask for 'defined columns' * 'asked limit' (where defined columns includes the row marker)
            // to account for the grouping of columns.
            // Since that doesn't work for maps/sets/lists, we now use the compositesToGroup option of SliceQueryFilter.
            // But we must preserve backward compatibility too (for mixed version cluster that is).
            int toGroup = cfDef.isCompact ? -1 : cfDef.clusteringColumnsCount();
            List<ByteBuffer> startBounds = getRequestedBound(Bound.START, variables);
            List<ByteBuffer> endBounds = getRequestedBound(Bound.END, variables);
            assert startBounds.size() == endBounds.size();

            // Handles fetching static columns. Note that for 2i, the filter is just used to restrict
            // the part of the index to query so adding the static slice would be useless and confusing.
            // For 2i, static columns are retrieve in CompositesSearcher with each index hit.
            ColumnSlice staticSlice = null;
            if (selectsStaticColumns && !usesSecondaryIndexing)
            {
                ColumnNameBuilder staticPrefix = cfDef.cfm.getStaticColumnNameBuilder();
                // Note: we could use staticPrefix.build() for the start bound, but EMPTY_BYTE_BUFFER gives us the
                // same effect while saving a few CPU cycles.
                staticSlice = isReversed
                            ? new ColumnSlice(staticPrefix.buildAsEndOfRange(), ByteBufferUtil.EMPTY_BYTE_BUFFER)
                            : new ColumnSlice(ByteBufferUtil.EMPTY_BYTE_BUFFER, staticPrefix.buildAsEndOfRange());

                // In the case where we only select static columns, we want to really only check the static columns.
                // So we return early as the rest of that method would actually make us query everything
                if (selectsOnlyStaticColumns)
                    return sliceFilter(staticSlice, limit, toGroup);
            }

            // The case where startBounds == 1 is common enough that it's worth optimizing
            if (startBounds.size() == 1)
            {
                ColumnSlice slice = new ColumnSlice(startBounds.get(0), endBounds.get(0));
                if (slice.isAlwaysEmpty(cfDef.cfm.comparator, isReversed))
                    return staticSlice == null ? null : sliceFilter(staticSlice, limit, toGroup);

                return staticSlice == null
                     ? sliceFilter(slice, limit, toGroup)
                     : (slice.includes(cfDef.cfm.comparator, staticSlice.finish) ? sliceFilter(new ColumnSlice(staticSlice.start, slice.finish), limit, toGroup)
                                                                                 : sliceFilter(new ColumnSlice[]{ staticSlice, slice }, limit, toGroup));
            }

            List<ColumnSlice> l = new ArrayList<ColumnSlice>(startBounds.size());
            for (int i = 0; i < startBounds.size(); i++)
            {
                ColumnSlice slice = new ColumnSlice(startBounds.get(i), endBounds.get(i));
                if (!slice.isAlwaysEmpty(cfDef.cfm.comparator, isReversed))
                    l.add(slice);
            }

            if (l.isEmpty())
                return staticSlice == null ? null : sliceFilter(staticSlice, limit, toGroup);
            if (staticSlice == null)
                return sliceFilter(l.toArray(new ColumnSlice[l.size()]), limit, toGroup);

            // The slices should not overlap. We know the slices built from startBounds/endBounds don't, but
            // if there is a static slice, it could overlap with the 2nd slice. Check for it and correct if
            // that's the case
            ColumnSlice[] slices;
            if (isReversed)
            {
                if (l.get(l.size() - 1).includes(cfDef.cfm.comparator, staticSlice.start))
                {
                    slices = l.toArray(new ColumnSlice[l.size()]);
                    slices[slices.length-1] = new ColumnSlice(slices[slices.length-1].start, ByteBufferUtil.EMPTY_BYTE_BUFFER);
                }
                else
                {
                    slices = l.toArray(new ColumnSlice[l.size()+1]);
                    slices[slices.length-1] = staticSlice;
                }
            }
            else
            {
                if (l.get(0).includes(cfDef.cfm.comparator, staticSlice.finish))
                {
                    slices = new ColumnSlice[l.size()];
                    slices[0] = new ColumnSlice(ByteBufferUtil.EMPTY_BYTE_BUFFER, l.get(0).finish);
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
            SortedSet<ByteBuffer> cellNames = getRequestedColumns(variables);
            if (cellNames == null) // in case of IN () for the last column of the key
                return null;
            QueryProcessor.validateCellNames(cellNames);
            return new NamesQueryFilter(cellNames, true);
        }
    }

    private SliceQueryFilter sliceFilter(ColumnSlice slice, int limit, int toGroup)
    {
        return sliceFilter(new ColumnSlice[]{ slice }, limit, toGroup);
    }

    private SliceQueryFilter sliceFilter(ColumnSlice[] slices, int limit, int toGroup)
    {
        return new SliceQueryFilter(slices, isReversed, limit, toGroup);
    }

    private int getLimit(List<ByteBuffer> variables) throws InvalidRequestException
    {
        int l = Integer.MAX_VALUE;
        if (limit != null)
        {
            ByteBuffer b = limit.bindAndGet(variables);
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

        // Internally, we don't support exclusive bounds for slices. Instead,
        // we query one more element if necessary and exclude
        if (sliceRestriction != null && (!sliceRestriction.isInclusive(Bound.START) || !sliceRestriction.isInclusive(Bound.END)) && l != Integer.MAX_VALUE)
            l += 1;

        return l;
    }

    private Collection<ByteBuffer> getKeys(final List<ByteBuffer> variables) throws InvalidRequestException
    {
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        ColumnNameBuilder builder = cfDef.getKeyNameBuilder();
        for (CFDefinition.Name name : cfDef.partitionKeys())
        {
            Restriction r = keyRestrictions[name.position];
            assert r != null && !r.isSlice();

            List<ByteBuffer> values = r.values(variables);

            if (builder.remainingCount() == 1)
            {
                for (ByteBuffer val : values)
                {
                    if (val == null)
                        throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", name));
                    keys.add(builder.copy().add(val).build());
                }
            }
            else
            {
                // Note: for backward compatibility reasons, we let INs with 1 value slide
                if (values.size() != 1)
                    throw new InvalidRequestException("IN is only supported on the last column of the partition key");
                ByteBuffer val = values.get(0);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", name));
                builder.add(val);
            }
        }
        return keys;
    }

    private ByteBuffer getKeyBound(Bound b, List<ByteBuffer> variables) throws InvalidRequestException
    {
        // Deal with unrestricted partition key components (special-casing is required to deal with 2i queries on the first
        // component of a composite partition key).
        for (int i = 0; i < keyRestrictions.length; i++)
            if (keyRestrictions[i] == null)
                return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        // We deal with IN queries for keys in other places, so we know buildBound will return only one result
        return buildBound(b, cfDef.partitionKeys(), keyRestrictions, false, cfDef.getKeyNameBuilder(), variables).get(0);
    }

    private Token getTokenBound(Bound b, List<ByteBuffer> variables, IPartitioner<?> p) throws InvalidRequestException
    {
        assert onToken;

        Restriction keyRestriction = keyRestrictions[0];
        ByteBuffer value;
        if (keyRestriction.isEQ())
        {
            value = keyRestriction.values(variables).get(0);
        }
        else
        {
            Restriction.Slice slice = (Restriction.Slice)keyRestriction;
            if (!slice.hasBound(b))
                return p.getMinimumToken();

            value = slice.bound(b, variables);
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
                return ((Restriction.Slice)r).isInclusive(b);
        }
        // All equality
        return true;
    }

    private boolean isColumnRange()
    {
        // Due to CASSANDRA-5762, we always do a slice for CQL3 tables (not compact, composite).
        // Static CF (non compact but non composite) never entails a column slice however
        if (!cfDef.isCompact)
            return cfDef.isComposite;

        // Otherwise (i.e. for compact table where we don't have a row marker anyway and thus don't care about CASSANDRA-5762),
        // it is a range query if it has at least one the column alias for which no relation is defined or is not EQ.
        for (Restriction r : columnRestrictions)
        {
            if (r == null || r.isSlice())
                return true;
        }
        return false;
    }

    private SortedSet<ByteBuffer> getRequestedColumns(List<ByteBuffer> variables) throws InvalidRequestException
    {
        // Note: getRequestedColumns don't handle static columns, but due to CASSANDRA-5762
        // we always do a slice for CQL3 tables, so it's ok to ignore them here
        assert !isColumnRange();

        ColumnNameBuilder builder = cfDef.getColumnNameBuilder();
        Iterator<CFDefinition.Name> idIter = cfDef.clusteringColumns().iterator();
        for (Restriction r : columnRestrictions)
        {
            ColumnIdentifier id = idIter.next().name;
            assert r != null && !r.isSlice();

            List<ByteBuffer> values = r.values(variables);
            if (values.size() == 1)
            {
                ByteBuffer val = values.get(0);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for clustering key part %s", id));
                builder.add(val);
            }
            else
            {
                // We have a IN, which we only support for the last column.
                // If compact, just add all values and we're done. Otherwise,
                // for each value of the IN, creates all the columns corresponding to the selection.
                if (values.isEmpty())
                    return null;
                SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(cfDef.cfm.comparator);
                Iterator<ByteBuffer> iter = values.iterator();
                while (iter.hasNext())
                {
                    ByteBuffer val = iter.next();
                    ColumnNameBuilder b = iter.hasNext() ? builder.copy() : builder;
                    if (val == null)
                        throw new InvalidRequestException(String.format("Invalid null value for clustering key part %s", id));
                    b.add(val);
                    if (cfDef.isCompact)
                        columns.add(b.build());
                    else
                        columns.addAll(addSelectedColumns(b));
                }
                return columns;
            }
        }

        return addSelectedColumns(builder);
    }

    private SortedSet<ByteBuffer> addSelectedColumns(ColumnNameBuilder builder)
    {
        if (cfDef.isCompact)
        {
            return FBUtilities.singleton(builder.build(), cfDef.cfm.comparator);
        }
        else
        {
            // Collections require doing a slice query because a given collection is a
            // non-know set of columns, so we shouldn't get there
            assert !selectACollection();

            SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(cfDef.cfm.comparator);

            // We need to query the selected column as well as the marker
            // column (for the case where the row exists but has no columns outside the PK)
            // Two exceptions are "static CF" (non-composite non-compact CF) and "super CF"
            // that don't have marker and for which we must query all columns instead
            if (cfDef.isComposite && !cfDef.cfm.isSuper())
            {
                // marker
                columns.add(builder.copy().add(ByteBufferUtil.EMPTY_BYTE_BUFFER).build());

                // selected columns
                for (ColumnIdentifier id : selection.regularAndStaticColumnsToFetch())
                    columns.add(builder.copy().add(id.key).build());
            }
            else
            {
                // We now that we're not composite so we can ignore static columns
                Iterator<CFDefinition.Name> iter = cfDef.regularColumns().iterator();
                while (iter.hasNext())
                {
                    ColumnIdentifier name = iter.next().name;
                    ColumnNameBuilder b = iter.hasNext() ? builder.copy() : builder;
                    ByteBuffer cname = b.add(name.key).build();
                    columns.add(cname);
                }
            }
            return columns;
        }
    }

    private boolean selectACollection()
    {
        if (!cfDef.hasCollections)
            return false;

        for (CFDefinition.Name name : selection.getColumnsList())
        {
            if (name.type instanceof CollectionType)
                return true;
        }

        return false;
    }

    private List<ByteBuffer> buildBound(Bound bound,
                                        Collection<CFDefinition.Name> names,
                                        Restriction[] restrictions,
                                        boolean isReversed,
                                        ColumnNameBuilder builder,
                                        List<ByteBuffer> variables) throws InvalidRequestException
    {
        // The end-of-component of composite doesn't depend on whether the
        // component type is reversed or not (i.e. the ReversedType is applied
        // to the component comparator but not to the end-of-component itself),
        // it only depends on whether the slice is reversed
        Bound eocBound = isReversed ? Bound.reverse(bound) : bound;
        for (Iterator<CFDefinition.Name> iter = names.iterator(); iter.hasNext();)
        {
            CFDefinition.Name name = iter.next();

            // In a restriction, we always have Bound.START < Bound.END for the "base" comparator.
            // So if we're doing a reverse slice, we must inverse the bounds when giving them as start and end of the slice filter.
            // But if the actual comparator itself is reversed, we must inversed the bounds too.
            Bound b = isReversed == isReversedType(name) ? bound : Bound.reverse(bound);
            Restriction r = restrictions[name.position];
            if (isNullRestriction(r, b))
            {
                // There wasn't any non EQ relation on that key, we select all records having the preceding component as prefix.
                // For composites, if there was preceding component and we're computing the end, we must change the last component
                // End-Of-Component, otherwise we would be selecting only one record.
                return Collections.singletonList(builder.componentCount() > 0 && eocBound == Bound.END
                                                 ? builder.buildAsEndOfRange()
                                                 : builder.build());
            }

            if (r.isSlice())
            {
                builder.add(getSliceValue(name, r, b, variables));
                Relation.Type relType = ((Restriction.Slice)r).getRelation(eocBound, b);

                // We can have more non null restriction if the "scalar" notation was used for the bound (#4851).
                // In that case, we need to add them all, and end the cell name with the correct end-of-component.
                while (iter.hasNext())
                {
                    name = iter.next();
                    r = restrictions[name.position];
                    if (isNullRestriction(r, b))
                        break;

                    builder.add(getSliceValue(name, r, b, variables));
                }
                return Collections.singletonList(builder.buildForRelation(relType));
            }
            else
            {
                List<ByteBuffer> values = r.values(variables);
                if (values.size() != 1)
                {
                    // IN query, we only support it on the clustering column
                    assert name.position == names.size() - 1;
                    // The IN query might not have listed the values in comparator order, so we need to re-sort
                    // the bounds lists to make sure the slices works correctly (also, to avoid duplicates).
                    TreeSet<ByteBuffer> s = new TreeSet<ByteBuffer>(isReversed ? cfDef.cfm.comparator.reverseComparator : cfDef.cfm.comparator);
                    for (ByteBuffer val : values)
                    {
                        if (val == null)
                            throw new InvalidRequestException(String.format("Invalid null clustering key part %s", name));
                        ColumnNameBuilder copy = builder.copy().add(val);
                        // See below for why this
                        s.add((b == Bound.END && copy.remainingCount() > 0) ? copy.buildAsEndOfRange() : copy.build());
                    }
                    return new ArrayList<ByteBuffer>(s);
                }

                ByteBuffer val = values.get(0);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null clustering key part %s", name));
                builder.add(val);
            }
        }
        // Means no relation at all or everything was an equal
        // Note: if the builder is "full", there is no need to use the end-of-component bit. For columns selection,
        // it would be harmless to do it. However, we use this method got the partition key too. And when a query
        // with 2ndary index is done, and with the the partition provided with an EQ, we'll end up here, and in that
        // case using the eoc would be bad, since for the random partitioner we have no guarantee that
        // builder.buildAsEndOfRange() will sort after builder.build() (see #5240).
        return Collections.singletonList((bound == Bound.END && builder.remainingCount() > 0) ? builder.buildAsEndOfRange() : builder.build());
    }

    private static boolean isNullRestriction(Restriction r, Bound b)
    {
        return r == null || (r.isSlice() && !((Restriction.Slice)r).hasBound(b));
    }

    private static ByteBuffer getSliceValue(CFDefinition.Name name, Restriction r, Bound b, List<ByteBuffer> variables) throws InvalidRequestException
    {
        Restriction.Slice slice = (Restriction.Slice)r;
        assert slice.hasBound(b);
        ByteBuffer val = slice.bound(b, variables);
        if (val == null)
            throw new InvalidRequestException(String.format("Invalid null clustering key part %s", name));
        return val;
    }

    private List<ByteBuffer> getRequestedBound(Bound b, List<ByteBuffer> variables) throws InvalidRequestException
    {
        assert isColumnRange();
        return buildBound(b, cfDef.clusteringColumns(), columnRestrictions, isReversed, cfDef.getColumnNameBuilder(), variables);
    }

    public List<IndexExpression> getIndexExpressions(List<ByteBuffer> variables) throws InvalidRequestException
    {
        if (!usesSecondaryIndexing || restrictedNames.isEmpty())
            return Collections.emptyList();

        List<IndexExpression> expressions = new ArrayList<IndexExpression>();
        for (CFDefinition.Name name : restrictedNames)
        {
            Restriction restriction;
            switch (name.kind)
            {
                case KEY_ALIAS:
                    restriction = keyRestrictions[name.position];
                    break;
                case COLUMN_ALIAS:
                    restriction = columnRestrictions[name.position];
                    break;
                case COLUMN_METADATA:
                case STATIC:
                    restriction = metadataRestrictions.get(name);
                    break;
                default:
                    // We don't allow restricting a VALUE_ALIAS for now in prepare.
                    throw new AssertionError();
            }

            if (restriction.isSlice())
            {
                Restriction.Slice slice = (Restriction.Slice)restriction;
                for (Bound b : Bound.values())
                {
                    if (slice.hasBound(b))
                    {
                        ByteBuffer value = slice.bound(b, variables);
                        if (value == null)
                            throw new InvalidRequestException(String.format("Unsupported null value for indexed column %s", name));
                        if (value.remaining() > 0xFFFF)
                            throw new InvalidRequestException("Index expression values may not be larger than 64K");
                        expressions.add(new IndexExpression(name.name.key, slice.getIndexOperator(b), value));
                    }
                }
            }
            else
            {
                List<ByteBuffer> values = restriction.values(variables);

                if (values.size() != 1)
                    throw new InvalidRequestException("IN restrictions are not supported on indexed columns");

                ByteBuffer value = values.get(0);
                if (value == null)
                    throw new InvalidRequestException(String.format("Unsupported null value for indexed column %s", name));
                if (value.remaining() > 0xFFFF)
                    throw new InvalidRequestException("Index expression values may not be larger than 64K");
                expressions.add(new IndexExpression(name.name.key, IndexOperator.EQ, value));
            }
        }
        return expressions;
    }

    private ResultSet process(List<Row> rows, List<ByteBuffer> variables, int limit, long now) throws InvalidRequestException
    {
        Selection.ResultSetBuilder result = selection.resultSetBuilder(now);
        for (org.apache.cassandra.db.Row row : rows)
        {
            // Not columns match the query, skip
            if (row.cf == null)
                continue;

            processColumnFamily(row.key.key, row.cf, variables, now, result);
        }

        ResultSet cqlRows = result.build();

        orderResults(cqlRows);

        // Internal calls always return columns in the comparator order, even when reverse was set
        if (isReversed)
            cqlRows.reverse();

        // Trim result if needed to respect the limit
        cqlRows.trim(limit);
        return cqlRows;
    }

    // Used by ModificationStatement for CAS operations
    void processColumnFamily(ByteBuffer key, ColumnFamily cf, List<ByteBuffer> variables, long now, Selection.ResultSetBuilder result)
    throws InvalidRequestException
    {
        ByteBuffer[] keyComponents = cfDef.hasCompositeKey
                                   ? ((CompositeType)cfDef.cfm.getKeyValidator()).split(key)
                                   : new ByteBuffer[]{ key };

        if (parameters.isDistinct)
        {
            if (!cf.hasOnlyTombstones(now))
            {
                result.newRow();
                // selection.getColumnsList() will contain only the partition key components - all of them.
                for (CFDefinition.Name name : selection.getColumnsList())
                    result.add(keyComponents[name.position]);
            }
        }
        else if (cfDef.isCompact)
        {
            // One cqlRow per column
            for (Column c : cf)
            {
                if (c.isMarkedForDelete(now))
                    continue;

                ByteBuffer[] components = null;
                if (cfDef.isComposite)
                {
                    components = ((CompositeType)cfDef.cfm.comparator).split(c.name());
                }
                else if (sliceRestriction != null)
                {
                    Comparator<ByteBuffer> comp = cfDef.cfm.comparator;
                    // For dynamic CF, the column could be out of the requested bounds, filter here
                    if (!sliceRestriction.isInclusive(Bound.START) && comp.compare(c.name(), sliceRestriction.bound(Bound.START, variables)) == 0)
                        continue;
                    if (!sliceRestriction.isInclusive(Bound.END) && comp.compare(c.name(), sliceRestriction.bound(Bound.END, variables)) == 0)
                        continue;
                }

                result.newRow();
                // Respect selection order
                for (CFDefinition.Name name : selection.getColumnsList())
                {
                    switch (name.kind)
                    {
                        case KEY_ALIAS:
                            result.add(keyComponents[name.position]);
                            break;
                        case COLUMN_ALIAS:
                            ByteBuffer val = cfDef.isComposite
                                           ? (name.position < components.length ? components[name.position] : null)
                                           : c.name();
                            result.add(val);
                            break;
                        case VALUE_ALIAS:
                            result.add(c);
                            break;
                        case COLUMN_METADATA:
                        case STATIC:
                            // This should not happen for compact CF
                            throw new AssertionError();
                        default:
                            throw new AssertionError();
                    }
                }
            }
        }
        else if (cfDef.isComposite)
        {
            // Sparse case: group column in cqlRow when composite prefix is equal
            CompositeType composite = (CompositeType)cfDef.cfm.comparator;

            ColumnGroupMap.Builder builder = new ColumnGroupMap.Builder(composite, cfDef.hasCollections, now);

            for (Column c : cf)
            {
                if (c.isMarkedForDelete(now))
                    continue;

                builder.add(c);
            }

            Map<CFDefinition.Name, ByteBuffer> staticValues = Collections.emptyMap();
            // Gather up static values first
            if (!builder.isEmpty() && builder.firstGroup().isStatic)
            {
                staticValues = new HashMap<>();
                ColumnGroupMap group = builder.firstGroup();
                for (CFDefinition.Name name : Iterables.filter(selection.getColumnsList(), isStaticFilter))
                    staticValues.put(name, getValue(name, group));
                builder.discardFirst();

                // If there was static columns but there is no actual row, then provided the select was a full
                // partition selection (i.e. not a 2ndary index search and there was no condition on clustering columns)
                // then we want to include the static columns in the result set.
                if (!staticValues.isEmpty() && builder.isEmpty() && !usesSecondaryIndexing && hasNoClusteringColumnsRestriction())
                {
                    result.newRow();
                    for (CFDefinition.Name name : selection.getColumnsList())
                    {
                        if (name.kind == CFDefinition.Name.Kind.KEY_ALIAS)
                            result.add(keyComponents[name.position]);
                        else
                            result.add(name.kind == CFDefinition.Name.Kind.STATIC ? staticValues.get(name) : null);
                    }
                    return;
                }
            }

            for (ColumnGroupMap group : builder.groups())
                handleGroup(selection, result, keyComponents, group, staticValues);
        }
        else
        {
            if (cf.hasOnlyTombstones(now))
                return;

            // Static case: One cqlRow for all columns
            result.newRow();
            for (CFDefinition.Name name : selection.getColumnsList())
            {
                if (name.kind == CFDefinition.Name.Kind.KEY_ALIAS)
                    result.add(keyComponents[name.position]);
                else
                    result.add(cf.getColumn(name.name.key));
            }
        }
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
    private void orderResults(ResultSet cqlRows)
    {
        if (cqlRows.size() == 0 || !needsPostQueryOrdering())
            return;

        assert orderingIndexes != null;

        // optimization when only *one* order condition was given
        // because there is no point of using composite comparator if there is only one order condition
        if (parameters.orderings.size() == 1)
        {
            CFDefinition.Name ordering = cfDef.get(parameters.orderings.keySet().iterator().next());
            Collections.sort(cqlRows.rows, new SingleColumnComparator(orderingIndexes.get(ordering), ordering.type));
            return;
        }

        // builds a 'composite' type for multi-column comparison from the comparators of the ordering components
        // and passes collected position information and built composite comparator to CompositeComparator to do
        // an actual comparison of the CQL rows.
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(parameters.orderings.size());
        int[] positions = new int[parameters.orderings.size()];

        int idx = 0;
        for (ColumnIdentifier identifier : parameters.orderings.keySet())
        {
            CFDefinition.Name orderingColumn = cfDef.get(identifier);
            types.add(orderingColumn.type);
            positions[idx++] = orderingIndexes.get(orderingColumn);
        }

        Collections.sort(cqlRows.rows, new CompositeComparator(types, positions));
    }

    private void handleGroup(Selection selection,
                             Selection.ResultSetBuilder result,
                             ByteBuffer[] keyComponents,
                             ColumnGroupMap columns,
                             Map<CFDefinition.Name, ByteBuffer> staticValues) throws InvalidRequestException
    {
        // Respect requested order
        result.newRow();
        for (CFDefinition.Name name : selection.getColumnsList())
        {
            switch (name.kind)
            {
                case KEY_ALIAS:
                    result.add(keyComponents[name.position]);
                    break;
                case COLUMN_ALIAS:
                    result.add(columns.getKeyComponent(name.position));
                    break;
                case VALUE_ALIAS:
                    // This should not happen for SPARSE
                    throw new AssertionError();
                case COLUMN_METADATA:
                    if (name.type.isCollection())
                    {
                        result.add(getCollectionValue(name, columns));
                    }
                    else
                    {
                        result.add(columns.getSimple(name.name.key));
                    }
                    break;
                case STATIC:
                    result.add(staticValues.get(name));
                    break;
            }
        }
    }

    private static ByteBuffer getValue(CFDefinition.Name name, ColumnGroupMap columns)
    {
        if (name.type.isCollection())
            return getCollectionValue(name, columns);
        else if (name.type.isCommutative())
            return getCounterValue(name, columns);

        return getSimpleValue(name, columns);
    }

    private static ByteBuffer getCollectionValue(CFDefinition.Name name, ColumnGroupMap columns)
    {
        List<Pair<ByteBuffer, Column>> collection = columns.getCollection(name.name.key);
        return collection == null ? null : ((CollectionType)name.type).serialize(collection);
    }

    private static ByteBuffer getSimpleValue(CFDefinition.Name name, ColumnGroupMap columns)
    {
        Column c = columns.getSimple(name.name.key);
        return c == null ? null : c.value();
    }

    private static ByteBuffer getCounterValue(CFDefinition.Name name, ColumnGroupMap columns)
    {
        Column c = columns.getSimple(name.name.key);
        return c == null ? null : CounterColumnType.instance.decompose(CounterContext.instance().total(c.value()));
    }

    private static boolean isReversedType(CFDefinition.Name name)
    {
        return name.type instanceof ReversedType;
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

    public static class RawStatement extends CFStatement
    {
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

            CFDefinition cfDef = cfm.getCfDef();

            VariableSpecifications names = getBoundVariables();

            // Select clause
            if (parameters.isCount && !selectClause.isEmpty())
                throw new InvalidRequestException("Only COUNT(*) and COUNT(1) operations are currently supported.");

            Selection selection = selectClause.isEmpty()
                                ? Selection.wildcard(cfDef)
                                : Selection.fromSelectors(cfDef, selectClause);

            if (parameters.isDistinct)
                validateDistinctSelection(selection.getColumnsList(), cfDef.partitionKeys());

            Term prepLimit = null;
            if (limit != null)
            {
                prepLimit = limit.prepare(limitReceiver());
                prepLimit.collectMarkerSpecification(names);
            }

            SelectStatement stmt = new SelectStatement(cfDef, names.size(), parameters, selection, prepLimit);

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
            for (Relation rel : whereClause)
            {
                CFDefinition.Name name = cfDef.get(rel.getEntity());
                if (name == null)
                {
                    if (containsAlias(rel.getEntity()))
                        throw new InvalidRequestException(String.format("Aliases aren't allowed in where clause ('%s')", rel));
                    else
                        throw new InvalidRequestException(String.format("Undefined name %s in where clause ('%s')", rel.getEntity(), rel));
                }

                ColumnDefinition def = cfDef.cfm.getColumnDefinition(name.name.key);
                stmt.restrictedNames.add(name);
                if (def.isIndexed() && rel.operator() == Relation.Type.EQ)
                {
                    hasQueriableIndex = true;
                    if (name.kind == CFDefinition.Name.Kind.COLUMN_ALIAS)
                        hasQueriableClusteringColumnIndex = true;
                }

                switch (name.kind)
                {
                    case KEY_ALIAS:
                        stmt.keyRestrictions[name.position] = updateRestriction(cfm, name, stmt.keyRestrictions[name.position], rel, names);
                        break;
                    case COLUMN_ALIAS:
                        stmt.columnRestrictions[name.position] = updateRestriction(cfm, name, stmt.columnRestrictions[name.position], rel, names);
                        break;
                    case VALUE_ALIAS:
                        throw new InvalidRequestException(String.format("Predicates on the non-primary-key column (%s) of a COMPACT table are not yet supported", name.name));
                    case COLUMN_METADATA:
                    case STATIC:
                        // We only all IN on the row key and last clustering key so far, never on non-PK columns, and this even if there's an index
                        Restriction r = updateRestriction(cfm, name, stmt.metadataRestrictions.get(name), rel, names);
                        if (r.isIN() && !((Restriction.IN)r).canHaveOnlyOneValue())
                            // Note: for backward compatibility reason, we conside a IN of 1 value the same as a EQ, so we let that slide.
                            throw new InvalidRequestException(String.format("IN predicates on non-primary-key columns (%s) is not yet supported", name));
                        stmt.metadataRestrictions.put(name, r);
                        break;
                }
            }

            /*
             * At this point, the select statement if fully constructed, but we still have a few things to validate
             */

            // If there is a queriable index, no special condition are required on the other restrictions.
            // But we still need to know 2 things:
            //   - If we don't have a queriable index, is the query ok
            //   - Is it queriable without 2ndary index, which is always more efficient
            // If a component of the partition key is restricted by a relation, all preceding
            // components must have a EQ. Only the last partition key component can be in IN relation.
            boolean canRestrictFurtherComponents = true;
            CFDefinition.Name previous = null;
            stmt.keyIsInRelation = false;
            Iterator<CFDefinition.Name> iter = cfDef.partitionKeys().iterator();
            for (int i = 0; i < stmt.keyRestrictions.length; i++)
            {
                CFDefinition.Name cname = iter.next();
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
                        throw new InvalidRequestException(String.format("Partition key part %s must be restricted since preceding part is", cname));
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
                    throw new InvalidRequestException(String.format("partition key part %s cannot be restricted (preceding part %s is either not restricted or by a non-EQ relation)", cname, previous));
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
                            throw new InvalidRequestException(String.format("Partition KEY part %s cannot be restricted by IN relation (only the last part of the partition key can)", cname));
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
                previous = cname;
            }

            // All (or none) of the partition key columns have been specified;
            // hence there is no need to turn these restrictions into index expressions.
            if (!stmt.usesSecondaryIndexing)
                stmt.restrictedNames.removeAll(cfDef.partitionKeys());

            if (stmt.selectsOnlyStaticColumns && stmt.hasClusteringColumnsRestriction())
                throw new InvalidRequestException("Cannot restrict clustering columns when selecting only static columns");

            // If a clustering key column is restricted by a non-EQ relation, all preceding
            // columns must have a EQ, and all following must have no restriction. Unless
            // the column is indexed that is.
            canRestrictFurtherComponents = true;
            previous = null;
            boolean previousIsSlice = false;
            iter = cfDef.clusteringColumns().iterator();
            for (int i = 0; i < stmt.columnRestrictions.length; i++)
            {
                CFDefinition.Name cname = iter.next();
                Restriction restriction = stmt.columnRestrictions[i];

                if (restriction == null)
                {
                    canRestrictFurtherComponents = false;
                    previousIsSlice = false;
                }
                else if (!canRestrictFurtherComponents)
                {
                    // We're here if the previous clustering column was either not restricted or was a slice.
                    // We can't restrict the current column unless:
                    //   1) we're in the special case of the 'tuple' notation from #4851 which we expand as multiple
                    //      consecutive slices: in which case we're good with this restriction and we continue
                    //   2) we have a 2ndary index, in which case we have to use it but can skip more validation
                    boolean hasTuple = false;
                    boolean hasRestrictedNotTuple = false;
                    if (!(previousIsSlice && restriction.isSlice() && ((Restriction.Slice)restriction).isPartOfTuple()))
                    {
                        if (hasQueriableIndex)
                        {
                            stmt.usesSecondaryIndexing = true; // handle gaps and non-keyrange cases.
                            break;
                        }
                        throw new InvalidRequestException(String.format("PRIMARY KEY part %s cannot be restricted (preceding part %s is either not restricted or by a non-EQ relation)", cname, previous));
                    }
                }
                else if (restriction.isSlice())
                {
                    canRestrictFurtherComponents = false;
                    previousIsSlice = true;
                    Restriction.Slice slice = (Restriction.Slice)restriction;
                    // For non-composite slices, we don't support internally the difference between exclusive and
                    // inclusive bounds, so we deal with it manually.
                    if (!cfDef.isComposite && (!slice.isInclusive(Bound.START) || !slice.isInclusive(Bound.END)))
                        stmt.sliceRestriction = slice;
                }
                else if (restriction.isIN())
                {
                    // We only support IN for the last name and for compact storage so far
                    // TODO: #3885 allows us to extend to non compact as well, but that remains to be done
                    if (i != stmt.columnRestrictions.length - 1)
                        throw new InvalidRequestException(String.format("PRIMARY KEY part %s cannot be restricted by IN relation", cname));
                    else if (stmt.selectACollection())
                        throw new InvalidRequestException(String.format("Cannot restrict PRIMARY KEY part %s by IN relation as a collection is selected by the query", cname));
                }

                previous = cname;
            }

            // Covers indexes on the first clustering column (among others).
            if (stmt.isKeyRange && hasQueriableClusteringColumnIndex)
                stmt.usesSecondaryIndexing = true;

            if (!stmt.usesSecondaryIndexing)
                stmt.restrictedNames.removeAll(cfDef.clusteringColumns());

            // Even if usesSecondaryIndexing is false at this point, we'll still have to use one if
            // there is restrictions not covered by the PK.
            if (!stmt.metadataRestrictions.isEmpty())
            {
                if (!hasQueriableIndex)
                    throw new InvalidRequestException("No indexed columns present in by-columns clause with Equal operator");
                stmt.usesSecondaryIndexing = true;
            }

            if (stmt.usesSecondaryIndexing)
            {
                if (stmt.keyIsInRelation)
                    throw new InvalidRequestException("Select on indexed columns and with IN clause for the PRIMARY KEY are not supported");
                // When the user only select static columns, the intent is that we don't query the whole partition but just
                // the static parts. But 1) we don't have an easy way to do that with 2i and 2) since we don't support index on static columns
                // so far, 2i means that you've restricted a non static column, so the query is somewhat non-sensical.
                if (stmt.selectsOnlyStaticColumns)
                    throw new InvalidRequestException("Queries using 2ndary indexes don't support selecting only static columns");
            }

            if (!stmt.parameters.orderings.isEmpty())
            {
                if (stmt.usesSecondaryIndexing)
                    throw new InvalidRequestException("ORDER BY with 2ndary indexes is not supported.");

                if (stmt.isKeyRange)
                    throw new InvalidRequestException("ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");

                // If we order an IN query, we'll have to do a manual sort post-query. Currently, this sorting requires that we
                // have queried the column on which we sort (TODO: we should update it to add the column on which we sort to the one
                // queried automatically, and then removing it from the resultSet afterwards if needed)
                if (stmt.keyIsInRelation)
                {
                    stmt.orderingIndexes = new HashMap<CFDefinition.Name, Integer>();
                    for (ColumnIdentifier column : stmt.parameters.orderings.keySet())
                    {
                        final CFDefinition.Name name = cfDef.get(column);
                        if (name == null)
                        {
                            if (containsAlias(column))
                                throw new InvalidRequestException(String.format("Aliases are not allowed in order by clause ('%s')", column));
                            else
                                throw new InvalidRequestException(String.format("Order by on unknown column %s", column));
                        }

                        if (selectClause.isEmpty()) // wildcard
                        {
                            stmt.orderingIndexes.put(name, Iterables.indexOf(cfDef, new Predicate<CFDefinition.Name>()
                                                                                    {
                                                                                        public boolean apply(CFDefinition.Name n)
                                                                                        {
                                                                                            return name.equals(n);
                                                                                        }
                                                                                    }));
                        }
                        else
                        {
                            boolean hasColumn = false;
                            for (int i = 0; i < selectClause.size(); i++)
                            {
                                RawSelector selector = selectClause.get(i);
                                if (name.name.equals(selector.selectable))
                                {
                                    stmt.orderingIndexes.put(name, i);
                                    hasColumn = true;
                                    break;
                                }
                            }

                            if (!hasColumn)
                                throw new InvalidRequestException("ORDER BY could not be used on columns missing in select clause.");
                        }
                    }
                }

                Boolean[] reversedMap = new Boolean[cfDef.clusteringColumnsCount()];
                int i = 0;
                for (Map.Entry<ColumnIdentifier, Boolean> entry : stmt.parameters.orderings.entrySet())
                {
                    ColumnIdentifier column = entry.getKey();
                    boolean reversed = entry.getValue();

                    CFDefinition.Name name = cfDef.get(column);
                    if (name == null)
                    {
                        if (containsAlias(column))
                            throw new InvalidRequestException(String.format("Aliases are not allowed in order by clause ('%s')", column));
                        else
                            throw new InvalidRequestException(String.format("Order by on unknown column %s", column));
                    }

                    if (name.kind != CFDefinition.Name.Kind.COLUMN_ALIAS)
                        throw new InvalidRequestException(String.format("Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", column));

                    if (i++ != name.position)
                        throw new InvalidRequestException(String.format("Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY"));

                    reversedMap[name.position] = (reversed != isReversedType(name));
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
                    if (isReversed != b)
                        throw new InvalidRequestException(String.format("Unsupported order by relation"));
                }
                assert isReversed != null;
                stmt.isReversed = isReversed;
            }

            // Make sure this queries is allowed (note: non key range non indexed cannot involve filtering underneath)
            if (!parameters.allowFiltering && (stmt.isKeyRange || stmt.usesSecondaryIndexing))
            {
                // We will potentially filter data if either:
                //  - Have more than one IndexExpression
                //  - Have no index expression and the column filter is not the identity
                if (stmt.restrictedNames.size() > 1 || (stmt.restrictedNames.isEmpty() && !stmt.columnFilterIsIdentity()))
                    throw new InvalidRequestException("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. "
                                                    + "If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING");
            }

            return new ParsedStatement.Prepared(stmt, names);
        }

        private void validateDistinctSelection(Collection<CFDefinition.Name> requestedColumns, Collection<CFDefinition.Name> partitionKey)
        throws InvalidRequestException
        {
            for (CFDefinition.Name name : requestedColumns)
                if (!partitionKey.contains(name))
                    throw new InvalidRequestException(String.format("SELECT DISTINCT queries must only request partition key columns (not %s)", name));

            for (CFDefinition.Name name : partitionKey)
                if (!requestedColumns.contains(name))
                    throw new InvalidRequestException(String.format("SELECT DISTINCT queries must request all the partition key columns (missing %s)", name));
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

        Restriction updateRestriction(CFMetaData cfm, CFDefinition.Name name, Restriction restriction, Relation newRel, VariableSpecifications boundNames) throws InvalidRequestException
        {
            ColumnSpecification receiver = name;
            if (newRel.onToken)
            {
                if (name.kind != CFDefinition.Name.Kind.KEY_ALIAS)
                    throw new InvalidRequestException(String.format("The token() function is only supported on the partition key, found on %s", name));

                receiver = new ColumnSpecification(name.ksName,
                                                   name.cfName,
                                                   new ColumnIdentifier("partition key token", true),
                                                   StorageService.getPartitioner().getTokenValidator());
            }

            // We can only use the tuple notation of #4851 on clustering columns for now
            if (newRel.previousInTuple != null && name.kind != CFDefinition.Name.Kind.COLUMN_ALIAS)
                throw new InvalidRequestException(String.format("Tuple notation can only be used on clustering columns but found on %s", name));

            switch (newRel.operator())
            {
                case EQ:
                    {
                        if (restriction != null)
                            throw new InvalidRequestException(String.format("%s cannot be restricted by more than one relation if it includes an Equal", name));
                        Term t = newRel.getValue().prepare(receiver);
                        t.collectMarkerSpecification(boundNames);
                        restriction = new Restriction.EQ(t, newRel.onToken);
                    }
                    break;
                case IN:
                    if (restriction != null)
                        throw new InvalidRequestException(String.format("%s cannot be restricted by more than one relation if it includes a IN", name));

                    if (newRel.getInValues() == null)
                    {
                        // Means we have a "SELECT ... IN ?"
                        assert newRel.getValue() != null;
                        Term t = newRel.getValue().prepare(receiver);
                        t.collectMarkerSpecification(boundNames);
                        restriction = Restriction.IN.create(t);
                    }
                    else
                    {
                        List<Term> inValues = new ArrayList<Term>(newRel.getInValues().size());
                        for (Term.Raw raw : newRel.getInValues())
                        {
                            Term t = raw.prepare(receiver);
                            t.collectMarkerSpecification(boundNames);
                            inValues.add(t);
                        }
                        restriction = Restriction.IN.create(inValues);
                    }
                    break;
                case GT:
                case GTE:
                case LT:
                case LTE:
                    {
                        if (restriction == null)
                            restriction = new Restriction.Slice(newRel.onToken);
                        else if (!restriction.isSlice())
                            throw new InvalidRequestException(String.format("%s cannot be restricted by both an equal and an inequal relation", name));
                        Term t = newRel.getValue().prepare(receiver);
                        t.collectMarkerSpecification(boundNames);
                        if (newRel.previousInTuple != null && (name.position == 0 || !cfm.clusteringKeyColumns().get(name.position - 1).name.equals(newRel.previousInTuple.key)))
                            throw new InvalidRequestException(String.format("Invalid tuple notation, column %s is not before column %s in the clustering order", newRel.previousInTuple, name.name));
                        ((Restriction.Slice)restriction).setBound(name.name, newRel.operator(), t, newRel.previousInTuple);
                    }
                    break;
            }
            return restriction;
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
        private final Map<ColumnIdentifier, Boolean> orderings;
        private final boolean isDistinct;
        private final boolean isCount;
        private final ColumnIdentifier countAlias;
        private final boolean allowFiltering;

        public Parameters(Map<ColumnIdentifier, Boolean> orderings,
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
        private final AbstractType<?> comparator;

        public SingleColumnComparator(int columnIndex, AbstractType<?> orderer)
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
        private final List<AbstractType<?>> orderTypes;
        private final int[] positions;

        private CompositeComparator(List<AbstractType<?>> orderTypes, int[] positions)
        {
            this.orderTypes = orderTypes;
            this.positions = positions;
        }

        public int compare(List<ByteBuffer> a, List<ByteBuffer> b)
        {
            for (int i = 0; i < positions.length; i++)
            {
                AbstractType<?> type = orderTypes.get(i);
                int columnPos = positions[i];

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
