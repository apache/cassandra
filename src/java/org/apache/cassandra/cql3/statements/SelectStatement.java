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

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.RangeSliceVerbHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.Pageable;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.QueryPagers;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 */
public class SelectStatement implements CQLStatement
{
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
    private Restriction sliceRestriction;

    private boolean isReversed;
    private boolean onToken;
    private boolean isKeyRange;
    private boolean keyIsInRelation;
    private boolean usesSecondaryIndexing;

    private Map<CFDefinition.Name, Integer> orderingIndexes;

    // Used by forSelection below
    private static final Parameters defaultParameters = new Parameters(Collections.<ColumnIdentifier, Boolean>emptyMap(), false, null, false);

    private static enum Bound
    {
        START(0), END(1);

        public final int idx;

        Bound(int idx)
        {
            this.idx = idx;
        }

        public static Bound reverse(Bound b)
        {
            return b == START ? END : START;
        }
    }

    public SelectStatement(CFDefinition cfDef, int boundTerms, Parameters parameters, Selection selection, Term limit)
    {
        this.cfDef = cfDef;
        this.boundTerms = boundTerms;
        this.selection = selection;
        this.keyRestrictions = new Restriction[cfDef.keys.size()];
        this.columnRestrictions = new Restriction[cfDef.columns.size()];
        this.parameters = parameters;
        this.limit = limit;
    }

    // Creates a simple select based on the given selection.
    // Note that the results select statement should not be used for actual queries, but only for processing already
    // queried data through processColumnFamily.
    static SelectStatement forSelection(CFDefinition cfDef, Selection selection)
    {
        return new SelectStatement(cfDef, 0, defaultParameters, selection, null);
    }

    public int getBoundsTerms()
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

    public ResultMessage.Rows execute(ConsistencyLevel cl, QueryState state, List<ByteBuffer> variables, int pageSize) throws RequestExecutionException, RequestValidationException
    {
        if (cl == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        cl.validateForRead(keyspace());


        int limit = getLimit(variables);
        long now = System.currentTimeMillis();
        Pageable command = isKeyRange || usesSecondaryIndexing
                         ? getRangeCommand(variables, limit, now)
                         : new Pageable.ReadCommands(getSliceCommands(variables, limit, now));

        // A count query will never be paged for the user, but we always page it internally to avoid OOM.
        // If we user provided a pageSize we'll use that to page internally (because why not), otherwise we use our default
        if (parameters.isCount && pageSize < 0)
            pageSize = DEFAULT_COUNT_PAGE_SIZE;

        if (pageSize < 0 || !QueryPagers.mayNeedPaging(command, pageSize))
        {
            return execute(command, cl, variables, limit, now);
        }
        else
        {
            QueryPager pager = QueryPagers.pager(command, cl);
            return parameters.isCount
                 ? pageCountQuery(pager, variables, pageSize)
                 : setupPaging(pager, state, variables, limit, pageSize);
        }
    }

    private ResultMessage.Rows execute(Pageable command, ConsistencyLevel cl, List<ByteBuffer> variables, int limit, long now) throws RequestValidationException, RequestExecutionException
    {
        List<Row> rows = command instanceof Pageable.ReadCommands
                       ? StorageProxy.read(((Pageable.ReadCommands)command).commands, cl)
                       : StorageProxy.getRangeSlice((RangeSliceCommand)command, cl);

        return processResults(rows, variables, limit, now);
    }

    // TODO: we could probably refactor processResults so it doesn't needs the variables, so we don't have to keep around. But that can wait.
    private ResultMessage.Rows setupPaging(QueryPager pager, QueryState state, List<ByteBuffer> variables, int limit, int pageSize) throws RequestValidationException, RequestExecutionException
    {
        List<Row> page = pager.fetchPage(pageSize);

        ResultMessage.Rows msg = processResults(page, variables, limit, pager.timestamp());

        // Don't bother setting up the pager if we actually don't need to.
        if (pager.isExhausted())
            return msg;

        state.attachPager(pager, this, variables);
        msg.result.metadata.setHasMorePages();
        return msg;
    }

    private ResultMessage.Rows pageCountQuery(QueryPager pager, List<ByteBuffer> variables, int pageSize) throws RequestValidationException, RequestExecutionException
    {
        int count = 0;
        while (!pager.isExhausted())
        {
            int maxLimit = pager.maxRemaining();
            ResultSet rset = process(pager.fetchPage(pageSize), variables, maxLimit, pager.timestamp());
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
        List<Row> rows = isKeyRange || usesSecondaryIndexing
                       ? getRangeCommand(variables, limit, now).executeLocally()
                       : readLocally(keyspace(), getSliceCommands(variables, limit, now));

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
        List<ReadCommand> commands = new ArrayList<ReadCommand>(keys.size());

        IDiskAtomFilter filter = makeFilter(variables, limit);
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
        List<IndexExpression> expressions = getIndexExpressions(variables);
        // The LIMIT provided by the user is the number of CQL row he wants returned.
        // We want to have getRangeSlice to count the number of columns, not the number of keys.
        return new RangeSliceCommand(keyspace(), columnFamily(), now,  filter, getKeyBounds(variables), expressions, limit, true, false);
    }

    private AbstractBounds<RowPosition> getKeyBounds(List<ByteBuffer> variables) throws InvalidRequestException
    {
        IPartitioner<?> p = StorageService.getPartitioner();
        AbstractBounds<RowPosition> bounds;

        if (onToken)
        {
            Token startToken = getTokenBound(Bound.START, variables, p);
            Token endToken = getTokenBound(Bound.END, variables, p);

            RowPosition start = includeKeyBound(Bound.START) ? startToken.minKeyBound() : startToken.maxKeyBound();
            RowPosition end = includeKeyBound(Bound.END) ? endToken.maxKeyBound() : endToken.minKeyBound();
            bounds = new Range<RowPosition>(start, end);
        }
        else
        {
            ByteBuffer startKeyBytes = getKeyBound(Bound.START, variables);
            ByteBuffer finishKeyBytes = getKeyBound(Bound.END, variables);

            RowPosition startKey = RowPosition.forKey(startKeyBytes, p);
            RowPosition finishKey = RowPosition.forKey(finishKeyBytes, p);
            if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
            {
                if (p.preservesOrder())
                    throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
                else
                    throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all under random partitioner");
            }
            if (includeKeyBound(Bound.START))
            {
                bounds = includeKeyBound(Bound.END)
                    ? new Bounds<RowPosition>(startKey, finishKey)
                    : new IncludingExcludingBounds<RowPosition>(startKey, finishKey);
            }
            else
            {
                bounds = includeKeyBound(Bound.END)
                    ? new Range<RowPosition>(startKey, finishKey)
                    : new ExcludingBounds<RowPosition>(startKey, finishKey);
            }
        }
        return bounds;
    }

    private IDiskAtomFilter makeFilter(List<ByteBuffer> variables, int limit)
    throws InvalidRequestException
    {
        if (isColumnRange())
        {
            // For sparse, we used to ask for 'defined columns' * 'asked limit' (where defined columns includes the row marker)
            // to account for the grouping of columns.
            // Since that doesn't work for maps/sets/lists, we now use the compositesToGroup option of SliceQueryFilter.
            // But we must preserve backward compatibility too (for mixed version cluster that is).
            int toGroup = cfDef.isCompact ? -1 : cfDef.columns.size();
            ColumnSlice slice = new ColumnSlice(getRequestedBound(Bound.START, variables),
                                                getRequestedBound(Bound.END, variables));
            SliceQueryFilter filter = new SliceQueryFilter(new ColumnSlice[]{slice},
                                                           isReversed,
                                                           limit,
                                                           toGroup);
            QueryProcessor.validateSliceFilter(cfDef.cfm, filter);
            return filter;
        }
        else
        {
            SortedSet<ByteBuffer> columnNames = getRequestedColumns(variables);
            QueryProcessor.validateColumnNames(columnNames);
            return new NamesQueryFilter(columnNames, true);
        }
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
        if (sliceRestriction != null && !sliceRestriction.isInclusive(Bound.START) && l != Integer.MAX_VALUE)
            l += 1;

        return l;
    }

    private Collection<ByteBuffer> getKeys(final List<ByteBuffer> variables) throws InvalidRequestException
    {
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        ColumnNameBuilder builder = cfDef.getKeyNameBuilder();
        for (CFDefinition.Name name : cfDef.keys.values())
        {
            Restriction r = keyRestrictions[name.position];
            assert r != null;
            if (builder.remainingCount() == 1)
            {
                for (Term t : r.eqValues)
                {
                    ByteBuffer val = t.bindAndGet(variables);
                    if (val == null)
                        throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", name));
                    keys.add(builder.copy().add(val).build());
                }
            }
            else
            {
                if (r.eqValues.size() > 1)
                    throw new InvalidRequestException("IN is only supported on the last column of the partition key");
                ByteBuffer val = r.eqValues.get(0).bindAndGet(variables);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", name));
                builder.add(val);
            }
        }
        return keys;
    }

    private ByteBuffer getKeyBound(Bound b, List<ByteBuffer> variables) throws InvalidRequestException
    {
        return buildBound(b, cfDef.keys.values(), keyRestrictions, false, cfDef.getKeyNameBuilder(), variables);
    }

    private Token getTokenBound(Bound b, List<ByteBuffer> variables, IPartitioner<?> p) throws InvalidRequestException
    {
        assert onToken;

        Restriction keyRestriction = keyRestrictions[0];
        Term t = keyRestriction.isEquality()
               ? keyRestriction.eqValues.get(0)
               : keyRestriction.bound(b);

        if (t == null)
            return p.getMinimumToken();

        ByteBuffer value = t.bindAndGet(variables);
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
            else if (!r.isEquality())
                return r.isInclusive(b);
        }
        // All equality
        return true;
    }

    private boolean isColumnRange()
    {
        // Static CF never entails a column slice
        if (!cfDef.isCompact && !cfDef.isComposite)
            return false;

        // However, collections always entails one
        if (selectACollection())
            return true;

        // Otherwise, it is a range query if it has at least one the column alias
        // for which no relation is defined or is not EQ.
        for (Restriction r : columnRestrictions)
        {
            if (r == null || !r.isEquality())
                return true;
        }
        return false;
    }

    private SortedSet<ByteBuffer> getRequestedColumns(List<ByteBuffer> variables) throws InvalidRequestException
    {
        assert !isColumnRange();

        ColumnNameBuilder builder = cfDef.getColumnNameBuilder();
        Iterator<ColumnIdentifier> idIter = cfDef.columns.keySet().iterator();
        for (Restriction r : columnRestrictions)
        {
            ColumnIdentifier id = idIter.next();
            assert r != null && r.isEquality();
            if (r.eqValues.size() > 1)
            {
                // We have a IN, which we only support for the last column.
                // If compact, just add all values and we're done. Otherwise,
                // for each value of the IN, creates all the columns corresponding to the selection.
                SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(cfDef.cfm.comparator);
                Iterator<Term> iter = r.eqValues.iterator();
                while (iter.hasNext())
                {
                    Term v = iter.next();
                    ColumnNameBuilder b = iter.hasNext() ? builder.copy() : builder;
                    ByteBuffer val = v.bindAndGet(variables);
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
            else
            {
                ByteBuffer val = r.eqValues.get(0).bindAndGet(variables);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for clustering key part %s", id));
                builder.add(val);
            }
        }

        return addSelectedColumns(builder);
    }

    private SortedSet<ByteBuffer> addSelectedColumns(ColumnNameBuilder builder)
    {
        if (cfDef.isCompact)
        {
            return FBUtilities.singleton(builder.build());
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
                for (ColumnIdentifier id : selection.regularColumnsToFetch())
                    columns.add(builder.copy().add(id.key).build());
            }
            else
            {
                Iterator<ColumnIdentifier> iter = cfDef.metadata.keySet().iterator();
                while (iter.hasNext())
                {
                    ColumnIdentifier name = iter.next();
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

    private static ByteBuffer buildBound(Bound bound,
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
        for (CFDefinition.Name name : names)
        {
            // In a restriction, we always have Bound.START < Bound.END for the "base" comparator.
            // So if we're doing a reverse slice, we must inverse the bounds when giving them as start and end of the slice filter.
            // But if the actual comparator itself is reversed, we must inversed the bounds too.
            Bound b = isReversed == isReversedType(name) ? bound : Bound.reverse(bound);
            Restriction r = restrictions[name.position];
            if (r == null || (!r.isEquality() && r.bound(b) == null))
            {
                // There wasn't any non EQ relation on that key, we select all records having the preceding component as prefix.
                // For composites, if there was preceding component and we're computing the end, we must change the last component
                // End-Of-Component, otherwise we would be selecting only one record.
                if (builder.componentCount() > 0 && eocBound == Bound.END)
                    return builder.buildAsEndOfRange();
                else
                    return builder.build();
            }

            if (r.isEquality())
            {
                assert r.eqValues.size() == 1;
                ByteBuffer val = r.eqValues.get(0).bindAndGet(variables);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null clustering key part %s", name));
                builder.add(val);
            }
            else
            {
                Term t = r.bound(b);
                assert t != null;
                ByteBuffer val = t.bindAndGet(variables);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null clustering key part %s", name));
                return builder.add(val, r.getRelation(eocBound, b)).build();
            }
        }
        // Means no relation at all or everything was an equal
        // Note: if the builder is "full", there is no need to use the end-of-component bit. For columns selection,
        // it would be harmless to do it. However, we use this method got the partition key too. And when a query
        // with 2ndary index is done, and with the the partition provided with an EQ, we'll end up here, and in that
        // case using the eoc would be bad, since for the random partitioner we have no guarantee that
        // builder.buildAsEndOfRange() will sort after builder.build() (see #5240).
        return (bound == Bound.END && builder.remainingCount() > 0) ? builder.buildAsEndOfRange() : builder.build();
    }

    private ByteBuffer getRequestedBound(Bound b, List<ByteBuffer> variables) throws InvalidRequestException
    {
        assert isColumnRange();
        return buildBound(b, cfDef.columns.values(), columnRestrictions, isReversed, cfDef.getColumnNameBuilder(), variables);
    }

    private List<IndexExpression> getIndexExpressions(List<ByteBuffer> variables) throws InvalidRequestException
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
                    restriction = metadataRestrictions.get(name);
                    break;
                default:
                    // We don't allow restricting a VALUE_ALIAS for now in prepare.
                    throw new AssertionError();
            }
            if (restriction.isEquality())
            {
                for (Term t : restriction.eqValues)
                {
                    ByteBuffer value = t.bindAndGet(variables);
                    if (value == null)
                        throw new InvalidRequestException(String.format("Unsupported null value for indexed column %s", name));
                    if (value.remaining() > 0xFFFF)
                        throw new InvalidRequestException("Index expression values may not be larger than 64K");
                    expressions.add(new IndexExpression(name.name.key, IndexOperator.EQ, value));
                }
            }
            else
            {
                for (Bound b : Bound.values())
                {
                    if (restriction.bound(b) != null)
                    {
                        ByteBuffer value = restriction.bound(b).bindAndGet(variables);
                        if (value == null)
                            throw new InvalidRequestException(String.format("Unsupported null value for indexed column %s", name));
                        if (value.remaining() > 0xFFFF)
                            throw new InvalidRequestException("Index expression values may not be larger than 64K");
                        expressions.add(new IndexExpression(name.name.key, restriction.getIndexOperator(b), value));
                    }
                }
            }
        }
        return expressions;
    }


    private Iterable<Column> columnsInOrder(final ColumnFamily cf, final List<ByteBuffer> variables) throws InvalidRequestException
    {
        if (columnRestrictions.length == 0)
            return cf.getSortedColumns();

        // If the restriction for the last column alias is an IN, respect
        // requested order
        Restriction last = columnRestrictions[columnRestrictions.length - 1];
        if (last == null || !last.isEquality())
            return cf.getSortedColumns();

        ColumnNameBuilder builder = cfDef.getColumnNameBuilder();
        for (int i = 0; i < columnRestrictions.length - 1; i++)
            builder.add(columnRestrictions[i].eqValues.get(0).bindAndGet(variables));

        final List<ByteBuffer> requested = new ArrayList<ByteBuffer>(last.eqValues.size());
        Iterator<Term> iter = last.eqValues.iterator();
        while (iter.hasNext())
        {
            Term t = iter.next();
            ColumnNameBuilder b = iter.hasNext() ? builder.copy() : builder;
            requested.add(b.add(t.bindAndGet(variables)).build());
        }

        return new Iterable<Column>()
        {
            public Iterator<Column> iterator()
            {
                return new AbstractIterator<Column>()
                {
                    Iterator<ByteBuffer> iter = requested.iterator();
                    public Column computeNext()
                    {
                        if (!iter.hasNext())
                            return endOfData();
                        Column column = cf.getColumn(iter.next());
                        return column == null ? computeNext() : column;
                    }
                };
            }
        };
    }

    private ResultSet process(List<Row> rows, List<ByteBuffer> variables, int limit, long now) throws InvalidRequestException
    {
        Selection.ResultSetBuilder result = selection.resultSetBuilder(now);
        for (org.apache.cassandra.db.Row row : rows)
        {
            // Not columns match the query, skip
            if (row.cf == null)
                continue;

            processColumnFamily(row.key.key, row.cf, variables, limit, now, result);
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
    void processColumnFamily(ByteBuffer key, ColumnFamily cf, List<ByteBuffer> variables, int limit, long now, Selection.ResultSetBuilder result) throws InvalidRequestException
    {
        ByteBuffer[] keyComponents = cfDef.hasCompositeKey
                                   ? ((CompositeType)cfDef.cfm.getKeyValidator()).split(key)
                                   : new ByteBuffer[]{ key };

        if (cfDef.isCompact)
        {
            // One cqlRow per column
            for (Column c : columnsInOrder(cf, variables))
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
                    // For dynamic CF, the column could be out of the requested bounds, filter here
                    if (!sliceRestriction.isInclusive(Bound.START) && c.name().equals(sliceRestriction.bound(Bound.START).bindAndGet(variables)))
                        continue;
                    if (!sliceRestriction.isInclusive(Bound.END) && c.name().equals(sliceRestriction.bound(Bound.END).bindAndGet(variables)))
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

            for (ColumnGroupMap group : builder.groups())
                handleGroup(selection, result, keyComponents, group);
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

    /**
     * Orders results when multiple keys are selected (using IN)
     */
    private void orderResults(ResultSet cqlRows)
    {
        // There is nothing to do if
        //   a. there are no results,
        //   b. no ordering information where given,
        //   c. key restriction is a Range or not an IN expression
        if (cqlRows.size() == 0 || parameters.orderings.isEmpty() || isKeyRange || !keyIsInRelation)
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

    private void handleGroup(Selection selection, Selection.ResultSetBuilder result, ByteBuffer[] keyComponents, ColumnGroupMap columns) throws InvalidRequestException
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
                        List<Pair<ByteBuffer, Column>> collection = columns.getCollection(name.name.key);
                        ByteBuffer value = collection == null
                                         ? null
                                         : ((CollectionType)name.type).serialize(collection);
                        result.add(value);
                    }
                    else
                    {
                        result.add(columns.getSimple(name.name.key));
                    }
                    break;
            }
        }
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

            ColumnSpecification[] names = new ColumnSpecification[getBoundsTerms()];
            IPartitioner partitioner = StorageService.getPartitioner();

            // Select clause
            if (parameters.isCount && !selectClause.isEmpty())
                throw new InvalidRequestException("Only COUNT(*) and COUNT(1) operations are currently supported.");

            Selection selection = selectClause.isEmpty()
                                ? Selection.wildcard(cfDef)
                                : Selection.fromSelectors(cfDef, selectClause);

            Term prepLimit = null;
            if (limit != null)
            {
                prepLimit = limit.prepare(limitReceiver());
                prepLimit.collectMarkerSpecification(names);
            }

            SelectStatement stmt = new SelectStatement(cfDef, getBoundsTerms(), parameters, selection, prepLimit);

            /*
             * WHERE clause. For a given entity, rules are:
             *   - EQ relation conflicts with anything else (including a 2nd EQ)
             *   - Can't have more than one LT(E) relation (resp. GT(E) relation)
             *   - IN relation are restricted to row keys (for now) and conflics with anything else
             *     (we could allow two IN for the same entity but that doesn't seem very useful)
             *   - The value_alias cannot be restricted in any way (we don't support wide rows with indexed value in CQL so far)
             */
            boolean hasQueriableIndex = false;
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
                if (def.isIndexed())
                {
                    if (rel.operator() == Relation.Type.EQ)
                        hasQueriableIndex = true;
                }

                switch (name.kind)
                {
                    case KEY_ALIAS:
                        stmt.keyRestrictions[name.position] = updateRestriction(name, stmt.keyRestrictions[name.position], rel, names);
                        break;
                    case COLUMN_ALIAS:
                        stmt.columnRestrictions[name.position] = updateRestriction(name, stmt.columnRestrictions[name.position], rel, names);
                        break;
                    case VALUE_ALIAS:
                        throw new InvalidRequestException(String.format("Predicates on the non-primary-key column (%s) of a COMPACT table are not yet supported", name.name));
                    case COLUMN_METADATA:
                        stmt.metadataRestrictions.put(name, updateRestriction(name, stmt.metadataRestrictions.get(name), rel, names));
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

            // If a component of the partition key is restricted by a non-EQ relation, all preceding
            // components must have a EQ, and all following must have no restriction
            boolean shouldBeDone = false;
            CFDefinition.Name previous = null;
            stmt.keyIsInRelation = false;
            Iterator<CFDefinition.Name> iter = cfDef.keys.values().iterator();
            int lastRestrictedPartitionKey = stmt.keyRestrictions.length - 1;
            for (int i = 0; i < stmt.keyRestrictions.length; i++)
            {
                CFDefinition.Name cname = iter.next();
                Restriction restriction = stmt.keyRestrictions[i];

                if (restriction == null)
                {
                    if (!shouldBeDone)
                        lastRestrictedPartitionKey = i - 1;

                    if (stmt.onToken)
                        throw new InvalidRequestException("The token() function must be applied to all partition key components or none of them");

                    // Under a non order perserving partitioner, the only time not restricting a key part is allowed is if none are restricted
                    if (!partitioner.preservesOrder() && i > 0 && stmt.keyRestrictions[i-1] != null)
                    {
                        if (hasQueriableIndex)
                        {
                            stmt.usesSecondaryIndexing = true;
                            break;
                        }
                        throw new InvalidRequestException(String.format("Partition key part %s must be restricted since preceding part is", cname));
                    }

                    stmt.isKeyRange = true;
                    shouldBeDone = true;
                }
                else if (shouldBeDone)
                {
                    if (hasQueriableIndex)
                    {
                        stmt.usesSecondaryIndexing = true;
                        break;
                    }
                    throw new InvalidRequestException(String.format("partition key part %s cannot be restricted (preceding part %s is either not restricted or by a non-EQ relation)", cname, previous));
                }
                else if (restriction.onToken)
                {
                    // If this is a query on tokens, it's necessary a range query (there can be more than one key per token), so reject IN queries (as we don't know how to do them)
                    stmt.isKeyRange = true;
                    stmt.onToken = true;

                    if (restriction.isEquality() && restriction.eqValues.size() > 1)
                        throw new InvalidRequestException("Select using the token() function don't support IN clause");
                }
                else if (stmt.onToken)
                {
                    throw new InvalidRequestException(String.format("The token() function must be applied to all partition key components or none of them"));
                }
                else if (restriction.isEquality())
                {
                    if (restriction.eqValues.size() > 1)
                    {
                        // We only support IN for the last name so far
                        if (i != stmt.keyRestrictions.length - 1)
                            throw new InvalidRequestException(String.format("Partition KEY part %s cannot be restricted by IN relation (only the last part of the partition key can)", cname));
                        stmt.keyIsInRelation = true;
                    }
                }
                else
                {
                    if (hasQueriableIndex)
                    {
                        stmt.usesSecondaryIndexing = true;
                        break;
                    }
                    throw new InvalidRequestException("Only EQ and IN relation are supported on the partition key for random partitioners (unless you use the token() function)");
                }
                previous = cname;
            }

            // If a cluster key column is restricted by a non-EQ relation, all preceding
            // columns must have a EQ, and all following must have no restriction. Unless
            // the column is indexed that is.
            shouldBeDone = false;
            previous = null;
            iter = cfDef.columns.values().iterator();
            int lastRestrictedClusteringKey = stmt.columnRestrictions.length - 1;
            for (int i = 0; i < stmt.columnRestrictions.length; i++)
            {
                CFDefinition.Name cname = iter.next();
                Restriction restriction = stmt.columnRestrictions[i];

                if (restriction == null)
                {
                    if (!shouldBeDone)
                        lastRestrictedClusteringKey = i - 1;
                    shouldBeDone = true;
                }
                else
                {
                    if (shouldBeDone)
                    {
                        if (hasQueriableIndex)
                        {
                            stmt.usesSecondaryIndexing = true;
                            break;
                        }
                        throw new InvalidRequestException(String.format("PRIMARY KEY part %s cannot be restricted (preceding part %s is either not restricted or by a non-EQ relation)", cname, previous));
                    }
                    else if (!restriction.isEquality())
                    {
                        lastRestrictedClusteringKey = i;
                        shouldBeDone = true;
                        // For non-composite slices, we don't support internally the difference between exclusive and
                        // inclusive bounds, so we deal with it manually.
                        if (!cfDef.isComposite && (!restriction.isInclusive(Bound.START) || !restriction.isInclusive(Bound.END)))
                            stmt.sliceRestriction = restriction;
                    }
                    // We only support IN for the last name and for compact storage so far
                    // TODO: #3885 allows us to extend to non compact as well, but that remains to be done
                    else if (restriction.eqValues.size() > 1)
                    {
                        if (i != stmt.columnRestrictions.length - 1)
                            throw new InvalidRequestException(String.format("PRIMARY KEY part %s cannot be restricted by IN relation", cname));
                        else if (stmt.selectACollection())
                            throw new InvalidRequestException(String.format("Cannot restrict PRIMARY KEY part %s by IN relation as a collection is selected by the query", cname));
                    }
                }

                previous = cname;
            }

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
            }

            iter = cfDef.keys.values().iterator();
            for (int i = 0; i < lastRestrictedPartitionKey + 1; i++)
                stmt.restrictedNames.remove(iter.next());

            iter = cfDef.columns.values().iterator();
            for (int i = 0; i < lastRestrictedClusteringKey + 1; i++)
                stmt.restrictedNames.remove(iter.next());

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

                Boolean[] reversedMap = new Boolean[cfDef.columns.size()];
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

            return new ParsedStatement.Prepared(stmt, Arrays.<ColumnSpecification>asList(names));
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

        Restriction updateRestriction(CFDefinition.Name name, Restriction restriction, Relation newRel, ColumnSpecification[] boundNames) throws InvalidRequestException
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

            switch (newRel.operator())
            {
                case EQ:
                    {
                        if (restriction != null)
                            throw new InvalidRequestException(String.format("%s cannot be restricted by more than one relation if it includes an Equal", name));
                        Term t = newRel.getValue().prepare(receiver);
                        t.collectMarkerSpecification(boundNames);
                        restriction = new Restriction(t, newRel.onToken);
                    }
                    break;
                case IN:
                    if (restriction != null)
                        throw new InvalidRequestException(String.format("%s cannot be restricted by more than one relation if it includes a IN", name));
                    List<Term> inValues = new ArrayList<Term>(newRel.getInValues().size());
                    for (Term.Raw raw : newRel.getInValues())
                    {
                        Term t = raw.prepare(receiver);
                        t.collectMarkerSpecification(boundNames);
                        inValues.add(t);
                    }
                    restriction = new Restriction(inValues);

                    break;
                case GT:
                case GTE:
                case LT:
                case LTE:
                    {
                        if (restriction == null)
                            restriction = new Restriction(newRel.onToken);
                        Term t = newRel.getValue().prepare(receiver);
                        t.collectMarkerSpecification(boundNames);
                        restriction.setBound(name.name, newRel.operator(), t);
                    }
                    break;
            }
            return restriction;
        }

        @Override
        public String toString()
        {
            return String.format("SelectRawStatement[name=%s, selectClause=%s, whereClause=%s, isCount=%s]",
                    cfName,
                    selectClause,
                    whereClause,
                    parameters.isCount);
        }
    }

    // A rather raw class that simplify validation and query for select
    // Don't made public as this can be easily badly used
    private static class Restriction
    {
        // for equality
        List<Term> eqValues; // if null, it's a restriction by bounds

        // for bounds
        private final Term[] bounds;
        private final boolean[] boundInclusive;

        final boolean onToken;


        Restriction(List<Term> values, boolean onToken)
        {
            this.eqValues = values;
            this.bounds = null;
            this.boundInclusive = null;
            this.onToken = onToken;
        }

        Restriction(List<Term> values)
        {
            this(values, false);
        }

        Restriction(Term value, boolean onToken)
        {
            this(Collections.singletonList(value), onToken);
        }

        Restriction(boolean onToken)
        {
            this.eqValues = null;
            this.bounds = new Term[2];
            this.boundInclusive = new boolean[2];
            this.onToken = onToken;
        }

        boolean isEquality()
        {
            return eqValues != null;
        }

        public Term bound(Bound b)
        {
            return bounds[b.idx];
        }

        public boolean isInclusive(Bound b)
        {
            return bounds[b.idx] == null || boundInclusive[b.idx];
        }

        public Relation.Type getRelation(Bound eocBound, Bound inclusiveBound)
        {
            switch (eocBound)
            {
                case START:
                    return boundInclusive[inclusiveBound.idx] ? Relation.Type.GTE : Relation.Type.GT;
                case END:
                    return boundInclusive[inclusiveBound.idx] ? Relation.Type.LTE : Relation.Type.LT;
            }
            throw new AssertionError();
        }

        public IndexOperator getIndexOperator(Bound b)
        {
            switch (b)
            {
                case START:
                    return boundInclusive[b.idx] ? IndexOperator.GTE : IndexOperator.GT;
                case END:
                    return boundInclusive[b.idx] ? IndexOperator.LTE : IndexOperator.LT;
            }
            throw new AssertionError();
        }

        public void setBound(ColumnIdentifier name, Relation.Type type, Term t) throws InvalidRequestException
        {
            Bound b;
            boolean inclusive;
            switch (type)
            {
                case GT:
                    b = Bound.START;
                    inclusive = false;
                    break;
                case GTE:
                    b = Bound.START;
                    inclusive = true;
                    break;
                case LT:
                    b = Bound.END;
                    inclusive = false;
                    break;
                case LTE:
                    b = Bound.END;
                    inclusive = true;
                    break;
                default:
                    throw new AssertionError();
            }

            if (bounds == null)
                throw new InvalidRequestException(String.format("%s cannot be restricted by both an equal and an inequal relation", name));

            if (bounds[b.idx] != null)
                throw new InvalidRequestException(String.format("Invalid restrictions found on %s", name));
            bounds[b.idx] = t;
            boundInclusive[b.idx] = inclusive;
        }

        @Override
        public String toString()
        {
            String s;
            if (eqValues == null)
            {
                s = String.format("SLICE(%s %s, %s %s)", boundInclusive[0] ? ">=" : ">",
                                                            bounds[0],
                                                            boundInclusive[1] ? "<=" : "<",
                                                            bounds[1]);
            }
            else
            {
                s = String.format("EQ(%s)", eqValues);
            }
            return onToken ? s + "*" : s;
        }
    }

    public static class Parameters
    {
        private final Map<ColumnIdentifier, Boolean> orderings;
        private final boolean isCount;
        private final ColumnIdentifier countAlias;
        private final boolean allowFiltering;

        public Parameters(Map<ColumnIdentifier, Boolean> orderings, boolean isCount, ColumnIdentifier countAlias, boolean allowFiltering)
        {
            this.orderings = orderings;
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
