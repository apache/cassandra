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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.RequestType;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
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
    private static final Logger logger = LoggerFactory.getLogger(SelectStatement.class);

    private final int boundTerms;
    public final CFDefinition cfDef;
    public final Parameters parameters;
    private final List<Pair<CFDefinition.Name, Selector>> selectedNames = new ArrayList<Pair<CFDefinition.Name, Selector>>(); // empty => wildcard

    private Restriction keyRestriction;
    private final Restriction[] columnRestrictions;
    private final Map<CFDefinition.Name, Restriction> metadataRestrictions = new HashMap<CFDefinition.Name, Restriction>();
    private Restriction sliceRestriction;

    private boolean isReversed;

    private static enum Bound
    {
        START(0), END(1);

        public final int idx;
        Bound(int idx)
        {
            this.idx = idx;
        }
    };

    public SelectStatement(CFDefinition cfDef, int boundTerms, Parameters parameters)
    {
        this.cfDef = cfDef;
        this.boundTerms = boundTerms;
        this.columnRestrictions = new Restriction[cfDef.columns.size()];
        this.parameters = parameters;
    }

    public int getBoundsTerms()
    {
        return boundTerms;
    }

    public void checkAccess(ClientState state) throws InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.READ);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        // Nothing to do, all validation has been done by RawStatement.prepare()
    }

    public ResultMessage.Rows execute(ClientState state, List<ByteBuffer> variables) throws InvalidRequestException, UnavailableException, TimedOutException
    {
        return new ResultMessage.Rows(executeInternal(state, variables));
    }

    public ResultSet executeInternal(ClientState state, List<ByteBuffer> variables) throws InvalidRequestException, UnavailableException, TimedOutException
    {
        List<Row> rows;
        if (isKeyRange())
        {
            rows = multiRangeSlice(variables);
        }
        else
        {
            rows = getSlice(variables);
        }

        // Even for count, we need to process the result as it'll group some column together in sparse column families
        ResultSet rset = process(rows, variables);
        rset = parameters.isCount ? rset.makeCountResult() : rset;
        return rset;
    }

    public ResultSet process(List<Row> rows) throws InvalidRequestException
    {
        assert !parameters.isCount; // not yet needed
        return process(rows, Collections.<ByteBuffer>emptyList());
    }

    public String keyspace()
    {
        return cfDef.cfm.ksName;
    }

    public String columnFamily()
    {
        return cfDef.cfm.cfName;
    }

    private List<Row> getSlice(List<ByteBuffer> variables) throws InvalidRequestException, TimedOutException, UnavailableException
    {
        QueryPath queryPath = new QueryPath(columnFamily());
        Collection<ByteBuffer> keys = getKeys(variables);
        List<ReadCommand> commands = new ArrayList<ReadCommand>(keys.size());

        // ...a range (slice) of column names
        if (isColumnRange())
        {
            ByteBuffer start = getColumnStart(variables);
            ByteBuffer finish = getColumnEnd(variables);

            SliceQueryFilter filter = new SliceQueryFilter(start, finish, isReversed, getLimit());
            QueryProcessor.validateSliceFilter(cfDef.cfm, filter);

            // Note that we use the total limit for every key. This is
            // potentially inefficient, but then again, IN + LIMIT is not a
            // very sensible choice
            for (ByteBuffer key : keys)
            {
                QueryProcessor.validateKey(key);
                commands.add(new SliceFromReadCommand(keyspace(), key, queryPath, filter));
            }
        }
        // ...of a list of column names
        else
        {
            Collection<ByteBuffer> columnNames = getRequestedColumns(variables);
            QueryProcessor.validateColumnNames(columnNames);

            for (ByteBuffer key: keys)
            {
                QueryProcessor.validateKey(key);
                commands.add(new SliceByNamesReadCommand(keyspace(), key, queryPath, columnNames));
            }
        }

        try
        {
            return StorageProxy.read(commands, parameters.consistencyLevel);
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private List<Row> multiRangeSlice(List<ByteBuffer> variables) throws InvalidRequestException, TimedOutException, UnavailableException
    {
        List<Row> rows;

        IFilter filter =  makeFilter(variables);
        QueryProcessor.validateFilter(cfDef.cfm, filter);

        List<IndexExpression> expressions = getIndexExpressions(variables);

        try
        {
            rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace(),
                                                                    columnFamily(),
                                                                    null,
                                                                    filter,
                                                                    getKeyBounds(variables),
                                                                    expressions,
                                                                    getLimit(),
                                                                    true, // limit by columns, not keys
                                                                    false),
                                              parameters.consistencyLevel);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
        return rows;
    }

    private AbstractBounds<RowPosition> getKeyBounds(List<ByteBuffer> variables) throws InvalidRequestException
    {
        IPartitioner<?> p = StorageService.getPartitioner();
        AbstractBounds<RowPosition> bounds;

        if (keyRestriction != null && keyRestriction.onToken)
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
                if (p instanceof RandomPartitioner)
                    throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
                else
                    throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
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

    private IFilter makeFilter(List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        if (isColumnRange())
        {
            return new SliceQueryFilter(getRequestedBound(isReversed ? Bound.END : Bound.START, variables),
                                        getRequestedBound(isReversed ? Bound.START : Bound.END, variables),
                                        isReversed,
                                        -1); // We use this for range slices, where the count is ignored in favor of the global column count
        }
        else
        {
            return new NamesQueryFilter(getRequestedColumns(variables));
        }
    }

    private int getLimit()
    {
        // Internally, we don't support exclusive bounds for slices. Instead,
        // we query one more element if necessary and exclude
        int limit = sliceRestriction != null && !sliceRestriction.isInclusive(Bound.START) ? parameters.limit + 1 : parameters.limit;
        // For sparse, we'll end up merging all defined colums into the same CqlRow. Thus we should query up
        // to 'defined columns' * 'asked limit' to be sure to have enough columns. We'll trim after query if
        // this end being too much.
        return cfDef.isCompact ? limit : cfDef.metadata.size() * limit;
    }

    private boolean isKeyRange()
    {
        // If indexed columns or a token range, they always use getRangeSlices
        if (!metadataRestrictions.isEmpty())
            return true;

        return keyRestriction == null || !keyRestriction.isEquality() || keyRestriction.onToken;
    }

    private Collection<ByteBuffer> getKeys(final List<ByteBuffer> variables) throws InvalidRequestException
    {
        assert keyRestriction != null && keyRestriction.isEquality();

        List<ByteBuffer> keys = new ArrayList<ByteBuffer>(keyRestriction.eqValues.size());
        for (Term t : keyRestriction.eqValues)
            keys.add(t.getByteBuffer(cfDef.key.type, variables));
        return keys;
    }

    private ByteBuffer getKeyBound(Bound b, List<ByteBuffer> variables) throws InvalidRequestException
    {
        if (keyRestriction == null)
        {
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }
        else if (keyRestriction.isEquality())
        {
            assert keyRestriction.eqValues.size() == 1;
            return keyRestriction.eqValues.get(0).getByteBuffer(cfDef.key.type, variables);
        }
        else
        {
            Term bound = keyRestriction.bound(b);
            return bound == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : bound.getByteBuffer(cfDef.key.type, variables);
        }
    }

    private Token getTokenBound(Bound b, List<ByteBuffer> variables, IPartitioner<?> p) throws InvalidRequestException
    {
        assert keyRestriction != null;
        if (keyRestriction.isEquality())
        {
            assert keyRestriction.eqValues.size() == 1;
            return keyRestriction.eqValues.get(0).getAsToken(cfDef.key.type, variables, p);
        }
        else
        {
            Term bound = keyRestriction.bound(b);
            return bound == null ? p.getMinimumToken() : bound.getAsToken(cfDef.key.type, variables, p);
        }
    }

    private boolean includeKeyBound(Bound b)
    {
        if (keyRestriction == null || keyRestriction.isEquality())
            return true;
        else
            return keyRestriction.isInclusive(b);
    }

    private boolean isColumnRange()
    {
        // Static CF never entails a column slice
        if (!cfDef.isCompact && !cfDef.isComposite)
            return false;

        // Otherwise, it is a range query if it has at least one the column alias
        // for which no relation is defined or is not EQ.
        for (Restriction r : columnRestrictions)
        {
            if (r == null || !r.isEquality())
                return true;
        }
        return false;
    }

    private boolean isWildcard()
    {
        return selectedNames.isEmpty();
    }

    private SortedSet<ByteBuffer> getRequestedColumns(List<ByteBuffer> variables) throws InvalidRequestException
    {
        assert !isColumnRange();

        ColumnNameBuilder builder = cfDef.getColumnNameBuilder();
        for (Restriction r : columnRestrictions)
        {
            assert r != null && r.isEquality();
            if (r.eqValues.size() > 1)
            {
                // We have a IN. We only support this for the last column, so just create all columns and return.
                SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(cfDef.cfm.comparator);
                Iterator<Term> iter = r.eqValues.iterator();
                while (iter.hasNext())
                {
                    Term v = iter.next();
                    ColumnNameBuilder b = iter.hasNext() ? builder.copy() : builder;
                    ByteBuffer cname = b.add(v, Relation.Type.EQ, variables).build();
                    columns.add(cname);
                }
                return columns;
            }
            else
            {
                builder.add(r.eqValues.get(0), Relation.Type.EQ, variables);
            }
        }

        if (cfDef.isCompact)
        {
            return FBUtilities.singleton(builder.build());
        }
        else
        {
            // Adds all columns (even if the user selected a few columns, we
            // need to query all columns to know if the row exists or not).
            // Note that when we allow IS NOT NULL in queries and if all
            // selected name are request 'not null', we will allow to only
            // query those.
            SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(cfDef.cfm.comparator);
            Iterator<ColumnIdentifier> iter = cfDef.metadata.keySet().iterator();
            while (iter.hasNext())
            {
                ColumnIdentifier name = iter.next();
                ColumnNameBuilder b = iter.hasNext() ? builder.copy() : builder;
                ByteBuffer cname = b.add(name.key).build();
                columns.add(cname);
            }
            return columns;
        }
    }

    private ByteBuffer getRequestedBound(Bound b, List<ByteBuffer> variables) throws InvalidRequestException
    {
        assert isColumnRange();

        ColumnNameBuilder builder = cfDef.getColumnNameBuilder();
        for (Restriction r : columnRestrictions)
        {
            if (r == null || (!r.isEquality() && r.bound(b) == null))
            {
                // There wasn't any non EQ relation on that key, we select all records having the preceding component as prefix.
                // For composites, if there was preceding component and we're computing the end, we must change the last component
                // End-Of-Component, otherwise we would be selecting only one record.
                if (builder.componentCount() > 0 && b == Bound.END)
                    return builder.buildAsEndOfRange();
                else
                    return builder.build();
            }

            if (r.isEquality())
            {
                assert r.eqValues.size() == 1;
                builder.add(r.eqValues.get(0), Relation.Type.EQ, variables);
            }
            else
            {
                Term t = r.bound(b);
                assert t != null;
                return builder.add(t, r.getRelation(b), variables).build();
            }
        }
        // Means no relation at all or everything was an equal
        return builder.build();
    }

    public ByteBuffer getColumnStart(List<ByteBuffer> variables) throws InvalidRequestException
    {
        return getRequestedBound(isReversed ? Bound.END : Bound.START, variables);
    }

    public ByteBuffer getColumnEnd(List<ByteBuffer> variables) throws InvalidRequestException
    {
        return getRequestedBound(isReversed ? Bound.START : Bound.END, variables);
    }

    private List<IndexExpression> getIndexExpressions(List<ByteBuffer> variables) throws InvalidRequestException
    {
        if (metadataRestrictions.isEmpty())
            return Collections.<IndexExpression>emptyList();

        List<IndexExpression> expressions = new ArrayList<IndexExpression>();
        for (Map.Entry<CFDefinition.Name, Restriction> entry : metadataRestrictions.entrySet())
        {
            CFDefinition.Name name = entry.getKey();
            Restriction restriction = entry.getValue();
            if (restriction.isEquality())
            {
                for (Term t : restriction.eqValues)
                {
                    ByteBuffer value = t.getByteBuffer(name.type, variables);
                    expressions.add(new IndexExpression(name.name.key, IndexOperator.EQ, value));
                }
            }
            else
            {
                for (Bound b : Bound.values())
                {
                    if (restriction.bound(b) != null)
                    {
                        ByteBuffer value = restriction.bound(b).getByteBuffer(name.type, variables);
                        expressions.add(new IndexExpression(name.name.key, restriction.getIndexOperator(b), value));
                    }
                }
            }
        }
        return expressions;
    }

    private List<Pair<CFDefinition.Name, Selector>> getExpandedSelection()
    {
        if (selectedNames.isEmpty())
        {
            List<Pair<CFDefinition.Name, Selector>> selection = new ArrayList<Pair<CFDefinition.Name, Selector>>();
            for (CFDefinition.Name name : cfDef)
                selection.add(Pair.<CFDefinition.Name, Selector>create(name, name.name));
            return selection;
        }
        else
        {
            return selectedNames;
        }
    }

    private ByteBuffer value(IColumn c)
    {
        return (c instanceof CounterColumn)
             ? ByteBufferUtil.bytes(CounterContext.instance().total(c.value()))
             : c.value();
    }

    private void addReturnValue(ResultSet cqlRows, Selector s, IColumn c)
    {
        if (c == null || c.isMarkedForDelete())
        {
            cqlRows.addColumnValue(null);
            return;
        }

        if (s.hasFunction())
        {
            switch (s.function())
            {
                case WRITE_TIME:
                    cqlRows.addColumnValue(ByteBufferUtil.bytes(c.timestamp()));
                    break;
                case TTL:
                    if (c instanceof ExpiringColumn)
                    {
                        int ttl = ((ExpiringColumn)c).getLocalDeletionTime() - (int) (System.currentTimeMillis() / 1000);
                        cqlRows.addColumnValue(ByteBufferUtil.bytes(ttl));
                    }
                    else
                    {
                        cqlRows.addColumnValue(null);
                    }
                    break;
            }
        }
        else
        {
            cqlRows.addColumnValue(value(c));
        }
    }

    private ResultSet createResult(List<Pair<CFDefinition.Name, Selector>> selection)
    {
        List<ColumnSpecification> names = new ArrayList<ColumnSpecification>(selection.size());
        for (Pair<CFDefinition.Name, Selector> p : selection)
        {
            if (p.right.hasFunction())
            {
                switch (p.right.function())
                {
                    case WRITE_TIME:
                        names.add(new ColumnSpecification(p.left.ksName, p.left.cfName, new ColumnIdentifier(p.right.toString(), true), LongType.instance));
                        break;
                    case TTL:
                        names.add(new ColumnSpecification(p.left.ksName, p.left.cfName, new ColumnIdentifier(p.right.toString(), true), Int32Type.instance));
                        break;
                }
            }
            else
            {
                names.add(p.left);
            }
        }
        return new ResultSet(names);
    }

    private Iterable<IColumn> columnsInOrder(final ColumnFamily cf, final List<ByteBuffer> variables) throws InvalidRequestException
    {
        // If the restriction for the last column alias is an IN, respect
        // requested order
        Restriction last = columnRestrictions[columnRestrictions.length - 1];
        if (last == null || !last.isEquality())
            return cf.getSortedColumns();

        ColumnNameBuilder builder = cfDef.getColumnNameBuilder();
        for (int i = 0; i < columnRestrictions.length - 1; i++)
            builder.add(columnRestrictions[i].eqValues.get(0), Relation.Type.EQ, variables);

        final List<ByteBuffer> requested = new ArrayList<ByteBuffer>(last.eqValues.size());
        Iterator<Term> iter = last.eqValues.iterator();
        while (iter.hasNext())
        {
            Term t = iter.next();
            ColumnNameBuilder b = iter.hasNext() ? builder.copy() : builder;
            requested.add(b.add(t, Relation.Type.EQ, variables).build());
        }

        return new Iterable<IColumn>()
        {
            public Iterator<IColumn> iterator()
            {
                return new AbstractIterator<IColumn>()
                {
                    Iterator<ByteBuffer> iter = requested.iterator();
                    public IColumn computeNext()
                    {
                        if (!iter.hasNext())
                            return endOfData();
                        IColumn column = cf.getColumn(iter.next());
                        return column == null ? computeNext() : column;
                    }
                };
            }
        };
    }

    private ResultSet process(List<Row> rows, List<ByteBuffer> variables) throws InvalidRequestException
    {
        List<Pair<CFDefinition.Name, Selector>> selection = getExpandedSelection();
        ResultSet cqlRows = createResult(selection);

        for (org.apache.cassandra.db.Row row : rows)
        {
            // Not columns match the query, skip
            if (row.cf == null)
                continue;

            if (cfDef.isCompact)
            {
                // One cqlRow per column
                for (IColumn c : columnsInOrder(row.cf, variables))
                {
                    if (c.isMarkedForDelete())
                        continue;

                    ByteBuffer[] components = null;

                    if (cfDef.isComposite)
                    {
                        components = ((CompositeType)cfDef.cfm.comparator).split(c.name());
                    }
                    else if (sliceRestriction != null)
                    {
                        // For dynamic CF, the column could be out of the requested bounds, filter here
                        if (!sliceRestriction.isInclusive(Bound.START) && c.name().equals(sliceRestriction.bound(Bound.START).getByteBuffer(cfDef.cfm.comparator, variables)))
                            continue;
                        if (!sliceRestriction.isInclusive(Bound.END) && c.name().equals(sliceRestriction.bound(Bound.END).getByteBuffer(cfDef.cfm.comparator, variables)))
                            continue;
                    }

                    // Respect selection order
                    for (Pair<CFDefinition.Name, Selector> p : selection)
                    {
                        CFDefinition.Name name = p.left;
                        Selector selector = p.right;
                        switch (name.kind)
                        {
                            case KEY_ALIAS:
                                cqlRows.addColumnValue(row.key.key);
                                break;
                            case COLUMN_ALIAS:
                                if (cfDef.isComposite)
                                {
                                    if (name.position < components.length)
                                        cqlRows.addColumnValue(components[name.position]);
                                    else
                                        cqlRows.addColumnValue(null);
                                }
                                else
                                {
                                    cqlRows.addColumnValue(c.name());
                                }
                                break;
                            case VALUE_ALIAS:
                                addReturnValue(cqlRows, selector, c);
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
                int last = composite.types.size() - 1;

                ByteBuffer[] previous = null;
                Map<ByteBuffer, IColumn> group = new HashMap<ByteBuffer, IColumn>();
                for (IColumn c : row.cf)
                {
                    if (c.isMarkedForDelete())
                        continue;

                    ByteBuffer[] current = composite.split(c.name());
                    // If current differs from previous, we've just finished a group
                    if (previous != null && !isSameRow(previous, current))
                    {
                        handleGroup(selection, row.key.key, previous, group, cqlRows);
                        group = new HashMap<ByteBuffer, IColumn>();
                    }

                    // Accumulate the current column
                    group.put(current[last], c);
                    previous = current;
                }
                // Handle the last group
                if (previous != null)
                    handleGroup(selection, row.key.key, previous, group, cqlRows);
            }
            else
            {
                if (row.cf.getLiveColumnCount() == 0)
                    continue;

                // Static case: One cqlRow for all columns
                // Respect selection order
                for (Pair<CFDefinition.Name, Selector> p : selection)
                {
                    CFDefinition.Name name = p.left;
                    Selector selector = p.right;
                    if (name.kind == CFDefinition.Name.Kind.KEY_ALIAS)
                    {
                        cqlRows.addColumnValue(row.key.key);
                        continue;
                    }

                    IColumn c = row.cf.getColumn(name.name.key);
                    addReturnValue(cqlRows, selector, c);
                }
            }
        }

        // Internal calls always return columns in the comparator order, even when reverse was set
        if (isReversed)
            cqlRows.reverse();

        // Trim result if needed to respect the limit
        cqlRows.trim(parameters.limit);
        return cqlRows;
    }

    /**
     * For sparse composite, returns wheter two columns belong to the same
     * cqlRow base on the full list of component in the name.
     * Two columns do belong together if they differ only by the last
     * component.
     */
    private static boolean isSameRow(ByteBuffer[] c1, ByteBuffer[] c2)
    {
        // Cql don't allow to insert columns who doesn't have all component of
        // the composite set for sparse composite. Someone coming from thrift
        // could hit that though. But since we have no way to handle this
        // correctly, better fail here and tell whomever may hit that (if
        // someone ever do) to change the definition to a dense composite
        assert c1.length == c2.length : "Sparse composite should not have partial column names";
        for (int i = 0; i < c1.length - 1; i++)
        {
            if (!c1[i].equals(c2[i]))
                return false;
        }
        return true;
    }

    private void handleGroup(List<Pair<CFDefinition.Name, Selector>> selection, ByteBuffer key, ByteBuffer[] components, Map<ByteBuffer, IColumn> columns, ResultSet cqlRows)
    {
        // Respect requested order
        for (Pair<CFDefinition.Name, Selector> p : selection)
        {
            CFDefinition.Name name = p.left;
            Selector selector = p.right;
            switch (name.kind)
            {
                case KEY_ALIAS:
                    cqlRows.addColumnValue(key);
                    break;
                case COLUMN_ALIAS:
                    cqlRows.addColumnValue(components[name.position]);
                    break;
                case VALUE_ALIAS:
                    // This should not happen for SPARSE
                    throw new AssertionError();
                case COLUMN_METADATA:
                    IColumn c = columns.get(name.name.key);
                    addReturnValue(cqlRows, selector, c);
                    break;
                default:
                    throw new AssertionError();
            }
        }
    }

    public static class RawStatement extends CFStatement
    {
        private final Parameters parameters;
        private final List<Selector> selectClause;
        private final List<Relation> whereClause;

        public RawStatement(CFName cfName, Parameters parameters, List<Selector> selectClause, List<Relation> whereClause)
        {
            super(cfName);
            this.parameters = parameters;
            this.selectClause = selectClause;
            this.whereClause = whereClause == null ? Collections.<Relation>emptyList() : whereClause;
        }

        public ParsedStatement.Prepared prepare() throws InvalidRequestException
        {
            CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
            ThriftValidation.validateConsistencyLevel(keyspace(), parameters.consistencyLevel, RequestType.READ);

            if (parameters.limit <= 0)
                throw new InvalidRequestException("LIMIT must be strictly positive");

            CFDefinition cfDef = cfm.getCfDef();
            SelectStatement stmt = new SelectStatement(cfDef, getBoundsTerms(), parameters);
            CFDefinition.Name[] names = new CFDefinition.Name[getBoundsTerms()];

            // Select clause
            if (parameters.isCount)
            {
                if (!selectClause.isEmpty())
                    throw new InvalidRequestException("Only COUNT(*) and COUNT(1) operations are currently supported.");
            }
            else
            {
                for (Selector t : selectClause)
                {
                    CFDefinition.Name name = cfDef.get(t.id());
                    if (name == null)
                        throw new InvalidRequestException(String.format("Undefined name %s in selection clause", t.id()));
                    if (t.hasFunction() && name.kind != CFDefinition.Name.Kind.COLUMN_METADATA && name.kind != CFDefinition.Name.Kind.VALUE_ALIAS)
                        throw new InvalidRequestException(String.format("Cannot use function %s on PRIMARY KEY part %s", t.function(), name));

                    stmt.selectedNames.add(Pair.create(name, t));
                }
            }

            /*
             * WHERE clause. For a given entity, rules are:
             *   - EQ relation conflicts with anything else (including a 2nd EQ)
             *   - Can't have more than one LT(E) relation (resp. GT(E) relation)
             *   - IN relation are restricted to row keys (for now) and conflics with anything else
             *     (we could allow two IN for the same entity but that doesn't seem very useful)
             *   - The value_alias cannot be restricted in any way (we don't support wide rows with indexed value in CQL so far)
             */
            for (Relation rel : whereClause)
            {
                CFDefinition.Name name = cfDef.get(rel.getEntity());
                if (name == null)
                    throw new InvalidRequestException(String.format("Undefined name %s in where clause ('%s')", rel.getEntity(), rel));

                if (rel.operator() == Relation.Type.IN)
                {
                    for (Term value : rel.getInValues())
                        if (value.isBindMarker())
                            names[value.bindIndex] = name;
                }
                else
                {
                    Term value = rel.getValue();
                    if (value.isBindMarker())
                        names[value.bindIndex] = name;
                }

                switch (name.kind)
                {
                    case KEY_ALIAS:
                        if (rel.operator() != Relation.Type.EQ && rel.operator() != Relation.Type.IN && !rel.onToken && !StorageService.getPartitioner().preservesOrder())
                            throw new InvalidRequestException("Only EQ and IN relation are supported on first component of the PRIMARY KEY for RandomPartitioner (unless you use the token() function)");
                        stmt.keyRestriction = updateRestriction(name, stmt.keyRestriction, rel);
                        break;
                    case COLUMN_ALIAS:
                        stmt.columnRestrictions[name.position] = updateRestriction(name, stmt.columnRestrictions[name.position], rel);
                        break;
                    case VALUE_ALIAS:
                        throw new InvalidRequestException(String.format("Restricting the value of a compact CF (%s) is not supported", name.name));
                    case COLUMN_METADATA:
                        stmt.metadataRestrictions.put(name, updateRestriction(name, stmt.metadataRestrictions.get(name), rel));
                        break;
                }
            }

            /*
             * At this point, the select statement if fully constructed, but we still have a few things to validate
             */

            // If a component of the PRIMARY KEY is restricted by a non-EQ relation, all preceding
            // components must have a EQ, and all following must have no restriction
            boolean shouldBeDone = false;
            CFDefinition.Name previous = null;
            Iterator<CFDefinition.Name> iter = cfDef.columns.values().iterator();
            for (int i = 0; i < stmt.columnRestrictions.length; i++)
            {
                CFDefinition.Name cname = iter.next();
                Restriction restriction = stmt.columnRestrictions[i];
                if (restriction == null)
                {
                    shouldBeDone = true;
                }
                else if (shouldBeDone)
                {
                    throw new InvalidRequestException(String.format("PRIMARY KEY part %s cannot be restricted (preceding part %s is either not restricted or by a non-EQ relation)", cname, previous));
                }
                else if (!restriction.isEquality())
                {
                    shouldBeDone = true;
                    // For non-composite slices, we don't support internally the difference between exclusive and
                    // inclusive bounds, so we deal with it manually.
                    if (!cfDef.isComposite && (!restriction.isInclusive(Bound.START) || !restriction.isInclusive(Bound.END)))
                        stmt.sliceRestriction = restriction;
                }
                // We only support IN for the last name so far
                else if (restriction.eqValues.size() > 1 && i != stmt.columnRestrictions.length - 1)
                {
                    throw new InvalidRequestException(String.format("PRIMARY KEY part %s cannot be restricted by IN relation (only the first and last parts can)", cname));
                }

                previous = cname;
            }

            // Deal with indexed columns
            if (!stmt.metadataRestrictions.isEmpty())
            {
                boolean hasEq = false;
                Set<ByteBuffer> indexed = Table.open(keyspace()).getColumnFamilyStore(columnFamily()).indexManager.getIndexedColumns();

                for (Map.Entry<CFDefinition.Name, Restriction> entry : stmt.metadataRestrictions.entrySet())
                {
                    if (entry.getValue().isEquality() && indexed.contains(entry.getKey().name.key))
                    {
                        hasEq = true;
                        break;
                    }
                }
                if (!hasEq)
                    throw new InvalidRequestException("No indexed columns present in by-columns clause with Equal operator");

                // If we have indexed columns and the key = X clause, we will do a range query, but if it's a IN relation, we don't know how to handle it.
                if (stmt.keyRestriction != null && stmt.keyRestriction.isEquality() && stmt.keyRestriction.eqValues.size() > 1)
                    throw new InvalidRequestException("Select on indexed columns and with IN clause for the PRIMARY KEY are not supported");
            }

            if (!stmt.parameters.orderings.isEmpty())
            {
                Boolean[] reversedMap = new Boolean[cfDef.columns.size()];
                int i = 0;
                for (Map.Entry<ColumnIdentifier, Boolean> entry : stmt.parameters.orderings.entrySet())
                {
                    ColumnIdentifier column = entry.getKey();
                    boolean reversed = entry.getValue();

                    CFDefinition.Name name = cfDef.get(column);
                    if (name == null)
                        throw new InvalidRequestException(String.format("Order by on unknown column %s", column));

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

                // Only allow ordering if the row key restriction is an equality,
                // since otherwise the order will be primarily on the row key.
                // TODO: we could allow ordering for IN queries, as we can do the
                // sorting post-query easily, but we will have to add it
                if (stmt.keyRestriction == null || !stmt.keyRestriction.isEquality() || stmt.keyRestriction.eqValues.size() != 1)
                    throw new InvalidRequestException("Ordering is only supported if the first part of the PRIMARY KEY is restricted by an Equal");
            }

            // If this is a query on tokens, it's necessary a range query (there can be more than one key per token), so reject IN queries (as we don't know how to do them)
            if (stmt.keyRestriction != null && stmt.keyRestriction.onToken && stmt.keyRestriction.isEquality() && stmt.keyRestriction.eqValues.size() > 1)
                throw new InvalidRequestException("Select using the token() function don't support IN clause");

            return new ParsedStatement.Prepared(stmt, Arrays.<ColumnSpecification>asList(names));
        }

        private static boolean isReversedType(CFDefinition.Name name)
        {
            return name.type instanceof ReversedType;
        }

        Restriction updateRestriction(CFDefinition.Name name, Restriction restriction, Relation newRel) throws InvalidRequestException
        {
            if (newRel.onToken && name.kind != CFDefinition.Name.Kind.KEY_ALIAS)
                throw new InvalidRequestException(String.format("The token() function is only supported on the partition key, found on %s", name));

            switch (newRel.operator())
            {
                case EQ:
                    if (restriction != null)
                        throw new InvalidRequestException(String.format("%s cannot be restricted by more than one relation if it includes an Equal", name));
                    restriction = new Restriction(newRel.getValue(), newRel.onToken);
                    break;
                case IN:
                    if (restriction != null)
                        throw new InvalidRequestException(String.format("%s cannot be restricted by more than one reation if it includes a IN", name));
                    restriction = new Restriction(newRel.getInValues());
                    break;
                case GT:
                case GTE:
                case LT:
                case LTE:
                    if (restriction == null)
                        restriction = new Restriction(newRel.onToken);
                    restriction.setBound(name.name, newRel.operator(), newRel.getValue());
                    break;
            }
            return restriction;
        }

        @Override
        public String toString()
        {
            return String.format("SelectRawStatement[name=%s, selectClause=%s, whereClause=%s, isCount=%s, cLevel=%s, limit=%s]",
                    cfName,
                    selectClause,
                    whereClause,
                    parameters.isCount,
                    parameters.consistencyLevel,
                    parameters.limit);
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

        Restriction(List<Term> values)
        {
            this.eqValues = values;
            this.bounds = null;
            this.boundInclusive = null;
            this.onToken = false;
        }

        Restriction(Term value, boolean onToken)
        {
            this(Collections.singletonList(value));
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

        public void setBound(Bound b, Term t)
        {
            bounds[b.idx] = t;
        }

        public void setInclusive(Bound b)
        {
            boundInclusive[b.idx] = true;
        }

        public Term bound(Bound b)
        {
            return bounds[b.idx];
        }

        public boolean isInclusive(Bound b)
        {
            return bounds[b.idx] == null || boundInclusive[b.idx];
        }

        public Relation.Type getRelation(Bound b)
        {
            switch (b)
            {
                case START:
                    return boundInclusive[b.idx] ? Relation.Type.GTE : Relation.Type.GT;
                case END:
                    return boundInclusive[b.idx] ? Relation.Type.LTE : Relation.Type.LT;
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
            Bound b = null;
            boolean inclusive = false;
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
            }

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
        private final int limit;
        private final ConsistencyLevel consistencyLevel;
        private final Map<ColumnIdentifier, Boolean> orderings;
        private final boolean isCount;

        public Parameters(ConsistencyLevel consistency, int limit, Map<ColumnIdentifier, Boolean> orderings, boolean isCount)
        {
            this.consistencyLevel = consistency;
            this.limit = limit;
            this.orderings = orderings;
            this.isCount = isCount;
        }
    }
}
