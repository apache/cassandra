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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.functions.AggregateFunction;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.CounterCell;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.collect.Iterators;

public abstract class Selection
{
    private final Collection<ColumnDefinition> columns;
    private final ResultSet.Metadata metadata;
    private final boolean collectTimestamps;
    private final boolean collectTTLs;

    protected Selection(Collection<ColumnDefinition> columns, List<ColumnSpecification> metadata, boolean collectTimestamps, boolean collectTTLs)
    {
        this.columns = columns;
        this.metadata = new ResultSet.Metadata(metadata);
        this.collectTimestamps = collectTimestamps;
        this.collectTTLs = collectTTLs;
    }

    // Overriden by SimpleSelection when appropriate.
    public boolean isWildcard()
    {
        return false;
    }

    public ResultSet.Metadata getResultMetadata()
    {
        return metadata;
    }

    public static Selection wildcard(CFMetaData cfm)
    {
        List<ColumnDefinition> all = new ArrayList<ColumnDefinition>(cfm.allColumns().size());
        Iterators.addAll(all, cfm.allColumnsInSelectOrder());
        return new SimpleSelection(all, true);
    }

    public static Selection forColumns(Collection<ColumnDefinition> columns)
    {
        return new SimpleSelection(columns, false);
    }

    public int addColumnForOrdering(ColumnDefinition c)
    {
        columns.add(c);
        metadata.addNonSerializedColumn(c);
        return columns.size() - 1;
    }

    private static boolean isUsingFunction(List<RawSelector> rawSelectors)
    {
        for (RawSelector rawSelector : rawSelectors)
        {
            if (!(rawSelector.selectable instanceof ColumnIdentifier))
                return true;
        }
        return false;
    }

    private static int addAndGetIndex(ColumnDefinition def, List<ColumnDefinition> l)
    {
        int idx = l.indexOf(def);
        if (idx < 0)
        {
            idx = l.size();
            l.add(def);
        }
        return idx;
    }

    private static Selector makeSelector(CFMetaData cfm, RawSelector raw, List<ColumnDefinition> defs, List<ColumnSpecification> metadata) throws InvalidRequestException
    {
        if (raw.selectable instanceof ColumnIdentifier)
        {
            ColumnDefinition def = cfm.getColumnDefinition((ColumnIdentifier)raw.selectable);
            if (def == null)
                throw new InvalidRequestException(String.format("Undefined name %s in selection clause", raw.selectable));
            if (metadata != null)
                metadata.add(raw.alias == null ? def : makeAliasSpec(cfm, def.type, raw.alias));
            return new SimpleSelector(def.name.toString(), addAndGetIndex(def, defs), def.type);
        }
        else if (raw.selectable instanceof Selectable.WritetimeOrTTL)
        {
            Selectable.WritetimeOrTTL tot = (Selectable.WritetimeOrTTL)raw.selectable;
            ColumnDefinition def = cfm.getColumnDefinition(tot.id);
            if (def == null)
                throw new InvalidRequestException(String.format("Undefined name %s in selection clause", tot.id));
            if (def.isPrimaryKeyColumn())
                throw new InvalidRequestException(String.format("Cannot use selection function %s on PRIMARY KEY part %s", tot.isWritetime ? "writeTime" : "ttl", def.name));
            if (def.type.isCollection())
                throw new InvalidRequestException(String.format("Cannot use selection function %s on collections", tot.isWritetime ? "writeTime" : "ttl"));

            if (metadata != null)
                metadata.add(makeWritetimeOrTTLSpec(cfm, tot, raw.alias));
            return new WritetimeOrTTLSelector(def.name.toString(), addAndGetIndex(def, defs), tot.isWritetime);
        }
        else if (raw.selectable instanceof Selectable.WithFieldSelection)
        {
            Selectable.WithFieldSelection withField = (Selectable.WithFieldSelection)raw.selectable;
            Selector selected = makeSelector(cfm, new RawSelector(withField.selected, null), defs, null);
            AbstractType<?> type = selected.getType();
            if (!(type instanceof UserType))
                throw new InvalidRequestException(String.format("Invalid field selection: %s of type %s is not a user type", withField.selected, type.asCQL3Type()));

            UserType ut = (UserType)type;
            for (int i = 0; i < ut.size(); i++)
            {
                if (!ut.fieldName(i).equals(withField.field.bytes))
                    continue;

                if (metadata != null)
                    metadata.add(makeFieldSelectSpec(cfm, withField, ut.fieldType(i), raw.alias));
                return new FieldSelector(ut, i, selected);
            }
            throw new InvalidRequestException(String.format("%s of type %s has no field %s", withField.selected, type.asCQL3Type(), withField.field));
        }
        else
        {
            Selectable.WithFunction withFun = (Selectable.WithFunction)raw.selectable;
            List<Selector> args = new ArrayList<>(withFun.args.size());
            for (Selectable rawArg : withFun.args)
                args.add(makeSelector(cfm, new RawSelector(rawArg, null), defs, null));

            // resolve built-in functions before user defined functions
            Function fun = Functions.get(cfm.ksName, withFun.functionName, args, cfm.ksName, cfm.cfName);
            if (fun == null)
                throw new InvalidRequestException(String.format("Unknown function '%s'", withFun.functionName));
            if (metadata != null)
                metadata.add(makeFunctionSpec(cfm, withFun, fun.returnType(), raw.alias));
            return fun.isAggregate() ? new AggregateFunctionSelector(fun, args)
                                     : new ScalarFunctionSelector(fun, args);
        }
    }

    private static ColumnSpecification makeWritetimeOrTTLSpec(CFMetaData cfm, Selectable.WritetimeOrTTL tot, ColumnIdentifier alias)
    {
        return new ColumnSpecification(cfm.ksName,
                                       cfm.cfName,
                                       alias == null ? new ColumnIdentifier(tot.toString(), true) : alias,
                                       tot.isWritetime ? LongType.instance : Int32Type.instance);
    }

    private static ColumnSpecification makeFieldSelectSpec(CFMetaData cfm, Selectable.WithFieldSelection s, AbstractType<?> type, ColumnIdentifier alias)
    {
        return new ColumnSpecification(cfm.ksName,
                                       cfm.cfName,
                                       alias == null ? new ColumnIdentifier(s.toString(), true) : alias,
                                       type);
    }

    private static ColumnSpecification makeFunctionSpec(CFMetaData cfm,
                                                        Selectable.WithFunction fun,
                                                        AbstractType<?> returnType,
                                                        ColumnIdentifier alias) throws InvalidRequestException
    {
        if (returnType == null)
            throw new InvalidRequestException(String.format("Unknown function %s called in selection clause", fun.functionName));

        return new ColumnSpecification(cfm.ksName,
                                       cfm.cfName,
                                       alias == null ? new ColumnIdentifier(fun.toString(), true) : alias,
                                       returnType);
    }

    private static ColumnSpecification makeAliasSpec(CFMetaData cfm, AbstractType<?> type, ColumnIdentifier alias)
    {
        return new ColumnSpecification(cfm.ksName, cfm.cfName, alias, type);
    }

    public static Selection fromSelectors(CFMetaData cfm, List<RawSelector> rawSelectors) throws InvalidRequestException
    {
        boolean usesFunction = isUsingFunction(rawSelectors);

        if (usesFunction)
        {
            List<ColumnDefinition> defs = new ArrayList<ColumnDefinition>();
            List<ColumnSpecification> metadata = new ArrayList<ColumnSpecification>(rawSelectors.size());
            List<Selector> selectors = new ArrayList<Selector>(rawSelectors.size());
            boolean collectTimestamps = false;
            boolean collectTTLs = false;
            for (RawSelector rawSelector : rawSelectors)
            {
                Selector selector = makeSelector(cfm, rawSelector, defs, metadata);
                selectors.add(selector);
                if (selector instanceof WritetimeOrTTLSelector)
                {
                    collectTimestamps |= ((WritetimeOrTTLSelector)selector).isWritetime;
                    collectTTLs |= !((WritetimeOrTTLSelector)selector).isWritetime;
                }
            }
            return new SelectionWithFunctions(defs, metadata, selectors, collectTimestamps, collectTTLs);
        }
        else
        {
            List<ColumnDefinition> defs = new ArrayList<ColumnDefinition>(rawSelectors.size());
            List<ColumnSpecification> metadata = new ArrayList<ColumnSpecification>(rawSelectors.size());
            for (RawSelector rawSelector : rawSelectors)
            {
                assert rawSelector.selectable instanceof ColumnIdentifier;
                ColumnDefinition def = cfm.getColumnDefinition((ColumnIdentifier)rawSelector.selectable);
                if (def == null)
                    throw new InvalidRequestException(String.format("Undefined name %s in selection clause", rawSelector.selectable));
                defs.add(def);
                metadata.add(rawSelector.alias == null ? def : makeAliasSpec(cfm, def.type, rawSelector.alias));
            }
            return new SimpleSelection(defs, metadata, false);
        }
    }

    protected abstract void addInputRow(ResultSetBuilder rs) throws InvalidRequestException;

    protected abstract boolean isAggregate();

    protected abstract List<ByteBuffer> getOutputRow() throws InvalidRequestException;

    protected abstract void reset();

    /**
     * @return the list of CQL3 columns value this SelectionClause needs.
     */
    public Collection<ColumnDefinition> getColumns()
    {
        return columns;
    }

    public ResultSetBuilder resultSetBuilder(long now)
    {
        return new ResultSetBuilder(now);
    }

    private static ByteBuffer value(Cell c)
    {
        return (c instanceof CounterCell)
            ? ByteBufferUtil.bytes(CounterContext.instance().total(c.value()))
            : c.value();
    }

    /**
     * Checks that selectors are either all aggregates or that none of them is.
     *
     * @param selectors the selectors to test.
     * @param msgTemplate the error message template
     * @param messageArgs the error message arguments
     * @throws InvalidRequestException if some of the selectors are aggregate but not all of them
     */
    private static void validateSelectors(List<Selector> selectors, String messageTemplate, Object... messageArgs)
            throws InvalidRequestException
    {
        int aggregates = 0;
        for (Selector s : selectors)
            if (s.isAggregate())
                ++aggregates;

        if (aggregates != 0 && aggregates != selectors.size())
            throw new InvalidRequestException(String.format(messageTemplate, messageArgs));
    }

    public class ResultSetBuilder
    {
        private final ResultSet resultSet;

        /*
         * We'll build CQL3 row one by one.
         * The currentRow is the values for the (CQL3) columns we've fetched.
         * We also collect timestamps and ttls for the case where the writetime and
         * ttl functions are used. Note that we might collect timestamp and/or ttls
         * we don't care about, but since the array below are allocated just once,
         * it doesn't matter performance wise.
         */
        List<ByteBuffer> current;
        final long[] timestamps;
        final int[] ttls;
        final long now;

        private ResultSetBuilder(long now)
        {
            this.resultSet = new ResultSet(getResultMetadata().copy(), new ArrayList<List<ByteBuffer>>());
            this.timestamps = collectTimestamps ? new long[columns.size()] : null;
            this.ttls = collectTTLs ? new int[columns.size()] : null;
            this.now = now;
        }

        public void add(ByteBuffer v)
        {
            current.add(v);
        }

        public void add(Cell c)
        {
            current.add(isDead(c) ? null : value(c));
            if (timestamps != null)
            {
                timestamps[current.size() - 1] = isDead(c) ? -1 : c.timestamp();
            }
            if (ttls != null)
            {
                int ttl = -1;
                if (!isDead(c) && c instanceof ExpiringCell)
                    ttl = c.getLocalDeletionTime() - (int) (now / 1000);
                ttls[current.size() - 1] = ttl;
            }
        }

        private boolean isDead(Cell c)
        {
            return c == null || !c.isLive(now);
        }

        public void newRow() throws InvalidRequestException
        {
            if (current != null)
            {
                addInputRow(this);
                if (!isAggregate())
                {
                    resultSet.addRow(getOutputRow());
                    reset();
                }
            }
            current = new ArrayList<ByteBuffer>(columns.size());
        }

        public ResultSet build() throws InvalidRequestException
        {
            if (current != null)
            {
                addInputRow(this);
                resultSet.addRow(getOutputRow());
                reset();
                current = null;
            }
            return resultSet;
        }
    }

    // Special cased selection for when no function is used (this save some allocations).
    private static class SimpleSelection extends Selection
    {
        private final boolean isWildcard;

        private List<ByteBuffer> current;

        public SimpleSelection(Collection<ColumnDefinition> columns, boolean isWildcard)
        {
            this(columns, new ArrayList<ColumnSpecification>(columns), isWildcard);
        }

        public SimpleSelection(Collection<ColumnDefinition> columns, List<ColumnSpecification> metadata, boolean isWildcard)
        {
            /*
             * In theory, even a simple selection could have multiple time the same column, so we
             * could filter those duplicate out of columns. But since we're very unlikely to
             * get much duplicate in practice, it's more efficient not to bother.
             */
            super(columns, metadata, false, false);
            this.isWildcard = isWildcard;
        }

        @Override
        public boolean isWildcard()
        {
            return isWildcard;
        }

        protected void addInputRow(ResultSetBuilder rs) throws InvalidRequestException
        {
            current = rs.current;
        }

        protected boolean isAggregate()
        {
            return false;
        }

        protected List<ByteBuffer> getOutputRow() throws InvalidRequestException
        {
            return current;
        }

        protected void reset()
        {
            current = null;
        }
    }

    private static abstract class Selector implements AssignmentTestable
    {
        public abstract void addInput(ResultSetBuilder rs) throws InvalidRequestException;

        public abstract ByteBuffer getOutput() throws InvalidRequestException;

        public abstract AbstractType<?> getType();

        public boolean isAggregate()
        {
            return false;
        }

        public abstract void reset();

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            if (receiver.type.equals(getType()))
                return AssignmentTestable.TestResult.EXACT_MATCH;
            else if (receiver.type.isValueCompatibleWith(getType()))
                return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
            else
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
        }
    }

    private static class SimpleSelector extends Selector
    {
        private final String columnName;
        private final int idx;
        private final AbstractType<?> type;
        private ByteBuffer current;

        public SimpleSelector(String columnName, int idx, AbstractType<?> type)
        {
            this.columnName = columnName;
            this.idx = idx;
            this.type = type;
        }

        public void addInput(ResultSetBuilder rs) throws InvalidRequestException
        {
            current = rs.current.get(idx);
        }

        public ByteBuffer getOutput() throws InvalidRequestException
        {
            return current;
        }

        public void reset()
        {
            current = null;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return columnName;
        }
    }

    private static abstract class AbstractFunctionSelector<T extends Function> extends Selector
    {
        protected final T fun;
        protected final List<Selector> argSelectors;

        public AbstractFunctionSelector(T fun, List<Selector> argSelectors)
        {
            this.fun = fun;
            this.argSelectors = argSelectors;
        }

        public AbstractType<?> getType()
        {
            return fun.returnType();
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(fun.name()).append("(");
            for (int i = 0; i < argSelectors.size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(argSelectors.get(i));
            }
            return sb.append(")").toString();
        }
    }

    private static class ScalarFunctionSelector extends AbstractFunctionSelector<ScalarFunction>
    {
        public ScalarFunctionSelector(Function fun, List<Selector> argSelectors) throws InvalidRequestException
        {
            super((ScalarFunction) fun, argSelectors);
            validateSelectors(argSelectors,
                              "the %s function arguments must be either all aggregates or all none aggregates",
                              fun.name().name);
        }

        public boolean isAggregate()
        {
            // We cannot just return true as it is possible to have a scalar function wrapping an aggregation function
            if (argSelectors.isEmpty())
                return false;

            return argSelectors.get(0).isAggregate();
        }

        public void addInput(ResultSetBuilder rs) throws InvalidRequestException
        {
            for (Selector s : argSelectors)
                s.addInput(rs);
        }

        public void reset()
        {
        }

        public ByteBuffer getOutput() throws InvalidRequestException
        {
            List<ByteBuffer> args = new ArrayList<ByteBuffer>(argSelectors.size());
            for (Selector s : argSelectors)
            {
                args.add(s.getOutput());
                s.reset();
            }
            return fun.execute(args);
        }
    }

    private static class AggregateFunctionSelector extends AbstractFunctionSelector<AggregateFunction>
    {
        private final AggregateFunction.Aggregate aggregate;

        public AggregateFunctionSelector(Function fun, List<Selector> argSelectors) throws InvalidRequestException
        {
            super((AggregateFunction) fun, argSelectors);

            validateAgruments(argSelectors);
            this.aggregate = this.fun.newAggregate();
        }

        public boolean isAggregate()
        {
            return true;
        }

        public void addInput(ResultSetBuilder rs) throws InvalidRequestException
        {
            List<ByteBuffer> args = new ArrayList<ByteBuffer>(argSelectors.size());
            // Aggregation of aggregation is not supported
            for (Selector s : argSelectors)
            {
                s.addInput(rs);
                args.add(s.getOutput());
                s.reset();
            }
            this.aggregate.addInput(args);
        }

        public ByteBuffer getOutput() throws InvalidRequestException
        {
            return aggregate.compute();
        }

        public void reset()
        {
            aggregate.reset();
        }

        /**
         * Checks that the arguments are not themselves aggregation functions.
         *
         * @param argSelectors the selector to check
         * @throws InvalidRequestException if on of the arguments is an aggregation function
         */
        private static void validateAgruments(List<Selector> argSelectors) throws InvalidRequestException
        {
            for (Selector selector : argSelectors)
                if (selector.isAggregate())
                    throw new InvalidRequestException(
                            "aggregate functions cannot be used as arguments of aggregate functions");
        }
    }

    private static class FieldSelector extends Selector
    {
        private final UserType type;
        private final int field;
        private final Selector selected;

        public FieldSelector(UserType type, int field, Selector selected)
        {
            this.type = type;
            this.field = field;
            this.selected = selected;
        }

        public boolean isAggregate()
        {
            return selected.isAggregate();
        }

        public void addInput(ResultSetBuilder rs) throws InvalidRequestException
        {
            selected.addInput(rs);
        }

        public ByteBuffer getOutput() throws InvalidRequestException
        {
            ByteBuffer value = selected.getOutput();
            if (value == null)
                return null;
            ByteBuffer[] buffers = type.split(value);
            return field < buffers.length ? buffers[field] : null;
        }

        public AbstractType<?> getType()
        {
            return type.fieldType(field);
        }

        public void reset()
        {
            selected.reset();
        }

        @Override
        public String toString()
        {
            return String.format("%s.%s", selected, UTF8Type.instance.getString(type.fieldName(field)));
        }
    }

    private static class WritetimeOrTTLSelector extends Selector
    {
        private final String columnName;
        private final int idx;
        private final boolean isWritetime;
        private ByteBuffer current;

        public WritetimeOrTTLSelector(String columnName, int idx, boolean isWritetime)
        {
            this.columnName = columnName;
            this.idx = idx;
            this.isWritetime = isWritetime;
        }

        public void addInput(ResultSetBuilder rs)
        {
            if (isWritetime)
            {
                long ts = rs.timestamps[idx];
                current = ts >= 0 ? ByteBufferUtil.bytes(ts) : null;
            }
            else
            {
                int ttl = rs.ttls[idx];
                current = ttl > 0 ? ByteBufferUtil.bytes(ttl) : null;
            }
        }

        public ByteBuffer getOutput()
        {
            return current;
        }

        public void reset()
        {
            current = null;
        }

        public AbstractType<?> getType()
        {
            return isWritetime ? LongType.instance : Int32Type.instance;
        }

        @Override
        public String toString()
        {
            return columnName;
        }
    }

    private static class SelectionWithFunctions extends Selection
    {
        private final List<Selector> selectors;

        public SelectionWithFunctions(Collection<ColumnDefinition> columns,
                                      List<ColumnSpecification> metadata,
                                      List<Selector> selectors,
                                      boolean collectTimestamps,
                                      boolean collectTTLs) throws InvalidRequestException
        {
            super(columns, metadata, collectTimestamps, collectTTLs);
            this.selectors = selectors;

            validateSelectors(selectors, "the select clause must either contains only aggregates or none");
        }

        protected void addInputRow(ResultSetBuilder rs) throws InvalidRequestException
        {
            for (Selector selector : selectors)
            {
                selector.addInput(rs);
            }
        }

        protected List<ByteBuffer> getOutputRow() throws InvalidRequestException
        {
            List<ByteBuffer> result = new ArrayList<ByteBuffer>();
            for (Selector selector : selectors)
            {
                result.add(selector.getOutput());
            }
            return result;
        }

        protected void reset()
        {
            for (Selector selector : selectors)
            {
                selector.reset();
            }
        }

        public boolean isAggregate()
        {
            return selectors.get(0).isAggregate();
        }
    }
}
