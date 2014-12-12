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
import java.util.List;

import com.google.common.collect.Iterators;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.CounterCell;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class Selection
{
    private final List<ColumnDefinition> columns;
    private final ResultSet.Metadata metadata;
    private final boolean collectTimestamps;
    private final boolean collectTTLs;

    protected Selection(List<ColumnDefinition> columns, List<ColumnSpecification> metadata, boolean collectTimestamps, boolean collectTTLs)
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

    public static Selection forColumns(List<ColumnDefinition> columns)
    {
        return new SimpleSelection(columns, false);
    }

    public int addColumnForOrdering(ColumnDefinition c)
    {
        columns.add(c);
        metadata.addNonSerializedColumn(c);
        return columns.size() - 1;
    }

    private static boolean requiresProcessing(List<RawSelector> rawSelectors)
    {
        for (RawSelector rawSelector : rawSelectors)
        {
            if (rawSelector.processesSelection())
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
        Selectable selectable = raw.selectable.prepare(cfm);
        return makeSelector(cfm, selectable, raw.alias, defs, metadata);
    }

    private static Selector makeSelector(CFMetaData cfm, Selectable selectable, ColumnIdentifier alias, List<ColumnDefinition> defs, List<ColumnSpecification> metadata) throws InvalidRequestException
    {
        if (selectable instanceof ColumnIdentifier)
        {
            ColumnDefinition def = cfm.getColumnDefinition((ColumnIdentifier)selectable);
            if (def == null)
                throw new InvalidRequestException(String.format("Undefined name %s in selection clause", selectable));

            if (metadata != null)
                metadata.add(alias == null ? def : makeAliasSpec(cfm, def.type, alias));
            return new SimpleSelector(def.name.toString(), addAndGetIndex(def, defs), def.type);
        }
        else if (selectable instanceof Selectable.WritetimeOrTTL)
        {
            Selectable.WritetimeOrTTL tot = (Selectable.WritetimeOrTTL)selectable;
            ColumnDefinition def = cfm.getColumnDefinition(tot.id);
            if (def == null)
                throw new InvalidRequestException(String.format("Undefined name %s in selection clause", tot.id));
            if (def.isPrimaryKeyColumn())
                throw new InvalidRequestException(String.format("Cannot use selection function %s on PRIMARY KEY part %s", tot.isWritetime ? "writeTime" : "ttl", def.name));
            if (def.type.isCollection())
                throw new InvalidRequestException(String.format("Cannot use selection function %s on collections", tot.isWritetime ? "writeTime" : "ttl"));

            if (metadata != null)
                metadata.add(makeWritetimeOrTTLSpec(cfm, tot, alias));
            return new WritetimeOrTTLSelector(def.name.toString(), addAndGetIndex(def, defs), tot.isWritetime);
        }
        else if (selectable instanceof Selectable.WithFieldSelection)
        {
            Selectable.WithFieldSelection withField = (Selectable.WithFieldSelection)selectable;
            Selector selected = makeSelector(cfm, withField.selected, null, defs, null);
            AbstractType<?> type = selected.getType();
            if (!(type instanceof UserType))
                throw new InvalidRequestException(String.format("Invalid field selection: %s of type %s is not a user type", withField.selected, type.asCQL3Type()));

            UserType ut = (UserType)type;
            for (int i = 0; i < ut.size(); i++)
            {
                if (!ut.fieldName(i).equals(withField.field.bytes))
                    continue;

                if (metadata != null)
                    metadata.add(makeFieldSelectSpec(cfm, withField, ut.fieldType(i), alias));
                return new FieldSelector(ut, i, selected);
            }
            throw new InvalidRequestException(String.format("%s of type %s has no field %s", withField.selected, type.asCQL3Type(), withField.field));
        }
        else
        {
            Selectable.WithFunction withFun = (Selectable.WithFunction)selectable;
            List<Selector> args = new ArrayList<Selector>(withFun.args.size());
            for (Selectable arg : withFun.args)
                args.add(makeSelector(cfm, arg, null, defs, null));

            AbstractType<?> returnType = Functions.getReturnType(withFun.functionName, cfm.ksName, cfm.cfName);
            if (returnType == null)
                throw new InvalidRequestException(String.format("Unknown function '%s'", withFun.functionName));

            ColumnSpecification spec = makeFunctionSpec(cfm, withFun, returnType, alias);
            Function fun = Functions.get(cfm.ksName, withFun.functionName, args, spec);
            if (metadata != null)
                metadata.add(spec);
            return new FunctionSelector(fun, args);
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
        if (requiresProcessing(rawSelectors))
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
                collectTimestamps |= selector.usesTimestamps();
                collectTTLs |= selector.usesTTLs();
            }
            return new SelectionWithProcessing(defs, metadata, selectors, collectTimestamps, collectTTLs);
        }
        else
        {
            List<ColumnDefinition> defs = new ArrayList<ColumnDefinition>(rawSelectors.size());
            List<ColumnSpecification> metadata = new ArrayList<ColumnSpecification>(rawSelectors.size());
            for (RawSelector rawSelector : rawSelectors)
            {
                assert rawSelector.selectable instanceof ColumnIdentifier.Raw;
                ColumnIdentifier id = (ColumnIdentifier) rawSelector.selectable.prepare(cfm);
                ColumnDefinition def = cfm.getColumnDefinition(id);
                if (def == null)
                    throw new InvalidRequestException(String.format("Undefined name %s in selection clause", id));
                defs.add(def);
                metadata.add(rawSelector.alias == null ? def : makeAliasSpec(cfm, def.type, rawSelector.alias));
            }
            return new SimpleSelection(defs, metadata, false);
        }
    }

    protected abstract List<ByteBuffer> handleRow(ResultSetBuilder rs) throws InvalidRequestException;

    /**
     * @return the list of CQL3 columns value this SelectionClause needs.
     */
    public List<ColumnDefinition> getColumns()
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
                timestamps[current.size() - 1] = isDead(c) ? Long.MIN_VALUE : c.timestamp();
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
                resultSet.addRow(handleRow(this));
            current = new ArrayList<ByteBuffer>(columns.size());
        }

        public ResultSet build() throws InvalidRequestException
        {
            if (current != null)
            {
                resultSet.addRow(handleRow(this));
                current = null;
            }
            return resultSet;
        }
    }

    // Special cased selection for when no function is used (this save some allocations).
    private static class SimpleSelection extends Selection
    {
        private final boolean isWildcard;

        public SimpleSelection(List<ColumnDefinition> columns, boolean isWildcard)
        {
            this(columns, new ArrayList<ColumnSpecification>(columns), isWildcard);
        }

        public SimpleSelection(List<ColumnDefinition> columns, List<ColumnSpecification> metadata, boolean isWildcard)
        {
            /*
             * In theory, even a simple selection could have multiple time the same column, so we
             * could filter those duplicate out of columns. But since we're very unlikely to
             * get much duplicate in practice, it's more efficient not to bother.
             */
            super(columns, metadata, false, false);
            this.isWildcard = isWildcard;
        }

        protected List<ByteBuffer> handleRow(ResultSetBuilder rs)
        {
            return rs.current;
        }

        @Override
        public boolean isWildcard()
        {
            return isWildcard;
        }
    }

    private static abstract class Selector implements AssignementTestable
    {
        public abstract ByteBuffer compute(ResultSetBuilder rs) throws InvalidRequestException;
        public abstract AbstractType<?> getType();

        public boolean isAssignableTo(String keyspace, ColumnSpecification receiver)
        {
            return receiver.type.isValueCompatibleWith(getType());
        }

        /** Returns true if the selector acts on a column's timestamp, false otherwise. */
        public boolean usesTimestamps()
        {
            return false;
        }

        /** Returns true if the selector acts on a column's TTL, false otherwise. */
        public boolean usesTTLs()
        {
            return false;
        }
    }

    private static class SimpleSelector extends Selector
    {
        private final String columnName;
        private final int idx;
        private final AbstractType<?> type;

        public SimpleSelector(String columnName, int idx, AbstractType<?> type)
        {
            this.columnName = columnName;
            this.idx = idx;
            this.type = type;
        }

        public ByteBuffer compute(ResultSetBuilder rs)
        {
            return rs.current.get(idx);
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

    private static class SelectionWithProcessing extends Selection
    {
        private final List<Selector> selectors;

        public SelectionWithProcessing(List<ColumnDefinition> columns, List<ColumnSpecification> metadata, List<Selector> selectors, boolean collectTimestamps, boolean collectTTLs)
        {
            super(columns, metadata, collectTimestamps, collectTTLs);
            this.selectors = selectors;
        }

        protected List<ByteBuffer> handleRow(ResultSetBuilder rs) throws InvalidRequestException
        {
            List<ByteBuffer> result = new ArrayList<>();
            for (Selector selector : selectors)
                result.add(selector.compute(rs));
            return result;
        }

        @Override
        public int addColumnForOrdering(ColumnDefinition c)
        {
            int index = super.addColumnForOrdering(c);
            selectors.add(new SimpleSelector(c.name.toString(), index, c.type));
            return index;
        }
    }

    private static class FunctionSelector extends Selector
    {
        private final Function fun;
        private final List<Selector> argSelectors;

        public FunctionSelector(Function fun, List<Selector> argSelectors)
        {
            this.fun = fun;
            this.argSelectors = argSelectors;
        }

        public ByteBuffer compute(ResultSetBuilder rs) throws InvalidRequestException
        {
            List<ByteBuffer> args = new ArrayList<ByteBuffer>(argSelectors.size());
            for (Selector s : argSelectors)
                args.add(s.compute(rs));

            return fun.execute(args);
        }

        public AbstractType<?> getType()
        {
            return fun.returnType();
        }

        public boolean usesTimestamps()
        {
            for (Selector s : argSelectors)
                if (s.usesTimestamps())
                    return true;
            return false;
        }

        public boolean usesTTLs()
        {
            for (Selector s : argSelectors)
                if (s.usesTTLs())
                    return true;
            return false;
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

        public ByteBuffer compute(ResultSetBuilder rs) throws InvalidRequestException
        {
            ByteBuffer value = selected.compute(rs);
            if (value == null)
                return null;
            ByteBuffer[] buffers = type.split(value);
            return field < buffers.length ? buffers[field] : null;
        }

        public AbstractType<?> getType()
        {
            return type.fieldType(field);
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

        public WritetimeOrTTLSelector(String columnName, int idx, boolean isWritetime)
        {
            this.columnName = columnName;
            this.idx = idx;
            this.isWritetime = isWritetime;
        }

        public ByteBuffer compute(ResultSetBuilder rs)
        {
            if (isWritetime)
            {
                long ts = rs.timestamps[idx];
                return ts != Long.MIN_VALUE ? ByteBufferUtil.bytes(ts) : null;
            }

            int ttl = rs.ttls[idx];
            return ttl > 0 ? ByteBufferUtil.bytes(ttl) : null;
        }

        public AbstractType<?> getType()
        {
            return isWritetime ? LongType.instance : Int32Type.instance;
        }


        public boolean usesTimestamps()
        {
            return isWritetime;
        }

        public boolean usesTTLs()
        {
            return !isWritetime;
        }

        @Override
        public String toString()
        {
            return columnName;
        }
    }
}
