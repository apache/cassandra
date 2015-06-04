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

import com.google.common.collect.*;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class Selection
{
    private final List<CFDefinition.Name> columns;
    private final SelectionColumns columnMapping;
    private final boolean collectTimestamps;
    private final boolean collectTTLs;

    protected Selection(List<CFDefinition.Name> columns, SelectionColumns columnMapping, boolean collectTimestamps, boolean collectTTLs)
    {
        this.columns = columns;
        this.columnMapping = columnMapping;
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
        return new ResultSet.Metadata(columnMapping.getColumnSpecifications());
    }

    public static Selection wildcard(CFDefinition cfDef)
    {
        List<CFDefinition.Name> all = new ArrayList<CFDefinition.Name>();
        for (CFDefinition.Name name : cfDef)
            all.add(name);
        return new SimpleSelection(all, true);
    }

    public static Selection forColumns(List<CFDefinition.Name> columns)
    {
        return new SimpleSelection(columns, false);
    }

    private static boolean selectionsNeedProcessing(List<RawSelector> rawSelectors)
    {
        for (RawSelector rawSelector : rawSelectors)
        {
            if (rawSelector.processesSelection())
                return true;
        }
        return false;
    }

    private static int addAndGetIndex(CFDefinition.Name name, List<CFDefinition.Name> l)
    {
        int idx = l.indexOf(name);
        if (idx < 0)
        {
            idx = l.size();
            l.add(name);
        }
        return idx;
    }

    private static Selector makeSelector(CFDefinition cfDef,
                                         RawSelector raw,
                                         List<CFDefinition.Name> names,
                                         SelectionColumnMapping columnMapping) throws InvalidRequestException
    {
        Selectable selectable = raw.selectable.prepare(cfDef.cfm);
        return makeSelector(cfDef, selectable, raw.alias, names, columnMapping);
    }

    private static Selector makeSelector(CFDefinition cfDef,
                                         Selectable selectable,
                                         ColumnIdentifier alias,
                                         List<CFDefinition.Name> names,
                                         SelectionColumnMapping columnMapping) throws InvalidRequestException
    {
        if (selectable instanceof ColumnIdentifier)
        {
            CFDefinition.Name name = cfDef.get((ColumnIdentifier) selectable);
            if (name == null)
                throw new InvalidRequestException(String.format("Undefined name %s in selection clause", selectable));
            if (columnMapping != null)
                columnMapping.addMapping(alias == null ? name : makeAliasSpec(cfDef, name.type, alias), name);
            return new SimpleSelector(name.toString(), addAndGetIndex(name, names), name.type);
        }
        else if (selectable instanceof Selectable.WritetimeOrTTL)
        {
            Selectable.WritetimeOrTTL tot = (Selectable.WritetimeOrTTL)selectable;
            CFDefinition.Name name = cfDef.get(tot.id);
            if (name == null)
                throw new InvalidRequestException(String.format("Undefined name %s in selection clause", tot.id));
            if (name.isPrimaryKeyColumn())
                throw new InvalidRequestException(String.format("Cannot use selection function %s on PRIMARY KEY part %s", tot.isWritetime ? "writeTime" : "ttl", name));
            if (name.type.isCollection())
                throw new InvalidRequestException(String.format("Cannot use selection function %s on collections", tot.isWritetime ? "writeTime" : "ttl"));
            if (columnMapping != null)
                columnMapping.addMapping(makeWritetimeOrTTLSpec(cfDef, tot, alias), name);
            return new WritetimeOrTTLSelector(name.toString(), addAndGetIndex(name, names), tot.isWritetime);
        }
        else
        {
            Selectable.WithFunction withFun = (Selectable.WithFunction)selectable;
            List<Selector> args = new ArrayList<Selector>(withFun.args.size());
            // use a temporary column mapping to collate the columns used by all the function args
            SelectionColumnMapping tmpMapping = SelectionColumnMapping.newMapping();
            for (Selectable rawArg : withFun.args)
                args.add(makeSelector(cfDef, rawArg, null, names, tmpMapping));

            AbstractType<?> returnType = Functions.getReturnType(withFun.functionName, cfDef.cfm.ksName, cfDef.cfm.cfName);
            if (returnType == null)
                throw new InvalidRequestException(String.format("Unknown function '%s'", withFun.functionName));
            ColumnSpecification spec = makeFunctionSpec(cfDef, withFun, returnType, alias);
            Function fun = Functions.get(withFun.functionName, args, spec);
            if (columnMapping != null)
                columnMapping.addMapping(spec, tmpMapping.getMappings().values());
            return new FunctionSelector(fun, args);
        }
    }

    private static ColumnSpecification makeWritetimeOrTTLSpec(CFDefinition cfDef, Selectable.WritetimeOrTTL tot, ColumnIdentifier alias)
    {
        return new ColumnSpecification(cfDef.cfm.ksName,
                                       cfDef.cfm.cfName,
                                       alias == null ? new ColumnIdentifier(tot.toString(), true) : alias,
                                       tot.isWritetime ? LongType.instance : Int32Type.instance);
    }

    private static ColumnSpecification makeFunctionSpec(CFDefinition cfDef,
                                                        Selectable.WithFunction fun,
                                                        AbstractType<?> returnType,
                                                        ColumnIdentifier alias) throws InvalidRequestException
    {
        if (returnType == null)
            throw new InvalidRequestException(String.format("Unknown function %s called in selection clause", fun.functionName));

        return new ColumnSpecification(cfDef.cfm.ksName,
                                       cfDef.cfm.cfName,
                                       alias == null ? new ColumnIdentifier(fun.toString(), true) : alias,
                                       returnType);
    }

    private static ColumnSpecification makeAliasSpec(CFDefinition cfDef, AbstractType<?> type, ColumnIdentifier alias)
    {
        return new ColumnSpecification(cfDef.cfm.ksName, cfDef.cfm.cfName, alias, type);
    }

    public static Selection fromSelectors(CFDefinition cfDef, List<RawSelector> rawSelectors) throws InvalidRequestException
    {
        boolean needsProcessing = selectionsNeedProcessing(rawSelectors);

        if (needsProcessing)
        {
            List<CFDefinition.Name> names = new ArrayList<CFDefinition.Name>();
            SelectionColumnMapping columnMapping = SelectionColumnMapping.newMapping();
            List<Selector> selectors = new ArrayList<Selector>(rawSelectors.size());
            boolean collectTimestamps = false;
            boolean collectTTLs = false;
            for (RawSelector rawSelector : rawSelectors)
            {
                Selector selector = makeSelector(cfDef, rawSelector, names, columnMapping);
                selectors.add(selector);
                collectTimestamps |= selector.usesTimestamps();
                collectTTLs |= selector.usesTTLs();
            }
            return new SelectionWithProcessing(names, columnMapping, selectors, collectTimestamps, collectTTLs);
        }
        else
        {
            List<CFDefinition.Name> names = new ArrayList<CFDefinition.Name>(rawSelectors.size());
            SelectionColumnMapping columnMapping = SelectionColumnMapping.newMapping();
            for (RawSelector rawSelector : rawSelectors)
            {
                assert rawSelector.selectable instanceof ColumnIdentifier.Raw;
                ColumnIdentifier id = ((ColumnIdentifier.Raw)rawSelector.selectable).prepare(cfDef.cfm);
                CFDefinition.Name name = cfDef.get(id);
                if (name == null)
                    throw new InvalidRequestException(String.format("Undefined name %s in selection clause", id));
                names.add(name);
                columnMapping.addMapping(rawSelector.alias == null ? name : makeAliasSpec(cfDef,
                                                                                          name.type,
                                                                                          rawSelector.alias),
                                         name);
            }
            return new SimpleSelection(names, columnMapping, false);
        }
    }

    protected abstract List<ByteBuffer> handleRow(ResultSetBuilder rs) throws InvalidRequestException;

    /**
     * @return the list of CQL3 "regular" (the "COLUMN_METADATA" ones) column names to fetch.
     */
    public List<ColumnIdentifier> regularAndStaticColumnsToFetch()
    {
        List<ColumnIdentifier> toFetch = new ArrayList<ColumnIdentifier>();
        for (CFDefinition.Name name : columns)
        {
            if (name.kind == CFDefinition.Name.Kind.COLUMN_METADATA || name.kind == CFDefinition.Name.Kind.STATIC)
                toFetch.add(name.name);
        }
        return toFetch;
    }

    /**
     * @return the list of CQL3 columns value this SelectionClause needs.
     */
    public List<CFDefinition.Name> getColumns()
    {
        return columns;
    }

    /**
     * @return the mappings between resultset columns and the underlying columns
     */
    public SelectionColumns getColumnMapping()
    {
        return columnMapping;
    }

    public ResultSetBuilder resultSetBuilder(long now)
    {
        return new ResultSetBuilder(now);
    }

    private static ByteBuffer value(Column c)
    {
        return (c instanceof CounterColumn)
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
            this.resultSet = new ResultSet(columnMapping.getColumnSpecifications());
            this.timestamps = collectTimestamps ? new long[columns.size()] : null;
            this.ttls = collectTTLs ? new int[columns.size()] : null;
            this.now = now;
        }

        public void add(ByteBuffer v)
        {
            current.add(v);
        }

        public void add(Column c)
        {
            current.add(isDead(c) ? null : value(c));
            if (timestamps != null)
            {
                timestamps[current.size() - 1] = isDead(c) ? Long.MIN_VALUE : c.timestamp();
            }
            if (ttls != null)
            {
                int ttl = -1;
                if (!isDead(c) && c instanceof ExpiringColumn)
                    ttl = c.getLocalDeletionTime() - (int) (now / 1000);
                ttls[current.size() - 1] = ttl;
            }
        }

        private boolean isDead(Column c)
        {
            return c == null || c.isMarkedForDelete(now);
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

        public SimpleSelection(List<CFDefinition.Name> columns, boolean isWildcard)
        {
            this(columns, SelectionColumnMapping.simpleMapping(columns), isWildcard);
        }

        public SimpleSelection(List<CFDefinition.Name> columns, SelectionColumnMapping columnMapping, boolean isWildcard)
        {
            /*
             * In theory, even a simple selection could have multiple time the same column, so we
             * could filter those duplicate out of columns. But since we're very unlikely to
             * get much duplicate in practice, it's more efficient not to bother.
             */
            super(columns, columnMapping, false, false);
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

    private static class SelectionWithProcessing extends Selection
    {
        private final List<Selector> selectors;

        public SelectionWithProcessing(List<CFDefinition.Name> columns, SelectionColumns columnMapping, List<Selector> selectors, boolean collectTimestamps, boolean collectTTLs)
        {
            super(columns, columnMapping, collectTimestamps, collectTTLs);
            this.selectors = selectors;
        }

        protected List<ByteBuffer> handleRow(ResultSetBuilder rs) throws InvalidRequestException
        {
            List<ByteBuffer> result = new ArrayList<ByteBuffer>();
            for (Selector selector : selectors)
            {
                result.add(selector.compute(rs));
            }
            return result;
        }
    }

    private interface Selector extends AssignementTestable
    {
        public ByteBuffer compute(ResultSetBuilder rs) throws InvalidRequestException;

        /** Returns true if the selector acts on a column's timestamp, false otherwise. */
        public boolean usesTimestamps();

        /** Returns true if the selector acts on a column's TTL, false otherwise. */
        public boolean usesTTLs();
    }

    private static class SimpleSelector implements Selector
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

        public boolean isAssignableTo(ColumnSpecification receiver)
        {
            return receiver.type.isValueCompatibleWith(type);
        }

        public boolean usesTimestamps()
        {
            return false;
        }

        public boolean usesTTLs()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return columnName;
        }
    }

    private static class FunctionSelector implements Selector
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

        public boolean isAssignableTo(ColumnSpecification receiver)
        {
            return receiver.type.isValueCompatibleWith(fun.returnType());
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

    private static class WritetimeOrTTLSelector implements Selector
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

        public boolean isAssignableTo(ColumnSpecification receiver)
        {
            return receiver.type.isValueCompatibleWith(isWritetime ? LongType.instance : Int32Type.instance);
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
