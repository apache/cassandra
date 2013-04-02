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

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.Functions;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Selection
{
    private static final Logger logger = LoggerFactory.getLogger(Selection.class);

    private final List<CFDefinition.Name> columnsList;
    private final List<ColumnSpecification> metadata;
    private final boolean collectTimestamps;
    private final boolean collectTTLs;

    protected Selection(List<CFDefinition.Name> columnsList, List<ColumnSpecification> metadata, boolean collectTimestamps, boolean collectTTLs)
    {
        this.columnsList = columnsList;
        this.metadata = metadata;
        this.collectTimestamps = collectTimestamps;
        this.collectTTLs = collectTTLs;
    }

    public static Selection wildcard(CFDefinition cfDef)
    {
        List<CFDefinition.Name> all = new ArrayList<CFDefinition.Name>();
        for (CFDefinition.Name name : cfDef)
            all.add(name);
        return new SimpleSelection(all);
    }

    private static boolean isUsingFunction(List<RawSelector> rawSelectors)
    {
        for (RawSelector rawSelector : rawSelectors)
        {
            if (!(rawSelector instanceof ColumnIdentifier))
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

    private static Selector makeSelector(CFDefinition cfDef, RawSelector raw, List<CFDefinition.Name> names, List<ColumnSpecification> metadata) throws InvalidRequestException
    {
        if (raw instanceof ColumnIdentifier)
        {
            CFDefinition.Name name = cfDef.get((ColumnIdentifier)raw);
            if (name == null)
                throw new InvalidRequestException(String.format("Undefined name %s in selection clause", raw));
            if (metadata != null)
                metadata.add(name);
            return new SimpleSelector(addAndGetIndex(name, names), name.type);
        }
        else if (raw instanceof RawSelector.WritetimeOrTTL)
        {
            RawSelector.WritetimeOrTTL tot = (RawSelector.WritetimeOrTTL)raw;
            CFDefinition.Name name = cfDef.get(tot.id);
            if (name == null)
                throw new InvalidRequestException(String.format("Undefined name %s in selection clause", tot.id));
            if (name.kind != CFDefinition.Name.Kind.COLUMN_METADATA && name.kind != CFDefinition.Name.Kind.VALUE_ALIAS)
                throw new InvalidRequestException(String.format("Cannot use selection function %s on PRIMARY KEY part %s", tot.isWritetime ? "writeTime" : "ttl", name));
            if (name.type.isCollection())
                throw new InvalidRequestException(String.format("Cannot use selection function %s on collections", tot.isWritetime ? "writeTime" : "ttl"));

            if (metadata != null)
                metadata.add(makeWritetimeOrTTLSpec(cfDef, tot));
            return new WritetimeOrTTLSelector(addAndGetIndex(name, names), tot.isWritetime);
        }
        else
        {
            RawSelector.WithFunction withFun = (RawSelector.WithFunction)raw;
            List<Selector> args = new ArrayList<Selector>(withFun.args.size());
            for (RawSelector rawArg : withFun.args)
                args.add(makeSelector(cfDef, rawArg, names, null));

            AbstractType<?> returnType = Functions.getReturnType(withFun.functionName, cfDef.cfm.ksName, cfDef.cfm.cfName);
            if (returnType == null)
                throw new InvalidRequestException(String.format("Unknown function '%s'", withFun.functionName));
            ColumnSpecification spec = makeFunctionSpec(cfDef, withFun, returnType);
            Function fun = Functions.get(withFun.functionName, args, spec);
            if (metadata != null)
                metadata.add(spec);
            return new FunctionSelector(fun, args);
        }
    }

    private static ColumnSpecification makeWritetimeOrTTLSpec(CFDefinition cfDef, RawSelector.WritetimeOrTTL tot)
    {
        return new ColumnSpecification(cfDef.cfm.ksName,
                                       cfDef.cfm.cfName,
                                       new ColumnIdentifier(tot.toString(), true),
                                       tot.isWritetime ? LongType.instance : Int32Type.instance);
    }

    private static ColumnSpecification makeFunctionSpec(CFDefinition cfDef, RawSelector.WithFunction fun, AbstractType<?> returnType) throws InvalidRequestException
    {
        if (returnType == null)
            throw new InvalidRequestException(String.format("Unknown function %s called in selection clause", fun.functionName));

        return new ColumnSpecification(cfDef.cfm.ksName,
                                       cfDef.cfm.cfName,
                                       new ColumnIdentifier(fun.toString(), true),
                                       returnType);
    }

    public static Selection fromSelectors(CFDefinition cfDef, List<RawSelector> rawSelectors) throws InvalidRequestException
    {
        boolean usesFunction = isUsingFunction(rawSelectors);

        if (usesFunction)
        {
            List<CFDefinition.Name> names = new ArrayList<CFDefinition.Name>();
            List<ColumnSpecification> metadata = new ArrayList<ColumnSpecification>(rawSelectors.size());
            List<Selector> selectors = new ArrayList<Selector>(rawSelectors.size());
            boolean collectTimestamps = false;
            boolean collectTTLs = false;
            for (RawSelector rawSelector : rawSelectors)
            {
                Selector selector = makeSelector(cfDef, rawSelector, names, metadata);
                selectors.add(selector);
                if (selector instanceof WritetimeOrTTLSelector)
                {
                    collectTimestamps |= ((WritetimeOrTTLSelector)selector).isWritetime;
                    collectTTLs |= !((WritetimeOrTTLSelector)selector).isWritetime;
                }
            }
            return new SelectionWithFunctions(names, metadata, selectors, collectTimestamps, collectTTLs);
        }
        else
        {
            List<CFDefinition.Name> names = new ArrayList<CFDefinition.Name>(rawSelectors.size());
            for (RawSelector rawSelector : rawSelectors)
            {
                assert rawSelector instanceof ColumnIdentifier;
                CFDefinition.Name name = cfDef.get((ColumnIdentifier)rawSelector);
                if (name == null)
                    throw new InvalidRequestException(String.format("Undefined name %s in selection clause", rawSelector));
                names.add(name);
            }
            return new SimpleSelection(names);
        }
    }

    protected abstract List<ByteBuffer> handleRow(ResultSetBuilder rs) throws InvalidRequestException;

    /**
     * @return the list of CQL3 "regular" (the "COLUMN_METADATA" ones) column names to fetch.
     */
    public List<ColumnIdentifier> regularColumnsToFetch()
    {
        List<ColumnIdentifier> toFetch = new ArrayList<ColumnIdentifier>();
        for (CFDefinition.Name name : columnsList)
        {
            if (name.kind == CFDefinition.Name.Kind.COLUMN_METADATA)
                toFetch.add(name.name);
        }
        return toFetch;
    }

    /**
     * @return the list of CQL3 columns value this SelectionClause needs.
     */
    public List<CFDefinition.Name> getColumnsList()
    {
        return columnsList;
    }

    public ResultSetBuilder resultSetBuilder()
    {
        return new ResultSetBuilder();
    }

    private static ByteBuffer value(IColumn c)
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

        private ResultSetBuilder()
        {
            this.resultSet = new ResultSet(metadata);
            this.timestamps = collectTimestamps ? new long[columnsList.size()] : null;
            this.ttls = collectTTLs ? new int[columnsList.size()] : null;
        }

        public void add(ByteBuffer v)
        {
            current.add(v);
        }

        public void add(IColumn c)
        {
            current.add(isDead(c) ? null : value(c));
            if (timestamps != null)
            {
                timestamps[current.size() - 1] = isDead(c) ? -1 : c.timestamp();
            }
            if (ttls != null)
            {
                int ttl = -1;
                if (!isDead(c) && c instanceof ExpiringColumn)
                    ttl = ((ExpiringColumn)c).getLocalDeletionTime() - (int) (System.currentTimeMillis() / 1000);
                ttls[current.size() - 1] = ttl;
            }
        }

        private boolean isDead(IColumn c)
        {
            return c == null || c.isMarkedForDelete();
        }

        public void newRow() throws InvalidRequestException
        {
            if (current != null)
                resultSet.addRow(handleRow(this));
            current = new ArrayList<ByteBuffer>(columnsList.size());
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
        public SimpleSelection(List<CFDefinition.Name> columnsList)
        {
            /*
             * In theory, even a simple selection could have multiple time the same column, so we
             * could filter those duplicate out of columnsList. But since we're very unlikely to
             * get much duplicate in practice, it's more efficient not to bother.
             */
            super(columnsList, new ArrayList<ColumnSpecification>(columnsList), false, false);
        }

        protected List<ByteBuffer> handleRow(ResultSetBuilder rs)
        {
            return rs.current;
        }
    }

    private interface Selector extends AssignementTestable
    {
        public ByteBuffer compute(ResultSetBuilder rs) throws InvalidRequestException;
    }

    private static class SimpleSelector implements Selector
    {
        private final int idx;
        private final AbstractType<?> type;

        public SimpleSelector(int idx, AbstractType<?> type)
        {
            this.idx = idx;
            this.type = type;
        }

        public ByteBuffer compute(ResultSetBuilder rs)
        {
            return rs.current.get(idx);
        }

        public boolean isAssignableTo(ColumnSpecification receiver)
        {
            return type.equals(receiver.type);
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
            return fun.returnType().equals(receiver.type);
        }
    }

    private static class WritetimeOrTTLSelector implements Selector
    {
        private final int idx;
        private final boolean isWritetime;

        public WritetimeOrTTLSelector(int idx, boolean isWritetime)
        {
            this.idx = idx;
            this.isWritetime = isWritetime;
        }

        public ByteBuffer compute(ResultSetBuilder rs)
        {
            if (isWritetime)
            {
                long ts = rs.timestamps[idx];
                return ts >= 0 ? ByteBufferUtil.bytes(ts) : null;
            }

            int ttl = rs.ttls[idx];
            return ttl > 0 ? ByteBufferUtil.bytes(ttl) : null;
        }

        public boolean isAssignableTo(ColumnSpecification receiver)
        {
            return receiver.type.asCQL3Type().equals(isWritetime ? CQL3Type.Native.BIGINT : CQL3Type.Native.INT);
        }
    }

    private static class SelectionWithFunctions extends Selection
    {
        private final List<Selector> selectors;

        public SelectionWithFunctions(List<CFDefinition.Name> columnsList, List<ColumnSpecification> metadata, List<Selector> selectors, boolean collectTimestamps, boolean collectTTLs)
        {
            super(columnsList, metadata, collectTimestamps, collectTTLs);
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
}
