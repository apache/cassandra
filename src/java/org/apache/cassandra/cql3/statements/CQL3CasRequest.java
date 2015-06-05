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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

/**
 * Processed CAS conditions and update on potentially multiple rows of the same partition.
 */
public class CQL3CasRequest implements CASRequest
{
    private final CFMetaData cfm;
    private final ByteBuffer key;
    private final long now;
    private final boolean isBatch;
    private final BatchStatement.Attrs batchAttrs;

    // We index RowCondition by the prefix of the row they applied to for 2 reasons:
    //   1) this allows to keep things sorted to build the ColumnSlice array below
    //   2) this allows to detect when contradictory conditions are set (not exists with some other conditions on the same row)
    private final SortedMap<Composite, RowCondition> conditions;

    private final List<RowUpdate> updates = new ArrayList<>();

    public static final IVersionedSerializer<CASRequest> serializer = new Serializer();

    public CQL3CasRequest(CFMetaData cfm, ByteBuffer key)
    {
        this(cfm, key, null);
    }

    public CQL3CasRequest(CFMetaData cfm, ByteBuffer key, BatchStatement.Attrs batchAttrs)
    {
        this.cfm = cfm;
        // When checking if conditions apply, we want to use a fixed reference time for a whole request to check
        // for expired cells. Note that this is unrelated to the cell timestamp.
        this.now = batchAttrs != null ? batchAttrs.ts : System.currentTimeMillis();
        this.key = key;
        this.conditions = new TreeMap<>(cfm.comparator);
        this.isBatch = batchAttrs != null;
        this.batchAttrs = batchAttrs;
    }

    public ByteBuffer getKey()
    {
        return key;
    }

    @Override
    public Type getType()
    {
        return Type.CQL;
    }

    public void addRowUpdate(Composite prefix, ModificationStatement stmt, QueryOptions options, long timestamp)
    {
        updates.add(new RowUpdate(prefix, stmt, options, timestamp));
    }

    public void addNotExist(Composite prefix) throws InvalidRequestException
    {
        RowCondition previous = conditions.put(prefix, new NotExistCondition(prefix, now));
        if (previous != null && !(previous instanceof NotExistCondition))
        {
            // these should be prevented by the parser, but it doesn't hurt to check
            if (previous instanceof ExistCondition)
                throw new InvalidRequestException("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row");
            else
                throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
        }
    }

    public void addExist(Composite prefix) throws InvalidRequestException
    {
        RowCondition previous = conditions.put(prefix, new ExistCondition(prefix, now));
        // this should be prevented by the parser, but it doesn't hurt to check
        if (previous instanceof NotExistCondition)
            throw new InvalidRequestException("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row");
    }

    public void addConditions(Composite prefix, Collection<ColumnCondition> conds, QueryOptions options) throws InvalidRequestException
    {
        RowCondition condition = conditions.get(prefix);
        if (condition == null)
        {
            condition = new ColumnsConditions(prefix, now);
            conditions.put(prefix, condition);
        }
        else if (!(condition instanceof ColumnsConditions))
        {
            throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
        }
        ((ColumnsConditions)condition).addConditions(conds, options);
    }

    public IDiskAtomFilter readFilter()
    {
        assert !conditions.isEmpty();
        ColumnSlice[] slices = new ColumnSlice[conditions.size()];
        int i = 0;
        // We always read CQL rows entirely as on CAS failure we want to be able to distinguish between "row exists
        // but all values for which there were conditions are null" and "row doesn't exists", and we can't rely on the
        // row marker for that (see #6623)
        for (Composite prefix : conditions.keySet())
            slices[i++] = prefix.slice();

        int toGroup = cfm.comparator.isDense() ? -1 : cfm.clusteringColumns().size();
        slices = ColumnSlice.deoverlapSlices(slices, cfm.comparator);
        assert ColumnSlice.validateSlices(slices, cfm.comparator, false);
        return new SliceQueryFilter(slices, false, slices.length, toGroup);
    }

    public boolean appliesTo(ColumnFamily current) throws InvalidRequestException
    {
        for (RowCondition condition : conditions.values())
        {
            if (!condition.appliesTo(current))
                return false;
        }
        return true;
    }

    public ColumnFamily makeUpdates(ColumnFamily current) throws InvalidRequestException
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfm);
        for (RowUpdate upd : updates)
            upd.applyUpdates(current, cf);

        if (isBatch)
            BatchStatement.verifyBatchSize(Collections.singleton(cf));

        return cf;
    }

    public CFMetaData getCfm()
    {
        return cfm;
    }

    public static class BatchAttrs
    {
        public final String query;
        public final String keyspace;
        public final QueryOptions options;
        public final long ts;

        public BatchAttrs(String query, String keyspace, QueryOptions options, long ts)
        {
            this.query = query;
            this.keyspace = keyspace;
            this.options = options;
            this.ts = ts;
        }
    }

    /**
     * Due to some operation on lists, we can't generate the update that a given Modification statement does before
     * we get the values read by the initial read of Paxos. A RowUpdate thus just store the relevant information
     * (include the statement iself) to generate those updates. We'll have multiple RowUpdate for a Batch, otherwise
     * we'll have only one.
     */
    private class RowUpdate
    {
        private final Composite rowPrefix;
        private final ModificationStatement stmt;
        private final QueryOptions options;
        private final long timestamp;

        private RowUpdate(Composite rowPrefix, ModificationStatement stmt, QueryOptions options, long timestamp)
        {
            this.rowPrefix = rowPrefix;
            this.stmt = stmt;
            this.options = options;
            this.timestamp = timestamp;
        }

        public void applyUpdates(ColumnFamily current, ColumnFamily updates) throws InvalidRequestException
        {
            Map<ByteBuffer, CQL3Row> map = null;
            if (stmt.requiresRead())
            {
                // Uses the "current" values read by Paxos for lists operation that requires a read
                Iterator<CQL3Row> iter = cfm.comparator.CQL3RowBuilder(cfm, now).group(current.iterator(new ColumnSlice[]{ rowPrefix.slice() }));
                if (iter.hasNext())
                {
                    map = Collections.singletonMap(key, iter.next());
                    assert !iter.hasNext() : "We shoudn't be updating more than one CQL row per-ModificationStatement";
                }
            }

            UpdateParameters params = new UpdateParameters(cfm, options, timestamp, stmt.getTimeToLive(options), map);
            stmt.addUpdateForKey(updates, key, rowPrefix, params);
        }
    }

    private static abstract class RowCondition
    {
        public final Composite rowPrefix;
        protected final long now;

        protected RowCondition(Composite rowPrefix, long now)
        {
            this.rowPrefix = rowPrefix;
            this.now = now;
        }

        public abstract boolean appliesTo(ColumnFamily current) throws InvalidRequestException;
    }

    private static class NotExistCondition extends RowCondition
    {
        private NotExistCondition(Composite rowPrefix, long now)
        {
            super(rowPrefix, now);
        }

        public boolean appliesTo(ColumnFamily current)
        {
            if (current == null)
                return true;

            Iterator<Cell> iter = current.iterator(new ColumnSlice[]{ rowPrefix.slice() });
            while (iter.hasNext())
                if (iter.next().isLive(now))
                    return false;
            return true;
        }
    }

    private static class ExistCondition extends RowCondition
    {
        private ExistCondition(Composite rowPrefix, long now)
        {
            super (rowPrefix, now);
        }

        public boolean appliesTo(ColumnFamily current)
        {
            if (current == null)
                return false;

            Iterator<Cell> iter = current.iterator(new ColumnSlice[]{ rowPrefix.slice() });
            while (iter.hasNext())
                if (iter.next().isLive(now))
                    return true;
            return false;
        }
    }

    private static class ColumnsConditions extends RowCondition
    {
        private final Multimap<Pair<ColumnIdentifier, ByteBuffer>, ColumnCondition.Bound> conditions = HashMultimap.create();

        private ColumnsConditions(Composite rowPrefix, long now)
        {
            super(rowPrefix, now);
        }

        public void addConditions(Collection<ColumnCondition> conds, QueryOptions options) throws InvalidRequestException
        {
            for (ColumnCondition condition : conds)
            {
                ColumnCondition.Bound current = condition.bind(options);
                conditions.put(Pair.create(condition.column.name, current.getCollectionElementValue()), current);
            }
        }

        public boolean appliesTo(ColumnFamily current) throws InvalidRequestException
        {
            if (current == null)
                return conditions.isEmpty();

            for (ColumnCondition.Bound condition : conditions.values())
                if (!condition.appliesTo(rowPrefix, current, now))
                    return false;
            return true;
        }
    }

    /**
     * this is pretty ghetto
     */
    private static class Serializer implements IVersionedSerializer<CASRequest>
    {

        private final ConcurrentMap<Pair<String, String>, ParsedStatement.Prepared> modificationStmts = new ConcurrentHashMap<>();

        @Override
        public void serialize(CASRequest request, DataOutputPlus out, int version) throws IOException
        {
            assert request instanceof CQL3CasRequest;
            CQL3CasRequest req = (CQL3CasRequest) request;

            out.writeLong(req.now);
            out.writeBoolean(req.isBatch);
            if (req.isBatch)
            {
                out.writeBoolean(req.batchAttrs.query != null);
                if (req.batchAttrs.query != null)
                {
                    // query batch
                    out.writeUTF(req.batchAttrs.query);
                    QueryOptions options = req.batchAttrs.options.forStatement(0);
                    QueryOptions.serializer.serialize(options, out, version);
                }
                else
                {
                    // batch message batch
                    out.writeInt(req.batchAttrs.type.ordinal());
                    QueryOptions.serializer.serialize(req.batchAttrs.options.getWrapped(), out, version);
                    out.writeInt(req.updates.size());
                    for (int i=0; i<req.updates.size(); i++)
                    {
                        RowUpdate update = req.updates.get(i);
                        out.writeUTF(update.stmt.getQueryString());

                        List<ByteBuffer> values = update.options.getValues();
                        out.writeInt(values.size());
                        for (ByteBuffer value: values)
                        {
                            ByteBufferUtil.writeWithShortLength(value, out);
                        }
                    }
                }
            }
            else
            {
                assert req.updates.size() == 1;
                out.writeUTF(req.cfm.ksName);
                RowUpdate update = req.updates.get(0);
                out.writeUTF(update.stmt.getQueryString());
                QueryOptions.serializer.serialize(update.options, out, version);
            }
        }

        private ParsedStatement.Prepared getStatement(String ks, String query) throws IOException
        {
            Pair<String, String> cacheKey = Pair.create(ks, query);
            ParsedStatement.Prepared prepared = modificationStmts.get(cacheKey);
            if (prepared == null)
            {
                try
                {
                    ParsedStatement parsedStatement = QueryProcessor.parseStatement(query);
                    if (parsedStatement instanceof ModificationStatement.Parsed)
                    {
                        ModificationStatement.Parsed parsed = (ModificationStatement.Parsed) parsedStatement;
                        parsed.prepareKeyspace(ks);
                        parsed.setQueryString(query);
                        prepared = parsed.prepare();
                    }
                    else if (parsedStatement instanceof BatchStatement.Parsed)
                    {
                        BatchStatement.Parsed parsed = (BatchStatement.Parsed) parsedStatement;
                        parsed.setQueryString(query);
                        prepared = parsed.prepare();
                    }
                    else
                    {
                        throw new AssertionError("Unhandled parsed statement type: " + parsedStatement.getClass().getName());
                    }

                    ParsedStatement.Prepared previous = modificationStmts.putIfAbsent(cacheKey, prepared);
                    prepared = previous == null ? prepared : previous;
                }
                catch (InvalidRequestException | SyntaxException e)
                {
                    throw new IOException(e);
                }
            }
            return prepared;
        }

        @Override
        public CASRequest deserialize(DataInput in, int version) throws IOException
        {

            final long ts = in.readLong();
            try
            {
                if (in.readBoolean())
                {
                    if (in.readBoolean())
                    {
                        String query = in.readUTF();
                        QueryOptions options = QueryOptions.serializer.deserialize(in, version);
                        ParsedStatement.Prepared prepared = getStatement(null, query);
                        options.prepare(prepared.boundNames);
                        BatchStatement statement = (BatchStatement) prepared.statement;
                        BatchQueryOptions batchOptions = BatchQueryOptions.withoutPerStatementVariables(options);
                        return statement.getCasRequest(batchOptions, ts);
                    }
                    else
                    {
                        BatchStatement.Type type = BatchStatement.Type.values()[in.readInt()];
                        QueryOptions wrapped = QueryOptions.serializer.deserialize(in, version);
                        int numStmts = in.readInt();

                        List<ModificationStatement> statements = new ArrayList<>(numStmts);
                        List<Object> queryOrIdList = new ArrayList<>(numStmts);
                        List<List<ByteBuffer>> values = new ArrayList<>(numStmts);

                        for (int i=0; i<numStmts; i++)
                        {
                            String query = in.readUTF();
                            ParsedStatement.Prepared prepared = getStatement(null, query);
                            statements.add((ModificationStatement) prepared.statement);
                            queryOrIdList.add(query);

                            int numVals = in.readInt();
                            List<ByteBuffer> vals = new ArrayList<>(numVals);
                            for (int j=0; j<numVals; j++)
                            {
                                vals.add(ByteBufferUtil.readWithShortLength(in));
                            }
                            values.add(vals);
                        }

                        BatchQueryOptions batchOptions = BatchQueryOptions.withPerStatementVariables(wrapped, values, queryOrIdList);
                        BatchStatement batch = new BatchStatement(-1, type, statements, Attributes.none());
                        return batch.getCasRequest(batchOptions, ts);
                    }
                }
                else
                {
                    String ksName = in.readUTF();
                    String query = in.readUTF();
                    QueryOptions options = QueryOptions.serializer.deserialize(in, version);

                    ParsedStatement.Prepared prepared = getStatement(ksName, query);


                    QueryState queryState = new QueryState(null) {
                        @Override
                        public long getTimestamp()
                        {
                            return ts;
                        }
                    };

                    options.prepare(prepared.boundNames);
                    ModificationStatement statement = (ModificationStatement) prepared.statement;

                    return statement.createCasRequest(queryState, options);
                }
            }
            catch (InvalidRequestException e)
            {
                throw new IOException(e);
            }
        }

        @Override
        public long serializedSize(CASRequest request, int version)
        {
            assert request instanceof CQL3CasRequest;
            CQL3CasRequest req = (CQL3CasRequest) request;

            long size = 0;
            size += 8; // out.writeLong(req.now);
            size += 1; // out.writeBoolean(req.isBatch);
            if (req.isBatch)
            {
                size += 1; // out.writeBoolean(req.batchAttrs.query != null);
                if (req.batchAttrs.query != null)
                {
                    // query batch
                    size += TypeSizes.NATIVE.sizeof(req.batchAttrs.query); // out.writeUTF(req.batchAttrs.query);
                    QueryOptions options = req.batchAttrs.options.forStatement(0);
                    size += QueryOptions.serializer.serializedSize(options, version);

                    return size;
                }
                else
                {
                    // batch message batch
                    size += 4; // out.writeInt(req.batchAttrs.type.ordinal());
                    size += QueryOptions.serializer.serializedSize(req.batchAttrs.options.getWrapped(), version);
                    size += 4; // out.writeInt(req.updates.size());
                    for (int i=0; i<req.updates.size(); i++)
                    {
                        RowUpdate update = req.updates.get(i);
                        size += TypeSizes.NATIVE.sizeof(update.stmt.getQueryString()); // out.writeUTF(update.stmt.getQueryString());

                        List<ByteBuffer> values = update.options.getValues();
                        size += 4; // out.writeInt(values.size());
                        for (ByteBuffer value: values)
                        {
                            size += 2 + value.remaining();
                        }
                    }

                    return size;
                }
            }
            else
            {
                assert req.updates.size() == 1;
                size += TypeSizes.NATIVE.sizeof(req.cfm.ksName); // out.writeUTF(req.cfm.ksName);
                RowUpdate update = req.updates.get(0);
                size += TypeSizes.NATIVE.sizeof(update.stmt.getQueryString()); // out.writeUTF(update.stmt.getQueryString());
                size += QueryOptions.serializer.serializedSize(update.options, version);

                return size;
            }
        }
    }
}
