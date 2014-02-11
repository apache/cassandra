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

import org.github.jamm.MemoryMeter;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.CASConditions;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Pair;

/*
 * Abstract parent class of individual modifications, i.e. INSERT, UPDATE and DELETE.
 */
public abstract class ModificationStatement implements CQLStatement, MeasurableForPreparedCache
{
    private static final ColumnIdentifier CAS_RESULT_COLUMN = new ColumnIdentifier("[applied]", false);

    private final int boundTerms;
    public final CFMetaData cfm;
    public final Attributes attrs;

    private final Map<ColumnIdentifier, Restriction> processedKeys = new HashMap<ColumnIdentifier, Restriction>();
    private final List<Operation> columnOperations = new ArrayList<Operation>();

    private List<Operation> columnConditions;
    private boolean ifNotExists;

    public ModificationStatement(int boundTerms, CFMetaData cfm, Attributes attrs)
    {
        this.boundTerms = boundTerms;
        this.cfm = cfm;
        this.attrs = attrs;
    }

    public long measureForPreparedCache(MemoryMeter meter)
    {
        return meter.measure(this)
             + meter.measureDeep(attrs)
             + meter.measureDeep(processedKeys)
             + meter.measureDeep(columnOperations)
             + (columnConditions == null ? 0 : meter.measureDeep(columnConditions));
    }

    public abstract boolean requireFullClusteringKey();
    public abstract ColumnFamily updateForKey(ByteBuffer key, Composite prefix, UpdateParameters params) throws InvalidRequestException;

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public String keyspace()
    {
        return cfm.ksName;
    }

    public String columnFamily()
    {
        return cfm.cfName;
    }

    public boolean isCounter()
    {
        return cfm.isCounter();
    }

    public long getTimestamp(long now, List<ByteBuffer> variables) throws InvalidRequestException
    {
        return attrs.getTimestamp(now, variables);
    }

    public boolean isTimestampSet()
    {
        return attrs.isTimestampSet();
    }

    public int getTimeToLive(List<ByteBuffer> variables) throws InvalidRequestException
    {
        return attrs.getTimeToLive(variables);
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.MODIFY);

        // CAS updates can be used to simulate a SELECT query, so should require Permission.SELECT as well.
        if (hasConditions())
            state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.SELECT);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (hasConditions() && attrs.isTimestampSet())
            throw new InvalidRequestException("Cannot provide custom timestamp for conditional updates");

        if (isCounter() && attrs.isTimestampSet())
            throw new InvalidRequestException("Cannot provide custom timestamp for counter updates");

        if (isCounter() && attrs.isTimeToLiveSet())
            throw new InvalidRequestException("Cannot provide custom TTL for counter updates");
    }

    public void addOperation(Operation op)
    {
        columnOperations.add(op);
    }

    public List<Operation> getOperations()
    {
        return columnOperations;
    }

    public void addCondition(Operation op)
    {
        if (columnConditions == null)
            columnConditions = new ArrayList<Operation>();

        columnConditions.add(op);
    }

    public void setIfNotExistCondition()
    {
        ifNotExists = true;
    }

    private void addKeyValues(ColumnIdentifier name, Restriction values) throws InvalidRequestException
    {
        if (processedKeys.put(name, values) != null)
            throw new InvalidRequestException(String.format("Multiple definitions found for PRIMARY KEY part %s", name));
    }

    public void addKeyValue(ColumnIdentifier name, Term value) throws InvalidRequestException
    {
        addKeyValues(name, new Restriction.EQ(value, false));
    }

    public void processWhereClause(List<Relation> whereClause, VariableSpecifications names) throws InvalidRequestException
    {
        for (Relation rel : whereClause)
        {
            ColumnDefinition def = cfm.getColumnDefinition(rel.getEntity());
            if (def == null)
                throw new InvalidRequestException(String.format("Unknown key identifier %s", rel.getEntity()));

            switch (def.kind)
            {
                case PARTITION_KEY:
                case CLUSTERING_COLUMN:
                    Restriction restriction;

                    if (rel.operator() == Relation.Type.EQ)
                    {
                        Term t = rel.getValue().prepare(keyspace(), def);
                        t.collectMarkerSpecification(names);
                        restriction = new Restriction.EQ(t, false);
                    }
                    else if (def.kind == ColumnDefinition.Kind.PARTITION_KEY && rel.operator() == Relation.Type.IN)
                    {
                        if (rel.getValue() != null)
                        {
                            Term t = rel.getValue().prepare(keyspace(), def);
                            t.collectMarkerSpecification(names);
                            restriction = Restriction.IN.create(t);
                        }
                        else
                        {
                            List<Term> values = new ArrayList<Term>(rel.getInValues().size());
                            for (Term.Raw raw : rel.getInValues())
                            {
                                Term t = raw.prepare(keyspace(), def);
                                t.collectMarkerSpecification(names);
                                values.add(t);
                            }
                            restriction = Restriction.IN.create(values);
                        }
                    }
                    else
                    {
                        throw new InvalidRequestException(String.format("Invalid operator %s for PRIMARY KEY part %s", rel.operator(), def.name));
                    }

                    addKeyValues(def.name, restriction);
                    break;
                case COMPACT_VALUE:
                case REGULAR:
                    throw new InvalidRequestException(String.format("Non PRIMARY KEY %s found in where clause", def.name));
            }
        }
    }

    public List<ByteBuffer> buildPartitionKeyNames(List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        CBuilder keyBuilder = cfm.getKeyValidatorAsCType().builder();
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        for (ColumnDefinition def : cfm.partitionKeyColumns())
        {
            Restriction r = processedKeys.get(def.name);
            if (r == null)
                throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", def.name));

            List<ByteBuffer> values = r.values(variables);

            if (keyBuilder.remainingCount() == 1)
            {
                for (ByteBuffer val : values)
                {
                    if (val == null)
                        throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", def.name));
                    keys.add(keyBuilder.buildWith(val).toByteBuffer());
                }
            }
            else
            {
                if (values.size() != 1)
                    throw new InvalidRequestException("IN is only supported on the last column of the partition key");
                ByteBuffer val = values.get(0);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", def.name));
                keyBuilder.add(val);
            }
        }
        return keys;
    }

    public Composite createClusteringPrefix(List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        CBuilder builder = cfm.comparator.prefixBuilder();
        ColumnDefinition firstEmptyKey = null;
        for (ColumnDefinition def : cfm.clusteringColumns())
        {
            Restriction r = processedKeys.get(def.name);
            if (r == null)
            {
                firstEmptyKey = def;
                if (requireFullClusteringKey() && !cfm.comparator.isDense() && cfm.comparator.isCompound())
                    throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", def.name));
            }
            else if (firstEmptyKey != null)
            {
                throw new InvalidRequestException(String.format("Missing PRIMARY KEY part %s since %s is set", firstEmptyKey.name, def.name));
            }
            else
            {
                List<ByteBuffer> values = r.values(variables);
                assert values.size() == 1; // We only allow IN for row keys so far
                ByteBuffer val = values.get(0);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for clustering key part %s", def.name));
                builder.add(val);
            }
        }
        return builder.build();
    }

    protected ColumnDefinition getFirstEmptyKey()
    {
        for (ColumnDefinition def : cfm.clusteringColumns())
        {
            if (processedKeys.get(def.name) == null)
                return def;
        }
        return null;
    }

    protected Map<ByteBuffer, CQL3Row> readRequiredRows(List<ByteBuffer> partitionKeys, Composite clusteringPrefix, boolean local, ConsistencyLevel cl)
    throws RequestExecutionException, RequestValidationException
    {
        // Lists SET operation incurs a read.
        Set<ColumnIdentifier> toRead = null;
        for (Operation op : columnOperations)
        {
            if (op.requiresRead())
            {
                if (toRead == null)
                    toRead = new TreeSet<ColumnIdentifier>();
                toRead.add(op.column.name);
            }
        }

        return toRead == null ? null : readRows(partitionKeys, clusteringPrefix, toRead, cfm, local, cl);
    }

    protected Map<ByteBuffer, CQL3Row> readRows(List<ByteBuffer> partitionKeys, Composite rowPrefix, Set<ColumnIdentifier> toRead, CFMetaData cfm, boolean local, ConsistencyLevel cl)
    throws RequestExecutionException, RequestValidationException
    {
        try
        {
            cl.validateForRead(keyspace());
        }
        catch (InvalidRequestException e)
        {
            throw new InvalidRequestException(String.format("Write operation require a read but consistency %s is not supported on reads", cl));
        }

        ColumnSlice[] slices = new ColumnSlice[toRead.size()];
        int i = 0;
        for (ColumnIdentifier name : toRead)
            slices[i++] = cfm.comparator.create(rowPrefix, name).slice();

        List<ReadCommand> commands = new ArrayList<ReadCommand>(partitionKeys.size());
        long now = System.currentTimeMillis();
        for (ByteBuffer key : partitionKeys)
            commands.add(new SliceFromReadCommand(keyspace(),
                                                  key,
                                                  columnFamily(),
                                                  now,
                                                  new SliceQueryFilter(slices, false, Integer.MAX_VALUE)));

        List<Row> rows = local
                       ? SelectStatement.readLocally(keyspace(), commands)
                       : StorageProxy.read(commands, cl);

        Map<ByteBuffer, CQL3Row> map = new HashMap<ByteBuffer, CQL3Row>();
        for (Row row : rows)
        {
            if (row.cf == null || row.cf.isEmpty())
                continue;

            Iterator<CQL3Row> iter = cfm.comparator.CQL3RowBuilder(now).group(row.cf.getSortedColumns().iterator());
            if (iter.hasNext())
            {
                map.put(row.key.key, iter.next());
                // We can only update one CQ3Row per partition key at a time (we don't allow IN for clustering key)
                assert !iter.hasNext();
            }
        }
        return map;
    }

    public boolean hasConditions()
    {
        return ifNotExists || (columnConditions != null && !columnConditions.isEmpty());
    }

    public ResultMessage execute(QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        if (options.getConsistency() == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        if (hasConditions() && options.getProtocolVersion() == 1)
            throw new InvalidRequestException("Conditional updates are not supported by the protocol version in use. You need to upgrade to a driver using the native protocol v2.");

        return hasConditions()
             ? executeWithCondition(queryState, options)
             : executeWithoutCondition(queryState, options);
    }

    private ResultMessage executeWithoutCondition(QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        ConsistencyLevel cl = options.getConsistency();
        if (isCounter())
            cl.validateCounterForWrite(cfm);
        else
            cl.validateForWrite(cfm.ksName);

        Collection<? extends IMutation> mutations = getMutations(options.getValues(), false, cl, queryState.getTimestamp(), false);
        if (!mutations.isEmpty())
            StorageProxy.mutateWithTriggers(mutations, cl, false);

        return null;
    }

    public ResultMessage executeWithCondition(QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> variables = options.getValues();
        List<ByteBuffer> keys = buildPartitionKeyNames(variables);
        // We don't support IN for CAS operation so far
        if (keys.size() > 1)
            throw new InvalidRequestException("IN on the partition key is not supported with conditional updates");

        Composite clusteringPrefix = createClusteringPrefix(variables);

        ByteBuffer key = keys.get(0);
        ThriftValidation.validateKey(cfm, key);

        UpdateParameters updParams = new UpdateParameters(cfm, variables, queryState.getTimestamp(), getTimeToLive(variables), null);
        ColumnFamily updates = updateForKey(key, clusteringPrefix, updParams);

        // It's cleaner to use the query timestamp below, but it's in seconds while the conditions expects microseconds, so just
        // put it back in millis (we don't really lose precision because the ultimate consumer, Column.isLive, re-divide it).
        long now = queryState.getTimestamp() * 1000;
        CASConditions conditions = ifNotExists
                                 ? new NotExistCondition(clusteringPrefix, now)
                                 : new ColumnsConditions(clusteringPrefix, cfm, key, columnConditions, variables, now);

        ColumnFamily result = StorageProxy.cas(keyspace(),
                                               columnFamily(),
                                               key,
                                               conditions,
                                               updates,
                                               options.getSerialConsistency(),
                                               options.getConsistency());
        return new ResultMessage.Rows(buildCasResultSet(key, result));
    }

    private ResultSet buildCasResultSet(ByteBuffer key, ColumnFamily cf) throws InvalidRequestException
    {
        boolean success = cf == null;

        ColumnSpecification spec = new ColumnSpecification(keyspace(), columnFamily(), CAS_RESULT_COLUMN, BooleanType.instance);
        ResultSet.Metadata metadata = new ResultSet.Metadata(Collections.singletonList(spec));
        List<List<ByteBuffer>> rows = Collections.singletonList(Collections.singletonList(BooleanType.instance.decompose(success)));

        ResultSet rs = new ResultSet(metadata, rows);
        return success ? rs : merge(rs, buildCasFailureResultSet(key, cf));
    }

    private static ResultSet merge(ResultSet left, ResultSet right)
    {
        if (left.size() == 0)
            return right;
        else if (right.size() == 0)
            return left;

        assert left.size() == 1 && right.size() == 1;
        int size = left.metadata.names.size() + right.metadata.names.size();
        List<ColumnSpecification> specs = new ArrayList<ColumnSpecification>(size);
        specs.addAll(left.metadata.names);
        specs.addAll(right.metadata.names);
        List<ByteBuffer> row = new ArrayList<ByteBuffer>(size);
        row.addAll(left.rows.get(0));
        row.addAll(right.rows.get(0));
        return new ResultSet(new ResultSet.Metadata(specs), Collections.singletonList(row));
    }

    private ResultSet buildCasFailureResultSet(ByteBuffer key, ColumnFamily cf) throws InvalidRequestException
    {
        Selection selection;
        if (ifNotExists)
        {
            selection = Selection.wildcard(cfm);
        }
        else
        {
            List<ColumnDefinition> defs = new ArrayList<>(columnConditions.size());
            for (Operation condition : columnConditions)
                defs.add(condition.column);
            selection = Selection.forColumns(defs);
        }

        long now = System.currentTimeMillis();
        Selection.ResultSetBuilder builder = selection.resultSetBuilder(now);
        SelectStatement.forSelection(cfm, selection).processColumnFamily(key, cf, Collections.<ByteBuffer>emptyList(), now, builder);

        return builder.build();
    }

    public ResultMessage executeInternal(QueryState queryState) throws RequestValidationException, RequestExecutionException
    {
        if (hasConditions())
            throw new UnsupportedOperationException();

        for (IMutation mutation : getMutations(Collections.<ByteBuffer>emptyList(), true, null, queryState.getTimestamp(), false))
        {
            // We don't use counters internally.
            assert mutation instanceof Mutation;
            ((Mutation) mutation).apply();
        }
        return null;
    }

    /**
     * Convert statement into a list of mutations to apply on the server
     *
     * @param variables value for prepared statement markers
     * @param local if true, any requests (for collections) performed by getMutation should be done locally only.
     * @param cl the consistency to use for the potential reads involved in generating the mutations (for lists set/delete operations)
     * @param now the current timestamp in microseconds to use if no timestamp is user provided.
     *
     * @return list of the mutations
     * @throws InvalidRequestException on invalid requests
     */
    public Collection<? extends IMutation> getMutations(List<ByteBuffer> variables, boolean local, ConsistencyLevel cl, long now, boolean isBatch)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> keys = buildPartitionKeyNames(variables);
        Composite clusteringPrefix = createClusteringPrefix(variables);

        // Some lists operation requires reading
        Map<ByteBuffer, CQL3Row> rows = readRequiredRows(keys, clusteringPrefix, local, cl);
        UpdateParameters params = new UpdateParameters(cfm, variables, getTimestamp(now, variables), getTimeToLive(variables), rows);

        Collection<IMutation> mutations = new ArrayList<IMutation>();
        for (ByteBuffer key: keys)
        {
            ThriftValidation.validateKey(cfm, key);
            ColumnFamily cf = updateForKey(key, clusteringPrefix, params);
            mutations.add(makeMutation(key, cf, cl, isBatch));
        }
        return mutations;
    }

    private IMutation makeMutation(ByteBuffer key, ColumnFamily cf, ConsistencyLevel cl, boolean isBatch)
    {
        Mutation mutation;
        if (isBatch)
        {
            // we might group other mutations together with this one later, so make it mutable
            mutation = new Mutation(cfm.ksName, key);
            mutation.add(cf);
        }
        else
        {
            mutation = new Mutation(cfm.ksName, key, cf);
        }
        return isCounter() ? new CounterMutation(mutation, cl) : mutation;
    }

    private static abstract class CQL3CasConditions implements CASConditions
    {
        protected final Composite rowPrefix;
        protected final long now;

        protected CQL3CasConditions(Composite rowPrefix, long now)
        {
            this.rowPrefix = rowPrefix;
            this.now = now;
        }

        public IDiskAtomFilter readFilter()
        {
            // We always read the row entirely as on CAS failure we want to be able to distinguish between "row exists
            // but all values on why there were conditions are null" and "row doesn't exists", and we can't rely on the
            // row marker for that (see #6623)
            return new SliceQueryFilter(rowPrefix.slice(), false, 1, rowPrefix.size());
        }
    }

    private static class NotExistCondition extends CQL3CasConditions
    {
        private NotExistCondition(Composite rowPrefix, long now)
        {
            super(rowPrefix, now);
        }

        public boolean appliesTo(ColumnFamily current)
        {
            return current == null || current.hasOnlyTombstones(now);
        }
    }

    private static class ColumnsConditions extends CQL3CasConditions
    {
        private final ColumnFamily expected;

        private ColumnsConditions(Composite rowPrefix,
                                  CFMetaData cfm,
                                  ByteBuffer key,
                                  Collection<Operation> conditions,
                                  List<ByteBuffer> variables,
                                  long now) throws InvalidRequestException
        {
            super(rowPrefix, now);
            this.expected = ArrayBackedSortedColumns.factory.create(cfm);

            // When building the conditions, we should not use a TTL. It's not useful, and if a very low ttl (1 seconds) is used, it's possible
            // for it to expire before the actual build of the conditions which would break since we would then testing for the presence of tombstones.
            UpdateParameters params = new UpdateParameters(cfm, variables, now, 0, null);

            // Conditions
            for (Operation condition : conditions)
                condition.execute(key, expected, rowPrefix, params);
        }

        public boolean appliesTo(ColumnFamily current)
        {
            if (current == null)
                return false;

            for (Cell e : expected)
            {
                Cell c = current.getColumn(e.name());
                if (e.isLive(now))
                {
                    if (c == null || !c.isLive(now) || !c.value().equals(e.value()))
                        return false;
                }
                else
                {
                    // If we have a tombstone in expected, it means the condition tests that the column is
                    // null, so check that we have no value
                    if (c != null && c.isLive(now))
                        return false;
                }
            }
            return true;
        }

        @Override
        public String toString()
        {
            return expected.toString();
        }
    }

    public static abstract class Parsed extends CFStatement
    {
        protected final Attributes.Raw attrs;
        private final List<Pair<ColumnIdentifier, Operation.RawUpdate>> conditions;
        private final boolean ifNotExists;

        protected Parsed(CFName name, Attributes.Raw attrs, List<Pair<ColumnIdentifier, Operation.RawUpdate>> conditions, boolean ifNotExists)
        {
            super(name);
            this.attrs = attrs;
            this.conditions = conditions == null ? Collections.<Pair<ColumnIdentifier, Operation.RawUpdate>>emptyList() : conditions;
            this.ifNotExists = ifNotExists;
        }

        public ParsedStatement.Prepared prepare() throws InvalidRequestException
        {
            VariableSpecifications boundNames = getBoundVariables();
            ModificationStatement statement = prepare(boundNames);
            return new ParsedStatement.Prepared(statement, boundNames);
        }

        public ModificationStatement prepare(VariableSpecifications boundNames) throws InvalidRequestException
        {
            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());

            Attributes preparedAttributes = attrs.prepare(keyspace(), columnFamily());
            preparedAttributes.collectMarkerSpecification(boundNames);

            ModificationStatement stmt = prepareInternal(metadata, boundNames, preparedAttributes);

            if (ifNotExists || !conditions.isEmpty())
            {
                if (stmt.isCounter())
                    throw new InvalidRequestException("Conditional updates are not supported on counter tables");

                if (attrs.timestamp != null)
                    throw new InvalidRequestException("Cannot provide custom timestamp for conditional updates");

                if (ifNotExists)
                {
                    // To have both 'IF NOT EXISTS' and some other conditions doesn't make sense.
                    // So far this is enforced by the parser, but let's assert it for sanity if ever the parse changes.
                    assert conditions.isEmpty();
                    stmt.setIfNotExistCondition();
                }
                else
                {
                    for (Pair<ColumnIdentifier, Operation.RawUpdate> entry : conditions)
                    {
                        ColumnDefinition def = metadata.getColumnDefinition(entry.left);
                        if (def == null)
                            throw new InvalidRequestException(String.format("Unknown identifier %s", entry.left));

                        /*
                         * Lists column names are based on a server-side generated timeuuid. So we can't allow lists
                         * operation or that would yield unexpected results (update that should apply wouldn't). So for
                         * now, we just refuse lists, which also save use from having to bother about the read that some
                         * list operation involve.
                         */
                        if (def.type instanceof ListType)
                            throw new InvalidRequestException(String.format("List operation (%s) are not allowed in conditional updates", def.name));

                        Operation condition = entry.right.prepare(keyspace(), def);
                        assert !condition.requiresRead();

                        condition.collectMarkerSpecification(boundNames);

                        switch (def.kind)
                        {
                            case PARTITION_KEY:
                            case CLUSTERING_COLUMN:
                                throw new InvalidRequestException(String.format("PRIMARY KEY part %s found in SET part", entry.left));
                            case COMPACT_VALUE:
                            case REGULAR:
                                stmt.addCondition(condition);
                                break;
                        }
                    }
                }
            }
            return stmt;
        }

        protected abstract ModificationStatement prepareInternal(CFMetaData cfm, VariableSpecifications boundNames, Attributes attrs) throws InvalidRequestException;
    }
}
