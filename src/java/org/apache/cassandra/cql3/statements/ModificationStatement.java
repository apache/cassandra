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

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.ByteBufferUtil;

/*
 * Abstract parent class of individual modifications, i.e. INSERT, UPDATE and DELETE.
 */
public abstract class ModificationStatement implements CQLStatement
{
    private static final ColumnIdentifier CAS_RESULT_COLUMN = new ColumnIdentifier("[applied]", false);

    private final int boundTerms;
    public final CFMetaData cfm;
    private final Attributes attrs;

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

    protected abstract boolean requireFullClusteringKey();
    public abstract ColumnFamily updateForKey(ByteBuffer key, ColumnNameBuilder builder, UpdateParameters params) throws InvalidRequestException;

    public int getBoundsTerms()
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
        return cfm.getDefaultValidator().isCommutative();
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
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (hasConditions() && attrs.isTimestampSet())
            throw new InvalidRequestException("Custom timestamps are not allowed when conditions are used");
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
        CFDefinition cfDef = cfm.getCfDef();
        for (Relation rel : whereClause)
        {
            CFDefinition.Name name = cfDef.get(rel.getEntity());
            if (name == null)
                throw new InvalidRequestException(String.format("Unknown key identifier %s", rel.getEntity()));

            switch (name.kind)
            {
                case KEY_ALIAS:
                case COLUMN_ALIAS:
                    Restriction restriction;

                    if (rel.operator() == Relation.Type.EQ)
                    {
                        Term t = rel.getValue().prepare(name);
                        t.collectMarkerSpecification(names);
                        restriction = new Restriction.EQ(t, false);
                    }
                    else if (name.kind == CFDefinition.Name.Kind.KEY_ALIAS && rel.operator() == Relation.Type.IN)
                    {
                        if (rel.getValue() != null)
                        {
                            Term t = rel.getValue().prepare(name);
                            t.collectMarkerSpecification(names);
                            restriction = Restriction.IN.create(t);
                        }
                        else
                        {
                            List<Term> values = new ArrayList<Term>(rel.getInValues().size());
                            for (Term.Raw raw : rel.getInValues())
                            {
                                Term t = raw.prepare(name);
                                t.collectMarkerSpecification(names);
                                values.add(t);
                            }
                            restriction = Restriction.IN.create(values);
                        }
                    }
                    else
                    {
                        throw new InvalidRequestException(String.format("Invalid operator %s for PRIMARY KEY part %s", rel.operator(), name));
                    }

                    addKeyValues(name.name, restriction);
                    break;
                case VALUE_ALIAS:
                case COLUMN_METADATA:
                    throw new InvalidRequestException(String.format("Non PRIMARY KEY %s found in where clause", name));
            }
        }
    }

    private List<ByteBuffer> buildPartitionKeyNames(List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        CFDefinition cfDef = cfm.getCfDef();
        ColumnNameBuilder keyBuilder = cfDef.getKeyNameBuilder();
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        for (CFDefinition.Name name : cfDef.keys.values())
        {
            Restriction r = processedKeys.get(name.name);
            if (r == null)
                throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", name));

            List<ByteBuffer> values = r.values(variables);

            if (keyBuilder.remainingCount() == 1)
            {
                for (ByteBuffer val : values)
                {
                    if (val == null)
                        throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", name));
                    keys.add(keyBuilder.copy().add(val).build());
                }
            }
            else
            {
                if (values.size() != 1)
                    throw new InvalidRequestException("IN is only supported on the last column of the partition key");
                ByteBuffer val = values.get(0);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", name));
                keyBuilder.add(val);
            }
        }
        return keys;
    }

    private ColumnNameBuilder createClusteringPrefixBuilder(List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        CFDefinition cfDef = cfm.getCfDef();
        ColumnNameBuilder builder = cfDef.getColumnNameBuilder();
        CFDefinition.Name firstEmptyKey = null;
        for (CFDefinition.Name name : cfDef.columns.values())
        {
            Restriction r = processedKeys.get(name.name);
            if (r == null)
            {
                firstEmptyKey = name;
                if (requireFullClusteringKey() && cfDef.isComposite && !cfDef.isCompact)
                    throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", name));
            }
            else if (firstEmptyKey != null)
            {
                throw new InvalidRequestException(String.format("Missing PRIMARY KEY part %s since %s is set", firstEmptyKey.name, name.name));
            }
            else
            {
                List<ByteBuffer> values = r.values(variables);
                assert values.size() == 1; // We only allow IN for row keys so far
                ByteBuffer val = values.get(0);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for clustering key part %s", name));
                builder.add(val);
            }
        }
        return builder;
    }

    protected CFDefinition.Name getFirstEmptyKey()
    {
        for (CFDefinition.Name name : cfm.getCfDef().columns.values())
        {
            if (processedKeys.get(name.name) == null)
                return name;
        }
        return null;
    }

    protected Map<ByteBuffer, ColumnGroupMap> readRequiredRows(List<ByteBuffer> partitionKeys, ColumnNameBuilder clusteringPrefix, boolean local, ConsistencyLevel cl)
    throws RequestExecutionException, RequestValidationException
    {
        // Lists SET operation incurs a read.
        Set<ByteBuffer> toRead = null;
        for (Operation op : columnOperations)
        {
            if (op.requiresRead())
            {
                if (toRead == null)
                    toRead = new TreeSet<ByteBuffer>(UTF8Type.instance);
                toRead.add(op.columnName.key);
            }
        }

        return toRead == null ? null : readRows(partitionKeys, clusteringPrefix, toRead, (CompositeType)cfm.comparator, local, cl);
    }

    private Map<ByteBuffer, ColumnGroupMap> readRows(List<ByteBuffer> partitionKeys, ColumnNameBuilder clusteringPrefix, Set<ByteBuffer> toRead, CompositeType composite, boolean local, ConsistencyLevel cl)
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
        for (ByteBuffer name : toRead)
        {
            ByteBuffer start = clusteringPrefix.copy().add(name).build();
            ByteBuffer finish = clusteringPrefix.copy().add(name).buildAsEndOfRange();
            slices[i++] = new ColumnSlice(start, finish);
        }

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

        Map<ByteBuffer, ColumnGroupMap> map = new HashMap<ByteBuffer, ColumnGroupMap>();
        for (Row row : rows)
        {
            if (row.cf == null || row.cf.getColumnCount() == 0)
                continue;

            ColumnGroupMap.Builder groupBuilder = new ColumnGroupMap.Builder(composite, true, now);
            for (Column column : row.cf)
                groupBuilder.add(column);

            List<ColumnGroupMap> groups = groupBuilder.groups();
            assert groups.isEmpty() || groups.size() == 1;
            if (!groups.isEmpty())
                map.put(row.key.key, groups.get(0));
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

        ColumnNameBuilder clusteringPrefix = createClusteringPrefixBuilder(variables);

        ByteBuffer key = keys.get(0);
        ThriftValidation.validateKey(cfm, key);

        UpdateParameters updParams = new UpdateParameters(cfm, variables, queryState.getTimestamp(), getTimeToLive(variables), null);
        ColumnFamily updates = updateForKey(key, clusteringPrefix, updParams);

        // When building the conditions, we should not use the TTL. It's not useful, and if a very low ttl (1 seconds) is used, it's possible
        // for it to expire before actually build the conditions which would break since we would then test for the presence of tombstones.
        UpdateParameters condParams = new UpdateParameters(cfm, variables, queryState.getTimestamp(), 0, null);
        ColumnFamily expected = buildConditions(key, clusteringPrefix, condParams);

        ColumnFamily result = StorageProxy.cas(keyspace(),
                                               columnFamily(),
                                               key,
                                               clusteringPrefix,
                                               expected,
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
        CFDefinition cfDef = cfm.getCfDef();

        Selection selection;
        if (ifNotExists)
        {
            selection = Selection.wildcard(cfDef);
        }
        else
        {
            List<CFDefinition.Name> names = new ArrayList<CFDefinition.Name>(columnConditions.size());
            for (Operation condition : columnConditions)
                names.add(cfDef.get(condition.columnName));
            selection = Selection.forColumns(names);
        }

        long now = System.currentTimeMillis();
        Selection.ResultSetBuilder builder = selection.resultSetBuilder(now);
        SelectStatement.forSelection(cfDef, selection).processColumnFamily(key, cf, Collections.<ByteBuffer>emptyList(), now, builder);

        return builder.build();
    }

    public ResultMessage executeInternal(QueryState queryState) throws RequestValidationException, RequestExecutionException
    {
        if (hasConditions())
            throw new UnsupportedOperationException();

        for (IMutation mutation : getMutations(Collections.<ByteBuffer>emptyList(), true, null, queryState.getTimestamp(), false))
            mutation.apply();
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
        ColumnNameBuilder clusteringPrefix = createClusteringPrefixBuilder(variables);

        // Some lists operation requires reading
        Map<ByteBuffer, ColumnGroupMap> rows = readRequiredRows(keys, clusteringPrefix, local, cl);
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
        RowMutation rm;
        if (isBatch)
        {
            // we might group other mutations together with this one later, so make it mutable
            rm = new RowMutation(cfm.ksName, key);
            rm.add(cf);
        }
        else
        {
            rm = new RowMutation(cfm.ksName, key, cf);
        }
        return isCounter() ? new CounterMutation(rm, cl) : rm;
    }

    private ColumnFamily buildConditions(ByteBuffer key, ColumnNameBuilder clusteringPrefix, UpdateParameters params)
    throws InvalidRequestException
    {
        if (ifNotExists)
            return null;

        ColumnFamily cf = TreeMapBackedSortedColumns.factory.create(cfm);

        // CQL row marker
        CFDefinition cfDef = cfm.getCfDef();
        if (cfDef.isComposite && !cfDef.isCompact && !cfm.isSuper())
        {
            ByteBuffer name = clusteringPrefix.copy().add(ByteBufferUtil.EMPTY_BYTE_BUFFER).build();
            cf.addColumn(params.makeColumn(name, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        }

        // Conditions
        for (Operation condition : columnConditions)
            condition.execute(key, cf, clusteringPrefix.copy(), params);

        assert !cf.isEmpty();
        return cf;
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
            VariableSpecifications boundNames = getBoundsVariables();
            ModificationStatement statement = prepare(boundNames);
            return new ParsedStatement.Prepared(statement, boundNames);
        }

        public ModificationStatement prepare(VariableSpecifications boundNames) throws InvalidRequestException
        {
            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
            CFDefinition cfDef = metadata.getCfDef();

            Attributes preparedAttributes = attrs.prepare(keyspace(), columnFamily());
            preparedAttributes.collectMarkerSpecification(boundNames);

            ModificationStatement stmt = prepareInternal(cfDef, boundNames, preparedAttributes);

            if (ifNotExists || (conditions != null && !conditions.isEmpty()))
            {
                if (stmt.isCounter())
                    throw new InvalidRequestException("Conditional updates are not supported on counter tables");

                if (attrs.timestamp != null)
                    throw new InvalidRequestException("Cannot provide custom timestamp for conditional update");

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
                        CFDefinition.Name name = cfDef.get(entry.left);
                        if (name == null)
                            throw new InvalidRequestException(String.format("Unknown identifier %s", entry.left));

                        /*
                         * Lists column names are based on a server-side generated timeuuid. So we can't allow lists
                         * operation or that would yield unexpected results (update that should apply wouldn't). So for
                         * now, we just refuse lists, which also save use from having to bother about the read that some
                         * list operation involve.
                         */
                        if (name.type instanceof ListType)
                            throw new InvalidRequestException(String.format("List operation (%s) are not allowed in conditional updates", name));

                        Operation condition = entry.right.prepare(name);
                        assert !condition.requiresRead();

                        condition.collectMarkerSpecification(boundNames);

                        switch (name.kind)
                        {
                            case KEY_ALIAS:
                            case COLUMN_ALIAS:
                                throw new InvalidRequestException(String.format("PRIMARY KEY part %s found in SET part", entry.left));
                            case VALUE_ALIAS:
                            case COLUMN_METADATA:
                                stmt.addCondition(condition);
                                break;
                        }
                    }
                }
            }
            return stmt;
        }

        protected abstract ModificationStatement prepareInternal(CFDefinition cfDef, VariableSpecifications boundNames, Attributes attrs) throws InvalidRequestException;
    }
}
