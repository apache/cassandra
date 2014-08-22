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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.github.jamm.MemoryMeter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.exceptions.*;
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

    private static final Logger logger = LoggerFactory.getLogger(ModificationStatement.class);

    private static boolean loggedCounterTTL = false;
    private static boolean loggedCounterTimestamp = false;

    public static enum StatementType { INSERT, UPDATE, DELETE }
    public final StatementType type;

    public final CFMetaData cfm;
    public final Attributes attrs;

    private final Map<ColumnIdentifier, Restriction> processedKeys = new HashMap<ColumnIdentifier, Restriction>();
    private final List<Operation> columnOperations = new ArrayList<Operation>();

    private int boundTerms;
    // Separating normal and static conditions makes things somewhat easier
    private List<ColumnCondition> columnConditions;
    private List<ColumnCondition> staticConditions;
    private boolean ifNotExists;
    private boolean ifExists;

    private boolean hasNoClusteringColumns = true;

    private boolean setsStaticColumns;
    private boolean setsRegularColumns;

    private final Function<ColumnCondition, ColumnIdentifier> getColumnForCondition = new Function<ColumnCondition, ColumnIdentifier>()
    {
        public ColumnIdentifier apply(ColumnCondition cond)
        {
            return cond.column.name;
        }
    };

    public ModificationStatement(StatementType type, CFMetaData cfm, Attributes attrs)
    {
        this.type = type;
        this.cfm = cfm;
        this.attrs = attrs;
    }

    public long measureForPreparedCache(MemoryMeter meter)
    {
        return meter.measure(this)
             + meter.measureDeep(attrs)
             + meter.measureDeep(processedKeys)
             + meter.measureDeep(columnOperations)
             + (columnConditions == null ? 0 : meter.measureDeep(columnConditions))
             + (staticConditions == null ? 0 : meter.measureDeep(staticConditions));
    }

    public abstract boolean requireFullClusteringKey();
    public abstract void addUpdateForKey(ColumnFamily updates, ByteBuffer key, ColumnNameBuilder builder, UpdateParameters params) throws InvalidRequestException;

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

        // CAS updates can be used to simulate a SELECT query, so should require Permission.SELECT as well.
        if (hasConditions())
            state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.SELECT);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (hasConditions())
        {
            if (attrs.isTimestampSet())
                throw new InvalidRequestException("Cannot provide custom timestamp for conditional update");

            if (requiresRead())
                throw new InvalidRequestException("Operations on lists requiring a read (setting by index and deletions by index or value) are not allowed with IF conditions");
        }

        if (isCounter())
        {
            if (attrs.isTimestampSet() && !loggedCounterTimestamp)
            {
                logger.warn("Detected use of 'USING TIMESTAMP' in a counter UPDATE. This is invalid " +
                            "because counters do not use timestamps, and the timestamp has been ignored. " +
                            "Such queries will be rejected in Cassandra 2.1+ - please fix your queries before then.");
                loggedCounterTimestamp = true;
            }

            if (attrs.isTimeToLiveSet() && !loggedCounterTTL)
            {
                logger.warn("Detected use of 'USING TTL' in a counter UPDATE. This is invalid " +
                            "because counter tables do not support TTL, and the TTL value has been ignored. " +
                            "Such queries will be rejected in Cassandra 2.1+ - please fix your queries before then.");
                loggedCounterTTL = true;
            }
        }
    }

    public void addOperation(Operation op)
    {
        if (op.isStatic(cfm))
            setsStaticColumns = true;
        else
            setsRegularColumns = true;
        columnOperations.add(op);
    }

    public List<Operation> getOperations()
    {
        return columnOperations;
    }

    public Iterable<ColumnIdentifier> getColumnsWithConditions()
    {
        if (ifNotExists || ifExists)
            return null;

        return Iterables.concat(columnConditions == null ? Collections.<ColumnIdentifier>emptyList() : Iterables.transform(columnConditions, getColumnForCondition),
                                staticConditions == null ? Collections.<ColumnIdentifier>emptyList() : Iterables.transform(staticConditions, getColumnForCondition));
    }

    public void addCondition(ColumnCondition cond) throws InvalidRequestException
    {
        List<ColumnCondition> conds = null;
        if (cond.column.kind == CFDefinition.Name.Kind.STATIC)
        {
            setsStaticColumns = true;
            if (staticConditions == null)
                staticConditions = new ArrayList<ColumnCondition>();
            conds = staticConditions;
        }
        else
        {
            setsRegularColumns = true;
            if (columnConditions == null)
                columnConditions = new ArrayList<ColumnCondition>();
            conds = columnConditions;
        }
        conds.add(cond);
    }

    public void setIfNotExistCondition()
    {
        ifNotExists = true;
    }

    public boolean hasIfNotExistCondition()
    {
        return ifNotExists;
    }

    public void setIfExistCondition()
    {
        ifExists = true;
    }

    public boolean hasIfExistCondition()
    {
        return ifExists;
    }

    private void addKeyValues(CFDefinition.Name name, Restriction values) throws InvalidRequestException
    {
        if (name.kind == CFDefinition.Name.Kind.COLUMN_ALIAS)
            hasNoClusteringColumns = false;
        if (processedKeys.put(name.name, values) != null)
            throw new InvalidRequestException(String.format("Multiple definitions found for PRIMARY KEY part %s", name.name));
    }

    public void addKeyValue(CFDefinition.Name name, Term value) throws InvalidRequestException
    {
        addKeyValues(name, new SingleColumnRestriction.EQ(value, false));
    }

    public void processWhereClause(List<Relation> whereClause, VariableSpecifications names) throws InvalidRequestException
    {
        CFDefinition cfDef = cfm.getCfDef();
        for (Relation relation : whereClause)
        {
            if (!(relation instanceof SingleColumnRelation))
            {
                throw new InvalidRequestException(
                        String.format("Multi-column relations cannot be used in WHERE clauses for modification statements: %s", relation));
            }
            SingleColumnRelation rel = (SingleColumnRelation) relation;

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
                        restriction = new SingleColumnRestriction.EQ(t, false);
                    }
                    else if (name.kind == CFDefinition.Name.Kind.KEY_ALIAS && rel.operator() == Relation.Type.IN)
                    {
                        if (rel.getValue() != null)
                        {
                            Term t = rel.getValue().prepare(name);
                            t.collectMarkerSpecification(names);
                            restriction = new SingleColumnRestriction.InWithMarker((Lists.Marker)t);
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
                            restriction = new SingleColumnRestriction.InWithValues(values);
                        }
                    }
                    else
                    {
                        throw new InvalidRequestException(String.format("Invalid operator %s for PRIMARY KEY part %s", rel.operator(), name));
                    }

                    addKeyValues(name, restriction);
                    break;
                case VALUE_ALIAS:
                case COLUMN_METADATA:
                case STATIC:
                    throw new InvalidRequestException(String.format("Non PRIMARY KEY %s found in where clause", name));
            }
        }
    }

    public List<ByteBuffer> buildPartitionKeyNames(List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        CFDefinition cfDef = cfm.getCfDef();
        ColumnNameBuilder keyBuilder = cfDef.getKeyNameBuilder();
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        for (CFDefinition.Name name : cfDef.partitionKeys())
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
                    ByteBuffer key = keyBuilder.copy().add(val).build();
                    ThriftValidation.validateKey(cfm, key);
                    keys.add(key);
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

    public ColumnNameBuilder createClusteringPrefixBuilder(List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        // If the only updated/deleted columns are static, then we don't need clustering columns.
        // And in fact, unless it is an INSERT, we reject if clustering colums are provided as that
        // suggest something unintended. For instance, given:
        //   CREATE TABLE t (k int, v int, s int static, PRIMARY KEY (k, v))
        // it can make sense to do:
        //   INSERT INTO t(k, v, s) VALUES (0, 1, 2)
        // but both
        //   UPDATE t SET s = 3 WHERE k = 0 AND v = 1
        //   DELETE v FROM t WHERE k = 0 AND v = 1
        // sounds like you don't really understand what your are doing.
        if (setsStaticColumns && !setsRegularColumns)
        {
            // If we set no non-static columns, then it's fine not to have clustering columns
            if (hasNoClusteringColumns)
                return cfm.getStaticColumnNameBuilder();

            // If we do have clustering columns however, then either it's an INSERT and the query is valid
            // but we still need to build a proper prefix, or it's not an INSERT, and then we want to reject
            // (see above)
            if (type != StatementType.INSERT)
            {
                for (CFDefinition.Name name : cfm.getCfDef().clusteringColumns())
                    if (processedKeys.get(name.name) != null)
                        throw new InvalidRequestException(String.format("Invalid restriction on clustering column %s since the %s statement modifies only static columns", name.name, type));
                // we should get there as it contradicts hasNoClusteringColumns == false
                throw new AssertionError();
            }
        }

        return createClusteringPrefixBuilderInternal(variables);
    }

    private ColumnNameBuilder updatePrefixFor(ByteBuffer name, ColumnNameBuilder prefix)
    {
        return isStatic(name) ? cfm.getStaticColumnNameBuilder() : prefix;
    }

    public boolean isStatic(ByteBuffer name)
    {
        ColumnDefinition def = cfm.getColumnDefinition(name);
        return def != null && def.type == ColumnDefinition.Type.STATIC;
    }

    private ColumnNameBuilder createClusteringPrefixBuilderInternal(List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        CFDefinition cfDef = cfm.getCfDef();
        ColumnNameBuilder builder = cfDef.getColumnNameBuilder();
        CFDefinition.Name firstEmptyKey = null;
        for (CFDefinition.Name name : cfDef.clusteringColumns())
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
        for (CFDefinition.Name name : cfm.getCfDef().clusteringColumns())
        {
            if (processedKeys.get(name.name) == null)
                return name;
        }
        return null;
    }

    public boolean requiresRead()
    {
        for (Operation op : columnOperations)
            if (op.requiresRead())
                return true;
        return false;
    }

    protected Map<ByteBuffer, ColumnGroupMap> readRequiredRows(Collection<ByteBuffer> partitionKeys, ColumnNameBuilder clusteringPrefix, boolean local, ConsistencyLevel cl)
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

    private Map<ByteBuffer, ColumnGroupMap> readRows(Collection<ByteBuffer> partitionKeys, ColumnNameBuilder clusteringPrefix, Set<ByteBuffer> toRead, CompositeType composite, boolean local, ConsistencyLevel cl)
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
            ColumnNameBuilder prefix = updatePrefixFor(name, clusteringPrefix);
            ByteBuffer start = prefix.copy().add(name).build();
            ByteBuffer finish = prefix.copy().add(name).buildAsEndOfRange();
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
        return ifNotExists
            || ifExists
            || (columnConditions != null && !columnConditions.isEmpty())
            || (staticConditions != null && !staticConditions.isEmpty());
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

        Collection<? extends IMutation> mutations = getMutations(options.getValues(), false, cl, queryState.getTimestamp());
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

        ByteBuffer key = keys.get(0);

        CQL3CasConditions conditions = new CQL3CasConditions(cfm, queryState.getTimestamp());
        ColumnNameBuilder prefix = createClusteringPrefixBuilder(variables);
        ColumnFamily updates = UnsortedColumns.factory.create(cfm);
        addUpdatesAndConditions(key, prefix, updates, conditions, variables, getTimestamp(queryState.getTimestamp(), variables));

        ColumnFamily result = StorageProxy.cas(keyspace(),
                                               columnFamily(),
                                               key,
                                               conditions,
                                               updates,
                                               options.getSerialConsistency(),
                                               options.getConsistency());
        return new ResultMessage.Rows(buildCasResultSet(key, result));
    }

    public void addUpdatesAndConditions(ByteBuffer key, ColumnNameBuilder clusteringPrefix, ColumnFamily updates, CQL3CasConditions conditions, List<ByteBuffer> variables, long now)
    throws InvalidRequestException
    {
        UpdateParameters updParams = new UpdateParameters(cfm, variables, now, getTimeToLive(variables), null);
        addUpdateForKey(updates, key, clusteringPrefix, updParams);

        if (ifNotExists)
        {
            // If we use ifNotExists, if the statement applies to any non static columns, then the condition is on the row of the non-static
            // columns and the prefix should be the rowPrefix. But if only static columns are set, then the ifNotExists apply to the existence
            // of any static columns and we should use the prefix for the "static part" of the partition.
            conditions.addNotExist(clusteringPrefix);
        }
        else if (ifExists)
        {
            conditions.addExist(clusteringPrefix);
        }
        else
        {
            if (columnConditions != null)
                conditions.addConditions(clusteringPrefix, columnConditions, variables);
            if (staticConditions != null)
                conditions.addConditions(cfm.getStaticColumnNameBuilder(), staticConditions, variables);
        }
    }

    private ResultSet buildCasResultSet(ByteBuffer key, ColumnFamily cf) throws InvalidRequestException
    {
        return buildCasResultSet(keyspace(), key, columnFamily(), cf, getColumnsWithConditions(), false);
    }

    public static ResultSet buildCasResultSet(String ksName, ByteBuffer key, String cfName, ColumnFamily cf, Iterable<ColumnIdentifier> columnsWithConditions, boolean isBatch)
    throws InvalidRequestException
    {
        boolean success = cf == null;

        ColumnSpecification spec = new ColumnSpecification(ksName, cfName, CAS_RESULT_COLUMN, BooleanType.instance);
        ResultSet.Metadata metadata = new ResultSet.Metadata(Collections.singletonList(spec));
        List<List<ByteBuffer>> rows = Collections.singletonList(Collections.singletonList(BooleanType.instance.decompose(success)));

        ResultSet rs = new ResultSet(metadata, rows);
        return success ? rs : merge(rs, buildCasFailureResultSet(key, cf, columnsWithConditions, isBatch));
    }

    private static ResultSet merge(ResultSet left, ResultSet right)
    {
        if (left.size() == 0)
            return right;
        else if (right.size() == 0)
            return left;

        assert left.size() == 1;
        int size = left.metadata.names.size() + right.metadata.names.size();
        List<ColumnSpecification> specs = new ArrayList<ColumnSpecification>(size);
        specs.addAll(left.metadata.names);
        specs.addAll(right.metadata.names);
        List<List<ByteBuffer>> rows = new ArrayList<>(right.size());
        for (int i = 0; i < right.size(); i++)
        {
            List<ByteBuffer> row = new ArrayList<ByteBuffer>(size);
            row.addAll(left.rows.get(0));
            row.addAll(right.rows.get(i));
            rows.add(row);
        }
        return new ResultSet(new ResultSet.Metadata(specs), rows);
    }

    private static ResultSet buildCasFailureResultSet(ByteBuffer key, ColumnFamily cf, Iterable<ColumnIdentifier> columnsWithConditions, boolean isBatch)
    throws InvalidRequestException
    {
        CFDefinition cfDef = cf.metadata().getCfDef();

        Selection selection;
        if (columnsWithConditions == null)
        {
            selection = Selection.wildcard(cfDef);
        }
        else
        {
            // We can have multiple conditions on the same columns (for collections) so use a set
            // to avoid duplicate, but preserve the order just to it follows the order of IF in the query in general
            Set<CFDefinition.Name> names = new LinkedHashSet<CFDefinition.Name>();
            // Adding the partition key for batches to disambiguate if the conditions span multipe rows (we don't add them outside
            // of batches for compatibility sakes).
            if (isBatch)
            {
                names.addAll(cfDef.partitionKeys());
                names.addAll(cfDef.clusteringColumns());
            }
            for (ColumnIdentifier id : columnsWithConditions)
                names.add(cfDef.get(id));
            selection = Selection.forColumns(names);
        }

        long now = System.currentTimeMillis();
        Selection.ResultSetBuilder builder = selection.resultSetBuilder(now);
        SelectStatement.forSelection(cfDef, selection).processColumnFamily(key, cf, Collections.<ByteBuffer>emptyList(), now, builder);

        return builder.build();
    }

    public ResultMessage executeInternal(QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        if (hasConditions())
            throw new UnsupportedOperationException();

        List<ByteBuffer> variables = options.getValues();
        for (IMutation mutation : getMutations(variables, true, null, queryState.getTimestamp()))
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
    private Collection<? extends IMutation> getMutations(List<ByteBuffer> variables, boolean local, ConsistencyLevel cl, long now)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> keys = buildPartitionKeyNames(variables);
        ColumnNameBuilder clusteringPrefix = createClusteringPrefixBuilder(variables);

        UpdateParameters params = makeUpdateParameters(keys, clusteringPrefix, variables, local, cl, now);

        Collection<IMutation> mutations = new ArrayList<IMutation>();
        for (ByteBuffer key: keys)
        {
            ThriftValidation.validateKey(cfm, key);
            ColumnFamily cf = UnsortedColumns.factory.create(cfm);
            addUpdateForKey(cf, key, clusteringPrefix, params);
            RowMutation rm = new RowMutation(cfm.ksName, key, cf);
            mutations.add(isCounter() ? new CounterMutation(rm, cl) : rm);
        }
        return mutations;
    }

    public UpdateParameters makeUpdateParameters(Collection<ByteBuffer> keys,
                                                 ColumnNameBuilder prefix,
                                                 List<ByteBuffer> variables,
                                                 boolean local,
                                                 ConsistencyLevel cl,
                                                 long now)
    throws RequestExecutionException, RequestValidationException
    {
        // Some lists operation requires reading
        Map<ByteBuffer, ColumnGroupMap> rows = readRequiredRows(keys, prefix, local, cl);
        return new UpdateParameters(cfm, variables, getTimestamp(now, variables), getTimeToLive(variables), rows);
    }

    public static abstract class Parsed extends CFStatement
    {
        protected final Attributes.Raw attrs;
        protected final List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions;
        private final boolean ifNotExists;
        private final boolean ifExists;

        protected Parsed(CFName name, Attributes.Raw attrs, List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions, boolean ifNotExists, boolean ifExists)
        {
            super(name);
            this.attrs = attrs;
            this.conditions = conditions == null ? Collections.<Pair<ColumnIdentifier, ColumnCondition.Raw>>emptyList() : conditions;
            this.ifNotExists = ifNotExists;
            this.ifExists = ifExists;
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
            CFDefinition cfDef = metadata.getCfDef();

            // The collected count in the beginning of preparation.
            // Will start at non-zero for statements nested inside a BatchStatement (the second and the further ones).
            int collected = boundNames.getCollectedCount();

            Attributes preparedAttributes = attrs.prepare(keyspace(), columnFamily());
            preparedAttributes.collectMarkerSpecification(boundNames);

            ModificationStatement stmt = prepareInternal(cfDef, boundNames, preparedAttributes);

            if (ifNotExists || ifExists || !conditions.isEmpty())
            {
                if (stmt.isCounter())
                    throw new InvalidRequestException("Conditional updates are not supported on counter tables");

                if (ifNotExists)
                {
                    // To have both 'IF NOT EXISTS' and some other conditions doesn't make sense.
                    // So far this is enforced by the parser, but let's assert it for sanity if ever the parse changes.
                    assert conditions.isEmpty();
                    assert !ifExists;
                    stmt.setIfNotExistCondition();
                }
                else if (ifExists)
                {
                    assert conditions.isEmpty();
                    assert !ifNotExists;
                    stmt.setIfExistCondition();
                }
                else
                {
                    for (Pair<ColumnIdentifier, ColumnCondition.Raw> entry : conditions)
                    {
                        CFDefinition.Name name = cfDef.get(entry.left);
                        if (name == null)
                            throw new InvalidRequestException(String.format("Unknown identifier %s", entry.left));

                        ColumnCondition condition = entry.right.prepare(name);
                        condition.collectMarkerSpecification(boundNames);

                        switch (name.kind)
                        {
                            case KEY_ALIAS:
                            case COLUMN_ALIAS:
                                throw new InvalidRequestException(String.format("PRIMARY KEY part %s found in SET part", entry.left));
                            case VALUE_ALIAS:
                            case COLUMN_METADATA:
                            case STATIC:
                                stmt.addCondition(condition);
                                break;
                        }
                    }
                }
            }

            stmt.boundTerms = boundNames.getCollectedCount() - collected;
            return stmt;
        }

        protected abstract ModificationStatement prepareInternal(CFDefinition cfDef, VariableSpecifications boundNames, Attributes attrs) throws InvalidRequestException;
    }
}
