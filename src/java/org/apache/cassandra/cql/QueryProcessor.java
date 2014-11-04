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
package org.apache.cassandra.cql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cli.CliUtils;
import org.apache.cassandra.cql.hooks.ExecutionContext;
import org.apache.cassandra.cql.hooks.PostPreparationHook;
import org.apache.cassandra.cql.hooks.PreExecutionHook;
import org.apache.cassandra.cql.hooks.PreparationContext;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.thrift.ThriftClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SemanticVersion;
import org.antlr.runtime.*;


import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

public class QueryProcessor
{
    public static final SemanticVersion CQL_VERSION = new SemanticVersion("2.0.0");

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

    public static final String DEFAULT_KEY_NAME = CFMetaData.DEFAULT_KEY_ALIAS.toUpperCase();

    private static final List<PreExecutionHook> preExecutionHooks = new CopyOnWriteArrayList<>();
    private static final List<PostPreparationHook> postPreparationHooks = new CopyOnWriteArrayList<>();

    public static void addPreExecutionHook(PreExecutionHook hook)
    {
        preExecutionHooks.add(hook);
    }

    public static void removePreExecutionHook(PreExecutionHook hook)
    {
        preExecutionHooks.remove(hook);
    }

    public static void addPostPreparationHook(PostPreparationHook hook)
    {
        postPreparationHooks.add(hook);
    }

    public static void removePostPreparationHook(PostPreparationHook hook)
    {
        postPreparationHooks.remove(hook);
    }

    private static List<org.apache.cassandra.db.Row> getSlice(CFMetaData metadata, SelectStatement select, List<ByteBuffer> variables, long now)
    throws InvalidRequestException, ReadTimeoutException, UnavailableException, IsBootstrappingException
    {
        List<ReadCommand> commands = new ArrayList<ReadCommand>();

        // ...of a list of column names
        if (!select.isColumnRange())
        {
            SortedSet<CellName> columnNames = getColumnNames(select, metadata, variables);
            validateColumnNames(columnNames);

            for (Term rawKey: select.getKeys())
            {
                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

                validateKey(key);
                commands.add(new SliceByNamesReadCommand(metadata.ksName, key, select.getColumnFamily(), now, new NamesQueryFilter(columnNames)));
            }
        }
        // ...a range (slice) of column names
        else
        {
            AbstractType<?> at = metadata.comparator.asAbstractType();
            Composite start = metadata.comparator.fromByteBuffer(select.getColumnStart().getByteBuffer(at,variables));
            Composite finish = metadata.comparator.fromByteBuffer(select.getColumnFinish().getByteBuffer(at,variables));

            for (Term rawKey : select.getKeys())
            {
                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator(),variables);

                validateKey(key);
                validateSliceFilter(metadata, start, finish, select.isColumnsReversed());
                commands.add(new SliceFromReadCommand(metadata.ksName,
                                                      key,
                                                      select.getColumnFamily(),
                                                      now,
                                                      new SliceQueryFilter(start, finish, select.isColumnsReversed(), select.getColumnsLimit())));
            }
        }

        return StorageProxy.read(commands, select.getConsistencyLevel());
    }

    private static SortedSet<CellName> getColumnNames(SelectStatement select, CFMetaData metadata, List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        String keyString = metadata.getCQL2KeyName();
        List<Term> selectColumnNames = select.getColumnNames();
        SortedSet<CellName> columnNames = new TreeSet<>(metadata.comparator);
        for (Term column : selectColumnNames)
        {
            // skip the key for the slice op; we'll add it to the resultset in extractThriftColumns
            if (!column.getText().equalsIgnoreCase(keyString))
                columnNames.add(metadata.comparator.cellFromByteBuffer(column.getByteBuffer(metadata.comparator.asAbstractType(),variables)));
        }
        return columnNames;
    }

    private static List<org.apache.cassandra.db.Row> multiRangeSlice(CFMetaData metadata, SelectStatement select, List<ByteBuffer> variables, long now)
    throws ReadTimeoutException, UnavailableException, InvalidRequestException
    {
        IPartitioner p = StorageService.getPartitioner();

        AbstractType<?> keyType = Schema.instance.getCFMetaData(metadata.ksName, select.getColumnFamily()).getKeyValidator();

        ByteBuffer startKeyBytes = (select.getKeyStart() != null)
                                   ? select.getKeyStart().getByteBuffer(keyType,variables)
                                   : null;

        ByteBuffer finishKeyBytes = (select.getKeyFinish() != null)
                                    ? select.getKeyFinish().getByteBuffer(keyType,variables)
                                    : null;

        RowPosition startKey = RowPosition.ForKey.get(startKeyBytes, p), finishKey = RowPosition.ForKey.get(finishKeyBytes, p);
        if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum(p))
        {
            if (p instanceof RandomPartitioner)
                throw new InvalidRequestException("Start key sorts after end key. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
            else
                throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
        }
        AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(startKey, finishKey);

        IDiskAtomFilter columnFilter = filterFromSelect(select, metadata, variables);
        validateFilter(metadata, columnFilter);

        List<Relation> columnRelations = select.getColumnRelations();
        List<IndexExpression> expressions = new ArrayList<IndexExpression>(columnRelations.size());
        for (Relation columnRelation : columnRelations)
        {
            // Left and right side of relational expression encoded according to comparator/validator.
            ByteBuffer entity = columnRelation.getEntity().getByteBuffer(metadata.comparator.asAbstractType(), variables);
            ByteBuffer value = columnRelation.getValue().getByteBuffer(metadata.getValueValidator(metadata.comparator.cellFromByteBuffer(entity)), variables);

            expressions.add(new IndexExpression(entity,
                                                Operator.valueOf(columnRelation.operator().name()),
                                                value));
        }

        int limit = select.isKeyRange() && select.getKeyStart() != null
                  ? select.getNumRecords() + 1
                  : select.getNumRecords();

        List<org.apache.cassandra.db.Row> rows = StorageProxy.getRangeSlice(new RangeSliceCommand(metadata.ksName,
                                                                                                  select.getColumnFamily(),
                                                                                                  now,
                                                                                                  columnFilter,
                                                                                                  bounds,
                                                                                                  expressions,
                                                                                                  limit),
                                                                            select.getConsistencyLevel());

        // if start key was set and relation was "greater than"
        if (select.getKeyStart() != null && !select.includeStartKey() && !rows.isEmpty())
        {
            if (rows.get(0).key.getKey().equals(startKeyBytes))
                rows.remove(0);
        }

        // if finish key was set and relation was "less than"
        if (select.getKeyFinish() != null && !select.includeFinishKey() && !rows.isEmpty())
        {
            int lastIndex = rows.size() - 1;
            if (rows.get(lastIndex).key.getKey().equals(finishKeyBytes))
                rows.remove(lastIndex);
        }

        return rows.subList(0, select.getNumRecords() < rows.size() ? select.getNumRecords() : rows.size());
    }

    private static IDiskAtomFilter filterFromSelect(SelectStatement select, CFMetaData metadata, List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        if (select.isColumnRange() || select.getColumnNames().size() == 0)
        {
            AbstractType<?> comparator = metadata.comparator.asAbstractType();
            return new SliceQueryFilter(metadata.comparator.fromByteBuffer(select.getColumnStart().getByteBuffer(comparator, variables)),
                                        metadata.comparator.fromByteBuffer(select.getColumnFinish().getByteBuffer(comparator, variables)),
                                        select.isColumnsReversed(),
                                        select.getColumnsLimit());
        }
        else
        {
            return new NamesQueryFilter(getColumnNames(select, metadata, variables));
        }
    }

    /* Test for SELECT-specific taboos */
    private static void validateSelect(String keyspace, SelectStatement select, List<ByteBuffer> variables) throws InvalidRequestException
    {
        select.getConsistencyLevel().validateForRead(keyspace);

        // Finish key w/o start key (KEY < foo)
        if (!select.isKeyRange() && (select.getKeyFinish() != null))
            throw new InvalidRequestException("Key range clauses must include a start key (i.e. KEY > term)");

        // Key range and by-key(s) combined (KEY > foo AND KEY = bar)
        if (select.isKeyRange() && select.getKeys().size() > 0)
            throw new InvalidRequestException("You cannot combine key range and by-key clauses in a SELECT");

        // Start and finish keys, *and* column relations (KEY > foo AND KEY < bar and name1 = value1).
        if (select.isKeyRange() && (select.getKeyFinish() != null) && (select.getColumnRelations().size() > 0))
            throw new InvalidRequestException("You cannot combine key range and by-column clauses in a SELECT");

        // Can't use more than one KEY =
        if (!select.isMultiKey() && select.getKeys().size() > 1)
            throw new InvalidRequestException("You cannot use more than one KEY = in a SELECT");

        if (select.getColumnRelations().size() > 0)
        {
            ColumnFamilyStore cfstore = Keyspace.open(keyspace).getColumnFamilyStore(select.getColumnFamily());
            CellNameType comparator = cfstore.metadata.comparator;
            AbstractType<?> at = comparator.asAbstractType();
            SecondaryIndexManager idxManager = cfstore.indexManager;
            for (Relation relation : select.getColumnRelations())
            {
                ByteBuffer name = relation.getEntity().getByteBuffer(at, variables);
                if ((relation.operator() == RelationType.EQ) && idxManager.indexes(comparator.cellFromByteBuffer(name)))
                    return;
            }
            throw new InvalidRequestException("No indexed columns present in by-columns clause with \"equals\" operator");
        }
    }

    public static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new InvalidRequestException("Key may not be empty");
        }

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
        }
    }

    public static void validateKeyAlias(CFMetaData cfm, String key) throws InvalidRequestException
    {
        assert key.toUpperCase().equals(key); // should always be uppercased by caller
        String realKeyAlias = cfm.getCQL2KeyName().toUpperCase();
        if (!realKeyAlias.equals(key))
            throw new InvalidRequestException(String.format("Expected key '%s' to be present in WHERE clause for '%s'", realKeyAlias, cfm.cfName));
    }

    private static void validateColumnNames(Iterable<CellName> columns)
    throws InvalidRequestException
    {
        for (CellName name : columns)
        {
            if (name.dataSize() > org.apache.cassandra.db.Cell.MAX_NAME_LENGTH)
                throw new InvalidRequestException(String.format("column name is too long (%s > %s)",
                                                                name.dataSize(),
                                                                org.apache.cassandra.db.Cell.MAX_NAME_LENGTH));
            if (name.isEmpty())
                throw new InvalidRequestException("zero-length column name");
        }
    }

    public static void validateColumnName(CellName column)
    throws InvalidRequestException
    {
        validateColumnNames(Arrays.asList(column));
    }

    public static void validateColumn(CFMetaData metadata, CellName name, ByteBuffer value)
    throws InvalidRequestException
    {
        validateColumnName(name);
        AbstractType<?> validator = metadata.getValueValidator(name);

        try
        {
            if (validator != null)
                validator.validate(value);
        }
        catch (MarshalException me)
        {
            throw new InvalidRequestException(String.format("Invalid column value for column (name=%s); %s",
                                                            ByteBufferUtil.bytesToHex(name.toByteBuffer()),
                                                            me.getMessage()));
        }
    }

    private static void validateFilter(CFMetaData metadata, IDiskAtomFilter filter)
    throws InvalidRequestException
    {
        if (filter instanceof SliceQueryFilter)
            validateSliceFilter(metadata, (SliceQueryFilter)filter);
        else
            validateColumnNames(((NamesQueryFilter)filter).columns);
    }

    private static void validateSliceFilter(CFMetaData metadata, SliceQueryFilter range)
    throws InvalidRequestException
    {
        validateSliceFilter(metadata, range.start(), range.finish(), range.reversed);
    }

    private static void validateSliceFilter(CFMetaData metadata, Composite start, Composite finish, boolean reversed)
    throws InvalidRequestException
    {
        CellNameType comparator = metadata.comparator;
        Comparator<Composite> orderedComparator = reversed ? comparator.reverseComparator(): comparator;
        if (!start.isEmpty() && !finish.isEmpty() && orderedComparator.compare(start, finish) > 0)
            throw new InvalidRequestException("range finish must come after start in traversal order");
    }

    public static CqlResult processStatement(CQLStatement statement, ExecutionContext context)
    throws RequestExecutionException, RequestValidationException
    {
        String keyspace = null;
        ThriftClientState clientState = context.clientState;
        List<ByteBuffer> variables = context.variables;

        // Some statements won't have (or don't need) a keyspace (think USE, or CREATE).
        if (statement.type != StatementType.SELECT && StatementType.REQUIRES_KEYSPACE.contains(statement.type))
            keyspace = clientState.getKeyspace();

        CqlResult result = new CqlResult();

        if (!preExecutionHooks.isEmpty())
            for (PreExecutionHook hook : preExecutionHooks)
                statement = hook.processStatement(statement, context);

        if (logger.isDebugEnabled()) logger.debug("CQL statement type: {}", statement.type.toString());
        CFMetaData metadata;
        switch (statement.type)
        {
            case SELECT:
                SelectStatement select = (SelectStatement)statement.statement;

                final String oldKeyspace = clientState.getRawKeyspace();

                if (select.isSetKeyspace())
                {
                    keyspace = CliUtils.unescapeSQLString(select.getKeyspace());
                    ThriftValidation.validateKeyspace(keyspace);
                }
                else if (oldKeyspace == null)
                    throw new InvalidRequestException("no keyspace has been specified");
                else
                    keyspace = oldKeyspace;

                clientState.hasColumnFamilyAccess(keyspace, select.getColumnFamily(), Permission.SELECT);
                metadata = validateColumnFamily(keyspace, select.getColumnFamily());

                // need to do this in here because we need a CFMD.getKeyName()
                select.extractKeyAliasFromColumns(metadata);

                if (select.getKeys().size() > 0)
                    validateKeyAlias(metadata, select.getKeyAlias());

                validateSelect(keyspace, select, variables);

                List<org.apache.cassandra.db.Row> rows;

                long now = System.currentTimeMillis();
                // By-key
                if (!select.isKeyRange() && (select.getKeys().size() > 0))
                {
                    rows = getSlice(metadata, select, variables, now);
                }
                else
                {
                    rows = multiRangeSlice(metadata, select, variables, now);
                }

                // count resultset is a single column named "count"
                result.type = CqlResultType.ROWS;
                if (select.isCountOperation())
                {
                    validateCountOperation(select);

                    ByteBuffer countBytes = ByteBufferUtil.bytes("count");
                    result.schema = new CqlMetadata(Collections.<ByteBuffer, String>emptyMap(),
                                                    Collections.<ByteBuffer, String>emptyMap(),
                                                    "AsciiType",
                                                    "LongType");
                    List<Column> columns = Collections.singletonList(new Column(countBytes).setValue(ByteBufferUtil.bytes((long) rows.size())));
                    result.rows = Collections.singletonList(new CqlRow(countBytes, columns));
                    return result;
                }

                // otherwise create resultset from query results
                result.schema = new CqlMetadata(new HashMap<ByteBuffer, String>(),
                                                new HashMap<ByteBuffer, String>(),
                                                TypeParser.getShortName(metadata.comparator.asAbstractType()),
                                                TypeParser.getShortName(metadata.getDefaultValidator()));
                List<CqlRow> cqlRows = new ArrayList<CqlRow>(rows.size());
                for (org.apache.cassandra.db.Row row : rows)
                {
                    List<Column> thriftColumns = new ArrayList<Column>();
                    if (select.isColumnRange())
                    {
                        if (select.isFullWildcard())
                        {
                            // prepend key
                            ByteBuffer keyName = ByteBufferUtil.bytes(metadata.getCQL2KeyName());
                            thriftColumns.add(new Column(keyName).setValue(row.key.getKey()).setTimestamp(-1));
                            result.schema.name_types.put(keyName, TypeParser.getShortName(AsciiType.instance));
                            result.schema.value_types.put(keyName, TypeParser.getShortName(metadata.getKeyValidator()));
                        }

                        // preserve comparator order
                        if (row.cf != null)
                        {
                            for (org.apache.cassandra.db.Cell c : row.cf.getSortedColumns())
                            {
                                if (!c.isLive(now))
                                    continue;

                                ColumnDefinition cd = metadata.getColumnDefinition(c.name());
                                if (cd != null)
                                    result.schema.value_types.put(c.name().toByteBuffer(), TypeParser.getShortName(cd.type));

                                thriftColumns.add(thriftify(c));
                            }
                        }
                    }
                    else
                    {
                        String keyString = metadata.getCQL2KeyName();

                        // order columns in the order they were asked for
                        for (Term term : select.getColumnNames())
                        {
                            if (term.getText().equalsIgnoreCase(keyString))
                            {
                                // preserve case of key as it was requested
                                ByteBuffer requestedKey = ByteBufferUtil.bytes(term.getText());
                                thriftColumns.add(new Column(requestedKey).setValue(row.key.getKey()).setTimestamp(-1));
                                result.schema.name_types.put(requestedKey, TypeParser.getShortName(AsciiType.instance));
                                result.schema.value_types.put(requestedKey, TypeParser.getShortName(metadata.getKeyValidator()));
                                continue;
                            }

                            if (row.cf == null)
                                continue;

                            ByteBuffer nameBytes;
                            try
                            {
                                nameBytes = term.getByteBuffer(metadata.comparator.asAbstractType(), variables);
                            }
                            catch (InvalidRequestException e)
                            {
                                throw new AssertionError(e);
                            }

                            CellName name = metadata.comparator.cellFromByteBuffer(nameBytes);
                            ColumnDefinition cd = metadata.getColumnDefinition(name);
                            if (cd != null)
                                result.schema.value_types.put(nameBytes, TypeParser.getShortName(cd.type));
                            org.apache.cassandra.db.Cell c = row.cf.getColumn(name);
                            if (c == null || !c.isLive())
                                thriftColumns.add(new Column().setName(nameBytes));
                            else
                                thriftColumns.add(thriftify(c));
                        }
                    }

                    // Create a new row, add the columns to it, and then add it to the list of rows
                    CqlRow cqlRow = new CqlRow();
                    cqlRow.key = row.key.getKey();
                    cqlRow.columns = thriftColumns;
                    if (select.isColumnsReversed())
                        Collections.reverse(cqlRow.columns);
                    cqlRows.add(cqlRow);
                }

                result.rows = cqlRows;
                return result;

            case INSERT: // insert uses UpdateStatement
            case UPDATE:
                UpdateStatement update = (UpdateStatement)statement.statement;
                update.getConsistencyLevel().validateForWrite(keyspace);

                keyspace = update.keyspace == null ? clientState.getKeyspace() : update.keyspace;
                // permission is checked in prepareRowMutations()
                List<IMutation> rowMutations = update.prepareRowMutations(keyspace, clientState, variables);

                for (IMutation mutation : rowMutations)
                {
                    validateKey(mutation.key());
                }

                StorageProxy.mutateWithTriggers(rowMutations, update.getConsistencyLevel(), false);

                result.type = CqlResultType.VOID;
                return result;

            case BATCH:
                BatchStatement batch = (BatchStatement) statement.statement;
                batch.getConsistencyLevel().validateForWrite(keyspace);

                if (batch.getTimeToLive() != 0)
                    throw new InvalidRequestException("Global TTL on the BATCH statement is not supported.");

                for (AbstractModification up : batch.getStatements())
                {
                    if (up.isSetConsistencyLevel())
                        throw new InvalidRequestException(
                                "Consistency level must be set on the BATCH, not individual statements");

                    if (batch.isSetTimestamp() && up.isSetTimestamp())
                        throw new InvalidRequestException(
                                "Timestamp must be set either on BATCH or individual statements");
                }

                List<IMutation> mutations = batch.getMutations(keyspace, clientState, variables);
                for (IMutation mutation : mutations)
                {
                    validateKey(mutation.key());
                }

                StorageProxy.mutateWithTriggers(mutations, batch.getConsistencyLevel(), false);

                result.type = CqlResultType.VOID;
                return result;

            case USE:
                clientState.validateLogin();
                clientState.setKeyspace(CliUtils.unescapeSQLString((String) statement.statement));

                result.type = CqlResultType.VOID;
                return result;

            case TRUNCATE:
                Pair<String, String> columnFamily = (Pair<String, String>)statement.statement;
                keyspace = columnFamily.left == null ? clientState.getKeyspace() : columnFamily.left;

                validateColumnFamily(keyspace, columnFamily.right);
                clientState.hasColumnFamilyAccess(keyspace, columnFamily.right, Permission.MODIFY);

                try
                {
                    StorageProxy.truncateBlocking(keyspace, columnFamily.right);
                }
                catch (TimeoutException e)
                {
                    throw new TruncateException(e);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }

                result.type = CqlResultType.VOID;
                return result;

            case DELETE:
                DeleteStatement delete = (DeleteStatement)statement.statement;

                keyspace = delete.keyspace == null ? clientState.getKeyspace() : delete.keyspace;
                // permission is checked in prepareRowMutations()
                List<IMutation> deletions = delete.prepareRowMutations(keyspace, clientState, variables);
                for (IMutation deletion : deletions)
                {
                    validateKey(deletion.key());
                }

                StorageProxy.mutateWithTriggers(deletions, delete.getConsistencyLevel(), false);

                result.type = CqlResultType.VOID;
                return result;

            case CREATE_KEYSPACE:
                CreateKeyspaceStatement create = (CreateKeyspaceStatement)statement.statement;
                create.validate();
                ThriftValidation.validateKeyspaceNotSystem(create.getName());
                clientState.hasAllKeyspacesAccess(Permission.CREATE);

                try
                {
                    KSMetaData ksm = KSMetaData.newKeyspace(create.getName(),
                                                            create.getStrategyClass(),
                                                            create.getStrategyOptions(),
                                                            true);
                    ThriftValidation.validateKeyspaceNotYetExisting(ksm.name);
                    MigrationManager.announceNewKeyspace(ksm);
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case CREATE_COLUMNFAMILY:
                CreateColumnFamilyStatement createCf = (CreateColumnFamilyStatement)statement.statement;
                clientState.hasKeyspaceAccess(keyspace, Permission.CREATE);

                try
                {
                    MigrationManager.announceNewColumnFamily(createCf.getCFMetaData(keyspace, variables));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case CREATE_INDEX:
                CreateIndexStatement createIdx = (CreateIndexStatement)statement.statement;
                clientState.hasColumnFamilyAccess(keyspace, createIdx.getColumnFamily(), Permission.ALTER);
                CFMetaData oldCfm = Schema.instance.getCFMetaData(keyspace, createIdx.getColumnFamily());
                if (oldCfm == null)
                    throw new InvalidRequestException("No such column family: " + createIdx.getColumnFamily());

                boolean columnExists = false;
                ByteBuffer columnName = createIdx.getColumnName().getByteBuffer();
                // mutating oldCfm directly would be bad, but mutating a copy is fine.
                CFMetaData cfm = oldCfm.copy();
                for (ColumnDefinition cd : cfm.regularColumns())
                {
                    if (cd.name.bytes.equals(columnName))
                    {
                        if (cd.getIndexType() != null)
                            throw new InvalidRequestException("Index already exists");
                        if (logger.isDebugEnabled())
                            logger.debug("Updating column {} definition for index {}", cfm.comparator.getString(cfm.comparator.fromByteBuffer(columnName)), createIdx.getIndexName());
                        cd.setIndexType(IndexType.KEYS, Collections.<String, String>emptyMap());
                        cd.setIndexName(createIdx.getIndexName());
                        columnExists = true;
                        break;
                    }
                }
                if (!columnExists)
                    throw new InvalidRequestException("No column definition found for column " + oldCfm.comparator.getString(cfm.comparator.fromByteBuffer(columnName)));

                try
                {
                    cfm.addDefaultIndexNames();
                    MigrationManager.announceColumnFamilyUpdate(cfm, true); // As far as metadata are concerned, CQL2 == thrift
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case DROP_INDEX:
                DropIndexStatement dropIdx = (DropIndexStatement)statement.statement;
                keyspace = clientState.getKeyspace();
                dropIdx.setKeyspace(keyspace);
                clientState.hasColumnFamilyAccess(keyspace, dropIdx.getColumnFamily(), Permission.ALTER);

                try
                {
                    CFMetaData updatedCF = dropIdx.generateCFMetadataUpdate();
                    MigrationManager.announceColumnFamilyUpdate(updatedCF, true); // As far as metadata are concerned, CQL2 == thrift
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case DROP_KEYSPACE:
                String deleteKeyspace = (String)statement.statement;
                ThriftValidation.validateKeyspaceNotSystem(deleteKeyspace);
                clientState.hasKeyspaceAccess(deleteKeyspace, Permission.DROP);

                try
                {
                    MigrationManager.announceKeyspaceDrop(deleteKeyspace);
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case DROP_COLUMNFAMILY:
                String deleteColumnFamily = (String)statement.statement;
                clientState.hasColumnFamilyAccess(keyspace, deleteColumnFamily, Permission.DROP);

                try
                {
                    MigrationManager.announceColumnFamilyDrop(keyspace, deleteColumnFamily);
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case ALTER_TABLE:
                AlterTableStatement alterTable = (AlterTableStatement) statement.statement;

                validateColumnFamily(keyspace, alterTable.columnFamily);
                clientState.hasColumnFamilyAccess(keyspace, alterTable.columnFamily, Permission.ALTER);

                try
                {
                    MigrationManager.announceColumnFamilyUpdate(alterTable.getCFMetaData(keyspace), true); // As far as metadata are concerned, CQL2 == thrift
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;
        }
        return null;    // We should never get here.
    }

    public static CqlResult process(String queryString, ThriftClientState clientState)
    throws RequestValidationException, RequestExecutionException
    {
        logger.trace("CQL QUERY: {}", queryString);
        return processStatement(getStatement(queryString),
                                new ExecutionContext(clientState, queryString, Collections.<ByteBuffer>emptyList()));
    }

    public static CqlPreparedResult prepare(String queryString, ThriftClientState clientState)
    throws RequestValidationException
    {
        logger.trace("CQL QUERY: {}", queryString);

        CQLStatement statement = getStatement(queryString);
        int statementId = makeStatementId(queryString);
        logger.trace("Discovered "+ statement.boundTerms + " bound variables.");

        clientState.getPrepared().put(statementId, statement);
        logger.trace(String.format("Stored prepared statement #%d with %d bind markers",
                                   statementId,
                                   statement.boundTerms));

        if (!postPreparationHooks.isEmpty())
        {
            PreparationContext context = new PreparationContext(clientState, queryString, statement);
            for (PostPreparationHook hook : postPreparationHooks)
                hook.processStatement(statement, context);
        }

        return new CqlPreparedResult(statementId, statement.boundTerms);
    }

    public static CqlResult processPrepared(CQLStatement statement, ThriftClientState clientState, List<ByteBuffer> variables)
    throws RequestValidationException, RequestExecutionException
    {
        // Check to see if there are any bound variables to verify
        if (!(variables.isEmpty() && (statement.boundTerms == 0)))
        {
            if (variables.size() != statement.boundTerms)
                throw new InvalidRequestException(String.format("there were %d markers(?) in CQL but %d bound variables",
                                                                statement.boundTerms,
                                                                variables.size()));

            // at this point there is a match in count between markers and variables that is non-zero

            if (logger.isTraceEnabled())
                for (int i = 0; i < variables.size(); i++)
                    logger.trace("[{}] '{}'", i+1, variables.get(i));
        }

        return processStatement(statement, new ExecutionContext(clientState, null, variables));
    }

    private static final int makeStatementId(String cql)
    {
        // use the hash of the string till something better is provided
        return cql.hashCode();
    }

    private static Column thriftify(org.apache.cassandra.db.Cell c)
    {
        ByteBuffer value = (c instanceof CounterCell)
                           ? ByteBufferUtil.bytes(CounterContext.instance().total(c.value()))
                           : c.value();
        return new Column(c.name().toByteBuffer()).setValue(value).setTimestamp(c.timestamp());
    }

    private static CQLStatement getStatement(String queryStr) throws SyntaxException
    {
        try
        {
            // Lexer and parser
            CharStream stream = new ANTLRStringStream(queryStr);
            CqlLexer lexer = new CqlLexer(stream);
            TokenStream tokenStream = new CommonTokenStream(lexer);
            CqlParser parser = new CqlParser(tokenStream);

            // Parse the query string to a statement instance
            CQLStatement statement = parser.query();

            // The lexer and parser queue up any errors they may have encountered
            // along the way, if necessary, we turn them into exceptions here.
            lexer.throwLastRecognitionError();
            parser.throwLastRecognitionError();

            return statement;
        }
        catch (RuntimeException re)
        {
            SyntaxException ire = new SyntaxException("Failed parsing statement: [" + queryStr + "] reason: " + re.getClass().getSimpleName() + " " + re.getMessage());
            throw ire;
        }
        catch (RecognitionException e)
        {
            SyntaxException ire = new SyntaxException("Invalid or malformed CQL query string: " + e.getMessage());
            throw ire;
        }
    }

    private static void validateCountOperation(SelectStatement select) throws InvalidRequestException
    {
        if (select.isWildcard())
            return; // valid count(*)

        if (!select.isColumnRange())
        {
            List<Term> columnNames = select.getColumnNames();
            String firstColumn = columnNames.get(0).getText();

            if (columnNames.size() == 1 && (firstColumn.equals("*") || firstColumn.equals("1")))
                return; // valid count(*) || count(1)
        }

        throw new InvalidRequestException("Only COUNT(*) and COUNT(1) operations are currently supported.");
    }
}
