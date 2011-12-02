/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

package org.apache.cassandra.cql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cli.CliUtils;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.migration.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import org.antlr.runtime.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

public class QueryProcessor
{
    public static final String CQL_VERSION = "2.0.0";

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

    private static final long timeLimitForSchemaAgreement = 10 * 1000;

    public static final String DEFAULT_KEY_NAME = bufferToString(CFMetaData.DEFAULT_KEY_NAME);

    private static List<org.apache.cassandra.db.Row> getSlice(CFMetaData metadata, SelectStatement select)
    throws InvalidRequestException, TimedOutException, UnavailableException
    {
        QueryPath queryPath = new QueryPath(select.getColumnFamily());
        List<ReadCommand> commands = new ArrayList<ReadCommand>();

        // ...of a list of column names
        if (!select.isColumnRange())
        {
            Collection<ByteBuffer> columnNames = getColumnNames(select, metadata);
            validateColumnNames(columnNames);

            for (Term rawKey: select.getKeys())
            {
                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator());

                validateKey(key);
                commands.add(new SliceByNamesReadCommand(metadata.ksName, key, queryPath, columnNames));
            }
        }
        // ...a range (slice) of column names
        else
        {
            AbstractType<?> comparator = select.getComparator(metadata.ksName);
            ByteBuffer start = select.getColumnStart().getByteBuffer(comparator);
            ByteBuffer finish = select.getColumnFinish().getByteBuffer(comparator);

            for (Term rawKey : select.getKeys())
            {
                ByteBuffer key = rawKey.getByteBuffer(metadata.getKeyValidator());

                validateKey(key);
                validateSliceRange(metadata, start, finish, select.isColumnsReversed());
                commands.add(new SliceFromReadCommand(metadata.ksName,
                                                      key,
                                                      queryPath,
                                                      start,
                                                      finish,
                                                      select.isColumnsReversed(),
                                                      select.getColumnsLimit()));
            }
        }

        try
        {
            return StorageProxy.read(commands, select.getConsistencyLevel());
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

    private static List<ByteBuffer> getColumnNames(SelectStatement select, CFMetaData metadata) throws InvalidRequestException
    {
        String keyString = getKeyString(metadata);
        List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
        for (Term column : select.getColumnNames())
        {
            // skip the key for the slice op; we'll add it to the resultset in extractThriftColumns
            if (!column.getText().equalsIgnoreCase(keyString))
                columnNames.add(column.getByteBuffer(metadata.comparator));
        }
        return columnNames;
    }

    private static List<org.apache.cassandra.db.Row> multiRangeSlice(CFMetaData metadata, SelectStatement select)
    throws TimedOutException, UnavailableException, InvalidRequestException
    {
        List<org.apache.cassandra.db.Row> rows;
        IPartitioner<?> p = StorageService.getPartitioner();

        AbstractType<?> keyType = Schema.instance.getCFMetaData(metadata.ksName, select.getColumnFamily()).getKeyValidator();

        ByteBuffer startKey = (select.getKeyStart() != null)
                               ? select.getKeyStart().getByteBuffer(keyType)
                               : (new Term()).getByteBuffer();

        ByteBuffer finishKey = (select.getKeyFinish() != null)
                                ? select.getKeyFinish().getByteBuffer(keyType)
                                : (new Term()).getByteBuffer();

        Token startToken = p.getToken(startKey), finishToken = p.getToken(finishKey);
        if (startToken.compareTo(finishToken) > 0 && !finishToken.equals(p.getMinimumToken()))
        {
            if (p instanceof RandomPartitioner)
                throw new InvalidRequestException("Start key's md5 sorts after end key's md5. This is not allowed; you probably should not specify end key at all, under RandomPartitioner");
            else
                throw new InvalidRequestException("Start key must sort before (or equal to) finish key in your partitioner!");
        }
        AbstractBounds bounds = new Bounds(startToken, finishToken);
        
        // XXX: Our use of Thrift structs internally makes me Sad. :(
        SlicePredicate thriftSlicePredicate = slicePredicateFromSelect(select, metadata);
        validateSlicePredicate(metadata, thriftSlicePredicate);

        int limit = select.isKeyRange() && select.getKeyStart() != null
                  ? select.getNumRecords() + 1
                  : select.getNumRecords();

        try
        {
            rows = StorageProxy.getRangeSlice(new RangeSliceCommand(metadata.ksName,
                                                                    select.getColumnFamily(),
                                                                    null,
                                                                    thriftSlicePredicate,
                                                                    bounds,
                                                                    limit),
                                                                    select.getConsistencyLevel());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (org.apache.cassandra.thrift.UnavailableException e)
        {
            throw new UnavailableException();
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }

        // if start key was set and relation was "greater than"
        if (select.getKeyStart() != null && !select.includeStartKey() && !rows.isEmpty())
        {
            if (rows.get(0).key.key.equals(startKey))
                rows.remove(0);
        }

        // if finish key was set and relation was "less than"
        if (select.getKeyFinish() != null && !select.includeFinishKey() && !rows.isEmpty())
        {
            int lastIndex = rows.size() - 1;
            if (rows.get(lastIndex).key.key.equals(finishKey))
                rows.remove(lastIndex);
        }

        return rows.subList(0, select.getNumRecords() < rows.size() ? select.getNumRecords() : rows.size());
    }
    
    private static List<org.apache.cassandra.db.Row> getIndexedSlices(CFMetaData metadata, SelectStatement select)
    throws TimedOutException, UnavailableException, InvalidRequestException
    {
        // XXX: Our use of Thrift structs internally (still) makes me Sad. :~(
        SlicePredicate thriftSlicePredicate = slicePredicateFromSelect(select, metadata);
        validateSlicePredicate(metadata, thriftSlicePredicate);
        
        List<IndexExpression> expressions = new ArrayList<IndexExpression>();
        for (Relation columnRelation : select.getColumnRelations())
        {
            // Left and right side of relational expression encoded according to comparator/validator.
            ByteBuffer entity = columnRelation.getEntity().getByteBuffer(metadata.comparator);
            ByteBuffer value = columnRelation.getValue().getByteBuffer(select.getValueValidator(metadata.ksName, entity));
            
            expressions.add(new IndexExpression(entity,
                                                IndexOperator.valueOf(columnRelation.operator().toString()),
                                                value));
        }

        AbstractType<?> keyType = Schema.instance.getCFMetaData(metadata.ksName, select.getColumnFamily()).getKeyValidator();
        ByteBuffer startKey = (!select.isKeyRange()) ? (new Term()).getByteBuffer() : select.getKeyStart().getByteBuffer(keyType);
        IndexClause thriftIndexClause = new IndexClause(expressions, startKey, select.getNumRecords());
        
        List<org.apache.cassandra.db.Row> rows;
        try
        {
            rows = StorageProxy.scan(metadata.ksName,
                                     select.getColumnFamily(),
                                     thriftIndexClause,
                                     thriftSlicePredicate,
                                     select.getConsistencyLevel());
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
    
    private static void batchUpdate(ClientState clientState, List<UpdateStatement> updateStatements, ConsistencyLevel consistency)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        String globalKeyspace = clientState.getKeyspace();
        List<IMutation> rowMutations = new ArrayList<IMutation>();
        List<String> cfamsSeen = new ArrayList<String>();

        for (UpdateStatement update : updateStatements)
        {
            String keyspace = update.keyspace == null ? globalKeyspace : update.keyspace;

            // Avoid unnecessary authorizations.
            if (!(cfamsSeen.contains(update.getColumnFamily())))
            {
                clientState.hasColumnFamilyAccess(keyspace, update.getColumnFamily(), Permission.WRITE);
                cfamsSeen.add(update.getColumnFamily());
            }

            rowMutations.addAll(update.prepareRowMutations(keyspace, clientState));
        }
        
        try
        {
            StorageProxy.mutate(rowMutations, consistency);
        }
        catch (org.apache.cassandra.thrift.UnavailableException e)
        {
            throw new UnavailableException();
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
    }
    
    private static SlicePredicate slicePredicateFromSelect(SelectStatement select, CFMetaData metadata)
    throws InvalidRequestException
    {
        SlicePredicate thriftSlicePredicate = new SlicePredicate();
        
        if (select.isColumnRange() || select.getColumnNames().size() == 0)
        {
            SliceRange sliceRange = new SliceRange();
            sliceRange.start = select.getColumnStart().getByteBuffer(metadata.comparator);
            sliceRange.finish = select.getColumnFinish().getByteBuffer(metadata.comparator);
            sliceRange.reversed = select.isColumnsReversed();
            sliceRange.count = select.getColumnsLimit();
            thriftSlicePredicate.slice_range = sliceRange;
        }
        else
        {
            thriftSlicePredicate.column_names = getColumnNames(select, metadata);
        }
        
        return thriftSlicePredicate;
    }
    
    /* Test for SELECT-specific taboos */
    private static void validateSelect(String keyspace, SelectStatement select) throws InvalidRequestException
    {
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
            AbstractType<?> comparator = select.getComparator(keyspace);
            Set<ByteBuffer> indexed = Table.open(keyspace).getColumnFamilyStore(select.getColumnFamily()).indexManager.getIndexedColumns();
            for (Relation relation : select.getColumnRelations())
            {
                if ((relation.operator() == RelationType.EQ) && indexed.contains(relation.getEntity().getByteBuffer(comparator)))
                    return;
            }
            throw new InvalidRequestException("No indexed columns present in by-columns clause with \"equals\" operator");
        }
    }

    // Copypasta from o.a.c.thrift.CassandraDaemon
    private static void applyMigrationOnStage(final Migration m) throws SchemaDisagreementException, InvalidRequestException
    {
        Future<?> f = StageManager.getStage(Stage.MIGRATION).submit(new Callable<Object>()
        {
            public Object call() throws Exception
            {
                m.apply();
                m.announce();
                return null;
            }
        });
        try
        {
            f.get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            // this means call() threw an exception. deal with it directly.
            if (e.getCause() != null)
            {
                InvalidRequestException ex = new InvalidRequestException(e.getCause().getMessage());
                ex.initCause(e.getCause());
                throw ex;
            }
            else
            {
                InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                ex.initCause(e);
                throw ex;
            }
        }

        validateSchemaIsSettled();
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
        String realKeyAlias = bufferToString(cfm.getKeyName()).toUpperCase();
        if (!realKeyAlias.equals(key))
            throw new InvalidRequestException(String.format("Expected key '%s' to be present in WHERE clause for '%s'", key, cfm.cfName));
    }

    private static void validateColumnNames(Iterable<ByteBuffer> columns)
    throws InvalidRequestException
    {
        for (ByteBuffer name : columns)
        {
            if (name.remaining() > IColumn.MAX_NAME_LENGTH)
                throw new InvalidRequestException(String.format("column name is too long (%s > %s)",
                                                                name.remaining(),
                                                                IColumn.MAX_NAME_LENGTH));
            if (name.remaining() == 0)
                throw new InvalidRequestException("zero-length column name");
        }
    }

    public static void validateColumnName(ByteBuffer column)
    throws InvalidRequestException
    {
        validateColumnNames(Arrays.asList(column));
    }
    
    public static void validateColumn(CFMetaData metadata, ByteBuffer name, ByteBuffer value)
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
                                                            ByteBufferUtil.bytesToHex(name),
                                                            me.getMessage()));
        }
    }
    
    private static void validateSlicePredicate(CFMetaData metadata, SlicePredicate predicate)
    throws InvalidRequestException
    {
        if (predicate.slice_range != null)
            validateSliceRange(metadata, predicate.slice_range);
        else
            validateColumnNames(predicate.column_names);
    }
    
    private static void validateSliceRange(CFMetaData metadata, SliceRange range)
    throws InvalidRequestException
    {
        validateSliceRange(metadata, range.start, range.finish, range.reversed);
    }
    
    private static void validateSliceRange(CFMetaData metadata, ByteBuffer start, ByteBuffer finish, boolean reversed)
    throws InvalidRequestException
    {
        AbstractType<?> comparator = metadata.getComparatorFor(null);
        Comparator<ByteBuffer> orderedComparator = reversed ? comparator.reverseComparator: comparator;
        if (start.remaining() > 0 && finish.remaining() > 0 && orderedComparator.compare(start, finish) > 0)
            throw new InvalidRequestException("range finish must come after start in traversal order");
    }
    
    // Copypasta from CassandraServer (where it is private).
    private static void validateSchemaAgreement() throws SchemaDisagreementException
    {
       if (describeSchemaVersions().size() > 1)
            throw new SchemaDisagreementException();
    }

    private static Map<String, List<String>> describeSchemaVersions()
    {
        // unreachable hosts don't count towards disagreement
        return Maps.filterKeys(StorageProxy.describeSchemaVersions(),
                               Predicates.not(Predicates.equalTo(StorageProxy.UNREACHABLE)));
    }

    public static CqlResult process(String queryString, ClientState clientState)
    throws RecognitionException, UnavailableException, InvalidRequestException, TimedOutException, SchemaDisagreementException
    {
        logger.trace("CQL QUERY: {}", queryString);
        
        CQLStatement statement = getStatement(queryString);
        String keyspace = null;
        
        // Some statements won't have (or don't need) a keyspace (think USE, or CREATE).
        if (statement.type != StatementType.SELECT && StatementType.requiresKeyspace.contains(statement.type))
            keyspace = clientState.getKeyspace();

        CqlResult result = new CqlResult();
        
        logger.debug("CQL statement type: {}", statement.type.toString());
        CFMetaData metadata;
        switch (statement.type)
        {
            case SELECT:
                SelectStatement select = (SelectStatement)statement.statement;

                final String oldKeyspace = clientState.getRawKeyspace();

                if (select.isSetKeyspace())
                {
                    keyspace = CliUtils.unescapeSQLString(select.getKeyspace());
                    ThriftValidation.validateTable(keyspace);
                }
                else if (oldKeyspace == null)
                    throw new InvalidRequestException("no keyspace has been specified");
                else
                    keyspace = oldKeyspace;

                clientState.hasColumnFamilyAccess(keyspace, select.getColumnFamily(), Permission.READ);
                metadata = validateColumnFamily(keyspace, select.getColumnFamily());

                // need to do this in here because we need a CFMD.getKeyName()
                select.extractKeyAliasFromColumns(metadata);

                if (select.getKeys().size() > 0)
                    validateKeyAlias(metadata, select.getKeyAlias());

                validateSelect(keyspace, select);

                List<org.apache.cassandra.db.Row> rows;

                // By-key
                if (!select.isKeyRange() && (select.getKeys().size() > 0))
                {
                    rows = getSlice(metadata, select);
                }
                else
                {
                    // Range query
                    if ((select.getKeyFinish() != null) || (select.getColumnRelations().size() == 0))
                    {
                        rows = multiRangeSlice(metadata, select);
                    }
                    // Index scan
                    else
                    {
                        rows = getIndexedSlices(metadata, select);
                    }
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
                                                TypeParser.getShortName(metadata.comparator),
                                                TypeParser.getShortName(metadata.getDefaultValidator()));
                List<CqlRow> cqlRows = new ArrayList<CqlRow>();
                for (org.apache.cassandra.db.Row row : rows)
                {
                    List<Column> thriftColumns = new ArrayList<Column>();
                    if (select.isColumnRange())
                    {
                        if (select.isWildcard())
                        {
                            // prepend key
                            thriftColumns.add(new Column(metadata.getKeyName()).setValue(row.key.key).setTimestamp(-1));
                            result.schema.name_types.put(metadata.getKeyName(), TypeParser.getShortName(AsciiType.instance));
                            result.schema.value_types.put(metadata.getKeyName(), TypeParser.getShortName(metadata.getKeyValidator()));
                        }

                        // preserve comparator order
                        if (row.cf != null)
                        {
                            for (IColumn c : row.cf.getSortedColumns())
                            {
                                if (c.isMarkedForDelete())
                                    continue;

                                ColumnDefinition cd = metadata.getColumnDefinition(c.name());
                                if (cd != null)
                                    result.schema.value_types.put(c.name(), TypeParser.getShortName(cd.getValidator()));

                                thriftColumns.add(thriftify(c));
                            }
                        }
                    }
                    else
                    {
                        String keyString = getKeyString(metadata);

                        // order columns in the order they were asked for
                        for (Term term : select.getColumnNames())
                        {
                            if (term.getText().equalsIgnoreCase(keyString))
                            {
                                // preserve case of key as it was requested
                                ByteBuffer requestedKey = ByteBufferUtil.bytes(term.getText());
                                thriftColumns.add(new Column(requestedKey).setValue(row.key.key).setTimestamp(-1));
                                result.schema.name_types.put(requestedKey, TypeParser.getShortName(AsciiType.instance));
                                result.schema.value_types.put(requestedKey, TypeParser.getShortName(metadata.getKeyValidator()));
                                continue;
                            }

                            if (row.cf == null)
                                continue;

                            ByteBuffer name;
                            try
                            {
                                name = term.getByteBuffer(metadata.comparator);
                            }
                            catch (InvalidRequestException e)
                            {
                                throw new AssertionError(e);
                            }

                            ColumnDefinition cd = metadata.getColumnDefinition(name);
                            if (cd != null)
                                result.schema.value_types.put(name, TypeParser.getShortName(cd.getValidator()));
                            IColumn c = row.cf.getColumn(name);
                            if (c == null || c.isMarkedForDelete())
                                thriftColumns.add(new Column().setName(name));
                            else
                                thriftColumns.add(thriftify(c));
                        }
                    }

                    // Create a new row, add the columns to it, and then add it to the list of rows
                    CqlRow cqlRow = new CqlRow();
                    cqlRow.key = row.key.key;
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
                batchUpdate(clientState, Collections.singletonList(update), update.getConsistencyLevel());
                result.type = CqlResultType.VOID;
                return result;
                
            case BATCH:
                BatchStatement batch = (BatchStatement) statement.statement;

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

                try
                {
                    StorageProxy.mutate(batch.getMutations(keyspace, clientState), batch.getConsistencyLevel());
                }
                catch (org.apache.cassandra.thrift.UnavailableException e)
                {
                    throw new UnavailableException();
                }
                catch (TimeoutException e)
                {
                    throw new TimedOutException();
                }

                result.type = CqlResultType.VOID;
                return result;
                
            case USE:
                clientState.setKeyspace(CliUtils.unescapeSQLString((String) statement.statement));
                result.type = CqlResultType.VOID;
                
                return result;
            
            case TRUNCATE:
                Pair<String, String> columnFamily = (Pair<String, String>)statement.statement;
                keyspace = columnFamily.left == null ? clientState.getKeyspace() : columnFamily.left;

                validateColumnFamily(keyspace, columnFamily.right);
                clientState.hasColumnFamilyAccess(keyspace, columnFamily.right, Permission.WRITE);
                
                try
                {
                    StorageProxy.truncateBlocking(keyspace, columnFamily.right);
                }
                catch (TimeoutException e)
                {
                    throw (UnavailableException) new UnavailableException().initCause(e);
                }
                catch (IOException e)
                {
                    throw (UnavailableException) new UnavailableException().initCause(e);
                }
                
                result.type = CqlResultType.VOID;
                return result;
            
            case DELETE:
                DeleteStatement delete = (DeleteStatement)statement.statement;

                keyspace = delete.keyspace == null ? clientState.getKeyspace() : delete.keyspace;

                try
                {
                    StorageProxy.mutate(delete.prepareRowMutations(keyspace, clientState), delete.getConsistencyLevel());
                }
                catch (TimeoutException e)
                {
                    throw new TimedOutException();
                }
                
                result.type = CqlResultType.VOID;
                return result;
                
            case CREATE_KEYSPACE:
                CreateKeyspaceStatement create = (CreateKeyspaceStatement)statement.statement;
                create.validate();
                clientState.hasKeyspaceSchemaAccess(Permission.WRITE);
                validateSchemaAgreement();
                
                try
                {
                    KsDef ksd = new KsDef(create.getName(),
                                          create.getStrategyClass(),
                                          Collections.<CfDef>emptyList())
                                .setStrategy_options(create.getStrategyOptions());
                    ThriftValidation.validateKsDef(ksd);
                    ThriftValidation.validateKeyspaceNotYetExisting(create.getName());
                    applyMigrationOnStage(new AddKeyspace(KSMetaData.fromThrift(ksd)));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }
                
                result.type = CqlResultType.VOID;
                return result;
               
            case CREATE_COLUMNFAMILY:
                CreateColumnFamilyStatement createCf = (CreateColumnFamilyStatement)statement.statement;
                clientState.hasColumnFamilySchemaAccess(Permission.WRITE);
                validateSchemaAgreement();
                CFMetaData cfmd = createCf.getCFMetaData(keyspace);
                ThriftValidation.validateCfDef(cfmd.toThrift(), null);

                try
                {
                    applyMigrationOnStage(new AddColumnFamily(cfmd));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }
                
                result.type = CqlResultType.VOID;
                return result;
                
            case CREATE_INDEX:
                CreateIndexStatement createIdx = (CreateIndexStatement)statement.statement;
                clientState.hasColumnFamilySchemaAccess(Permission.WRITE);
                validateSchemaAgreement();
                CFMetaData oldCfm = Schema.instance.getCFMetaData(keyspace, createIdx.getColumnFamily());
                if (oldCfm == null)
                    throw new InvalidRequestException("No such column family: " + createIdx.getColumnFamily());

                boolean columnExists = false;
                ByteBuffer columnName = createIdx.getColumnName().getByteBuffer();
                // mutating oldCfm directly would be bad, but mutating a Thrift copy is fine.  This also
                // sets us up to use validateCfDef to check for index name collisions.
                CfDef cf_def = oldCfm.toThrift();
                for (ColumnDef cd : cf_def.column_metadata)
                {
                    if (cd.name.equals(columnName))
                    {
                        if (cd.index_type != null)
                            throw new InvalidRequestException("Index already exists");
                        logger.debug("Updating column {} definition for index {}", oldCfm.comparator.getString(columnName), createIdx.getIndexName());
                        cd.setIndex_type(IndexType.KEYS);
                        cd.setIndex_name(createIdx.getIndexName());
                        columnExists = true;
                        break;
                    }
                }
                if (!columnExists)
                    throw new InvalidRequestException("No column definition found for column " + oldCfm.comparator.getString(columnName));

                CFMetaData.addDefaultIndexNames(cf_def);
                ThriftValidation.validateCfDef(cf_def, oldCfm);
                try
                {
                    org.apache.cassandra.db.migration.avro.CfDef result1;
                    try
                    {
                        result1 = CFMetaData.fromThrift(cf_def).toAvro();
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                    applyMigrationOnStage(new UpdateColumnFamily(result1));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }
                
                result.type = CqlResultType.VOID;
                return result;

            case DROP_INDEX:
                DropIndexStatement dropIdx = (DropIndexStatement)statement.statement;
                clientState.hasColumnFamilySchemaAccess(Permission.WRITE);
                validateSchemaAgreement();

                try
                {
                    applyMigrationOnStage(dropIdx.generateMutation(clientState.getKeyspace()));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.toString());
                    ex.initCause(e);
                    throw ex;
                }

                result.type = CqlResultType.VOID;
                return result;

            case DROP_KEYSPACE:
                String deleteKeyspace = (String)statement.statement;
                clientState.hasKeyspaceSchemaAccess(Permission.WRITE);
                validateSchemaAgreement();
                
                try
                {
                    applyMigrationOnStage(new DropKeyspace(deleteKeyspace));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }
                
                result.type = CqlResultType.VOID;
                return result;
            
            case DROP_COLUMNFAMILY:
                String deleteColumnFamily = (String)statement.statement;
                clientState.hasColumnFamilySchemaAccess(Permission.WRITE);
                validateSchemaAgreement();
                    
                try
                {
                    applyMigrationOnStage(new DropColumnFamily(keyspace, deleteColumnFamily));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
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
                clientState.hasColumnFamilyAccess(alterTable.columnFamily, Permission.WRITE);
                validateSchemaAgreement();

                try
                {
                    applyMigrationOnStage(new UpdateColumnFamily(alterTable.getCfDef(keyspace)));
                }
                catch (ConfigurationException e)
                {
                    InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                    ex.initCause(e);
                    throw ex;
                }
                catch (IOException e)
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

    private static Column thriftify(IColumn c)
    {
        ByteBuffer value = (c instanceof CounterColumn)
                           ? ByteBufferUtil.bytes(CounterContext.instance().total(c.value()))
                           : c.value();
        return new Column(c.name()).setValue(value).setTimestamp(c.timestamp());
    }

    private static String getKeyString(CFMetaData metadata)
    {
        String keyString;
        try
        {
            keyString = ByteBufferUtil.string(metadata.getKeyName());
        }
        catch (CharacterCodingException e)
        {
            throw new AssertionError(e);
        }
        return keyString;
    }

    private static CQLStatement getStatement(String queryStr) throws InvalidRequestException, RecognitionException
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

    private static void validateSchemaIsSettled() throws SchemaDisagreementException
    {
        long limit = System.currentTimeMillis() + timeLimitForSchemaAgreement;

        outer:
        while (limit - System.currentTimeMillis() >= 0)
        {
            String currentVersionId = Schema.instance.getVersion().toString();
            for (String version : describeSchemaVersions().keySet())
            {
                if (!version.equals(currentVersionId))
                    continue outer;
            }

            // schemas agree
            return;
        }

        throw new SchemaDisagreementException();
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

    private static String bufferToString(ByteBuffer string)
    {
        try
        {
            return ByteBufferUtil.string(string);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
