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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.antlr.runtime.*;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.migration.AddColumnFamily;
import org.apache.cassandra.db.migration.AddKeyspace;
import org.apache.cassandra.db.migration.DropColumnFamily;
import org.apache.cassandra.db.migration.DropKeyspace;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.db.migration.UpdateColumnFamily;
import org.apache.cassandra.db.migration.avro.CfDef;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

public class QueryProcessor
{
    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);
    
    private static List<org.apache.cassandra.db.Row> getSlice(String keyspace, SelectStatement select)
    throws InvalidRequestException, TimedOutException, UnavailableException
    {
        List<org.apache.cassandra.db.Row> rows = null;
        QueryPath queryPath = new QueryPath(select.getColumnFamily());
        AbstractType<?> comparator = select.getComparator(keyspace);
        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        
        assert select.getKeys().size() == 1;
        
        CFMetaData metadata = validateColumnFamily(keyspace, select.getColumnFamily(), false);
        ByteBuffer key = select.getKeys().get(0).getByteBuffer(metadata.getKeyValidator());
        validateKey(key);

        // ...of a list of column names
        if (!select.isColumnRange())
        {
            Collection<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
            for (Term column : select.getColumnNames())
                columnNames.add(column.getByteBuffer(comparator));
            
            validateColumnNames(columnNames);
            commands.add(new SliceByNamesReadCommand(keyspace, key, queryPath, columnNames));
        }
        // ...a range (slice) of column names
        else
        {
            ByteBuffer start = select.getColumnStart().getByteBuffer(comparator);
            ByteBuffer finish = select.getColumnFinish().getByteBuffer(comparator);
            
            validateSliceRange(metadata, start, finish, select.isColumnsReversed());
            commands.add(new SliceFromReadCommand(keyspace,
                                                  key,
                                                  queryPath,
                                                  start,
                                                  finish,
                                                  select.isColumnsReversed(),
                                                  select.getColumnsLimit()));
        }

        try
        {
            rows = StorageProxy.read(commands, select.getConsistencyLevel());
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        
        return rows;
    }
    
    private static List<org.apache.cassandra.db.Row> multiRangeSlice(String keyspace, SelectStatement select)
    throws TimedOutException, UnavailableException, InvalidRequestException
    {
        List<org.apache.cassandra.db.Row> rows = null;
        
        AbstractType<?> keyType = DatabaseDescriptor.getCFMetaData(keyspace,
                                                                   select.getColumnFamily()).getKeyValidator();
        ByteBuffer startKey = (select.getKeyStart() != null) ? select.getKeyStart().getByteBuffer(keyType) : (new Term()).getByteBuffer();
        ByteBuffer finishKey = (select.getKeyFinish() != null) ? select.getKeyFinish().getByteBuffer(keyType) : (new Term()).getByteBuffer();
        IPartitioner<?> p = StorageService.getPartitioner();
        AbstractBounds bounds = new Bounds(p.getToken(startKey), p.getToken(finishKey));
        
        CFMetaData metadata = validateColumnFamily(keyspace, select.getColumnFamily(), false);
        AbstractType<?> comparator = metadata.getComparatorFor(null);
        // XXX: Our use of Thrift structs internally makes me Sad. :(
        SlicePredicate thriftSlicePredicate = slicePredicateFromSelect(select, comparator);
        validateSlicePredicate(metadata, thriftSlicePredicate);

        try
        {
            rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace,
                                                                    select.getColumnFamily(),
                                                                    null,
                                                                    thriftSlicePredicate,
                                                                    bounds,
                                                                    select.getNumRecords()),
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
        
        return rows;
    }
    
    private static List<org.apache.cassandra.db.Row> getIndexedSlices(String keyspace, SelectStatement select)
    throws TimedOutException, UnavailableException, InvalidRequestException
    {
        CFMetaData metadata = validateColumnFamily(keyspace, select.getColumnFamily(), false);
        AbstractType<?> comparator = metadata.getComparatorFor(null);
        // XXX: Our use of Thrift structs internally (still) makes me Sad. :~(
        SlicePredicate thriftSlicePredicate = slicePredicateFromSelect(select, comparator);
        validateSlicePredicate(metadata, thriftSlicePredicate);
        
        List<IndexExpression> expressions = new ArrayList<IndexExpression>();
        for (Relation columnRelation : select.getColumnRelations())
        {
            // Left and right side of relational expression encoded according to comparator/validator.
            ByteBuffer entity = columnRelation.getEntity().getByteBuffer(comparator);
            ByteBuffer value = columnRelation.getValue().getByteBuffer(select.getValueValidator(keyspace, entity));
            
            expressions.add(new IndexExpression(entity,
                                                IndexOperator.valueOf(columnRelation.operator().toString()),
                                                value));
        }
        
        AbstractType<?> keyType = DatabaseDescriptor.getCFMetaData(keyspace,
                                                                   select.getColumnFamily()).getKeyValidator();
        ByteBuffer startKey = (!select.isKeyRange()) ? (new Term()).getByteBuffer() : select.getKeyStart().getByteBuffer(keyType);
        IndexClause thriftIndexClause = new IndexClause(expressions, startKey, select.getNumRecords());
        
        List<org.apache.cassandra.db.Row> rows;
        try
        {
            rows = StorageProxy.scan(keyspace,
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
        String keyspace = clientState.getKeyspace();
        List<RowMutation> rowMutations = new ArrayList<RowMutation>();
        List<String> cfamsSeen = new ArrayList<String>();

        for (UpdateStatement update : updateStatements)
        {
            CFMetaData metadata = validateColumnFamily(keyspace, update.getColumnFamily(), false);
            // Avoid unnecessary authorizations.
            if (!(cfamsSeen.contains(update.getColumnFamily())))
            {
                clientState.hasColumnFamilyAccess(update.getColumnFamily(), Permission.WRITE);
                cfamsSeen.add(update.getColumnFamily());
            }
            
            ByteBuffer key = update.getKey().getByteBuffer(update.getKeyType(keyspace));
            validateKey(key);
            AbstractType<?> comparator = update.getComparator(keyspace);
            
            RowMutation rm = new RowMutation(keyspace, key);
            for (Map.Entry<Term, Term> column : update.getColumns().entrySet())
            {
                ByteBuffer colName = column.getKey().getByteBuffer(comparator);
                ByteBuffer colValue = column.getValue().getByteBuffer(update.getValueValidator(keyspace, colName));
                
                validateColumn(metadata, colName, colValue);
                rm.add(new QueryPath(update.getColumnFamily(), null, colName), colValue, System.currentTimeMillis());
            }
            
            rowMutations.add(rm);
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
    
    private static SlicePredicate slicePredicateFromSelect(SelectStatement select, AbstractType<?> comparator)
    throws InvalidRequestException
    {
        SlicePredicate thriftSlicePredicate = new SlicePredicate();
        
        if (select.isColumnRange() || select.getColumnNames().size() == 0)
        {
            SliceRange sliceRange = new SliceRange();
            sliceRange.start = select.getColumnStart().getByteBuffer(comparator);
            sliceRange.finish = select.getColumnFinish().getByteBuffer(comparator);
            sliceRange.reversed = select.isColumnsReversed();
            sliceRange.count = select.getColumnsLimit();
            thriftSlicePredicate.slice_range = sliceRange;
        }
        else
        {
            List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
            for (Term column : select.getColumnNames())
                columnNames.add(column.getByteBuffer(comparator));
            thriftSlicePredicate.column_names = columnNames;
        }
        
        return thriftSlicePredicate;
    }
    
    /* Test for SELECT-specific taboos */
    private static void validateSelect(String keyspace, SelectStatement select) throws InvalidRequestException
    {
        if (select.isCountOperation() && (select.isKeyRange() || select.getKeys().size() < 1))
            throw new InvalidRequestException("Counts can only be performed for a single record (Hint: KEY=term)");
        
        // Finish key w/o start key (KEY < foo)
        if (!select.isKeyRange() && (select.getKeyFinish() != null))
            throw new InvalidRequestException("Key range clauses must include a start key (i.e. KEY > term)");
        
        // Key range and by-key(s) combined (KEY > foo AND KEY = bar)
        if (select.isKeyRange() && select.getKeys().size() > 0)
            throw new InvalidRequestException("You cannot combine key range and by-key clauses in a SELECT");
        
        // Start and finish keys, *and* column relations (KEY > foo AND KEY < bar and name1 = value1).
        if (select.isKeyRange() && (select.getKeyFinish() != null) && (select.getColumnRelations().size() > 0))
            throw new InvalidRequestException("You cannot combine key range and by-column clauses in a SELECT");
        
        // Multiget scenario (KEY = foo AND KEY = bar ...)
        if (select.getKeys().size() > 1)
            throw new InvalidRequestException("SELECTs can contain only by by-key clause");
        
        AbstractType<?> comparator = select.getComparator(keyspace);
        
        if (select.getColumnRelations().size() > 0)
        {
            Set<ByteBuffer> indexed = Table.open(keyspace).getColumnFamilyStore(select.getColumnFamily()).getIndexedColumns();
            for (Relation relation : select.getColumnRelations())
            {
                if ((relation.operator().equals(RelationType.EQ)) && indexed.contains(relation.getEntity().getByteBuffer(comparator)))
                    return;
            }
            throw new InvalidRequestException("No indexed columns present in by-columns clause with \"equals\" operator");
        }
    }
    
    // Copypasta from o.a.c.thrift.CassandraDaemon
    private static void applyMigrationOnStage(final Migration m) throws InvalidRequestException
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
    }
    
    private static void validateKey(ByteBuffer key) throws InvalidRequestException
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

    private static void validateColumnName(ByteBuffer column)
    throws InvalidRequestException
    {
        validateColumnNames(Arrays.asList(column));
    }
    
    private static void validateColumn(CFMetaData metadata, ByteBuffer name, ByteBuffer value)
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
        // unreachable hosts don't count towards disagreement
        Map<String, List<String>> versions = Maps.filterKeys(StorageProxy.describeSchemaVersions(),
                                                             Predicates.not(Predicates.equalTo(StorageProxy.UNREACHABLE)));
        if (versions.size() > 1)
            throw new SchemaDisagreementException();
    }

    public static CqlResult process(String queryString, ClientState clientState)
    throws RecognitionException, UnavailableException, InvalidRequestException, TimedOutException, SchemaDisagreementException
    {
        logger.trace("CQL QUERY: {}", queryString);
        
        CQLStatement statement = getStatement(queryString);
        String keyspace = null;
        
        // Some statements won't have (or don't need) a keyspace (think USE, or CREATE).
        if (StatementType.requiresKeyspace.contains(statement.type))
            keyspace = clientState.getKeyspace();
        
        CqlResult result = new CqlResult();
        
        logger.debug("CQL statement type: {}", statement.type.toString());
        CFMetaData metadata;
        AbstractType<?> comparator;
        switch (statement.type)
        {
            case SELECT:
                SelectStatement select = (SelectStatement)statement.statement;
                clientState.hasColumnFamilyAccess(select.getColumnFamily(), Permission.READ);
                metadata = validateColumnFamily(keyspace, select.getColumnFamily(), false);
                comparator = metadata.getComparatorFor(null);
                validateSelect(keyspace, select);
                
                List<org.apache.cassandra.db.Row> rows = null;

                // By-key
                if (!select.isKeyRange() && (select.getKeys().size() > 0))
                {
                    rows = getSlice(keyspace, select);
                    
                    // Only return the column count, (of the at-most 1 row).
                    if (select.isCountOperation())
                    {
                        result.type = CqlResultType.INT;
                        if (rows.size() > 0)
                            result.setNum(rows.get(0).cf != null ? rows.get(0).cf.getSortedColumns().size() : 0);
                        else
                            result.setNum(0);
                        return result;
                    }
                }
                else
                {
                    // Range query
                    if ((select.getKeyFinish() != null) || (select.getColumnRelations().size() == 0))
                    {
                        rows = multiRangeSlice(keyspace, select);
                    }
                    // Index scan
                    else
                    {
                        rows = getIndexedSlices(keyspace, select);
                    }
                }
                
                List<CqlRow> cqlRows = new ArrayList<CqlRow>();
                result.type = CqlResultType.ROWS;
                
                // Create the result set
                for (org.apache.cassandra.db.Row row : rows)
                {
                    /// No results for this row
                    if (row.cf == null)
                        continue;

                    List<Column> thriftColumns = extractThriftColumns(select, comparator, row);
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
                
            case BATCH_UPDATE:
                BatchUpdateStatement batch = (BatchUpdateStatement)statement.statement;
                
                for (UpdateStatement up : batch.getUpdates())
                    if (up.isSetConsistencyLevel())
                        throw new InvalidRequestException(
                                "Consistency level must be set on the BATCH, not individual UPDATE statements");
                
                batchUpdate(clientState, batch.getUpdates(), batch.getConsistencyLevel());
                result.type = CqlResultType.VOID;
                return result;
                
            case USE:
                clientState.setKeyspace((String)statement.statement);
                result.type = CqlResultType.VOID;
                
                return result;
            
            case TRUNCATE:
                String columnFamily = (String)statement.statement;
                validateColumnFamily(keyspace, columnFamily);
                clientState.hasColumnFamilyAccess(columnFamily, Permission.WRITE);
                
                try
                {
                    StorageProxy.truncateBlocking(keyspace, columnFamily);
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
                clientState.hasColumnFamilyAccess(delete.getColumnFamily(), Permission.WRITE);
                metadata = validateColumnFamily(keyspace, delete.getColumnFamily(), false);
                comparator = metadata.getComparatorFor(null);
                AbstractType<?> keyType = DatabaseDescriptor.getCFMetaData(keyspace,
                                                                           delete.getColumnFamily()).getKeyValidator();
                
                List<RowMutation> rowMutations = new ArrayList<RowMutation>();
                for (Term key : delete.getKeys())
                {
                    RowMutation rm = new RowMutation(keyspace, key.getByteBuffer(keyType));
                    if (delete.getColumns().size() < 1)     // No columns, delete the row
                        rm.delete(new QueryPath(delete.getColumnFamily()), System.currentTimeMillis());
                    else    // Delete specific columns
                    {
                        for (Term column : delete.getColumns())
                        {
                            ByteBuffer columnName = column.getByteBuffer(comparator);
                            validateColumnName(columnName);
                            rm.delete(new QueryPath(delete.getColumnFamily(), null, columnName),
                                      System.currentTimeMillis());
                        }
                    }
                    rowMutations.add(rm);
                }
                
                try
                {
                    StorageProxy.mutate(rowMutations, delete.getConsistencyLevel());
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
                clientState.hasKeyspaceListAccess(Permission.WRITE);
                validateSchemaAgreement();
                
                try
                {
                    KSMetaData ksm = new KSMetaData(create.getName(),
                                                    AbstractReplicationStrategy.getClass(create.getStrategyClass()),
                                                    create.getStrategyOptions());
                    applyMigrationOnStage(new AddKeyspace(ksm));
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
                clientState.hasColumnFamilyListAccess(Permission.WRITE);
                validateSchemaAgreement();
                
                try
                {
                    applyMigrationOnStage(new AddColumnFamily(createCf.getCFMetaData(keyspace)));
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
                clientState.hasColumnFamilyListAccess(Permission.WRITE);
                validateSchemaAgreement();
                CFMetaData oldCfm = DatabaseDescriptor.getCFMetaData(CFMetaData.getId(keyspace,
                                                                                      createIdx.getColumnFamily()));
                if (oldCfm == null)
                    throw new InvalidRequestException("No such column family: " + createIdx.getColumnFamily());
                
                ByteBuffer columnName = createIdx.getColumnName().getByteBuffer();
                ColumnDefinition columnDef = oldCfm.getColumn_metadata().get(columnName);
                
                // Meta-data for this column already exists
                if (columnDef != null)
                {
                    // This column is already indexed, stop, drop, and roll.
                    if (columnDef.getIndexType() != null)
                        throw new InvalidRequestException("Index exists");
                    // Add index attrs to the existing definition
                    columnDef.setIndexName(createIdx.getIndexName());
                    columnDef.setIndexType(org.apache.cassandra.thrift.IndexType.KEYS);
                }
                // No meta-data, create a new column definition from scratch.
                else
                {
                    columnDef = new ColumnDefinition(columnName,
                                                     DatabaseDescriptor.getValueValidator(keyspace,
                                                                                          createIdx.getColumnFamily(),
                                                                                          columnName),
                                                     org.apache.cassandra.thrift.IndexType.KEYS,
                                                     createIdx.getIndexName());
                }
                
                CfDef cfamilyDef = CFMetaData.convertToAvro(oldCfm);
                cfamilyDef.column_metadata.add(columnDef.deflate());
                
                try
                {
                    applyMigrationOnStage(new UpdateColumnFamily(cfamilyDef));
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
                clientState.hasKeyspaceListAccess(Permission.WRITE);
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
                clientState.hasColumnFamilyListAccess(Permission.WRITE);
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
                
        }
        
        return null;    // We should never get here.
    }

    private static List<Column> extractThriftColumns(SelectStatement select, AbstractType<?> comparator, Row row)
    {
        List<Column> thriftColumns = new ArrayList<Column>();
        if (select.isColumnRange())
        {
            // preserve comparator order
            for (IColumn c : row.cf.getSortedColumns())
            {
                if (c.isMarkedForDelete())
                    continue;
                thriftColumns.add(new Column(c.name()).setValue(c.value()).setTimestamp(c.timestamp()));
            }
        }
        else
        {
            // order columns in the order they were asked for
            for (Term term : select.getColumnNames())
            {
                ByteBuffer name;
                try
                {
                    name = term.getByteBuffer(comparator);
                }
                catch (InvalidRequestException e)
                {
                    throw new AssertionError(e);
                }
                IColumn c = row.cf.getColumn(name);
                if (c == null || c.isMarkedForDelete())
                    thriftColumns.add(new Column().setName(name));
                else
                    thriftColumns.add(new Column(c.name()).setValue(c.value()).setTimestamp(c.timestamp()));
            }
        }
        return thriftColumns;
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
}
