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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.apache.cassandra.avro.Column;
import org.apache.cassandra.avro.CqlResult;
import org.apache.cassandra.avro.CqlResultType;
import org.apache.cassandra.avro.CqlRow;
import org.apache.cassandra.avro.InvalidRequestException;
import org.apache.cassandra.avro.TimedOutException;
import org.apache.cassandra.avro.UnavailableException;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.avro.AvroValidation.validateKey;
import static org.apache.cassandra.avro.AvroValidation.validateColumnFamily;
import static org.apache.cassandra.avro.AvroErrorFactory.newInvalidRequestException;

public class QueryProcessor
{
    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);
    
    private static List<org.apache.cassandra.db.Row> getSlice(String keyspace, SelectStatement select)
    throws InvalidRequestException, TimedOutException, UnavailableException
    {
        List<org.apache.cassandra.db.Row> rows = null;
        QueryPath queryPath = new QueryPath(select.getColumnFamily());
        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        
        assert select.getKeys().size() == 1;
        
        ByteBuffer key = select.getKeys().get(0).getByteBuffer();
        validateKey(key);
        
        // ...of a list of column names
        if (!select.isColumnRange())
        {
            Collection<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
            for (Term column : select.getColumnNames())
                columnNames.add(column.getByteBuffer());
            
            commands.add(new SliceByNamesReadCommand(keyspace, key, queryPath, columnNames));
        }
        // ...a range (slice) of column names
        else
        {
            commands.add(new SliceFromReadCommand(keyspace,
                                                  key,
                                                  queryPath,
                                                  select.getColumnStart().getByteBuffer(),
                                                  select.getColumnFinish().getByteBuffer(),
                                                  select.isColumnsReversed(),
                                                  select.getColumnsLimit()));
        }

        try
        {
            rows = StorageProxy.readProtocol(commands, select.getConsistencyLevel());
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (org.apache.cassandra.thrift.UnavailableException e)
        {
            UnavailableException error = new UnavailableException();
            error.initCause(e);
            throw error;
        }
        catch (org.apache.cassandra.thrift.InvalidRequestException e)
        {
            throw newInvalidRequestException(e);
        }
        
        return rows;
    }
    
    private static List<org.apache.cassandra.db.Row> multiRangeSlice(String keyspace, SelectStatement select)
    throws TimedOutException, UnavailableException
    {
        List<org.apache.cassandra.db.Row> rows = null;
        
        // FIXME: ranges can be open-ended, but a start must exist.  Assert so here.
        
        IPartitioner<?> p = StorageService.getPartitioner();
        AbstractBounds bounds = new Bounds(p.getToken(select.getKeyStart().getByteBuffer()),
                                           p.getToken(select.getKeyFinish().getByteBuffer()));
        
        
        // XXX: Our use of Thrift structs internally makes me Sad. :(
        SlicePredicate thriftSlicePredicate = new SlicePredicate();
        if (select.isColumnRange() || select.getColumnNames().size() == 0)
        {
            SliceRange sliceRange = new SliceRange();
            sliceRange.start = select.getColumnStart().getByteBuffer();
            sliceRange.finish = select.getColumnFinish().getByteBuffer();
            sliceRange.reversed = false;    // FIXME: hard-coded
            sliceRange.count = select.getColumnsLimit();
            thriftSlicePredicate.slice_range = sliceRange;
        }
        else
        {
            List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
            for (Term column : select.getColumnNames())
                columnNames.add(column.getByteBuffer());
            thriftSlicePredicate.column_names = columnNames;
        }

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

    public static CqlResult process(String queryString, ClientState clientState)
    throws RecognitionException, UnavailableException, InvalidRequestException, TimedOutException
    {
        logger.trace("CQL QUERY: {}", queryString);
        
        CqlParser parser = getParser(queryString);
        CQLStatement statement = parser.query();
        parser.throwLastRecognitionError();
        String keyspace = clientState.getKeyspace();
        
        CqlResult avroResult = new CqlResult();
        
        switch (statement.type)
        {
            case SELECT:
                SelectStatement select = (SelectStatement)statement.statement;
                
                List<CqlRow> avroRows = new ArrayList<CqlRow>();
                avroResult.type = CqlResultType.ROWS;
                List<org.apache.cassandra.db.Row> rows = null;
                
                if (!select.isKeyRange() && (select.getKeys().size() > 0))
                {
                    // Multiple keys (aka "multiget") is not allowed( any longer).
                    if (select.getKeys().size() > 1)
                        throw newInvalidRequestException("SELECTs can contain only one by-key clause (i.e. KEY = TERM)");
                    
                    rows = getSlice(keyspace, select);
                }
                else
                {
                    // Combining key ranges and column index queries is not currently allowed
                    if (select.getColumnRelations().size() > 0)
                        throw newInvalidRequestException("You cannot combine key ranges and by-column clauses " +
                                "(i.e. \"name\" = \"value\") in a SELECT statement");
                    
                    rows = multiRangeSlice(keyspace, select);
                }
                
                // Create the result set
                for (org.apache.cassandra.db.Row row : rows)
                {
                    /// No results for this row
                    if (row.cf == null)
                        continue;
                    
                    List<Column> avroColumns = new ArrayList<Column>();
                    
                    for (IColumn column : row.cf.getSortedColumns())
                    {
                        Column avroColumn = new Column();
                        avroColumn.name = column.name();
                        avroColumn.value = column.value();
                        avroColumn.timestamp = column.timestamp();
                        avroColumns.add(avroColumn);
                    }
                    
                    // Create a new row, add the columns to it, and then add it to the list of rows
                    CqlRow avroRow = new CqlRow();
                    avroRow.key = row.key.key;
                    avroRow.columns = avroColumns;
                    avroRows.add(avroRow);
                }
                
                avroResult.rows = avroRows;
                return avroResult;
                
            case UPDATE:
                UpdateStatement update = (UpdateStatement)statement.statement;
                validateColumnFamily(keyspace, update.getColumnFamily());
                
                avroResult.type = CqlResultType.VOID;
                
                List<RowMutation> rowMutations = new ArrayList<RowMutation>();
                
                for (Row row : update.getRows())
                {
                    validateKey(row.getKey().getByteBuffer());
                    RowMutation rm = new RowMutation(keyspace, row.getKey().getByteBuffer());
                    
                    for (org.apache.cassandra.cql.Column col : row.getColumns())
                    {
                        rm.add(new QueryPath(update.getColumnFamily(), null, col.getName().getByteBuffer()),
                               col.getValue().getByteBuffer(),
                               System.currentTimeMillis());
                    }
                    
                    rowMutations.add(rm);
                }
                
                try
                {
                    StorageProxy.mutate(rowMutations, update.getConsistencyLevel());
                }
                catch (org.apache.cassandra.thrift.UnavailableException e)
                {
                    throw new UnavailableException();
                }
                catch (TimeoutException e)
                {
                    throw new TimedOutException();
                }
                    
                return avroResult;
                
            case USE:
                clientState.setKeyspace((String)statement.statement);
                avroResult.type = CqlResultType.VOID;
                
                return avroResult;
        }
        
        return null;    // We should never get here.
    }
    
    private static CqlParser getParser(String queryStr)
    {
        CharStream stream = new ANTLRStringStream(queryStr);
        CqlLexer lexer = new CqlLexer(stream);
        TokenStream tokenStream = new CommonTokenStream(lexer);
        return new CqlParser(tokenStream);
    }
}
