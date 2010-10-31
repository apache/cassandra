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

public class QueryProcessor
{
    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);
    
    private static List<org.apache.cassandra.db.Row> multiSlice(String keyspace, SelectStatement select)
    throws InvalidRequestException, TimedOutException, UnavailableException
    {
        List<org.apache.cassandra.db.Row> rows = null;
        QueryPath queryPath = new QueryPath(select.getColumnFamily());
        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        
        for (Term keyName : select.getKeyPredicates().getTerms())
        {
            ByteBuffer key = keyName.getByteBuffer();
            validateKey(key);
            
            // ...of a list of column names
            if ((!select.getColumnPredicates().isRange()) && select.getColumnPredicates().isInitialized())
            {
                Collection<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
                for (Term column : select.getColumnPredicates().getTerms())
                    columnNames.add(column.getByteBuffer());
                
                commands.add(new SliceByNamesReadCommand(keyspace, key, queryPath, columnNames));
            }
            // ...a range (slice) of column names
            else
            {
                commands.add(new SliceFromReadCommand(keyspace,
                                                      key,
                                                      queryPath,
                                                      select.getColumnPredicates().getStart().getByteBuffer(),
                                                      select.getColumnPredicates().getFinish().getByteBuffer(),
                                                      select.reversed(),
                                                      select.getNumColumns()));
            }
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
            InvalidRequestException error = new InvalidRequestException();
            error.initCause(e);
            throw error;
        }
        
        return rows;
    }
    
    private static List<org.apache.cassandra.db.Row> multiRangeSlice(String keyspace, SelectStatement select)
    throws TimedOutException, UnavailableException
    {
        List<org.apache.cassandra.db.Row> rows = null;
        
        // FIXME: ranges can be open-ended, but a start must exist.  Assert so here.
        
        IPartitioner<?> p = StorageService.getPartitioner();
        AbstractBounds bounds = new Bounds(p.getToken(select.getKeyPredicates().getStart().getByteBuffer()),
                                           p.getToken(select.getKeyPredicates().getFinish().getByteBuffer()));
        
        // XXX: Our use of Thrift structs internally makes me Sad. :(
        SlicePredicate thriftSlicePredicate = new SlicePredicate();
        if (select.getColumnPredicates().isRange() || select.getColumnPredicates().getTerms().size() == 0)
        {
            SliceRange sliceRange = new SliceRange();
            sliceRange.start = select.getColumnPredicates().getStart().getByteBuffer();
            sliceRange.finish = select.getColumnPredicates().getFinish().getByteBuffer();
            sliceRange.reversed = false;    // FIXME: hard-coded
            sliceRange.count = select.getNumColumns();
            thriftSlicePredicate.slice_range = sliceRange;
        }
        else
        {
            List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
            for (Term column : select.getColumnPredicates().getTerms())
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
        logger.debug("CQL QUERY: {}", queryString);
        
        CqlParser parser = getParser(queryString);
        CQLStatement statement = parser.query();
        String keyspace = clientState.getKeyspace();
        
        CqlResult avroResult = new CqlResult();
        
        switch (statement.type)
        {
            case SELECT:
                SelectStatement select = (SelectStatement)statement.statement;
                
                List<CqlRow> avroRows = new ArrayList<CqlRow>();
                avroResult.type = CqlResultType.ROWS;
                List<org.apache.cassandra.db.Row> rows = null;
                
                if (!select.getKeyPredicates().isRange())
                    rows = multiSlice(keyspace, select);
                else
                    rows = multiRangeSlice(keyspace, select);
                
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
                avroResult.type = CqlResultType.VOID;
                
                List<RowMutation> rowMutations = new ArrayList<RowMutation>();
                
                for (Row row : update.getRows())
                {
                    RowMutation rm = new RowMutation(keyspace, row.getKey().getByteBuffer());
                    
                    for (org.apache.cassandra.cql.Column col : row.getColumns())
                    {
                        rm.add(new QueryPath(update.getColumnFamily(), null, col.getName().getByteBuffer()),
                               col.getValue().getByteBuffer(),
                               System.currentTimeMillis());
                        rowMutations.add(rm);
                    }
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
