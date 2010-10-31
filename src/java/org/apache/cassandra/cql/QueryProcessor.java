
package org.apache.cassandra.cql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ConsistencyLevel;

import static org.apache.cassandra.avro.AvroValidation.validateKey;

public class QueryProcessor
{

    public static Map<DecoratedKey<?>, ColumnFamily> readColumnFamily(List<ReadCommand> commands, ConsistencyLevel cLevel)
    throws UnavailableException, InvalidRequestException, TimedOutException
    {
        Map<DecoratedKey<?>, ColumnFamily> columnFamilyKeyMap = new HashMap<DecoratedKey<?>, ColumnFamily>();
        List<org.apache.cassandra.db.Row> rows;
        
        try
        {
            rows = StorageProxy.readProtocol(commands, cLevel);
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
        
        for (org.apache.cassandra.db.Row row: rows)
            columnFamilyKeyMap.put(row.key, row.cf);
        
        return columnFamilyKeyMap;
    }

    public static CqlResult process(String queryString, String keyspace)
    throws RecognitionException, UnavailableException, InvalidRequestException, TimedOutException
    {
        CqlParser parser = getParser(queryString);
        CQLStatement statement = parser.query();
        
        CqlResult avroResult = new CqlResult();
        
        switch (statement.type)
        {
            case SELECT:
                SelectStatement select = (SelectStatement)statement.statement;
                
                QueryPath queryPath = new QueryPath(select.getColumnFamily());
                List<ReadCommand> commands = new ArrayList<ReadCommand>();
                
                List<CqlRow> avroRows = new ArrayList<CqlRow>();
                avroResult.type = CqlResultType.ROWS;
                
                // It's a multiget...
                if (!select.getKeyPredicates().isRange())
                {
                    
                    for (Term keyName : select.getKeyPredicates().getTerms())
                    {
                        byte[] key = keyName.getBytes();  // FIXME: surely not good enough
                        validateKey(key);
                        
                        // ...of a list of column names
                        if (!select.getColumnPredicates().isRange())
                        {
                            Collection<byte[]> columnNames = new ArrayList<byte[]>();
                            for (Term column : select.getColumnPredicates().getTerms())
                                columnNames.add(column.getBytes());    // FIXME: surely not good enough
                            
                            commands.add(new SliceByNamesReadCommand(keyspace, key, queryPath, columnNames));
                        }
                        // ...a range (slice) of column names
                        else
                        {
                            commands.add(new SliceFromReadCommand(keyspace,
                                                                  key,
                                                                  queryPath,
                                                                  select.getColumnPredicates().getStart().getBytes(),
                                                                  select.getColumnPredicates().getFinish().getBytes(),
                                                                  select.reversed(),
                                                                  select.getNumColumns()));
                        }
                        
                        Map<DecoratedKey<?>, ColumnFamily> columnFamilies = readColumnFamily(commands,
                                                                                             select.getConsistencyLevel());
                        List<Column> avroColumns = new ArrayList<Column>();
                        
                        for (ReadCommand cmd : commands)
                        {
                            ColumnFamily cf = columnFamilies.get(StorageService.getPartitioner().decorateKey(cmd.key));
                            // TODO: handle reversing order
                            for (IColumn column : cf.getSortedColumns())
                            {
                                Column avroColumn = new Column();
                                avroColumn.name = ByteBuffer.wrap(column.name());
                                avroColumn.value = ByteBuffer.wrap(column.value());
                                avroColumns.add(avroColumn);
                            }
                        }
                        
                        // Create a new row, add the columns to it, and then add it to the list of rows
                        CqlRow avroRow = new CqlRow();
                        avroRow.key = ByteBuffer.wrap(key);
                        avroRow.columns = avroColumns;
                        avroRows.add(avroRow);
                    }
                }
                else    // It is a range query (range of keys).
                {
                    
                }
                
                avroResult.rows = avroRows;
                return avroResult;
                
            case UPDATE:
                UpdateStatement update = (UpdateStatement)statement.statement;
                avroResult.type = CqlResultType.VOID;
                
                List<RowMutation> rowMutations = new ArrayList<RowMutation>();
                
                for (Row row : update.getRows())
                {
                    RowMutation rm = new RowMutation(keyspace, row.getKey().getBytes());
                    
                    for (org.apache.cassandra.cql.Column col : row.getColumns())
                    {
                        rm.add(new QueryPath(update.getColumnFamily(), null, col.getName().getBytes()),
                               col.getValue().getBytes(),
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

    public static void main(String[] args) throws RecognitionException
    {
        CqlParser parser = getParser("SElecT FRoM Standard1 where KEY > \"foo\" and key < \"fnord\" and COLUMN=\"bar\";");
        CQLStatement statement = parser.query();
        
        switch (statement.type)
        {
            case SELECT:
                SelectStatement st = (SelectStatement)statement.statement;
                System.out.println(st.getColumnFamily() + " " + st.getKeyPredicates().getStart().getText() + 
                        " " + st.getColumnPredicates().getTerms() + " " + st.getKeyPredicates().isRange());
            case UPDATE:
                return;
        }
    }

}
