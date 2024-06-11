/*
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
 */
package org.apache.cassandra.stress.operations.predefined;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Function;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.settings.Command;
import org.apache.cassandra.stress.settings.ConnectionStyle;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class CqlOperation<V> extends PredefinedOperation
{

    public static final ByteBuffer[][] EMPTY_BYTE_BUFFERS = new ByteBuffer[0][];
    public static final byte[][] EMPTY_BYTE_ARRAYS = new byte[0][];

    protected abstract List<Object> getQueryParameters(byte[] key);
    protected abstract String buildQuery();
    protected abstract CqlRunOp<V> buildRunOp(QueryExecutor<?> queryExecutor, List<Object> params, ByteBuffer key);

    public CqlOperation(Command type, Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings)
    {
        super(type, timer, generator, seedManager, settings);
        if (settings.columns.variableColumnCount)
            throw new IllegalStateException("Variable column counts are not implemented for CQL");
    }

    protected CqlRunOp<V> run(final QueryExecutor<?> queryExecutor, final List<Object> queryParams, final ByteBuffer key) throws IOException
    {
        final CqlRunOp<V> op = buildRunOp(queryExecutor, queryParams, key);
        timeWithRetry(op);
        return op;
    }

    protected void run(final QueryExecutor<?> queryExecutor) throws IOException
    {
        final byte[] key = getKey().array();
        final List<Object> queryParams = getQueryParameters(key);
        run(queryExecutor, queryParams, ByteBuffer.wrap(key));
    }

    // Classes to process Cql results

    // Always succeeds so long as the query executes without error; provides a keyCount to increment on instantiation
    protected final class CqlRunOpAlwaysSucceed extends CqlRunOp<Integer>
    {

        final int keyCount;

        protected CqlRunOpAlwaysSucceed(QueryExecutor<?> queryExecutor, List<Object> params, ByteBuffer key, int keyCount)
        {
            super(queryExecutor, RowCountHandler.INSTANCE, params, key);
            this.keyCount = keyCount;
        }

        @Override
        public boolean validate(Integer result)
        {
            return true;
        }

        @Override
        public int partitionCount()
        {
            return keyCount;
        }

        @Override
        public int rowCount()
        {
            return keyCount;
        }
    }

    // Succeeds so long as the result set is nonempty, and the query executes without error
    protected final class CqlRunOpTestNonEmpty extends CqlRunOp<Integer>
    {

        protected CqlRunOpTestNonEmpty(QueryExecutor<?> queryExecutor, List<Object> params, ByteBuffer key)
        {
            super(queryExecutor, RowCountHandler.INSTANCE, params, key);
        }

        @Override
        public boolean validate(Integer result)
        {
            return result > 0;
        }

        @Override
        public int partitionCount()
        {
            return result;
        }

        @Override
        public int rowCount()
        {
            return result;
        }
    }

    protected final class CqlRunOpMatchResults extends CqlRunOp<ByteBuffer[][]>
    {

        final List<List<ByteBuffer>> expect;

        // a null value for an item in expect means we just check the row is present
        protected CqlRunOpMatchResults(QueryExecutor<?> queryExecutor, List<Object> params, ByteBuffer key, List<List<ByteBuffer>> expect)
        {
            super(queryExecutor, RowsHandler.INSTANCE, params, key);
            this.expect = expect;
        }

        @Override
        public int partitionCount()
        {
            return result == null ? 0 : result.length;
        }

        @Override
        public int rowCount()
        {
            return result == null ? 0 : result.length;
        }

        public boolean validate(ByteBuffer[][] result)
        {
            if (!settings.errors.skipReadValidation)
            {
                if (result.length != expect.size())
                    return false;
                for (int i = 0; i < result.length; i++)
                    if (expect.get(i) != null && !expect.get(i).equals(Arrays.asList(result[i])))
                        return false;
            }
            return true;
        }
    }

    // Cql
    protected abstract class CqlRunOp<V> implements RunOp
    {

        final QueryExecutor<?> queryExecutor;
        final List<Object> params;
        final ByteBuffer key;
        final ResultHandler<V> handler;
        V result;

        private CqlRunOp(QueryExecutor<?> queryExecutor, ResultHandler<V> handler, List<Object> params, ByteBuffer key)
        {
            this.queryExecutor = queryExecutor;
            this.handler = handler;
            this.params = params;
            this.key = key;
        }

        @Override
        public boolean run() throws Exception
        {
            return validate(result = queryExecutor.execute(key, params, handler));
        }

        public abstract boolean validate(V result);

    }


    /// LOTS OF WRAPPING/UNWRAPPING NONSENSE


    @Override
    public void run(SimpleClient client) throws IOException
    {
        run(new SimpleClientQueryExecutor(client, buildQuery()));
    }

    @Override
    public void run(JavaDriverClient client) throws IOException
    {
        run(new JavaDriverQueryExecutor(client, buildQuery()));
    }

    protected abstract class QueryExecutor<PS>
    {
        private final String query;
        private PS preparedStatement;

        public QueryExecutor(String query)
        {
            this.query = query;
        }

        private boolean isPrepared()
        {
            return settings.mode.style == ConnectionStyle.CQL_PREPARED;
        }

        abstract protected PS createPreparedStatement(String query);

        private PS getPreparedStatement()
        {
            if (preparedStatement == null)
            {
                preparedStatement = createPreparedStatement(this.query);
            }
            return preparedStatement;
        }

        /**
         * Constructs a CQL query string by replacing instances of the character
         * '?', with the corresponding parameter.
         *
         * @param query base query string to format
         * @param parms sequence of string query parameters
         * @return formatted CQL query string
         */
        private String formatCqlQuery(String query, List<Object> parms)
        {
            int marker, position = 0;
            StringBuilder result = new StringBuilder();

            if (-1 == (marker = query.indexOf('?')) || parms.size() == 0)
                return query;

            for (Object parm : parms)
            {
                result.append(query.substring(position, marker));

                if (parm instanceof ByteBuffer)
                    result.append(getUnQuotedCqlBlob((ByteBuffer) parm));
                else if (parm instanceof Long)
                    result.append(parm);
                else throw new AssertionError();

                position = marker + 1;
                if (-1 == (marker = query.indexOf('?', position + 1)))
                    break;
            }

            if (position < query.length())
                result.append(query.substring(position));

            return result.toString();
        }

        <V> V execute(ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler)
        {
            return isPrepared()
                   ? execute(getPreparedStatement(), key, queryParams, handler)
                   : execute(formatCqlQuery(this.query, queryParams), key, handler);
        }

        abstract <V> V execute(PS preparedStatement, ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler);
        abstract <V> V execute(String formattedQuery, ByteBuffer key, ResultHandler<V> handler);
    }

    private final class JavaDriverQueryExecutor extends QueryExecutor<PreparedStatement>
    {
        final JavaDriverClient client;
        private JavaDriverQueryExecutor(JavaDriverClient client, String query)
        {
            super(query);
            this.client = client;
        }

        protected <V> V execute(String formattedQuery, ByteBuffer key, ResultHandler<V> handler)
        {
            return handler.javaDriverHandler().apply(client.execute(formattedQuery, settings.command.consistencyLevel));
        }

        protected <V> V execute(PreparedStatement preparedStatement, ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler)
        {
            return handler.javaDriverHandler().apply(
                    client.executePrepared(
                            preparedStatement,
                            queryParams,
                            settings.command.consistencyLevel));
        }

        public PreparedStatement createPreparedStatement(String cqlQuery)
        {
            return client.prepare(cqlQuery);
        }
    }

    private final class SimpleClientQueryExecutor extends QueryExecutor<ResultMessage.Prepared>
    {
        final SimpleClient client;
        private SimpleClientQueryExecutor(SimpleClient client, String query)
        {
            super(query);
            this.client = client;
        }

        public <V> V execute(String formattedQuery, ByteBuffer key, ResultHandler<V> handler)
        {
            return handler.simpleClientHandler().apply(client.execute(formattedQuery, settings.command.consistencyLevel));
        }

        public <V> V execute(ResultMessage.Prepared preparedStatement, ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler)
        {
            return handler.simpleClientHandler().apply(
                    client.executePrepared(
                            preparedStatement,
                            toByteBufferParams(queryParams),
                            settings.command.consistencyLevel));
        }

        public ResultMessage.Prepared createPreparedStatement(String cqlQuery)
        {
            return client.prepare(cqlQuery);
        }
    }


    // interface for building functions to standardise results from each client
    protected static interface ResultHandler<V>
    {
        Function<ResultSet, V> javaDriverHandler();
        Function<ResultMessage, V> simpleClientHandler();
    }

    protected static class RowCountHandler implements ResultHandler<Integer>
    {
        static final RowCountHandler INSTANCE = new RowCountHandler();

        @Override
        public Function<ResultSet, Integer> javaDriverHandler()
        {
            return new Function<ResultSet, Integer>()
            {
                @Override
                public Integer apply(ResultSet rows)
                {
                    if (rows == null)
                        return 0;
                    return rows.all().size();
                }
            };
        }

        @Override
        public Function<ResultMessage, Integer> simpleClientHandler()
        {
            return new Function<ResultMessage, Integer>()
            {
                @Override
                public Integer apply(ResultMessage result)
                {
                    return result instanceof ResultMessage.Rows ? ((ResultMessage.Rows) result).result.size() : 0;
                }
            };
        }
    }

    // Processes results from each client into an array of all key bytes returned
    protected static final class RowsHandler implements ResultHandler<ByteBuffer[][]>
    {
        static final RowsHandler INSTANCE = new RowsHandler();

        @Override
        public Function<ResultSet, ByteBuffer[][]> javaDriverHandler()
        {
            return new Function<ResultSet, ByteBuffer[][]>()
            {

                @Override
                public ByteBuffer[][] apply(ResultSet result)
                {
                    if (result == null)
                        return EMPTY_BYTE_BUFFERS;
                    List<Row> rows = result.all();

                    ByteBuffer[][] r = new ByteBuffer[rows.size()][];
                    for (int i = 0 ; i < r.length ; i++)
                    {
                        Row row = rows.get(i);
                        r[i] = new ByteBuffer[row.getColumnDefinitions().size()];
                        for (int j = 0 ; j < row.getColumnDefinitions().size() ; j++)
                            r[i][j] = row.getBytes(j);
                    }
                    return r;
                }
            };
        }

        @Override
        public Function<ResultMessage, ByteBuffer[][]> simpleClientHandler()
        {
            return new Function<ResultMessage, ByteBuffer[][]>()
            {

                @Override
                public ByteBuffer[][] apply(ResultMessage result)
                {
                    if (!(result instanceof ResultMessage.Rows))
                        return EMPTY_BYTE_BUFFERS;

                    ResultMessage.Rows rows = ((ResultMessage.Rows) result);
                    ByteBuffer[][] r = new ByteBuffer[rows.result.size()][];
                    for (int i = 0 ; i < r.length ; i++)
                    {
                        List<ByteBuffer> row = rows.result.rows.get(i);
                        r[i] = new ByteBuffer[row.size()];
                        for (int j = 0 ; j < row.size() ; j++)
                            r[i][j] = row.get(j);
                    }
                    return r;
                }
            };
        }
    }
    // Processes results from each client into an array of all key bytes returned
    protected static final class KeysHandler implements ResultHandler<byte[][]>
    {
        static final KeysHandler INSTANCE = new KeysHandler();

        @Override
        public Function<ResultSet, byte[][]> javaDriverHandler()
        {
            return new Function<ResultSet, byte[][]>()
            {

                @Override
                public byte[][] apply(ResultSet result)
                {

                    if (result == null)
                        return EMPTY_BYTE_ARRAYS;
                    List<Row> rows = result.all();
                    byte[][] r = new byte[rows.size()][];
                    for (int i = 0 ; i < r.length ; i++)
                        r[i] = rows.get(i).getBytes(0).array();
                    return r;
                }
            };
        }

        @Override
        public Function<ResultMessage, byte[][]> simpleClientHandler()
        {
            return new Function<ResultMessage, byte[][]>()
            {

                @Override
                public byte[][] apply(ResultMessage result)
                {
                    if (result instanceof ResultMessage.Rows)
                    {
                        ResultMessage.Rows rows = ((ResultMessage.Rows) result);
                        byte[][] r = new byte[rows.result.size()][];
                        for (int i = 0 ; i < r.length ; i++)
                            r[i] = rows.result.rows.get(i).get(0).array();
                        return r;
                    }
                    return null;
                }
            };
        }
    }

    private static String getUnQuotedCqlBlob(ByteBuffer term)
    {
        return "0x" + ByteBufferUtil.bytesToHex(term);
    }

    private static List<ByteBuffer> toByteBufferParams(List<Object> params)
    {
        List<ByteBuffer> r = new ArrayList<>();
        for (Object param : params)
        {
            if (param instanceof ByteBuffer)
                r.add((ByteBuffer) param);
            else if (param instanceof Long)
                r.add(ByteBufferUtil.bytes((Long) param));
            else throw new AssertionError();
        }
        return r;
    }

    protected String wrapInQuotes(String string)
    {
        return "\"" + string + "\"";
    }

}
