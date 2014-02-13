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
package org.apache.cassandra.stress.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.settings.ConnectionStyle;
import org.apache.cassandra.stress.settings.CqlVersion;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;

public abstract class CqlOperation<V> extends Operation
{

    protected abstract List<ByteBuffer> getQueryParameters(byte[] key);
    protected abstract String buildQuery();
    protected abstract CqlRunOp<V> buildRunOp(ClientWrapper client, String query, Object queryId, List<ByteBuffer> params, String id, ByteBuffer key);

    public CqlOperation(State state, long idx)
    {
        super(state, idx);
        if (state.settings.columns.useSuperColumns)
            throw new IllegalStateException("Super columns are not implemented for CQL");
        if (state.settings.columns.variableColumnCount)
            throw new IllegalStateException("Variable column counts are not implemented for CQL");
    }

    protected CqlRunOp<V> run(final ClientWrapper client, final List<ByteBuffer> queryParams, final ByteBuffer key, final String keyid) throws IOException
    {
        final CqlRunOp<V> op;
        if (state.settings.mode.style == ConnectionStyle.CQL_PREPARED)
        {
            final Object id;
            Object idobj = state.getCqlCache();
            if (idobj == null)
            {
                try
                {
                    id = client.createPreparedStatement(buildQuery());
                } catch (TException e)
                {
                    throw new RuntimeException(e);
                }
                state.storeCqlCache(id);
            }
            else
                id = idobj;

            op = buildRunOp(client, null, id, queryParams, keyid, key);
        }
        else
        {
            final String query;
            Object qobj = state.getCqlCache();
            if (qobj == null)
                state.storeCqlCache(query = buildQuery());
            else
                query = qobj.toString();

            op = buildRunOp(client, query, null, queryParams, keyid, key);
        }

        timeWithRetry(op);
        return op;
    }

    protected void run(final ClientWrapper client) throws IOException
    {
        final byte[] key = getKey().array();
        final List<ByteBuffer> queryParams = getQueryParameters(key);
        run(client, queryParams, ByteBuffer.wrap(key), new String(key));
    }

    // Classes to process Cql results

    // Always succeeds so long as the query executes without error; provides a keyCount to increment on instantiation
    protected final class CqlRunOpAlwaysSucceed extends CqlRunOp<Integer>
    {

        final int keyCount;

        protected CqlRunOpAlwaysSucceed(ClientWrapper client, String query, Object queryId, List<ByteBuffer> params, String id, ByteBuffer key, int keyCount)
        {
            super(client, query, queryId, RowCountHandler.INSTANCE, params, id, key);
            this.keyCount = keyCount;
        }

        @Override
        public boolean validate(Integer result)
        {
            return true;
        }

        @Override
        public int keyCount()
        {
            return keyCount;
        }
    }

    // Succeeds so long as the result set is nonempty, and the query executes without error
    protected final class CqlRunOpTestNonEmpty extends CqlRunOp<Integer>
    {

        protected CqlRunOpTestNonEmpty(ClientWrapper client, String query, Object queryId, List<ByteBuffer> params, String id, ByteBuffer key)
        {
            super(client, query, queryId, RowCountHandler.INSTANCE, params, id, key);
        }

        @Override
        public boolean validate(Integer result)
        {
            return true;
        }

        @Override
        public int keyCount()
        {
            return result;
        }
    }

    // Requires a custom validate() method, but fetches and stores the keys from the result set for further processing
    protected abstract class CqlRunOpFetchKeys extends CqlRunOp<byte[][]>
    {

        protected CqlRunOpFetchKeys(ClientWrapper client, String query, Object queryId, List<ByteBuffer> params, String id, ByteBuffer key)
        {
            super(client, query, queryId, KeysHandler.INSTANCE, params, id, key);
        }

        @Override
        public int keyCount()
        {
            return result.length;
        }

    }

    protected final class CqlRunOpMatchResults extends CqlRunOp<ByteBuffer[][]>
    {

        final List<List<ByteBuffer>> expect;

        // a null value for an item in expect means we just check the row is present
        protected CqlRunOpMatchResults(ClientWrapper client, String query, Object queryId, List<ByteBuffer> params, String id, ByteBuffer key, List<List<ByteBuffer>> expect)
        {
            super(client, query, queryId, RowsHandler.INSTANCE, params, id, key);
            this.expect = expect;
        }

        @Override
        public int keyCount()
        {
            return result == null ? 0 : result.length;
        }

        public boolean validate(ByteBuffer[][] result)
        {
            if (result.length != expect.size())
                return false;
            for (int i = 0 ; i < result.length ; i++)
            {
                List<ByteBuffer> resultRow = Arrays.asList(result[i]);
                resultRow = resultRow.subList(1, resultRow.size());
                if (expect.get(i) != null && !expect.get(i).equals(resultRow))
                    return false;
            }
            return true;
        }
    }

    // Cql
    protected abstract class CqlRunOp<V> implements RunOp
    {

        final ClientWrapper client;
        final String query;
        final Object queryId;
        final List<ByteBuffer> params;
        final String id;
        final ByteBuffer key;
        final ResultHandler<V> handler;
        V result;

        private CqlRunOp(ClientWrapper client, String query, Object queryId, ResultHandler<V> handler, List<ByteBuffer> params, String id, ByteBuffer key)
        {
            this.client = client;
            this.query = query;
            this.queryId = queryId;
            this.handler = handler;
            this.params = params;
            this.id = id;
            this.key = key;
        }

        @Override
        public boolean run() throws Exception
        {
            return queryId != null
            ? validate(result = client.execute(queryId, key, params, handler))
            : validate(result = client.execute(query, key, params, handler));
        }

        @Override
        public String key()
        {
            return id;
        }

        public abstract boolean validate(V result);

    }


    /// LOTS OF WRAPPING/UNWRAPPING NONSENSE


    @Override
    public void run(final ThriftClient client) throws IOException
    {
        run(wrap(client));
    }

    @Override
    public void run(SimpleClient client) throws IOException
    {
        run(wrap(client));
    }

    @Override
    public void run(JavaDriverClient client) throws IOException
    {
        run(wrap(client));
    }

    public ClientWrapper wrap(ThriftClient client)
    {
        return state.isCql3()
                ? new Cql3CassandraClientWrapper(client)
                : new Cql2CassandraClientWrapper(client);

    }

    public ClientWrapper wrap(JavaDriverClient client)
    {
        return new JavaDriverWrapper(client);
    }

    public ClientWrapper wrap(SimpleClient client)
    {
        return new SimpleClientWrapper(client);
    }

    protected interface ClientWrapper
    {
        Object createPreparedStatement(String cqlQuery) throws TException;
        <V> V execute(Object preparedStatementId, ByteBuffer key, List<ByteBuffer> queryParams, ResultHandler<V> handler) throws TException;
        <V> V execute(String query, ByteBuffer key, List<ByteBuffer> queryParams, ResultHandler<V> handler) throws TException;
    }

    private final class JavaDriverWrapper implements ClientWrapper
    {
        final JavaDriverClient client;
        private JavaDriverWrapper(JavaDriverClient client)
        {
            this.client = client;
        }

        @Override
        public <V> V execute(String query, ByteBuffer key, List<ByteBuffer> queryParams, ResultHandler<V> handler)
        {
            String formattedQuery = formatCqlQuery(query, queryParams, state.isCql3());
            return handler.javaDriverHandler().apply(client.execute(formattedQuery, ThriftConversion.fromThrift(state.settings.command.consistencyLevel)));
        }

        @Override
        public <V> V execute(Object preparedStatementId, ByteBuffer key, List<ByteBuffer> queryParams, ResultHandler<V> handler)
        {
            return handler.javaDriverHandler().apply(
                    client.executePrepared(
                            (PreparedStatement) preparedStatementId,
                            queryParams,
                            ThriftConversion.fromThrift(state.settings.command.consistencyLevel)));
        }

        @Override
        public Object createPreparedStatement(String cqlQuery)
        {
            return client.prepare(cqlQuery);
        }
    }

    private final class SimpleClientWrapper implements ClientWrapper
    {
        final SimpleClient client;
        private SimpleClientWrapper(SimpleClient client)
        {
            this.client = client;
        }

        @Override
        public <V> V execute(String query, ByteBuffer key, List<ByteBuffer> queryParams, ResultHandler<V> handler)
        {
            String formattedQuery = formatCqlQuery(query, queryParams, state.isCql3());
            return handler.thriftHandler().apply(client.execute(formattedQuery, ThriftConversion.fromThrift(state.settings.command.consistencyLevel)));
        }

        @Override
        public <V> V execute(Object preparedStatementId, ByteBuffer key, List<ByteBuffer> queryParams, ResultHandler<V> handler)
        {
            return handler.thriftHandler().apply(
                    client.executePrepared(
                            (byte[]) preparedStatementId,
                            queryParams,
                            ThriftConversion.fromThrift(state.settings.command.consistencyLevel)));
        }

        @Override
        public Object createPreparedStatement(String cqlQuery)
        {
            return client.prepare(cqlQuery).statementId.bytes;
        }
    }

    // client wrapper for Cql3
    private final class Cql3CassandraClientWrapper implements ClientWrapper
    {
        final ThriftClient client;
        private Cql3CassandraClientWrapper(ThriftClient client)
        {
            this.client = client;
        }

        @Override
        public <V> V execute(String query, ByteBuffer key, List<ByteBuffer> queryParams, ResultHandler<V> handler) throws TException
        {
            String formattedQuery = formatCqlQuery(query, queryParams, true);
            return handler.simpleNativeHandler().apply(
                    client.execute_cql3_query(formattedQuery, key, Compression.NONE, state.settings.command.consistencyLevel)
            );
        }

        @Override
        public <V> V execute(Object preparedStatementId, ByteBuffer key, List<ByteBuffer> queryParams, ResultHandler<V> handler) throws TException
        {
            Integer id = (Integer) preparedStatementId;
            return handler.simpleNativeHandler().apply(
                    client.execute_prepared_cql3_query(id, key, queryParams, state.settings.command.consistencyLevel)
            );
        }

        @Override
        public Object createPreparedStatement(String cqlQuery) throws TException
        {
            return client.prepare_cql3_query(cqlQuery, Compression.NONE);
        }
    }

    // client wrapper for Cql2
    private final class Cql2CassandraClientWrapper implements ClientWrapper
    {
        final ThriftClient client;
        private Cql2CassandraClientWrapper(ThriftClient client)
        {
            this.client = client;
        }

        @Override
        public <V> V execute(String query, ByteBuffer key, List<ByteBuffer> queryParams, ResultHandler<V> handler) throws TException
        {
            String formattedQuery = formatCqlQuery(query, queryParams, false);
            return handler.simpleNativeHandler().apply(
                    client.execute_cql_query(formattedQuery, key, Compression.NONE)
            );
        }

        @Override
        public <V> V execute(Object preparedStatementId, ByteBuffer key, List<ByteBuffer> queryParams, ResultHandler<V> handler) throws TException
        {
            Integer id = (Integer) preparedStatementId;
            return handler.simpleNativeHandler().apply(
                    client.execute_prepared_cql_query(id, key, queryParams)
            );
        }

        @Override
        public Object createPreparedStatement(String cqlQuery) throws TException
        {
            return client.prepare_cql_query(cqlQuery, Compression.NONE);
        }
    }

    // interface for building functions to standardise results from each client
    protected static interface ResultHandler<V>
    {
        Function<ResultSet, V> javaDriverHandler();
        Function<ResultMessage, V> thriftHandler();
        Function<CqlResult, V> simpleNativeHandler();
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
        public Function<ResultMessage, Integer> thriftHandler()
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

        @Override
        public Function<CqlResult, Integer> simpleNativeHandler()
        {
            return new Function<CqlResult, Integer>()
            {

                @Override
                public Integer apply(CqlResult result)
                {
                    switch (result.getType())
                    {
                        case ROWS:
                            return result.getRows().size();
                        default:
                            return 1;
                    }
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
                        return new ByteBuffer[0][];
                    List<Row> rows = result.all();

                    ByteBuffer[][] r = new ByteBuffer[rows.size()][];
                    for (int i = 0 ; i < r.length ; i++)
                    {
                        Row row = rows.get(i);
                        r[i] = new ByteBuffer[row.getColumnDefinitions().size() - 1];
                        for (int j = 1 ; j < row.getColumnDefinitions().size() ; j++)
                            r[i][j - 1] = row.getBytes(j);
                    }
                    return r;
                }
            };
        }

        @Override
        public Function<ResultMessage, ByteBuffer[][]> thriftHandler()
        {
            return new Function<ResultMessage, ByteBuffer[][]>()
            {

                @Override
                public ByteBuffer[][] apply(ResultMessage result)
                {
                    if (!(result instanceof ResultMessage.Rows))
                        return new ByteBuffer[0][];

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

        @Override
        public Function<CqlResult, ByteBuffer[][]> simpleNativeHandler()
        {
            return new Function<CqlResult, ByteBuffer[][]>()
            {

                @Override
                public ByteBuffer[][] apply(CqlResult result)
                {
                    ByteBuffer[][] r = new ByteBuffer[result.getRows().size()][];
                    for (int i = 0 ; i < r.length ; i++)
                    {
                        CqlRow row = result.getRows().get(i);
                        r[i] = new ByteBuffer[row.getColumns().size()];
                        for (int j = 0 ; j < r[i].length ; j++)
                            r[i][j] = ByteBuffer.wrap(row.getColumns().get(j).getValue());
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
                        return new byte[0][];
                    List<Row> rows = result.all();
                    byte[][] r = new byte[rows.size()][];
                    for (int i = 0 ; i < r.length ; i++)
                        r[i] = rows.get(i).getBytes(0).array();
                    return r;
                }
            };
        }

        @Override
        public Function<ResultMessage, byte[][]> thriftHandler()
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

        @Override
        public Function<CqlResult, byte[][]> simpleNativeHandler()
        {
            return new Function<CqlResult, byte[][]>()
            {

                @Override
                public byte[][] apply(CqlResult result)
                {
                    byte[][] r = new byte[result.getRows().size()][];
                    for (int i = 0 ; i < r.length ; i++)
                        r[i] = result.getRows().get(i).getKey();
                    return r;
                }
            };
        }

    }

    private static String getUnQuotedCqlBlob(ByteBuffer term, boolean isCQL3)
    {
        return isCQL3
                ? "0x" + ByteBufferUtil.bytesToHex(term)
                : ByteBufferUtil.bytesToHex(term);
    }

    /**
     * Constructs a CQL query string by replacing instances of the character
     * '?', with the corresponding parameter.
     *
     * @param query base query string to format
     * @param parms sequence of string query parameters
     * @return formatted CQL query string
     */
    private static String formatCqlQuery(String query, List<ByteBuffer> parms, boolean isCql3)
    {
        int marker, position = 0;
        StringBuilder result = new StringBuilder();

        if (-1 == (marker = query.indexOf('?')) || parms.size() == 0)
            return query;

        for (ByteBuffer parm : parms)
        {
            result.append(query.substring(position, marker));
            result.append(getUnQuotedCqlBlob(parm, isCql3));

            position = marker + 1;
            if (-1 == (marker = query.indexOf('?', position + 1)))
                break;
        }

        if (position < query.length())
            result.append(query.substring(position));

        return result.toString();
    }

    protected String wrapInQuotesIfRequired(String string)
    {
        return state.settings.mode.cqlVersion == CqlVersion.CQL3
                ? "\"" + string + "\""
                : string;
    }

}
