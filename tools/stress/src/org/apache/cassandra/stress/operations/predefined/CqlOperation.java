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
import org.apache.cassandra.stress.settings.Command;
import org.apache.cassandra.stress.settings.ConnectionStyle;
import org.apache.cassandra.stress.settings.CqlVersion;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;

public abstract class CqlOperation<V> extends PredefinedOperation
{

    protected abstract List<Object> getQueryParameters(byte[] key);
    protected abstract String buildQuery();
    protected abstract CqlRunOp<V> buildRunOp(ClientWrapper client, String query, Object queryId, List<Object> params, ByteBuffer key);

    public CqlOperation(Command type, Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings)
    {
        super(type, timer, generator, seedManager, settings);
        if (settings.columns.variableColumnCount)
            throw new IllegalStateException("Variable column counts are not implemented for CQL");
    }

    protected CqlRunOp<V> run(final ClientWrapper client, final List<Object> queryParams, final ByteBuffer key) throws IOException
    {
        final CqlRunOp<V> op;
        if (settings.mode.style == ConnectionStyle.CQL_PREPARED)
        {
            final Object id;
            Object idobj = getCqlCache();
            if (idobj == null)
            {
                try
                {
                    id = client.createPreparedStatement(buildQuery());
                } catch (TException e)
                {
                    throw new RuntimeException(e);
                }
                storeCqlCache(id);
            }
            else
                id = idobj;

            op = buildRunOp(client, null, id, queryParams, key);
        }
        else
        {
            final String query;
            Object qobj = getCqlCache();
            if (qobj == null)
                storeCqlCache(query = buildQuery());
            else
                query = qobj.toString();

            op = buildRunOp(client, query, null, queryParams, key);
        }

        timeWithRetry(op);
        return op;
    }

    protected void run(final ClientWrapper client) throws IOException
    {
        final byte[] key = getKey().array();
        final List<Object> queryParams = getQueryParameters(key);
        run(client, queryParams, ByteBuffer.wrap(key));
    }

    // Classes to process Cql results

    // Always succeeds so long as the query executes without error; provides a keyCount to increment on instantiation
    protected final class CqlRunOpAlwaysSucceed extends CqlRunOp<Integer>
    {

        final int keyCount;

        protected CqlRunOpAlwaysSucceed(ClientWrapper client, String query, Object queryId, List<Object> params, ByteBuffer key, int keyCount)
        {
            super(client, query, queryId, RowCountHandler.INSTANCE, params, key);
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

        protected CqlRunOpTestNonEmpty(ClientWrapper client, String query, Object queryId, List<Object> params, ByteBuffer key)
        {
            super(client, query, queryId, RowCountHandler.INSTANCE, params, key);
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
        protected CqlRunOpMatchResults(ClientWrapper client, String query, Object queryId, List<Object> params, ByteBuffer key, List<List<ByteBuffer>> expect)
        {
            super(client, query, queryId, RowsHandler.INSTANCE, params, key);
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
            if (result.length != expect.size())
                return false;
            for (int i = 0 ; i < result.length ; i++)
                if (expect.get(i) != null && !expect.get(i).equals(Arrays.asList(result[i])))
                    return false;
            return true;
        }
    }

    // Cql
    protected abstract class CqlRunOp<V> implements RunOp
    {

        final ClientWrapper client;
        final String query;
        final Object queryId;
        final List<Object> params;
        final ByteBuffer key;
        final ResultHandler<V> handler;
        V result;

        private CqlRunOp(ClientWrapper client, String query, Object queryId, ResultHandler<V> handler, List<Object> params, ByteBuffer key)
        {
            this.client = client;
            this.query = query;
            this.queryId = queryId;
            this.handler = handler;
            this.params = params;
            this.key = key;
        }

        @Override
        public boolean run() throws Exception
        {
            return queryId != null
            ? validate(result = client.execute(queryId, key, params, handler))
            : validate(result = client.execute(query, key, params, handler));
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
        return isCql3()
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
        <V> V execute(Object preparedStatementId, ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler) throws TException;
        <V> V execute(String query, ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler) throws TException;
    }

    private final class JavaDriverWrapper implements ClientWrapper
    {
        final JavaDriverClient client;
        private JavaDriverWrapper(JavaDriverClient client)
        {
            this.client = client;
        }

        @Override
        public <V> V execute(String query, ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler)
        {
            String formattedQuery = formatCqlQuery(query, queryParams, isCql3());
            return handler.javaDriverHandler().apply(client.execute(formattedQuery, ThriftConversion.fromThrift(settings.command.consistencyLevel)));
        }

        @Override
        public <V> V execute(Object preparedStatementId, ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler)
        {
            return handler.javaDriverHandler().apply(
                    client.executePrepared(
                            (PreparedStatement) preparedStatementId,
                            queryParams,
                            ThriftConversion.fromThrift(settings.command.consistencyLevel)));
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
        public <V> V execute(String query, ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler)
        {
            String formattedQuery = formatCqlQuery(query, queryParams, isCql3());
            return handler.thriftHandler().apply(client.execute(formattedQuery, ThriftConversion.fromThrift(settings.command.consistencyLevel)));
        }

        @Override
        public <V> V execute(Object preparedStatementId, ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler)
        {
            return handler.thriftHandler().apply(
                    client.executePrepared(
                            (byte[]) preparedStatementId,
                            toByteBufferParams(queryParams),
                            ThriftConversion.fromThrift(settings.command.consistencyLevel)));
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
        public <V> V execute(String query, ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler) throws TException
        {
            String formattedQuery = formatCqlQuery(query, queryParams, true);
            return handler.simpleNativeHandler().apply(
                    client.execute_cql3_query(formattedQuery, key, Compression.NONE, settings.command.consistencyLevel)
            );
        }

        @Override
        public <V> V execute(Object preparedStatementId, ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler) throws TException
        {
            Integer id = (Integer) preparedStatementId;
            return handler.simpleNativeHandler().apply(
                    client.execute_prepared_cql3_query(id, key, toByteBufferParams(queryParams), settings.command.consistencyLevel)
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
        public <V> V execute(String query, ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler) throws TException
        {
            String formattedQuery = formatCqlQuery(query, queryParams, false);
            return handler.simpleNativeHandler().apply(
                    client.execute_cql_query(formattedQuery, key, Compression.NONE)
            );
        }

        @Override
        public <V> V execute(Object preparedStatementId, ByteBuffer key, List<Object> queryParams, ResultHandler<V> handler) throws TException
        {
            Integer id = (Integer) preparedStatementId;
            return handler.simpleNativeHandler().apply(
                    client.execute_prepared_cql_query(id, key, toByteBufferParams(queryParams))
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
                        r[i] = new ByteBuffer[row.getColumnDefinitions().size()];
                        for (int j = 0 ; j < row.getColumnDefinitions().size() ; j++)
                            r[i][j] = row.getBytes(j);
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
    private static String formatCqlQuery(String query, List<Object> parms, boolean isCql3)
    {
        int marker, position = 0;
        StringBuilder result = new StringBuilder();

        if (-1 == (marker = query.indexOf('?')) || parms.size() == 0)
            return query;

        for (Object parm : parms)
        {
            result.append(query.substring(position, marker));
            if (parm instanceof ByteBuffer)
                result.append(getUnQuotedCqlBlob((ByteBuffer) parm, isCql3));
            else if (parm instanceof Long)
                result.append(parm.toString());
            else throw new AssertionError();

            position = marker + 1;
            if (-1 == (marker = query.indexOf('?', position + 1)))
                break;
        }

        if (position < query.length())
            result.append(query.substring(position));

        return result.toString();
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

    protected String wrapInQuotesIfRequired(String string)
    {
        return settings.mode.cqlVersion == CqlVersion.CQL3
                ? "\"" + string + "\""
                : string;
    }

}
