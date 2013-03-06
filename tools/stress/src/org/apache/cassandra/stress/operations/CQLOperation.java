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

import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.List;

import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.ThriftConversion;

public abstract class CQLOperation extends Operation
{
    public CQLOperation(Session client, int idx)
    {
        super(client, idx);
    }

    protected abstract void run(CQLQueryExecutor executor) throws IOException;

    protected abstract boolean validateThriftResult(CqlResult result);

    protected abstract boolean validateNativeResult(ResultMessage result);

    public void run(final CassandraClient client) throws IOException
    {
        run(new CQLQueryExecutor()
        {
            public boolean execute(String cqlQuery, List<String> queryParams) throws Exception
            {
                CqlResult result = null;
                if (session.usePreparedStatements())
                {
                    Integer stmntId = getPreparedStatement(client, cqlQuery);
                    if (session.cqlVersion.startsWith("3"))
                        result = client.execute_prepared_cql3_query(stmntId, queryParamsAsByteBuffer(queryParams), session.getConsistencyLevel());
                    else
                        result = client.execute_prepared_cql_query(stmntId, queryParamsAsByteBuffer(queryParams));
                }
                else
                {
                    String formattedQuery = formatCqlQuery(cqlQuery, queryParams);
                    if (session.cqlVersion.startsWith("3"))
                        result = client.execute_cql3_query(ByteBuffer.wrap(formattedQuery.getBytes()), Compression.NONE, session.getConsistencyLevel());
                    else
                        result = client.execute_cql_query(ByteBuffer.wrap(formattedQuery.getBytes()), Compression.NONE);
                }
                return validateThriftResult(result);
            }
        });
    }

    public void run(final SimpleClient client) throws IOException
    {
        run(new CQLQueryExecutor()
        {
            public boolean execute(String cqlQuery, List<String> queryParams) throws Exception
            {
                ResultMessage result = null;
                if (session.usePreparedStatements())
                {
                    byte[] stmntId = getPreparedStatement(client, cqlQuery);
                    result = client.executePrepared(stmntId, queryParamsAsByteBuffer(queryParams), ThriftConversion.fromThrift(session.getConsistencyLevel()));
                }
                else
                {
                    String formattedQuery = formatCqlQuery(cqlQuery, queryParams);
                    result = client.execute(formattedQuery, ThriftConversion.fromThrift(session.getConsistencyLevel()));
                }
                return validateNativeResult(result);
            }
        });
    }
}
