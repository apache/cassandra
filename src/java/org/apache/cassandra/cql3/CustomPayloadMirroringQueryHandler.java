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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

/**
 * Custom QueryHandler that sends custom request payloads back with the result.
 * Used to facilitate testing.
 * Enabled with system property cassandra.custom_query_handler_class.
 */
public class CustomPayloadMirroringQueryHandler implements QueryHandler
{
    static QueryProcessor queryProcessor = QueryProcessor.instance;

    public ResultMessage process(String query,
                                 QueryState state,
                                 QueryOptions options,
                                 Map<String, ByteBuffer> customPayload)
    {
        ResultMessage result = queryProcessor.process(query, state, options, customPayload);
        result.setCustomPayload(customPayload);
        return result;
    }

    public ResultMessage.Prepared prepare(String query, QueryState state, Map<String, ByteBuffer> customPayload)
    {
        ResultMessage.Prepared prepared = queryProcessor.prepare(query, state, customPayload);
        prepared.setCustomPayload(customPayload);
        return prepared;
    }

    public ParsedStatement.Prepared getPrepared(MD5Digest id)
    {
        return queryProcessor.getPrepared(id);
    }

    public ParsedStatement.Prepared getPreparedForThrift(Integer id)
    {
        return queryProcessor.getPreparedForThrift(id);
    }

    public ResultMessage processPrepared(CQLStatement statement,
                                         QueryState state,
                                         QueryOptions options,
                                         Map<String, ByteBuffer> customPayload)
    {
        ResultMessage result = queryProcessor.processPrepared(statement, state, options, customPayload);
        result.setCustomPayload(customPayload);
        return result;
    }

    public ResultMessage processBatch(BatchStatement statement,
                                      QueryState state,
                                      BatchQueryOptions options,
                                      Map<String, ByteBuffer> customPayload)
    {
        ResultMessage result = queryProcessor.processBatch(statement, state, options, customPayload);
        result.setCustomPayload(customPayload);
        return result;
    }
}
