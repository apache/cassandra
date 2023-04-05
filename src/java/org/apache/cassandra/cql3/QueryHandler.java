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
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

public interface QueryHandler
{
    CQLStatement parse(String queryString, QueryState queryState, QueryOptions options);

    ResultMessage process(CQLStatement statement,
                          QueryState state,
                          QueryOptions options,
                          Map<String, ByteBuffer> customPayload,
                          long queryStartNanoTime) throws RequestExecutionException, RequestValidationException;

    ResultMessage.Prepared prepare(String query,
                                   ClientState clientState,
                                   Map<String, ByteBuffer> customPayload) throws RequestValidationException;

    QueryHandler.Prepared getPrepared(MD5Digest id);

    ResultMessage processPrepared(CQLStatement statement,
                                  QueryState state,
                                  QueryOptions options,
                                  Map<String, ByteBuffer> customPayload,
                                  long queryStartNanoTime) throws RequestExecutionException, RequestValidationException;

    ResultMessage processBatch(BatchStatement statement,
                               QueryState state,
                               BatchQueryOptions options,
                               Map<String, ByteBuffer> customPayload,
                               long queryStartNanoTime) throws RequestExecutionException, RequestValidationException;

    public static class Prepared
    {
        public final CQLStatement statement;

        public final MD5Digest resultMetadataId;

        /**
         * Contains the CQL statement source if the statement has been "regularly" perpared via
         * {@link QueryHandler#prepare(String, ClientState, Map)}.
         * Other usages of this class may or may not contain the CQL statement source.
         */
        public final String rawCQLStatement;
        public final String keyspace;
        public final boolean fullyQualified;

        public Prepared(CQLStatement statement, String rawCQLStatement, boolean fullyQualified, String keyspace)
        {
            this.statement = statement;
            this.rawCQLStatement = rawCQLStatement;
            this.resultMetadataId = ResultSet.ResultMetadata.fromPrepared(statement).getResultMetadataId();
            this.fullyQualified = fullyQualified;
            this.keyspace = keyspace;
        }
    }
}
