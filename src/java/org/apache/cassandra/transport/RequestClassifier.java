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

package org.apache.cassandra.transport;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.BatchMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;

public class RequestClassifier
{
    public static RequestMetadata classify(Message.Request request)
    {
        if (request instanceof ExecuteMessage execute)
        {
            QueryHandler.Prepared prepared = QueryProcessor.instance.getPrepared(execute.statementId);
            if (prepared == null)
                return RequestMetadata.UNKNOWN;

            CQLStatement statement = prepared.statement;
            RequestKind kind = classifyStatement(statement);

            ByteBuffer routingKey = null;
            if (kind == RequestKind.LWT)
            {
                execute.options.prepare(statement.getBindVariables());

                if (statement instanceof ModificationStatement mod)
                {
                    List<ByteBuffer> keys = mod.buildPartitionKeyNames(execute.options, ClientState.forInternalCalls());
                    if (keys != null && !keys.isEmpty())
                        routingKey = keys.get(0);
                }
                else if (statement instanceof BatchStatement batch)
                {
                    List<ModificationStatement> statements = batch.getStatements();
                    if (statements != null && !statements.isEmpty())
                    {
                        List<ByteBuffer> keys = statements.get(0).buildPartitionKeyNames(execute.options, ClientState.forInternalCalls());
                        if (keys != null && !keys.isEmpty())
                            routingKey = keys.get(0);
                    }
                }
            }

            return new RequestMetadata(kind, routingKey);
        }

        if (request instanceof BatchMessage)
        {
            return new RequestMetadata(RequestKind.WRITE, null);
        }

        return RequestMetadata.UNKNOWN;
    }

    private static RequestKind classifyStatement(CQLStatement statement)
    {
        if (statement instanceof SelectStatement)
            return RequestKind.READ;

        if (statement instanceof ModificationStatement mod)
            return mod.hasConditions() ? RequestKind.LWT : RequestKind.WRITE;

        if (statement instanceof BatchStatement batch)
            return batch.hasConditions() ? RequestKind.LWT : RequestKind.WRITE;

        return RequestKind.UNKNOWN;
    }

    public static class RequestMetadata
    {
        public static final RequestMetadata UNKNOWN = new RequestMetadata(RequestKind.UNKNOWN, null);

        public final RequestKind kind;
        public final ByteBuffer partitionKey;

        public RequestMetadata(RequestKind kind, ByteBuffer partitionKey)
        {
            this.kind = kind;
            this.partitionKey = partitionKey;
        }

        public boolean isLWT()
        {
            return kind == RequestKind.LWT;
        }

        @Override
        public String toString()
        {
            return "RequestMetadata{kind=" + kind + ", hasKey=" + (partitionKey != null) + '}';
        }
    }

    public enum RequestKind
    {
        READ,
        WRITE,
        LWT,
        TRANSACTION,
        // If the request is unprepared query, we don't know the kind of request, so we classify it as UNKNOWN
        UNKNOWN;
    }
}
