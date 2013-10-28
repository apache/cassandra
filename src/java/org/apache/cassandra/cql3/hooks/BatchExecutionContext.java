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
package org.apache.cassandra.cql3.hooks;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.service.QueryState;

/**
 * Contextual information about the execution of a CQL Batch.
 * Used by {@link org.apache.cassandra.cql3.hooks.PreExecutionHook} and
 * {@link org.apache.cassandra.cql3.hooks.PostExecutionHook}
 *
 * The {@code queryOrIdList} field, provides a list of objects which
 * may be used to identify the individual statements in the batch.
 * Currently, these objects will be one of two types (and the list may
 * contain a mixture of the two). A {@code String} indicates the statement is
 * a regular (i.e. non-prepared) statement, and is in fact the CQL
 * string for the statement. An {@code MD5Digest} object indicates a prepared
 * statement & may be used to retrieve the corresponding CQLStatement
 * using {@link org.apache.cassandra.cql3.QueryProcessor#getPrepared(org.apache.cassandra.utils.MD5Digest) QueryProcessor.getPrepared()}
 *
 */
public class BatchExecutionContext
{
    public final QueryState queryState;
    public final List<Object> queryOrIdList;
    public final List<List<ByteBuffer>> variables;

    public BatchExecutionContext(QueryState queryState, List<Object> queryOrIdList, List<List<ByteBuffer>> variables)
    {
        this.queryState = queryState;
        this.queryOrIdList = queryOrIdList;
        this.variables = variables;
    }
}
