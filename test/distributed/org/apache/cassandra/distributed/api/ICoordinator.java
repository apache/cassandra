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

package org.apache.cassandra.distributed.api;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.cassandra.distributed.shared.FutureUtils;

// The cross-version API requires that a Coordinator can be constructed without any constructor arguments
public interface ICoordinator
{
    default Object[][] execute(String query, ConsistencyLevel consistencyLevel, Object... boundValues)
    {
        return executeWithResult(query, consistencyLevel, boundValues).toObjectArrays();
    }

    default Object[][] execute(String query, ConsistencyLevel serialConsistencyLevel, ConsistencyLevel commitConsistencyLevel, Object... boundValues)
    {
        return executeWithResult(query, serialConsistencyLevel, commitConsistencyLevel, boundValues).toObjectArrays();
    }

    SimpleQueryResult executeWithResult(String query, ConsistencyLevel consistencyLevel, Object... boundValues);

    default SimpleQueryResult executeWithResult(String query, ConsistencyLevel serialConsistencyLevel, ConsistencyLevel commitConsistencyLevel, Object... boundValues)
    {
        throw new UnsupportedOperationException();
    }

    default Iterator<Object[]> executeWithPaging(String query, ConsistencyLevel consistencyLevel, int pageSize, Object... boundValues)
    {
        return executeWithPagingWithResult(query, consistencyLevel, pageSize, boundValues).map(Row::toObjectArray);
    }

    QueryResult executeWithPagingWithResult(String query, ConsistencyLevel consistencyLevel, int pageSize, Object... boundValues);

    default Future<Object[][]> asyncExecuteWithTracing(UUID sessionId, String query, ConsistencyLevel consistencyLevel, Object... boundValues)
    {
        return FutureUtils.map(asyncExecuteWithTracingWithResult(sessionId, query, consistencyLevel, boundValues), r -> r.toObjectArrays());
    }

    Future<SimpleQueryResult> asyncExecuteWithTracingWithResult(UUID sessionId, String query, ConsistencyLevel consistencyLevel, Object... boundValues);

    default Object[][] executeWithTracing(UUID sessionId, String query, ConsistencyLevel consistencyLevel, Object... boundValues)
    {
        return executeWithTracingWithResult(sessionId, query, consistencyLevel, boundValues).toObjectArrays();
    }

    default SimpleQueryResult executeWithTracingWithResult(UUID sessionId, String query, ConsistencyLevel consistencyLevel, Object... boundValues)
    {
        return FutureUtils.waitOn(asyncExecuteWithTracingWithResult(sessionId, query, consistencyLevel, boundValues));
    }

    IInstance instance();
}
