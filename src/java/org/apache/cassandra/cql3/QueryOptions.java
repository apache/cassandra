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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Options for a query.
 */
public interface QueryOptions
{
    ConsistencyLevel getConsistency();
    List<ByteBuffer> getValues();
    boolean skipMetadata();
    int getPageSize();
    PagingState getPagingState();
    ConsistencyLevel getSerialConsistency();
    long getTimestamp();
    String getKeyspace();
    int getNowInSeconds();
    int getTimeoutInMillis();
    ProtocolVersion getProtocolVersion();
    QueryOptions prepare(List<ColumnSpecification> specs);

    default long calculateTimeout(ToLongFunction<TimeUnit> configuredTimeout, TimeUnit timeUnit)
    {
        return Math.min(timeUnit.convert(getTimeoutInMillis(), TimeUnit.MILLISECONDS),
                        configuredTimeout.applyAsLong(timeUnit));
    }
}
