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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Client-specified options for a query.
 * Most of the getters are correspoding to the {@code query_options} as stated in the native_protocol specification
 */
public interface QueryOptions
{
    /**
     * @return the consistency level of the query
     */
    ConsistencyLevel getConsistency();

    /**
     * @return the list of values that are used for bound variables in the query.
     */
    List<ByteBuffer> getValues();

    /**
     * @return true if the SKIP_METADATA flag was set by client, otherwsie false.
     */
    boolean skipMetadata();

    /**
     * @return the pageSize for this query. Will be {@code <= 0} if not relevant for the query.
     */
    int getPageSize();

    /**
     * @return the paging state for this query, or null if not relevant.
     */
    PagingState getPagingState();

    /**
     * @return serial consistency for conditional updates.
     */
    ConsistencyLevel getSerialConsistency();

    /**
     * @return the client assigned timestamp in microseconds.
     */
    long getTimestamp();

    /**
     * @return the keyspace that the query should be executed in.
     */
    String getKeyspace();

    /**
     * @return the current time (now) in seconds for the query.
     */
    int getNowInSeconds();

    /**
     * @return the optional custom timeout in milliseconds for the query, otherwise defaults to {@code Integer.MAX_VALUE}
     */
    int getTimeoutInMillis();

    /**
     * @return the protocol version for the query.
     */
    ProtocolVersion getProtocolVersion();

    /**
     * Prepare the values for the query when {@code Flag#NAMES_FOR_VALUES} is present
     */
    QueryOptions prepare(List<ColumnSpecification> specs);

    /**
     * Tells whether or not this {@link QueryOptions} contains the column specifications for the bound variables.
     * The column specifications will be present only for prepared statements.
     * @return {@code true} this {@link QueryOptions} contains the column specifications for the bound
     * variables, {@code false} otherwise.
     */
    boolean hasColumnSpecifications();

    /**
     * Returns the column specifications for the bound variables (<i>optional operation</i>).
     *
     * The column specifications will be present only for prepared statements.
     *
     * Invoke the {@link #hasColumnSpecifications} method before invoking this method in order to ensure that this
     * {@link QueryOptions} contains the column specifications.
     *
     * @return the option names
     * @throws UnsupportedOperationException If this {@link QueryOptions} does not contains the column
     * specifications.
     */
    ImmutableList<ColumnSpecification> getColumnSpecifications();

    /**
     * Returns the term corresponding to column {@code columnName} in the JSON value of bind index {@code bindIndex}.
     *
     * This is functionally equivalent to:
     *   {@code Json.parseJson(UTF8Type.instance.getSerializer().deserialize(getValues().get(bindIndex)), expectedReceivers).get(columnName)}
     * but this caches the result of parsing the JSON, so that while this might be called for multiple columns on the same {@code bindIndex}
     * value, the underlying JSON value is only parsed/processed once.
     *
     * Note: this is a bit more involved in CQL specifics than this class generally is, but as we need to cache this per-query and in an object
     * that is available when we bind values, this is the easiest place to have this.
     *
     * @param bindIndex the index of the bind value that should be interpreted as a JSON value.
     * @param columnName the name of the column we want the value of.
     * @param expectedReceivers the columns expected in the JSON value at index {@code bindIndex}. This is only used when parsing the
     * json initially and no check is done afterwards. So in practice, any call of this method on the same QueryOptions object and with the same
     * {@code bindIndx} values should use the same value for this parameter, but this isn't validated in any way.
     *
     * @return the value correspong to column {@code columnName} in the (JSON) bind value at index {@code bindIndex}. This may return null if the
     * JSON value has no value for this column.
     */
    Term getJsonColumnValue(int bindIndex, ColumnIdentifier columnName, Collection<ColumnMetadata> expectedReceivers) throws InvalidRequestException;

    /**
     * Calculate the actual timeout by comparing and getting the minimum value between
     * the custom timeout {@link #getTimeoutInMillis()} and the one configured at {@link org.apache.cassandra.config.DatabaseDescriptor}
     * Effectively, it caps the custom timeout at most to the configured one.
     * @param configuredTimeout, function to get the configured timeout in the desired {@code timeUnit}
     * @param timeUnit, desired timeUnit to convert the timeout value to.
     * @return timeout value in the desired {@code timeUnit}
     */
    default long calculateTimeout(ToLongFunction<TimeUnit> configuredTimeout, TimeUnit timeUnit)
    {
        return Math.min(timeUnit.convert(getTimeoutInMillis(), TimeUnit.MILLISECONDS),
                        configuredTimeout.applyAsLong(timeUnit));
    }

    default long getTimestampWithFallback(QueryState state)
    {
        long tstamp = getTimestamp();
        return tstamp != Long.MIN_VALUE ? tstamp : state.getTimestamp();
    }

    default int getNowInSecondsWithFallback(QueryState state)
    {
        int nowInSeconds = getNowInSeconds();
        return nowInSeconds != Integer.MIN_VALUE ? nowInSeconds : state.getNowInSeconds();
    }
}
