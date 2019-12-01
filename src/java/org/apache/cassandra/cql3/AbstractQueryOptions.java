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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.ProtocolVersion;

// todo: YIFAN all methods are implemented, why abstract???
public abstract class AbstractQueryOptions implements QueryOptions
{
    protected final ConsistencyLevel consistency;
    protected final List<ByteBuffer> values;
    protected final boolean skipMetadata;
    protected final SpecificOptions specificOptions;
    protected final int pageSize;
    protected final PagingState state;
    protected final ConsistencyLevel serialConsistency;
    protected final long timestamp;
    protected final String keyspace;
    protected final int nowInSeconds;
    protected final int timeoutInMillis; // defaults to Integer.MAX_VALUE, if the flag was not set.
    protected final ProtocolVersion protocolVersion;

    // A cache of bind values parsed as JSON, see getJsonColumnValue for details.
    private List<Map<ColumnIdentifier, Term>> jsonValuesCache;

    AbstractQueryOptions(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, SpecificOptions specificOptions, ProtocolVersion protocolVersion)
    {
        this.consistency = consistency;
        this.values = values;
        this.skipMetadata = skipMetadata;
        this.specificOptions = specificOptions;
        this.pageSize = specificOptions.pageSize;
        this.state = specificOptions.state;
        this.serialConsistency = specificOptions.serialConsistency == null ? ConsistencyLevel.SERIAL : specificOptions.serialConsistency;
        this.timestamp = specificOptions.timestamp;
        this.keyspace = specificOptions.keyspace;
        this.nowInSeconds = specificOptions.nowInSeconds;
        this.timeoutInMillis = specificOptions.timeoutInMillis;
        this.protocolVersion = protocolVersion;
    }

    AbstractQueryOptions(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, ProtocolVersion protocolVersion)
    {
        this(consistency, values, skipMetadata, SpecificOptions.DEFAULT, protocolVersion);
    }

    AbstractQueryOptions(QueryOptions queryOptions)
    {
        this(queryOptions.getConsistency(),
             queryOptions.getValues(),
             queryOptions.skipMetadata(),
             new SpecificOptions(queryOptions),
             queryOptions.getProtocolVersion());
    }

    public ConsistencyLevel getConsistency()
    {
        return consistency;
    }

    public List<ByteBuffer> getValues()
    {
        return values;
    }

    public boolean skipMetadata()
    {
        return skipMetadata;
    }

    /**  The pageSize for this query. Will be {@code <= 0} if not relevant for the query.  */
    public int getPageSize()
    {
        return pageSize;
    }

    /** The paging state for this query, or null if not relevant. */
    public PagingState getPagingState()
    {
        return state;
    }

    /**  Serial consistency for conditional updates. */
    public ConsistencyLevel getSerialConsistency()
    {
        return serialConsistency;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public int getNowInSeconds()
    {
        return nowInSeconds;
    }

    public int getTimeoutInMillis()
    {
        return timeoutInMillis;
    }

    /** The keyspace that this query is bound to, or null if not relevant. */
    public String getKeyspace()
    {
        return keyspace;
    }

    /**
     * The protocol version for the query.
     */
    public ProtocolVersion getProtocolVersion()
    {
        return protocolVersion;
    }

    public SpecificOptions getSpecificOptions()
    {
        return specificOptions;
    }

    public QueryOptions prepare(List<ColumnSpecification> specs)
    {
        return this;
    }

    /**
     * Tells whether or not this <code>QueryOptions</code> contains the column specifications for the bound variables.
     * <p>The column specifications will be present only for prepared statements.</p>
     * @return <code>true</code> this <code>QueryOptions</code> contains the column specifications for the bound
     * variables, <code>false</code> otherwise.
     */
    public boolean hasColumnSpecifications()
    {
        return false;
    }

    /**
     * Returns the column specifications for the bound variables (<i>optional operation</i>).
     *
     * <p>The column specifications will be present only for prepared statements.</p>
     *
     * <p>Invoke the {@link #hasColumnSpecifications} method before invoking this method in order to ensure that this
     * <code>QueryOptions</code> contains the column specifications.</p>
     *
     * @return the option names
     * @throws UnsupportedOperationException If this <code>QueryOptions</code> does not contains the column
     * specifications.
     */
    public ImmutableList<ColumnSpecification> getColumnSpecifications()
    {
        throw new UnsupportedOperationException();
    }

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
    public Term getJsonColumnValue(int bindIndex, ColumnIdentifier columnName, Collection<ColumnMetadata> expectedReceivers) throws InvalidRequestException
    {
        if (jsonValuesCache == null)
            jsonValuesCache = new ArrayList<>(Collections.<Map<ColumnIdentifier, Term>>nCopies(getValues().size(), null));

        Map<ColumnIdentifier, Term> jsonValue = jsonValuesCache.get(bindIndex);
        if (jsonValue == null)
        {
            ByteBuffer value = getValues().get(bindIndex);
            if (value == null)
                throw new InvalidRequestException("Got null for INSERT JSON values");

            jsonValue = Json.parseJson(UTF8Type.instance.getSerializer().deserialize(value), expectedReceivers);
            jsonValuesCache.set(bindIndex, jsonValue);
        }

        return jsonValue.get(columnName);
    }

    // Options that are likely to not be present in most queries
    static class SpecificOptions
    {
        private static final SpecificOptions DEFAULT = new SpecificOptions(-1, null, null, Long.MIN_VALUE, null, Integer.MIN_VALUE, Integer.MAX_VALUE);

        private final int pageSize;
        private final PagingState state;
        private final ConsistencyLevel serialConsistency;
        private final long timestamp;
        private final String keyspace;
        private final int nowInSeconds;
        private final int timeoutInMillis; // defaults to Integer.MAX_VALUE, if the flag was not set.

        protected SpecificOptions(int pageSize,
                                  PagingState state,
                                  ConsistencyLevel serialConsistency,
                                  long timestamp,
                                  String keyspace,
                                  int nowInSeconds,
                                  int timeoutInMillis)
        {
            this.pageSize = pageSize;
            this.state = state;
            this.serialConsistency = serialConsistency == null ? ConsistencyLevel.SERIAL : serialConsistency;
            this.timestamp = timestamp;
            this.keyspace = keyspace;
            this.nowInSeconds = nowInSeconds;
            this.timeoutInMillis = timeoutInMillis;
        }

        private SpecificOptions(QueryOptions queryOptions)
        {
            this(queryOptions.getPageSize(),
                 queryOptions.getPagingState(),
                 queryOptions.getSerialConsistency(),
                 queryOptions.getTimestamp(),
                 queryOptions.getKeyspace(),
                 queryOptions.getNowInSeconds(),
                 queryOptions.getTimeoutInMillis());
        }
    }
}
