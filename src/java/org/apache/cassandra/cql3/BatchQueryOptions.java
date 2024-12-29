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
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.utils.MD5Digest;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.QueryState;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static org.apache.cassandra.utils.ByteArrayUtil.convertToByteBufferValue;

public abstract class BatchQueryOptions
{
    public static BatchQueryOptions DEFAULT = withoutPerStatementVariables(QueryOptions.DEFAULT);

    protected final QueryOptions wrapped;
    private final List<Object> queryOrIdList;

    protected BatchQueryOptions(QueryOptions wrapped, List<Object> queryOrIdList)
    {
        this.wrapped = wrapped;
        this.queryOrIdList = queryOrIdList;
    }

    public static BatchQueryOptions withoutPerStatementVariables(QueryOptions options)
    {
        return new WithoutPerStatementVariables(options, Collections.<Object>emptyList());
    }

    public static BatchQueryOptions withPerStatementVariables(QueryOptions options, List<byte[][]> variables, List<Object> queryOrIdList)
    {
        return new WithPerStatementVariables(options, variables, queryOrIdList);
    }

    public abstract QueryOptions forStatement(int i);

    public void prepareStatement(int i, ImmutableList<ColumnSpecification> boundNames)
    {
        forStatement(i).prepare(boundNames);
    }

    public ConsistencyLevel getConsistency()
    {
        return wrapped.getConsistency();
    }

    public String getKeyspace()
    {
        return wrapped.getKeyspace();
    }

    public ConsistencyLevel getSerialConsistency()
    {
        return wrapped.getSerialConsistency();
    }

    public List<Object> getQueryOrIdList()
    {
        return queryOrIdList;
    }

    public long getTimestamp(QueryState state)
    {
        return wrapped.getTimestamp(state);
    }

    public long getNowInSeconds(QueryState state)
    {
        return wrapped.getNowInSeconds(state);
    }

    private static class BatchQueryOptionsWrapper extends QueryOptions.QueryOptionsWrapper {
        private final byte[][] valuesAsByteArray;
        private List<ByteBuffer> values; // initialized on demand

        BatchQueryOptionsWrapper(QueryOptions wrapped, byte[][] vars)
        {
            super(wrapped);
            this.valuesAsByteArray = vars;
        }
        public List<ByteBuffer> getValues()
        {
            if (values == null)
            {
                values = new ArrayList<>(valuesAsByteArray.length);
                for (byte[] byteArrayValue : valuesAsByteArray)
                    values.add(convertToByteBufferValue(byteArrayValue));
            }
            return values;
        }

        public int getValuesSize()
        {
            return valuesAsByteArray.length;
        }

        public ByteBuffer getValue(int index)
        {
            if (values == null) // we convert values to ByteBuffer in a lazy way, on demand
                return convertToByteBufferValue(valuesAsByteArray[index]);
            else
                return values.get(index);
        }

        public boolean isByteArrayValuesGetSupported()
        {
            return true;
        }

        public byte[][] getByteArrayValues()
        {
            return valuesAsByteArray;
        }
    }

    private static class WithoutPerStatementVariables extends BatchQueryOptions
    {
        private WithoutPerStatementVariables(QueryOptions wrapped, List<Object> queryOrIdList)
        {
            super(wrapped, queryOrIdList);
        }

        public QueryOptions forStatement(int i)
        {
            return wrapped;
        }
    }

    private static class WithPerStatementVariables extends BatchQueryOptions
    {
        private final List<QueryOptions> perStatementOptions;

        private WithPerStatementVariables(QueryOptions wrapped, List<byte[][]> variables, List<Object> queryOrIdList)
        {
            super(wrapped, queryOrIdList);
            this.perStatementOptions = new ArrayList<>(variables.size());
            for (final byte[][] vars : variables)
                perStatementOptions.add(new BatchQueryOptionsWrapper(wrapped, vars));
        }

        public QueryOptions forStatement(int i)
        {
            return perStatementOptions.get(i);
        }

        @Override
        public void prepareStatement(int i, ImmutableList<ColumnSpecification> boundNames)
        {
            if (isPreparedStatement(i))
            {
                QueryOptions options = perStatementOptions.get(i);
                options.prepare(boundNames);
                options = QueryOptions.addColumnSpecifications(options, boundNames);
                perStatementOptions.set(i, options);
            }
            else
            {
                super.prepareStatement(i, boundNames);
            }
        }

        private boolean isPreparedStatement(int i)
        {
            return getQueryOrIdList().get(i) instanceof MD5Digest;
        }
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
