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

package org.apache.cassandra.tools.fqltool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.audit.FullQueryLogger;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.binlog.BinLog;

public abstract class FQLQuery implements Comparable<FQLQuery>
{
    public final long queryTime;
    public final QueryOptions queryOptions;
    public final int protocolVersion;
    public final String keyspace;

    public FQLQuery(String keyspace, int protocolVersion, QueryOptions queryOptions, long queryTime)
    {
        this.queryTime = queryTime;
        this.queryOptions = queryOptions;
        this.protocolVersion = protocolVersion;
        this.keyspace = keyspace;
    }

    public abstract ListenableFuture<ResultHandler.ComparableResultSet> execute(Session session);

    /**
     * used when storing the queries executed
     */
    public abstract BinLog.ReleaseableWriteMarshallable toMarshallable();

    /**
     * Make sure we catch any query errors
     *
     * On error, this creates a failed ComparableResultSet with the exception set to be able to store
     * this fact in the result file and handle comparison of failed result sets.
     */
    ListenableFuture<ResultHandler.ComparableResultSet> handleErrors(ListenableFuture<ResultSet> result)
    {
        FluentFuture<ResultHandler.ComparableResultSet> fluentFuture = FluentFuture.from(result)
                                                                                   .transform(DriverResultSet::new, MoreExecutors.directExecutor());
        return fluentFuture.catching(Throwable.class, DriverResultSet::failed, MoreExecutors.directExecutor());
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof FQLQuery)) return false;
        FQLQuery fqlQuery = (FQLQuery) o;
        return queryTime == fqlQuery.queryTime &&
               protocolVersion == fqlQuery.protocolVersion &&
               queryOptions.getValues().equals(fqlQuery.queryOptions.getValues()) &&
               Objects.equals(keyspace, fqlQuery.keyspace);
    }

    public int hashCode()
    {
        return Objects.hash(queryTime, queryOptions, protocolVersion, keyspace);
    }

    public int compareTo(FQLQuery other)
    {
        return Longs.compare(queryTime, other.queryTime);
    }

    public static class Single extends FQLQuery
    {
        public final String query;
        public final List<ByteBuffer> values;

        public Single(String keyspace, int protocolVersion, QueryOptions queryOptions, long queryTime, String queryString, List<ByteBuffer> values)
        {
            super(keyspace, protocolVersion, queryOptions, queryTime);
            this.query = queryString;
            this.values = values;
        }

        @Override
        public String toString()
        {
            return String.format("Query = %s, Options = %s, Values = %s",
                                 query,
                                 queryOptions,
                                 values.stream().map(ByteBufferUtil::bytesToHex).collect(Collectors.joining(",")));
        }

        public ListenableFuture<ResultHandler.ComparableResultSet> execute(Session session)
        {
            SimpleStatement ss = new SimpleStatement(query, values.toArray());
            ss.setConsistencyLevel(ConsistencyLevel.valueOf(queryOptions.getConsistency().name()));
            ListenableFuture<ResultSet> future = session.executeAsync(ss);
            return handleErrors(future);
        }

        public BinLog.ReleaseableWriteMarshallable toMarshallable()
        {
            return new FullQueryLogger.WeighableMarshallableQuery(query, keyspace, queryOptions, queryTime);
        }

        public int compareTo(FQLQuery other)
        {
            int cmp = super.compareTo(other);

            if (cmp == 0)
            {
                if (other instanceof Batch)
                    return -1;

                Single singleQuery = (Single) other;

                cmp = query.compareTo(singleQuery.query);
                if (cmp == 0)
                {
                    if (values.size() != singleQuery.values.size())
                        return values.size() - singleQuery.values.size();
                    for (int i = 0; i < values.size(); i++)
                    {
                        cmp = values.get(i).compareTo(singleQuery.values.get(i));
                        if (cmp != 0)
                            return cmp;
                    }
                }
            }
            return cmp;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof Single)) return false;
            if (!super.equals(o)) return false;
            Single single = (Single) o;
            return Objects.equals(query, single.query) &&
                   Objects.equals(values, single.values);
        }

        public int hashCode()
        {
            return Objects.hash(super.hashCode(), query, values);
        }
    }

    public static class Batch extends FQLQuery
    {
        public final BatchStatement.Type batchType;
        public final List<Single> queries;

        public Batch(String keyspace, int protocolVersion, QueryOptions queryOptions, long queryTime, BatchStatement.Type batchType, List<String> queries, List<List<ByteBuffer>> values)
        {
            super(keyspace, protocolVersion, queryOptions, queryTime);
            this.batchType = batchType;
            this.queries = new ArrayList<>(queries.size());
            for (int i = 0; i < queries.size(); i++)
                this.queries.add(new Single(keyspace, protocolVersion, queryOptions, queryTime, queries.get(i), values.get(i)));
        }

        public ListenableFuture<ResultHandler.ComparableResultSet> execute(Session session)
        {
            BatchStatement bs = new BatchStatement(batchType);
            bs.setConsistencyLevel(ConsistencyLevel.valueOf(queryOptions.getConsistency().name()));
            for (Single query : queries)
            {
                bs.add(new SimpleStatement(query.query, query.values.toArray()));
            }
            ListenableFuture<ResultSet> future = session.executeAsync(bs);
            return handleErrors(future);
        }

        public int compareTo(FQLQuery other)
        {
            int cmp = super.compareTo(other);

            if (cmp == 0)
            {
                if (other instanceof Single)
                    return 1;

                Batch otherBatch = (Batch) other;
                if (queries.size() != otherBatch.queries.size())
                    return queries.size() - otherBatch.queries.size();
                for (int i = 0; i < queries.size(); i++)
                {
                    cmp = queries.get(i).compareTo(otherBatch.queries.get(i));
                    if (cmp != 0)
                        return cmp;
                }
            }
            return cmp;
        }

        public BinLog.ReleaseableWriteMarshallable toMarshallable()
        {
            List<String> queryStrings = new ArrayList<>();
            List<List<ByteBuffer>> values = new ArrayList<>();
            for (Single q : queries)
            {
                queryStrings.add(q.query);
                values.add(q.values);
            }
            return new FullQueryLogger.WeighableMarshallableBatch(batchType.name(), keyspace, queryStrings, values, queryOptions, queryTime);
        }

        public String toString()
        {
            StringBuilder sb = new StringBuilder("batch: ").append(batchType).append('\n');
            for (Single q : queries)
            {
                sb.append(q.toString()).append('\n');
            }
            sb.append("end batch");
            return sb.toString();
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof Batch)) return false;
            if (!super.equals(o)) return false;
            Batch batch = (Batch) o;
            return batchType == batch.batchType &&
                   Objects.equals(queries, batch.queries);
        }

        public int hashCode()
        {
            return Objects.hash(super.hashCode(), batchType, queries);
        }
    }
}
