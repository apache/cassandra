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
import java.util.stream.Collectors;

import com.google.common.primitives.Longs;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.utils.ByteBufferUtil;

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

    public abstract ResultSetFuture execute(Session session);

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

        public ResultSetFuture execute(Session session)
        {
            SimpleStatement ss = new SimpleStatement(query, values.toArray());
            ss.setConsistencyLevel(ConsistencyLevel.valueOf(queryOptions.getConsistency().name()));
            return session.executeAsync(ss);
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

        public ResultSetFuture execute(Session session)
        {
            BatchStatement bs = new BatchStatement(batchType);
            bs.setConsistencyLevel(ConsistencyLevel.valueOf(queryOptions.getConsistency().name()));
            for (Single query : queries)
            {
                bs.add(new SimpleStatement(query.query, query.values.toArray()));
            }
            return session.executeAsync(bs);
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
    }
}
