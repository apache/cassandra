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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.primitives.Longs;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.audit.FullQueryLogger;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.binlog.BinLog;

public abstract class FQLQuery implements Comparable<FQLQuery>
{
    public final long queryStartTime;
    public final QueryOptions queryOptions;
    public final int protocolVersion;
    public final String keyspace;
    public final long generatedTimestamp;
    private final int generatedNowInSeconds;

    public FQLQuery(String keyspace, int protocolVersion, QueryOptions queryOptions, long queryStartTime, long generatedTimestamp, int generatedNowInSeconds)
    {
        this.queryStartTime = queryStartTime;
        this.queryOptions = queryOptions;
        this.protocolVersion = protocolVersion;
        this.keyspace = keyspace;
        this.generatedTimestamp = generatedTimestamp;
        this.generatedNowInSeconds = generatedNowInSeconds;
    }

    public abstract Statement toStatement();

    /**
     * used when storing the queries executed
     */
    public abstract BinLog.ReleaseableWriteMarshallable toMarshallable();

    public QueryState queryState()
    {
        ClientState clientState = keyspace != null ? ClientState.forInternalCalls(keyspace) : ClientState.forInternalCalls();

        return new QueryState(clientState, generatedTimestamp, generatedNowInSeconds);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof FQLQuery)) return false;
        FQLQuery fqlQuery = (FQLQuery) o;
        return queryStartTime == fqlQuery.queryStartTime &&
               protocolVersion == fqlQuery.protocolVersion &&
               generatedTimestamp == fqlQuery.generatedTimestamp &&
               generatedNowInSeconds == fqlQuery.generatedNowInSeconds &&
               Objects.equals(queryOptions.getValues(), fqlQuery.queryOptions.getValues()) &&
               Objects.equals(keyspace, fqlQuery.keyspace);
    }

    public int hashCode()
    {
        return Objects.hash(queryStartTime, queryOptions, protocolVersion, keyspace, generatedTimestamp, generatedNowInSeconds);
    }

    public int compareTo(FQLQuery other)
    {
        int cmp = Longs.compare(queryStartTime, other.queryStartTime);
        if (cmp != 0)
            return cmp;
        cmp = Longs.compare(generatedTimestamp, other.generatedTimestamp);
        if (cmp != 0)
            return cmp;

        return Longs.compare(generatedNowInSeconds, other.generatedNowInSeconds);
    }

    public String toString()
    {
        return "FQLQuery{" +
               "queryStartTime=" + queryStartTime +
               ", protocolVersion=" + protocolVersion +
               ", keyspace='" + keyspace + '\'' +
               ", generatedTimestamp=" + generatedTimestamp +
               ", generatedNowInSeconds=" + generatedNowInSeconds +
               '}';
    }

    public static class Single extends FQLQuery
    {
        public final String query;
        public final List<ByteBuffer> values;

        public Single(String keyspace, int protocolVersion, QueryOptions queryOptions, long queryStartTime, long generatedTimestamp, int generatedNowInSeconds, String queryString, List<ByteBuffer> values)
        {
            super(keyspace, protocolVersion, queryOptions, queryStartTime, generatedTimestamp, generatedNowInSeconds);
            this.query = queryString;
            this.values = values;
        }

        @Override
        public String toString()
        {
            return String.format("%s%nQuery = %s, Values = %s",
                                 super.toString(),
                                 query,
                                 values.stream().map(ByteBufferUtil::bytesToHex).collect(Collectors.joining(",")));
        }

        public Statement toStatement()
        {
            SimpleStatement ss = new SimpleStatement(query, values.toArray());
            ss.setConsistencyLevel(ConsistencyLevel.valueOf(queryOptions.getConsistency().name()));
            ss.setDefaultTimestamp(generatedTimestamp);
            return ss;
        }

        public BinLog.ReleaseableWriteMarshallable toMarshallable()
        {

            return new FullQueryLogger.Query(query, queryOptions, queryState(), queryStartTime);
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

        public Batch(String keyspace, int protocolVersion, QueryOptions queryOptions, long queryStartTime, long generatedTimestamp, int generatedNowInSeconds, BatchStatement.Type batchType, List<String> queries, List<List<ByteBuffer>> values)
        {
            super(keyspace, protocolVersion, queryOptions, queryStartTime, generatedTimestamp, generatedNowInSeconds);
            this.batchType = batchType;
            this.queries = new ArrayList<>(queries.size());
            for (int i = 0; i < queries.size(); i++)
                this.queries.add(new Single(keyspace, protocolVersion, queryOptions, queryStartTime, generatedTimestamp, generatedNowInSeconds, queries.get(i), values.get(i)));
        }

        public Statement toStatement()
        {
            BatchStatement bs = new BatchStatement(batchType);

            for (Single query : queries)
            {
                bs.add(new SimpleStatement(query.query, query.values.toArray()));
            }
            bs.setConsistencyLevel(ConsistencyLevel.valueOf(queryOptions.getConsistency().name()));
            bs.setDefaultTimestamp(generatedTimestamp); // todo: set actual server side generated time
            return bs;
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
            return new FullQueryLogger.Batch(org.apache.cassandra.cql3.statements.BatchStatement.Type.valueOf(batchType.name()), queryStrings, values, queryOptions, queryState(), queryStartTime);
        }

        public String toString()
        {
            StringBuilder sb = new StringBuilder(super.toString()).append("\nbatch: ").append(batchType).append('\n');
            for (Single q : queries)
                sb.append(q.toString()).append('\n');
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
