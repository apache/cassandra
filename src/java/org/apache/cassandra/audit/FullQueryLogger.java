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

package org.apache.cassandra.audit;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.binlog.BinLog;

/**
 * A logger that logs entire query contents after the query finishes (or times out).
 */
public class FullQueryLogger extends BinLogAuditLogger implements IAuditLogger
{
    @Override
    public void log(AuditLogEntry entry)
    {
        logQuery(entry.getOperation(), entry.getKeyspace(), entry.getOptions(), entry.getTimestamp());
    }

    /**
     * Log an invocation of a batch of queries
     * @param type The type of the batch
     * @param queries CQL text of the queries
     * @param values Values to bind to as parameters for the queries
     * @param queryOptions Options associated with the query invocation
     * @param batchTimeMillis Approximate time in milliseconds since the epoch since the batch was invoked
     */
    void logBatch(String type, String keyspace, List<String> queries, List<List<ByteBuffer>> values, QueryOptions queryOptions, long batchTimeMillis)
    {
        Preconditions.checkNotNull(type, "type was null");
        Preconditions.checkNotNull(queries, "queries was null");
        Preconditions.checkNotNull(values, "value was null");
        Preconditions.checkNotNull(queryOptions, "queryOptions was null");
        Preconditions.checkArgument(batchTimeMillis > 0, "batchTimeMillis must be > 0");

        //Don't construct the wrapper if the log is disabled
        BinLog binLog = this.binLog;
        if (binLog == null)
        {
            return;
        }

        WeighableMarshallableBatch wrappedBatch = new WeighableMarshallableBatch(type, keyspace, queries, values, queryOptions, batchTimeMillis);
        logRecord(wrappedBatch, binLog);
    }

    /**
     * Log a single CQL query
     * @param query CQL query text
     * @param queryOptions Options associated with the query invocation
     * @param queryTimeMillis Approximate time in milliseconds since the epoch since the batch was invoked
     */
    void logQuery(String query, String keyspace, QueryOptions queryOptions, long queryTimeMillis)
    {
        Preconditions.checkNotNull(query, "query was null");
        Preconditions.checkNotNull(queryOptions, "queryOptions was null");
        Preconditions.checkArgument(queryTimeMillis > 0, "queryTimeMillis must be > 0");

        //Don't construct the wrapper if the log is disabled
        BinLog binLog = this.binLog;
        if (binLog == null)
        {
            return;
        }

        WeighableMarshallableQuery wrappedQuery = new WeighableMarshallableQuery(query, keyspace, queryOptions, queryTimeMillis);
        logRecord(wrappedQuery, binLog);
    }

    static class WeighableMarshallableBatch extends AbstractWeighableMarshallable
    {
        private final int weight;
        private final String batchType;
        private final List<String> queries;
        private final List<List<ByteBuffer>> values;

        public WeighableMarshallableBatch(String batchType, String keyspace, List<String> queries, List<List<ByteBuffer>> values, QueryOptions queryOptions, long batchTimeMillis)
        {
           super(keyspace, queryOptions, batchTimeMillis);
           this.queries = queries;
           this.values = values;
           this.batchType = batchType;
           boolean success = false;
           try
           {
               //weight, batch type, queries, values
               int weightTemp = 8 + EMPTY_LIST_SIZE + EMPTY_LIST_SIZE;
               for (int ii = 0; ii < queries.size(); ii++)
               {
                   weightTemp += ObjectSizes.sizeOf(queries.get(ii));
               }

               weightTemp += EMPTY_LIST_SIZE * values.size();
               for (int ii = 0; ii < values.size(); ii++)
               {
                   List<ByteBuffer> sublist = values.get(ii);
                   weightTemp += EMPTY_BYTEBUFFER_SIZE * sublist.size();
                   for (int zz = 0; zz < sublist.size(); zz++)
                   {
                       weightTemp += sublist.get(zz).capacity();
                   }
               }
               weightTemp += super.weight();
               weightTemp += ObjectSizes.sizeOf(batchType);
               weight = weightTemp;
               success = true;
           }
           finally
           {
               if (!success)
               {
                   release();
               }
           }
        }

        @Override
        public void writeMarshallable(WireOut wire)
        {
            wire.write("type").text("batch");
            super.writeMarshallable(wire);
            wire.write("batch-type").text(batchType);
            ValueOut valueOut = wire.write("queries");
            valueOut.int32(queries.size());
            for (String query : queries)
            {
                valueOut.text(query);
            }
            valueOut = wire.write("values");
            valueOut.int32(values.size());
            for (List<ByteBuffer> subValues : values)
            {
                valueOut.int32(subValues.size());
                for (ByteBuffer value : subValues)
                {
                    valueOut.bytes(BytesStore.wrap(value));
                }
            }
        }

        @Override
        public int weight()
        {
            return weight;
        }
    }

    static class WeighableMarshallableQuery extends AbstractWeighableMarshallable
    {
        private final String query;

        public WeighableMarshallableQuery(String query, String keyspace, QueryOptions queryOptions, long queryTimeMillis)
        {
            super(keyspace, queryOptions, queryTimeMillis);
            this.query = query;
        }

        @Override
        public void writeMarshallable(WireOut wire)
        {
            wire.write("type").text("single");
            super.writeMarshallable(wire);
            wire.write("query").text(query);
        }

        @Override
        public int weight()
        {
            return Ints.checkedCast(ObjectSizes.sizeOf(query)) + super.weight();
        }
    }
}
