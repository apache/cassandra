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

package org.apache.cassandra.transport;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.net.AbstractMessageHandler;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.concurrent.NonBlockingRateLimiter;

import static org.apache.cassandra.utils.concurrent.NonBlockingRateLimiter.NO_OP_LIMITER;

public class BurnTestUtil
{
    public static class SizeCaps
    {
        public final int valueMinSize;
        public final int valueMaxSize;
        public final int columnCountCap;
        public final int rowsCountCap;

        public SizeCaps(int valueMinSize, int valueMaxSize, int columnCountCap, int rowsCountCap)
        {
            this.valueMinSize = valueMinSize;
            this.valueMaxSize = valueMaxSize;
            this.columnCountCap = columnCountCap;
            this.rowsCountCap = rowsCountCap;
        }
    }

    public static SimpleStatement generateQueryStatement(int idx, SizeCaps sizeCaps)
    {
        Random rnd = new Random(idx);

        ByteBuffer[] values = new ByteBuffer[sizeCaps.columnCountCap];
        for (int i = 0; i < sizeCaps.columnCountCap; i++)
            values[i] = bytes(rnd, sizeCaps.valueMinSize, sizeCaps.valueMaxSize);

        return new SimpleStatement(Integer.toString(idx), (Object[]) values);
    }

    public static QueryMessage generateQueryMessage(int idx, SizeCaps sizeCaps, ProtocolVersion version)
    {
        Random rnd = new Random(idx);
        List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < sizeCaps.columnCountCap * sizeCaps.rowsCountCap; i++)
            values.add(bytes(rnd, sizeCaps.valueMinSize, sizeCaps.valueMaxSize));

        QueryOptions queryOptions = QueryOptions.create(ConsistencyLevel.ONE,
                                                        values,
                                                        true,
                                                        10,
                                                        null,
                                                        null,
                                                        version,
                                                        "KEYSPACE");

        return new QueryMessage(Integer.toString(idx), queryOptions);
    }

    public static ResultMessage.Rows generateRows(int idx, SizeCaps sizeCaps)
    {
        Random rnd = new Random(idx);
        List<ColumnSpecification> columns = new ArrayList<>();
        for (int i = 0; i < sizeCaps.columnCountCap; i++)
        {
            columns.add(new ColumnSpecification("ks", "cf",
                                                new ColumnIdentifier(bytes(rnd, 5, 10), BytesType.instance),
                                                BytesType.instance));
        }

        List<List<ByteBuffer>> rows = new ArrayList<>();
        int count = rnd.nextInt(sizeCaps.rowsCountCap);
        for (int i = 0; i < count; i++)
        {
            List<ByteBuffer> row = new ArrayList<>();
            for (int j = 0; j < sizeCaps.columnCountCap; j++)
                row.add(bytes(rnd, sizeCaps.valueMinSize, sizeCaps.valueMaxSize));
            rows.add(row);
        }

        ResultSet resultSet = new ResultSet(new ResultSet.ResultMetadata(columns), rows);
        return new ResultMessage.Rows(resultSet);
    }

    public static ByteBuffer bytes(Random rnd, int minSize, int maxSize)
    {
        byte[] bytes = new byte[rnd.nextInt(maxSize) + minSize];
        rnd.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    public static Function<ClientResourceLimits.Allocator, ClientResourceLimits.ResourceProvider> observableResourceProvider(final CQLConnectionTest.AllocationObserver observer)
    {
        return allocator ->
        {
            final ClientResourceLimits.ResourceProvider.Default delegate = new ClientResourceLimits.ResourceProvider.Default(allocator);
            return new ClientResourceLimits.ResourceProvider()
            {
                public ResourceLimits.Limit globalLimit()
                {
                    return observer.global(delegate.globalLimit());
                }

                public AbstractMessageHandler.WaitQueue globalWaitQueue()
                {
                    return delegate.globalWaitQueue();
                }

                public ResourceLimits.Limit endpointLimit()
                {
                    return observer.endpoint(delegate.endpointLimit());
                }

                public AbstractMessageHandler.WaitQueue endpointWaitQueue()
                {
                    return delegate.endpointWaitQueue();
                }

                @Override
                public NonBlockingRateLimiter requestRateLimiter()
                {
                    return NO_OP_LIMITER;
                }
                
                public void release()
                {
                    delegate.release();
                }
            };
        };
    }
}
