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

import com.datastax.driver.core.BatchStatement;
import io.netty.buffer.Unpooled;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.transport.ProtocolVersion;

public class FQLQueryReader implements ReadMarshallable
{
    private FQLQuery query;
    private final boolean legacyFiles;

    public FQLQueryReader()
    {
        this(false);
    }

    public FQLQueryReader(boolean legacyFiles)
    {
        this.legacyFiles = legacyFiles;
    }

    public void readMarshallable(WireIn wireIn) throws IORuntimeException
    {
        String type = wireIn.read("type").text();
        int protocolVersion = wireIn.read("protocol-version").int32();
        QueryOptions queryOptions = QueryOptions.codec.decode(Unpooled.wrappedBuffer(wireIn.read("query-options").bytes()), ProtocolVersion.decode(protocolVersion));
        long queryTime = wireIn.read("query-time").int64();
        String keyspace = legacyFiles ? null : wireIn.read("keyspace").text();
        switch (type)
        {
            case "single":
                String queryString = wireIn.read("query").text();
                query = new FQLQuery.Single(keyspace, protocolVersion, queryOptions, queryTime, queryString, queryOptions.getValues());
                break;
            case "batch":
                BatchStatement.Type batchType = BatchStatement.Type.valueOf(wireIn.read("batch-type").text());
                ValueIn in = wireIn.read("queries");
                int queryCount = in.int32();

                List<String> queries = new ArrayList<>(queryCount);
                for (int i = 0; i < queryCount; i++)
                    queries.add(in.text());
                in = wireIn.read("values");
                int valueCount = in.int32();
                List<List<ByteBuffer>> values = new ArrayList<>(valueCount);
                for (int ii = 0; ii < valueCount; ii++)
                {
                    List<ByteBuffer> subValues = new ArrayList<>();
                    values.add(subValues);
                    int numSubValues = in.int32();
                    for (int zz = 0; zz < numSubValues; zz++)
                    {
                        subValues.add(ByteBuffer.wrap(in.bytes()));
                    }
                }
                query = new FQLQuery.Batch(keyspace, protocolVersion, queryOptions, queryTime, batchType, queries, values);
                break;
            default:
                throw new RuntimeException("Unknown type: " + type);
        }
    }

    public FQLQuery getQuery()
    {
        return query;
    }
}