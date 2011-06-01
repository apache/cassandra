/**
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
package org.apache.cassandra.stress.operations;

import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class MultiGetter extends Operation
{
    public MultiGetter(Session client, int index)
    {
        super(client, index);
    }

    public void run(Cassandra.Client client) throws IOException
    {
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                                      ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                                      false, session.getColumnsPerKey()));

        int offset = index * session.getKeysPerThread();
        Map<ByteBuffer,List<ColumnOrSuperColumn>> results;

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            List<ByteBuffer> keys = generateKeys(offset, offset + session.getKeysPerCall());

            for (int j = 0; j < session.getSuperColumns(); j++)
            {
                ColumnParent parent = new ColumnParent("Super1").setSuper_column(ByteBufferUtil.bytes("S" + j));

                long start = System.currentTimeMillis();

                boolean success = false;
                String exceptionMessage = null;

                for (int t = 0; t < session.getRetryTimes(); t++)
                {
                    if (success)
                        break;

                    try
                    {
                        results = client.multiget_slice(keys, parent, predicate, session.getConsistencyLevel());
                        success = (results.size() != 0);
                    }
                    catch (Exception e)
                    {
                        exceptionMessage = getExceptionMessage(e);
                    }
                }

                if (!success)
                {
                    error(String.format("Operation [%d] retried %d times - error on calling multiget_slice for keys %s %s%n",
                                        index,
                                        session.getRetryTimes(),
                                        keys,
                                        (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
                }

                session.operations.getAndIncrement();
                session.keys.getAndAdd(keys.size());
                session.latency.getAndAdd(System.currentTimeMillis() - start);

                offset += session.getKeysPerCall();
            }
        }
        else
        {
            ColumnParent parent = new ColumnParent("Standard1");

            List<ByteBuffer> keys = generateKeys(offset, offset + session.getKeysPerCall());

            long start = System.currentTimeMillis();

            boolean success = false;
            String exceptionMessage = null;

            for (int t = 0; t < session.getRetryTimes(); t++)
            {
                if (success)
                    break;

                try
                {
                    results = client.multiget_slice(keys, parent, predicate, session.getConsistencyLevel());
                    success = (results.size() != 0);
                }
                catch (Exception e)
                {
                    exceptionMessage = getExceptionMessage(e);
                    success = false;
                }
            }

            if (!success)
            {
                error(String.format("Operation [%d] retried %d times - error on calling multiget_slice for keys %s %s%n",
                                    index,
                                    session.getRetryTimes(),
                                    keys,
                                    (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
            }

            session.operations.getAndIncrement();
            session.keys.getAndAdd(keys.size());
            session.latency.getAndAdd(System.currentTimeMillis() - start);

            offset += session.getKeysPerCall();
        }
    }

    private List<ByteBuffer> generateKeys(int start, int limit)
    {
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();

        for (int i = start; i < limit; i++)
        {
            keys.add(ByteBuffer.wrap(generateKey()));
        }

        return keys;
    }
}
