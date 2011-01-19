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
package org.apache.cassandra.contrib.stress.tests;

import org.apache.cassandra.contrib.stress.util.OperationThread;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.thrift.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MultiGetter extends OperationThread
{
    public MultiGetter(int index)
    {
        super(index);
    }

    public void run()
    {
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBuffer.wrap(new byte[]{}),
                                                                                      ByteBuffer.wrap(new byte[] {}),
                                                                                      false, session.getColumnsPerKey()));

        int offset = index * session.getKeysPerThread();
        Map<ByteBuffer,List<ColumnOrSuperColumn>> results = null;
        int count  = (((index + 1) * session.getKeysPerThread()) - offset) / session.getKeysPerCall();

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            for (int i = 0; i < count; i++)
            {
                List<ByteBuffer> keys = generateKeys(offset, offset + session.getKeysPerCall());

                for (int j = 0; j < session.getSuperColumns(); j++)
                {
                    ColumnParent parent = new ColumnParent("Super1").setSuper_column(("S" + j).getBytes());

                    long start = System.currentTimeMillis();

                    try
                    {
                        results = client.multiget_slice(keys, parent, predicate, session.getConsistencyLevel());

                        if (results.size() == 0)
                        {
                            System.err.printf("Keys %s were not found.%n", keys);

                            if (!session.ignoreErrors())
                                break;
                        }
                    }
                    catch (Exception e)
                    {
                        System.err.printf("Error on multiget_slice call - %s%n", getExceptionMessage(e));

                        if (!session.ignoreErrors())
                            return;
                    }

                    session.operationCount.getAndIncrement(index);
                    session.keyCount.getAndAdd(index, keys.size());
                    session.latencies.getAndAdd(index, System.currentTimeMillis() - start);

                    offset += session.getKeysPerCall();
                }
            }
        }
        else
        {
            ColumnParent parent = new ColumnParent("Standard1");

            for (int i = 0; i < count; i++)
            {
                List<ByteBuffer> keys = generateKeys(offset, offset + session.getKeysPerCall());

                long start = System.currentTimeMillis();

                try
                {
                    results = client.multiget_slice(keys, parent, predicate, session.getConsistencyLevel());

                    if (results.size() == 0)
                    {
                        System.err.printf("Keys %s were not found.%n", keys);

                        if (!session.ignoreErrors())
                            break;
                    }
                }
                catch (Exception e)
                {
                    System.err.printf("Error on multiget_slice call - %s%n", getExceptionMessage(e));

                    if (!session.ignoreErrors())
                        return;
                }

                session.operationCount.getAndIncrement(index);
                session.keyCount.getAndAdd(index, keys.size());
                session.latencies.getAndAdd(index, System.currentTimeMillis() - start);

                offset += session.getKeysPerCall();
            }
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
