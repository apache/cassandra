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
package org.apache.cassandra.contrib.stress.operations;

import org.apache.cassandra.contrib.stress.util.OperationThread;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;
import java.lang.AssertionError;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.List;

public class Reader extends OperationThread
{
    public Reader(int index)
    {
        super(index);
    }

    public void run()
    {
        SliceRange sliceRange = new SliceRange();

        // start/finish
        sliceRange.setStart(new byte[] {}).setFinish(new byte[] {});

        // reversed/count
        sliceRange.setReversed(false).setCount(session.getColumnsPerKey());

        // initialize SlicePredicate with existing SliceRange
        SlicePredicate predicate = new SlicePredicate().setSlice_range(sliceRange);

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            runSuperColumnReader(predicate);
        }
        else
        {
            runColumnReader(predicate);
        }
    }

    private void runSuperColumnReader(SlicePredicate predicate)
    {
        for (int i = 0; i < session.getKeysPerThread(); i++)
        {
            byte[] rawKey = generateKey();
            ByteBuffer key = ByteBuffer.wrap(rawKey);

            for (int j = 0; j < session.getSuperColumns(); j++)
            {
                String superColumn = 'S' + Integer.toString(j);
                ColumnParent parent = new ColumnParent("Super1").setSuper_column(superColumn.getBytes());

                long start = System.currentTimeMillis();

                boolean success = false;
                String exceptionMessage = null;

                for (int t = 0; t < session.getRetryTimes(); t++)
                {
                    if (success)
                        break;

                    try
                    {
                        List<ColumnOrSuperColumn> columns;
                        columns = client.get_slice(key, parent, predicate, session.getConsistencyLevel());
                        success = (columns.size() != 0);
                    }
                    catch (Exception e)
                    {
                        exceptionMessage = getExceptionMessage(e);
                        success = false;
                    }
                }

                if (!success)
                {
                    System.err.printf("Thread [%d] retried %d times - error reading key %s %s%n", index,
                                                                                                  session.getRetryTimes(),
                                                                                                  new String(rawKey),
                                                                                                  (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")");

                    if (!session.ignoreErrors())
                        return;
                }

                session.operationCount.getAndIncrement(index);
                session.keyCount.getAndIncrement(index);
                session.latencies.getAndAdd(index, System.currentTimeMillis() - start);
            }
        }
    }

    private void runColumnReader(SlicePredicate predicate)
    {
        ColumnParent parent = new ColumnParent("Standard1");

        for (int i = 0; i < session.getKeysPerThread(); i++)
        {
            byte[] key = generateKey();
            ByteBuffer keyBuffer = ByteBuffer.wrap(key);

            long start = System.currentTimeMillis();

            boolean success = false;
            String exceptionMessage = null;

            for (int t = 0; t < session.getRetryTimes(); t++)
            {
                if (success)
                    break;

                try
                {
                    List<ColumnOrSuperColumn> columns;
                    columns = client.get_slice(keyBuffer, parent, predicate, session.getConsistencyLevel());
                    success = (columns.size() != 0);
                }
                catch (Exception e)
                {
                    exceptionMessage = getExceptionMessage(e);
                    success = false;
                }
            }

            if (!success)
            {
                System.err.printf("Thread [%d] retried %d times - error reading key %s %s%n", index,
                                                                                              session.getRetryTimes(),
                                                                                              new String(key),
                                                                                              (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")");

                if (!session.ignoreErrors())
                    return;
            }

            session.operationCount.getAndIncrement(index);
            session.keyCount.getAndIncrement(index);
            session.latencies.getAndAdd(index, System.currentTimeMillis() - start);
        }
    }
}
