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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class CounterGetter extends Operation
{
    public CounterGetter(Session client, int index)
    {
        super(client, index);
    }

    public void run(Cassandra.Client client) throws IOException
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
            runSuperCounterGetter(predicate, client);
        }
        else
        {
            runCounterGetter(predicate, client);
        }
    }

    private void runSuperCounterGetter(SlicePredicate predicate, Cassandra.Client client) throws IOException
    {
        byte[] rawKey = generateKey();
        ByteBuffer key = ByteBuffer.wrap(rawKey);

        for (int j = 0; j < session.getSuperColumns(); j++)
        {
            String superColumn = 'S' + Integer.toString(j);
            ColumnParent parent = new ColumnParent("SuperCounter1").setSuper_column(superColumn.getBytes());

            long start = System.currentTimeMillis();

            boolean success = false;
            String exceptionMessage = null;

            for (int t = 0; t < session.getRetryTimes(); t++)
            {
                if (success)
                    break;

                try
                {
                    List<ColumnOrSuperColumn> counters;
                    counters = client.get_slice(key, parent, predicate, session.getConsistencyLevel());
                    success = (counters.size() != 0);
                }
                catch (Exception e)
                {
                    exceptionMessage = getExceptionMessage(e);
                    success = false;
                }
            }

            if (!success)
            {
                error(String.format("Operation [%d] retried %d times - error reading counter key %s %s%n",
                                    index,
                                    session.getRetryTimes(),
                                    new String(rawKey),
                                    (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
            }

            session.operations.getAndIncrement();
            session.keys.getAndIncrement();
            session.latency.getAndAdd(System.currentTimeMillis() - start);
        }
    }

    private void runCounterGetter(SlicePredicate predicate, Cassandra.Client client) throws IOException
    {
        ColumnParent parent = new ColumnParent("Counter1");

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
                List<ColumnOrSuperColumn> counters;
                counters = client.get_slice(keyBuffer, parent, predicate, session.getConsistencyLevel());
                success = (counters.size() != 0);
            }
            catch (Exception e)
            {
                exceptionMessage = getExceptionMessage(e);
                success = false;
            }
        }

        if (!success)
        {
            error(String.format("Operation [%d] retried %d times - error reading counter key %s %s%n",
                                index,
                                session.getRetryTimes(),
                                new String(key),
                                (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndIncrement();
        session.latency.getAndAdd(System.currentTimeMillis() - start);
    }

}
