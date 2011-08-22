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

import static com.google.common.base.Charsets.UTF_8;

public class Reader extends Operation
{
    public Reader(Session client, int index)
    {
        super(client, index);
    }

    public void run(Cassandra.Client client) throws IOException
    {
        // initialize SlicePredicate with existing SliceRange
        SlicePredicate predicate = new SlicePredicate();

        if (session.columnNames == null)
            predicate.setSlice_range(getSliceRange());
        else // see CASSANDRA-3064 about why this is useful
            predicate.setColumn_names(session.columnNames);

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            runSuperColumnReader(predicate, client);
        }
        else
        {
            runColumnReader(predicate, client);
        }
    }

    private void runSuperColumnReader(SlicePredicate predicate, Cassandra.Client client) throws IOException
    {
        byte[] rawKey = generateKey();
        ByteBuffer key = ByteBuffer.wrap(rawKey);

        for (int j = 0; j < session.getSuperColumns(); j++)
        {
            String superColumn = 'S' + Integer.toString(j);
            ColumnParent parent = new ColumnParent("Super1").setSuper_column(superColumn.getBytes(UTF_8));

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
                error(String.format("Operation [%d] retried %d times - error reading key %s %s%n",
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

    private void runColumnReader(SlicePredicate predicate, Cassandra.Client client) throws IOException
    {
        ColumnParent parent = new ColumnParent("Standard1");

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
            error(String.format("Operation [%d] retried %d times - error reading key %s %s%n",
                                index,
                                session.getRetryTimes(),
                                new String(key),
                                (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndIncrement();
        session.latency.getAndAdd(System.currentTimeMillis() - start);
    }

    private SliceRange getSliceRange()
    {
        return new SliceRange()
                    .setStart(new byte[] {})
                    .setFinish(new byte[] {})
                    .setReversed(false)
                    .setCount(session.getColumnsPerKey());
    }
}
