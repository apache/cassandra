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

public class RangeSlicer extends Operation
{

    public RangeSlicer(Session client, int index)
    {
        super(client, index);
    }

    public void run(Cassandra.Client client) throws IOException
    {
        String format = "%0" + session.getTotalKeysLength() + "d";

        // initial values
        int count = session.getColumnsPerKey();

        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                                      ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                                      false,
                                                                                      count));

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            ByteBuffer start = ByteBufferUtil.bytes(String.format(format, index));

            List<KeySlice> slices = new ArrayList<KeySlice>();
            KeyRange range = new KeyRange(count).setStart_key(start).setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);

            for (int i = 0; i < session.getSuperColumns(); i++)
            {
                String superColumnName = "S" + Integer.toString(i);
                ColumnParent parent = new ColumnParent("Super1").setSuper_column(ByteBufferUtil.bytes(superColumnName));

                long startTime = System.currentTimeMillis();

                boolean success = false;
                String exceptionMessage = null;

                for (int t = 0; t < session.getRetryTimes(); t++)
                {
                    try
                    {
                        slices = client.get_range_slices(parent, predicate, range, session.getConsistencyLevel());
                        success = (slices.size() != 0);
                    }
                    catch (Exception e)
                    {
                        exceptionMessage = getExceptionMessage(e);
                        success = false;
                    }
                }

                if (!success)
                {
                    error(String.format("Operation [%d] retried %d times - error on calling get_range_slices for range offset %s %s%n",
                                        index,
                                        session.getRetryTimes(),
                                        ByteBufferUtil.string(start),
                                        (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
                }

                session.operations.getAndIncrement();
                session.latency.getAndAdd(System.currentTimeMillis() - startTime);
            }

            session.keys.getAndAdd(slices.size());
        }
        else
        {
            ColumnParent parent = new ColumnParent("Standard1");

            ByteBuffer start = ByteBufferUtil.bytes(String.format(format, index));

            List<KeySlice> slices = new ArrayList<KeySlice>();
            KeyRange range = new KeyRange(count).setStart_key(start).setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);

            long startTime = System.currentTimeMillis();

            boolean success = false;
            String exceptionMessage = null;

            for (int t = 0; t < session.getRetryTimes(); t++)
            {
                if (success)
                    break;

                try
                {
                    slices = client.get_range_slices(parent, predicate, range, session.getConsistencyLevel());
                    success = (slices.size() != 0);
                }
                catch (Exception e)
                {
                    exceptionMessage = getExceptionMessage(e);
                    success = false;
                }
            }

            if (!success)
            {
                error(String.format("Operation [%d] retried %d times - error on calling get_indexed_slices for range offset %s %s%n",
                                    index,
                                    session.getRetryTimes(),
                                    ByteBufferUtil.string(start),
                                    (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
            }

            session.operations.getAndIncrement();
            session.keys.getAndAdd(slices.size());
            session.latency.getAndAdd(System.currentTimeMillis() - startTime);
        }
    }
}
