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

public class RangeSlicer extends OperationThread
{

    public RangeSlicer(int index)
    {
        super(index);
    }

    public void run()
    {
        String format = "%0" + session.getTotalKeysLength() + "d";

        // initial values
        int current = range.begins();
        int limit   = range.limit();
        int count   = session.getColumnsPerKey();
        int last    = current + session.getKeysPerCall();

        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBuffer.wrap(new byte[] {}),
                                                                                      ByteBuffer.wrap(new byte[] {}),
                                                                                      false, count));

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            while (current < limit)
            {
                byte[] start = String.format(format, current).getBytes();
                byte[] end   = String.format(format, last).getBytes();

                List<KeySlice> slices = new ArrayList<KeySlice>();
                KeyRange range = new KeyRange(count).setStart_key(start).setEnd_key(end);

                for (int i = 0; i < session.getSuperColumns(); i++)
                {
                    String superColumnName = "S" + Integer.toString(i);
                    ColumnParent parent = new ColumnParent("Super1").setSuper_column(ByteBuffer.wrap(superColumnName.getBytes()));

                    long startTime = System.currentTimeMillis();

                    try
                    {
                        slices = client.get_range_slices(parent, predicate, range, session.getConsistencyLevel());

                        if (slices.size() == 0)
                        {
                            throw new RuntimeException(String.format("Key %s not found.", superColumnName));
                        }
                    }
                    catch (InvalidRequestException e)
                    {
                        System.err.println(e.getWhy());

                        if (!session.ignoreErrors())
                            return;
                    }
                    catch (Exception e)
                    {
                        System.err.println(e.getMessage());

                        if (!session.ignoreErrors())
                            return;
                    }

                    session.operationCount.getAndIncrement(index);
                    session.latencies.getAndAdd(index, System.currentTimeMillis() - startTime);
                }

                current += slices.size() + 1;
                last = current + slices.size() + 1;
                session.keyCount.getAndAdd(index, slices.size());
            }
        }
        else
        {
            ColumnParent parent = new ColumnParent("Standard1");

            while (current < limit)
            {
                byte[] start = String.format(format, current).getBytes();
                byte[] end   = String.format(format, last).getBytes();

                List<KeySlice> slices = new ArrayList<KeySlice>();
                KeyRange range = new KeyRange(count).setStart_key(start).setEnd_key(end);

                long startTime = System.currentTimeMillis();

                try
                {
                    slices = client.get_range_slices(parent, predicate, range, session.getConsistencyLevel());

                    if (slices.size() == 0)
                    {
                        throw new RuntimeException(String.format("Range %s %s not found.", String.format(format, current),
                                                                                           String.format(format, last)));
                    }
                }
                catch (InvalidRequestException e)
                {
                    System.err.println(e.getWhy());

                    if (!session.ignoreErrors())
                        return;
                }
                catch (Exception e)
                {
                    System.err.println(e.getMessage());

                    if (!session.ignoreErrors())
                        return;
                }

                current += slices.size() + 1;
                last = current + slices.size() + 1;

                session.operationCount.getAndIncrement(index);
                session.keyCount.getAndAdd(index, slices.size());
                session.latencies.getAndAdd(index, System.currentTimeMillis() - startTime);
            }
        }
    }
}
