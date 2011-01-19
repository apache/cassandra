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
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class IndexedRangeSlicer extends OperationThread
{
    public IndexedRangeSlicer(int index)
    {
        super(index);
    }

    public void run()
    {
        String format = "%0" + session.getTotalKeysLength() + "d";
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBuffer.wrap(new byte[]{}),
                                                                                      ByteBuffer.wrap(new byte[] {}),
                                                                                      false, session.getColumnsPerKey()));

        List<String> values = super.generateValues();
        ColumnParent parent = new ColumnParent("Standard1");
        int expectedPerValue = session.getNumKeys() / values.size();

        ByteBuffer columnName = ByteBuffer.wrap("C1".getBytes());

        for (int i = range.begins(); i < range.size(); i++)
        {
            int received = 0;

            String startOffset = "0";
            ByteBuffer value = ByteBuffer.wrap(values.get(i % values.size()).getBytes());

            IndexExpression expression = new IndexExpression(columnName, IndexOperator.EQ, value);

            while (received < expectedPerValue)
            {
                IndexClause clause = new IndexClause(Arrays.asList(expression), ByteBuffer.wrap(startOffset.getBytes()),
                                                                                session.getKeysPerCall());

                List<KeySlice> results = null;
                long start = System.currentTimeMillis();

                try
                {
                    results = client.get_indexed_slices(parent, clause, predicate, session.getConsistencyLevel());

                    if (results.size() == 0)
                    {
                        System.err.printf("No indexed values from offset received: %s%n", startOffset);

                        if (!session.ignoreErrors())
                            break;
                    }
                }
                catch (Exception e)
                {
                    System.err.printf("Error on get_indexed_slices call for offset  %s - %s%n", startOffset, getExceptionMessage(e));

                    if (!session.ignoreErrors())
                        return;
                }

                received += results.size();

                // convert max key found back to an integer, and increment it
                startOffset = String.format(format, (1 + getMaxKey(results)));

                session.operationCount.getAndIncrement(index);
                session.keyCount.getAndAdd(index, results.size());
                session.latencies.getAndAdd(index, System.currentTimeMillis() - start);
            }
        }
    }

    /**
     * Get maximum key from keySlice list
     * @param keySlices list of the KeySlice objects
     * @return maximum key value of the list
     */
    private int getMaxKey(List<KeySlice> keySlices)
    {
        byte[] firstKey = keySlices.get(0).getKey();
        int maxKey = FBUtilities.byteBufferToInt(ByteBuffer.wrap(firstKey));

        for (KeySlice k : keySlices)
        {
            int currentKey = FBUtilities.byteBufferToInt(ByteBuffer.wrap(k.getKey()));

            if (currentKey > maxKey)
            {
                maxKey = currentKey;
            }
        }

        return maxKey;
    }

}
