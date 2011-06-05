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
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class IndexedRangeSlicer extends Operation
{
    private static List<ByteBuffer> values = null;

    public IndexedRangeSlicer(Session client, int index)
    {
        super(client, index);
    }

    public void run(Cassandra.Client client) throws IOException
    {
        if (values == null)
            values = generateValues();

        String format = "%0" + session.getTotalKeysLength() + "d";
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                                      ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                                      false, session.getColumnsPerKey()));

        ColumnParent parent = new ColumnParent("Standard1");
        int expectedPerValue = session.getNumKeys() / values.size();

        ByteBuffer columnName = ByteBufferUtil.bytes("C1");

        int received = 0;

        String startOffset = String.format(format, 0);
        ByteBuffer value = values.get(1); // only C1 column is indexed

        IndexExpression expression = new IndexExpression(columnName, IndexOperator.EQ, value);

        while (received < expectedPerValue)
        {
            IndexClause clause = new IndexClause(Arrays.asList(expression),
                                                 ByteBufferUtil.bytes(startOffset),
                                                 session.getKeysPerCall());

            List<KeySlice> results = null;
            long start = System.currentTimeMillis();

            boolean success = false;
            String exceptionMessage = null;

            for (int t = 0; t < session.getRetryTimes(); t++)
            {
                if (success)
                    break;

                try
                {
                    results = client.get_indexed_slices(parent, clause, predicate, session.getConsistencyLevel());
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
                error(String.format("Operation [%d] retried %d times - error on calling get_indexed_slices for offset %s %s%n",
                                    index,
                                    session.getRetryTimes(),
                                    startOffset,
                                    (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
            }

            received += results.size();

            // convert max key found back to an integer, and increment it
            startOffset = String.format(format, (1 + getMaxKey(results)));

            session.operations.getAndIncrement();
            session.keys.getAndAdd(results.size());
            session.latency.getAndAdd(System.currentTimeMillis() - start);
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
        int maxKey = ByteBufferUtil.toInt(ByteBuffer.wrap(firstKey));

        for (KeySlice k : keySlices)
        {
            int currentKey = ByteBufferUtil.toInt(ByteBuffer.wrap(k.getKey()));

            if (currentKey > maxKey)
            {
                maxKey = currentKey;
            }
        }

        return maxKey;
    }

}
