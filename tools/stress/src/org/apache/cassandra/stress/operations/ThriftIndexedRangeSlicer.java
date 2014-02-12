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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.settings.SettingsCommandMulti;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class ThriftIndexedRangeSlicer extends Operation
{
    public ThriftIndexedRangeSlicer(State state, long index)
    {
        super(state, index);
        if (!state.rowGen.isDeterministic() || !state.keyGen.isDeterministic())
            throw new IllegalStateException("Only run with a isDeterministic row/key generator");
        if (state.settings.columns.useSuperColumns || state.columnParents.size() != 1)
            throw new IllegalStateException("Does not support super columns");
        if (state.settings.columns.useTimeUUIDComparator)
            throw new IllegalStateException("Does not support TimeUUID column names");
    }

    public void run(final ThriftClient client) throws IOException
    {

        final SlicePredicate predicate = new SlicePredicate()
                .setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                        false, state.settings.columns.maxColumnsPerKey));
        final List<ByteBuffer> columns = generateColumnValues(getKey());
        final ColumnParent parent = state.columnParents.get(0);

        final ByteBuffer columnName = getColumnNameBytes(1);
        final ByteBuffer value = columns.get(1); // only C1 column is indexed

        IndexExpression expression = new IndexExpression(columnName, IndexOperator.EQ, value);
        byte[] minKey = new byte[0];
        final List<KeySlice>[] results = new List[1];
        do
        {

            final boolean first = minKey.length == 0;
            final IndexClause clause = new IndexClause(Arrays.asList(expression),
                                                 ByteBuffer.wrap(minKey),
                                                ((SettingsCommandMulti) state.settings.command).keysAtOnce);

            timeWithRetry(new RunOp()
            {
                @Override
                public boolean run() throws Exception
                {
                    results[0] = client.get_indexed_slices(parent, clause, predicate, state.settings.command.consistencyLevel);
                    return !first || results[0].size() > 0;
                }

                @Override
                public String key()
                {
                    return new String(value.array());
                }

                @Override
                public int keyCount()
                {
                    return results[0].size();
                }
            });

            minKey = getNextMinKey(minKey, results[0]);

        } while (results[0].size() > 0);
    }

    /**
     * Get maximum key from keySlice list
     * @param slices list of the KeySlice objects
     * @return maximum key value of the list
     */
    private static byte[] getNextMinKey(byte[] cur, List<KeySlice> slices)
    {
        // find max
        for (KeySlice slice : slices)
            if (FBUtilities.compareUnsigned(cur, slice.getKey()) < 0)
                cur = slice.getKey();

        // increment
        for (int i = 0 ; i < cur.length ; i++)
            if (++cur[i] != 0)
                break;
        return cur;
    }

}
