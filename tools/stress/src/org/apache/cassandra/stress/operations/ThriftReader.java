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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;

public final class ThriftReader extends Operation
{

    public ThriftReader(State state, long index)
    {
        super(state, index);
    }

    public void run(final ThriftClient client) throws IOException
    {
        final SlicePredicate predicate = new SlicePredicate();
        if (state.settings.columns.names == null)
            predicate.setSlice_range(new SliceRange()
                    .setStart(new byte[] {})
                    .setFinish(new byte[] {})
                    .setReversed(false)
                    .setCount(state.settings.columns.maxColumnsPerKey)
            );
        else // see CASSANDRA-3064 about why this is useful
            predicate.setColumn_names(state.settings.columns.names);

        final ByteBuffer key = getKey();
        final List<ByteBuffer> expect = state.rowGen.isDeterministic() ? generateColumnValues(key) : null;
        for (final ColumnParent parent : state.columnParents)
        {
            timeWithRetry(new RunOp()
            {
                @Override
                public boolean run() throws Exception
                {
                    List<ColumnOrSuperColumn> row = client.get_slice(key, parent, predicate, state.settings.command.consistencyLevel);
                    if (expect == null)
                        return !row.isEmpty();
                    if (!state.settings.columns.useSuperColumns)
                    {
                        if (row.size() != expect.size())
                            return false;
                        for (int i = 0 ; i < row.size() ; i++)
                            if (!row.get(i).getColumn().bufferForValue().equals(expect.get(i)))
                                return false;
                    }
                    else
                    {
                        for (ColumnOrSuperColumn col : row)
                        {
                            SuperColumn superColumn = col.getSuper_column();
                            if (superColumn.getColumns().size() != expect.size())
                                return false;
                            for (int i = 0 ; i < expect.size() ; i++)
                                if (!superColumn.getColumns().get(i).bufferForValue().equals(expect.get(i)))
                                    return false;
                        }
                    }
                    return true;
                }

                @Override
                public String key()
                {
                    return new String(key.array());
                }

                @Override
                public int keyCount()
                {
                    return 1;
                }
            });
        }
    }

}
