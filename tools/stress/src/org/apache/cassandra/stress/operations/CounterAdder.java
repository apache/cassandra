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

import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CounterAdder extends Operation
{
    public CounterAdder(int index)
    {
        super(index);
    }

    public void run(Cassandra.Client client) throws IOException
    {
        List<CounterColumn> columns = new ArrayList<CounterColumn>();
        List<CounterSuperColumn> superColumns = new ArrayList<CounterSuperColumn>();

        // format used for keys
        String format = "%0" + session.getTotalKeysLength() + "d";

        for (int i = 0; i < session.getColumnsPerKey(); i++)
        {
            String columnName = ("C" + Integer.toString(i));

            columns.add(new CounterColumn(ByteBufferUtil.bytes(columnName), 1L));
        }

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            // supers = [SuperColumn('S' + str(j), columns) for j in xrange(supers_per_key)]
            for (int i = 0; i < session.getSuperColumns(); i++)
            {
                String superColumnName = "S" + Integer.toString(i);
                superColumns.add(new CounterSuperColumn(ByteBuffer.wrap(superColumnName.getBytes()), columns));
            }
        }

        String rawKey = String.format(format, index);
        Map<ByteBuffer, Map<String, List<CounterMutation>>> record = new HashMap<ByteBuffer, Map<String, List<CounterMutation>>>();

        record.put(ByteBufferUtil.bytes(rawKey), session.getColumnFamilyType() == ColumnFamilyType.Super
                                                                                ? getSuperColumnsMutationMap(superColumns)
                                                                                : getColumnsMutationMap(columns));

        long start = System.currentTimeMillis();

        boolean success = false;
        String exceptionMessage = null;

        for (int t = 0; t < session.getRetryTimes(); t++)
        {
            if (success)
                break;

            try
            {
                client.batch_add(record, session.getConsistencyLevel());
                success = true;
            }
            catch (Exception e)
            {
                exceptionMessage = getExceptionMessage(e);
                success = false;
            }
        }

        if (!success)
        {
            error(String.format("Operation [%d] retried %d times - error incrementing key %s %s%n",
                                index,
                                session.getRetryTimes(),
                                rawKey,
                                (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndIncrement();
        session.latency.getAndAdd(System.currentTimeMillis() - start);
    }

    private Map<String, List<CounterMutation>> getSuperColumnsMutationMap(List<CounterSuperColumn> superColumns)
    {
        List<CounterMutation> mutations = new ArrayList<CounterMutation>();
        Map<String, List<CounterMutation>> mutationMap = new HashMap<String, List<CounterMutation>>();

        for (CounterSuperColumn s : superColumns)
        {
            Counter counter = new Counter().setSuper_column(s);
            mutations.add(new CounterMutation().setCounter(counter));
        }

        mutationMap.put("SuperCounter1", mutations);

        return mutationMap;
    }

    private Map<String, List<CounterMutation>> getColumnsMutationMap(List<CounterColumn> columns)
    {
        List<CounterMutation> mutations = new ArrayList<CounterMutation>();
        Map<String, List<CounterMutation>> mutationMap = new HashMap<String, List<CounterMutation>>();

        for (CounterColumn c : columns)
        {
            Counter counter = new Counter().setColumn(c);
            mutations.add(new CounterMutation().setCounter(counter));
        }

        mutationMap.put("Counter1", mutations);

        return mutationMap;
    }
}
