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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Inserter extends OperationThread
{
    public Inserter(int index)
    {
        super(index);
    }

    public void run()
    {
        List<String> values  = generateValues();
        List<Column> columns = new ArrayList<Column>();
        List<SuperColumn> superColumns = new ArrayList<SuperColumn>();

        // format used for keys
        String format = "%0" + session.getTotalKeysLength() + "d";

        // columns = [Column('C' + str(j), 'unset', time.time() * 1000000) for j in xrange(columns_per_key)]
        for (int i = 0; i < session.getColumnsPerKey(); i++)
        {
            byte[] columnName = ("C" + Integer.toString(i)).getBytes();
            columns.add(new Column(ByteBuffer.wrap(columnName), ByteBuffer.wrap(new byte[] {}), System.currentTimeMillis()));
        }

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            // supers = [SuperColumn('S' + str(j), columns) for j in xrange(supers_per_key)]
            for (int i = 0; i < session.getSuperColumns(); i++)
            {
                String superColumnName = "S" + Integer.toString(i);
                superColumns.add(new SuperColumn(ByteBuffer.wrap(superColumnName.getBytes()), columns));
            }
        }

        for (int i : range)
        {
            ByteBuffer key = ByteBuffer.wrap(String.format(format, i).getBytes());
            Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

            record.put(key, session.getColumnFamilyType() == ColumnFamilyType.Super
                                                          ? getSuperColumnsMutationMap(superColumns)
                                                          : getColumnsMutationMap(columns));

            String value = values.get(i % values.size());

            for (Column c : columns)
                c.value = ByteBuffer.wrap(value.getBytes());

            long start = System.currentTimeMillis();

            try
            {
                client.batch_mutate(record, session.getConsistencyLevel());
            }
            catch (Exception e)
            {
                System.err.printf("Error while inserting key %s - %s%n", ByteBufferUtil.string(key), getExceptionMessage(e));

                if (!session.ignoreErrors())
                    return;
            }

            session.operationCount.getAndIncrement(index);
            session.keyCount.getAndIncrement(index);
            session.latencies.getAndAdd(index, System.currentTimeMillis() - start);
        }
    }

    private Map<String, List<Mutation>> getSuperColumnsMutationMap(List<SuperColumn> superColumns)
    {
        List<Mutation> mutations = new ArrayList<Mutation>();
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();

        for (SuperColumn s : superColumns)
        {
            ColumnOrSuperColumn superColumn = new ColumnOrSuperColumn().setSuper_column(s);
            mutations.add(new Mutation().setColumn_or_supercolumn(superColumn));
        }

        mutationMap.put("Super1", mutations);

        return mutationMap;
    }

    private Map<String, List<Mutation>> getColumnsMutationMap(List<Column> columns)
    {
        List<Mutation> mutations = new ArrayList<Mutation>();
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();

        for (Column c : columns)
        {
            ColumnOrSuperColumn column = new ColumnOrSuperColumn().setColumn(c);
            mutations.add(new Mutation().setColumn_or_supercolumn(column));
        }

        mutationMap.put("Standard1", mutations);

        return mutationMap;
    }
}
