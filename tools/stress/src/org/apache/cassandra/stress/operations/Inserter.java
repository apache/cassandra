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

import com.yammer.metrics.core.TimerContext;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class Inserter extends Operation
{
    private static List<ByteBuffer> values;

    public Inserter(Session client, int index)
    {
        super(client, index);
    }

    public void run(CassandraClient client) throws IOException
    {
        if (values == null)
            values = generateValues();

        List<Column> columns = new ArrayList<Column>(session.getColumnsPerKey());
        List<SuperColumn> superColumns = null;

        // format used for keys
        String format = "%0" + session.getTotalKeysLength() + "d";

        for (int i = 0; i < session.getColumnsPerKey(); i++)
        {
            columns.add(new Column(columnName(i, session.timeUUIDComparator))
                            .setValue(values.get(i % values.size()))
                            .setTimestamp(FBUtilities.timestampMicros()));
        }

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            superColumns = new ArrayList<SuperColumn>();
            // supers = [SuperColumn('S' + str(j), columns) for j in xrange(supers_per_key)]
            for (int i = 0; i < session.getSuperColumns(); i++)
            {
                String superColumnName = "S" + Integer.toString(i);
                superColumns.add(new SuperColumn(ByteBufferUtil.bytes(superColumnName), columns));
            }
        }

        String rawKey = String.format(format, index);
        Map<String, List<Mutation>> row = session.getColumnFamilyType() == ColumnFamilyType.Super
                                        ? getSuperColumnsMutationMap(superColumns)
                                        : getColumnsMutationMap(columns);
        Map<ByteBuffer, Map<String, List<Mutation>>> record = Collections.singletonMap(ByteBufferUtil.bytes(rawKey), row);

        TimerContext context = session.latency.time();

        boolean success = false;
        String exceptionMessage = null;
        for (int t = 0; t < session.getRetryTimes(); t++)
        {
            if (success)
                break;

            try
            {
                client.batch_mutate(record, session.getConsistencyLevel());
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
            error(String.format("Operation [%d] retried %d times - error inserting key %s %s%n",
                                index,
                                session.getRetryTimes(),
                                rawKey,
                                (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndIncrement();
        context.stop();
    }

    private Map<String, List<Mutation>> getSuperColumnsMutationMap(List<SuperColumn> superColumns)
    {
        List<Mutation> mutations = new ArrayList<Mutation>(superColumns.size());
        for (SuperColumn s : superColumns)
        {
            ColumnOrSuperColumn superColumn = new ColumnOrSuperColumn().setSuper_column(s);
            mutations.add(new Mutation().setColumn_or_supercolumn(superColumn));
        }

        return Collections.singletonMap("Super1", mutations);
    }

    private Map<String, List<Mutation>> getColumnsMutationMap(List<Column> columns)
    {
        List<Mutation> mutations = new ArrayList<Mutation>(columns.size());
        for (Column c : columns)
        {
            ColumnOrSuperColumn column = new ColumnOrSuperColumn().setColumn(c);
            mutations.add(new Mutation().setColumn_or_supercolumn(column));
        }

        return Collections.singletonMap("Standard1", mutations);
    }
}
