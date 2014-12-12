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
package org.apache.cassandra.stress.operations.predefined;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.settings.Command;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.FBUtilities;

public final class ThriftInserter extends PredefinedOperation
{

    public ThriftInserter(Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings)
    {
        super(Command.WRITE, timer, generator, seedManager, settings);
    }

    public boolean isWrite()
    {
        return true;
    }

    public void run(final ThriftClient client) throws IOException
    {
        final ByteBuffer key = getKey();
        final List<Column> columns = getColumns();

        List<Mutation> mutations = new ArrayList<>(columns.size());
        for (Column c : columns)
        {
            ColumnOrSuperColumn column = new ColumnOrSuperColumn().setColumn(c);
            mutations.add(new Mutation().setColumn_or_supercolumn(column));
        }
        Map<String, List<Mutation>> row = Collections.singletonMap(type.table, mutations);

        final Map<ByteBuffer, Map<String, List<Mutation>>> record = Collections.singletonMap(key, row);

        timeWithRetry(new RunOp()
        {
            @Override
            public boolean run() throws Exception
            {
                client.batch_mutate(record, settings.command.consistencyLevel);
                return true;
            }

            @Override
            public int partitionCount()
            {
                return 1;
            }

            @Override
            public int rowCount()
            {
                return 1;
            }
        });
    }

    protected List<Column> getColumns()
    {
        final ColumnSelection selection = select();
        final List<ByteBuffer> values = getColumnValues(selection);
        final List<Column> columns = new ArrayList<>(values.size());
        final List<ByteBuffer> names = select().select(settings.columns.names);
        for (int i = 0 ; i < values.size() ; i++)
            columns.add(new Column(names.get(i))
                        .setValue(values.get(i))
                        .setTimestamp(settings.columns.timestamp != null
                                      ? Long.parseLong(settings.columns.timestamp)
                                      : FBUtilities.timestampMicros()));
        return columns;
    }

}
