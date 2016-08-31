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

import org.apache.cassandra.stress.generate.Distribution;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.settings.Command;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.Mutation;

public class ThriftCounterAdder extends PredefinedOperation
{

    final Distribution counteradd;
    public ThriftCounterAdder(DistributionFactory counteradd, Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings)
    {
        super(Command.COUNTER_WRITE, timer, generator, seedManager, settings);
        this.counteradd = counteradd.get();
    }

    public boolean isWrite()
    {
        return true;
    }

    public void run(final ThriftClient client) throws IOException
    {
        List<CounterColumn> columns = new ArrayList<>();
        for (ByteBuffer name : select().select(settings.columns.names))
            columns.add(new CounterColumn(name, counteradd.next()));

        List<Mutation> mutations = new ArrayList<>(columns.size());
        for (CounterColumn c : columns)
        {
            ColumnOrSuperColumn cosc = new ColumnOrSuperColumn().setCounter_column(c);
            mutations.add(new Mutation().setColumn_or_supercolumn(cosc));
        }
        Map<String, List<Mutation>> row = Collections.singletonMap(type.table, mutations);

        final ByteBuffer key = getKey();
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

}
