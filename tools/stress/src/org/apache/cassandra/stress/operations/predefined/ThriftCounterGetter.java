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
import java.util.List;

import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.settings.Command;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.SlicePredicate;

public class ThriftCounterGetter extends PredefinedOperation
{
    public ThriftCounterGetter(Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings)
    {
        super(Command.COUNTER_READ, timer, generator, seedManager, settings);
    }

    public void run(final ThriftClient client) throws IOException
    {
        final SlicePredicate predicate = select().predicate();
        final ByteBuffer key = getKey();
        timeWithRetry(new RunOp()
        {
            @Override
            public boolean run() throws Exception
            {
                List<?> r = client.get_slice(key, new ColumnParent(type.table), predicate, settings.command.consistencyLevel);
                return r != null && r.size() > 0;
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
