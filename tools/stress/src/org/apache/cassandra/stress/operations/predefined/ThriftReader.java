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
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;

public final class ThriftReader extends PredefinedOperation
{

    public ThriftReader(Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings)
    {
        super(Command.READ, timer, generator, seedManager, settings);
    }

    public void run(final ThriftClient client) throws IOException
    {
        final ColumnSelection select = select();
        final ByteBuffer key = getKey();
        final List<ByteBuffer> expect = getColumnValues(select);
        timeWithRetry(new RunOp()
        {
            @Override
            public boolean run() throws Exception
            {
                List<ColumnOrSuperColumn> row = client.get_slice(key, new ColumnParent(type.table), select.predicate(), settings.command.consistencyLevel);
                if (expect == null)
                    return !row.isEmpty();
                if (row == null)
                    return false;
                if (row.size() != expect.size())
                    return false;
                for (int i = 0 ; i < row.size() ; i++)
                    if (!row.get(i).getColumn().bufferForValue().equals(expect.get(i)))
                        return false;
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
