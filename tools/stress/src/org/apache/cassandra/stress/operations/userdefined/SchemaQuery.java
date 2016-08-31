package org.apache.cassandra.stress.operations.userdefined;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.stress.generate.*;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.ThriftConversion;

public class SchemaQuery extends SchemaStatement
{
    public static enum ArgSelect
    {
        MULTIROW, SAMEROW;
        //TODO: FIRSTROW, LASTROW
    }

    final ArgSelect argSelect;
    final Object[][] randomBuffer;
    final Random random = new Random();

    public SchemaQuery(Timer timer, StressSettings settings, PartitionGenerator generator, SeedManager seedManager, Integer thriftId, PreparedStatement statement, ConsistencyLevel cl, ArgSelect argSelect)
    {
        super(timer, settings, new DataSpec(generator, seedManager, new DistributionFixed(1), settings.insert.rowPopulationRatio.get(), argSelect == ArgSelect.MULTIROW ? statement.getVariables().size() : 1), statement,
              statement.getVariables().asList().stream().map(d -> d.getName()).collect(Collectors.toList()), thriftId, cl);
        this.argSelect = argSelect;
        randomBuffer = new Object[argumentIndex.length][argumentIndex.length];
    }

    private class JavaDriverRun extends Runner
    {
        final JavaDriverClient client;

        private JavaDriverRun(JavaDriverClient client)
        {
            this.client = client;
        }

        public boolean run() throws Exception
        {
            ResultSet rs = client.getSession().execute(bindArgs());
            rowCount = rs.all().size();
            partitionCount = Math.min(1, rowCount);
            return true;
        }
    }

    private class ThriftRun extends Runner
    {
        final ThriftClient client;

        private ThriftRun(ThriftClient client)
        {
            this.client = client;
        }

        public boolean run() throws Exception
        {
            CqlResult rs = client.execute_prepared_cql3_query(thriftId, partitions.get(0).getToken(), thriftArgs(), ThriftConversion.toThrift(cl));
            rowCount = rs.getRowsSize();
            partitionCount = Math.min(1, rowCount);
            return true;
        }
    }

    private int fillRandom()
    {
        int c = 0;
        PartitionIterator iterator = partitions.get(0);
        while (iterator.hasNext())
        {
            Row row = iterator.next();
            Object[] randomBufferRow = randomBuffer[c++];
            for (int i = 0 ; i < argumentIndex.length ; i++)
                randomBufferRow[i] = row.get(argumentIndex[i]);
            if (c >= randomBuffer.length)
                break;
        }
        assert c > 0;
        return c;
    }

    BoundStatement bindArgs()
    {
        switch (argSelect)
        {
            case MULTIROW:
                int c = fillRandom();
                for (int i = 0 ; i < argumentIndex.length ; i++)
                {
                    int argIndex = argumentIndex[i];
                    bindBuffer[i] = randomBuffer[argIndex < 0 ? 0 : random.nextInt(c)][i];
                }
                return statement.bind(bindBuffer);
            case SAMEROW:
                return bindRow(partitions.get(0).next());
            default:
                throw new IllegalStateException();
        }
    }

    List<ByteBuffer> thriftArgs()
    {
        switch (argSelect)
        {
            case MULTIROW:
                List<ByteBuffer> args = new ArrayList<>();
                int c = fillRandom();
                for (int i = 0 ; i < argumentIndex.length ; i++)
                {
                    int argIndex = argumentIndex[i];
                    args.add(spec.partitionGenerator.convert(argIndex, randomBuffer[argIndex < 0 ? 0 : random.nextInt(c)][i]));
                }
                return args;
            case SAMEROW:
                return thriftRowArgs(partitions.get(0).next());
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public void run(JavaDriverClient client) throws IOException
    {
        timeWithRetry(new JavaDriverRun(client));
    }

    @Override
    public void run(ThriftClient client) throws IOException
    {
        timeWithRetry(new ThriftRun(client));
    }

}
