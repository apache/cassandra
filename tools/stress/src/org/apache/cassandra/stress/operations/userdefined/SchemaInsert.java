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
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.stress.generate.Distribution;
import org.apache.cassandra.stress.generate.Partition;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.RatioDistribution;
import org.apache.cassandra.stress.generate.Row;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.settings.ValidationType;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;

public class SchemaInsert extends SchemaStatement
{

    private final BatchStatement.Type batchType;
    private final RatioDistribution selectChance;

    public SchemaInsert(Timer timer, PartitionGenerator generator, StressSettings settings, Distribution batchSize, RatioDistribution selectChance, Integer thriftId, PreparedStatement statement, ConsistencyLevel cl, BatchStatement.Type batchType)
    {
        super(timer, generator, settings, batchSize, statement, thriftId, cl, ValidationType.NOT_FAIL);
        this.batchType = batchType;
        this.selectChance = selectChance;
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
            Partition.RowIterator[] iterators = new Partition.RowIterator[partitions.size()];
            for (int i = 0 ; i < iterators.length ; i++)
                iterators[i] = partitions.get(i).iterator(selectChance.next(), true);
            List<BoundStatement> stmts = new ArrayList<>();
            partitionCount = partitions.size();

            for (Partition.RowIterator iterator : iterators)
            {
                if (iterator.done())
                    continue;

                for (Row row : iterator.next())
                    stmts.add(bindRow(row));
            }
            rowCount += stmts.size();

            // 65535 is max number of stmts per batch, so if we have more, we need to manually batch them
            for (int j = 0 ; j < stmts.size() ; j += 65535)
            {
                List<BoundStatement> substmts = stmts.subList(j, Math.min(j + stmts.size(), j + 65535));
                Statement stmt;
                if (stmts.size() == 1)
                {
                    stmt = substmts.get(0);
                }
                else
                {
                    BatchStatement batch = new BatchStatement(batchType);
                    batch.setConsistencyLevel(JavaDriverClient.from(cl));
                    batch.addAll(substmts);
                    stmt = batch;
                }

                try
                {
                    validate(client.getSession().execute(stmt));
                }
                catch (ClassCastException e)
                {
                    e.printStackTrace();
                }
            }

            for (Partition.RowIterator iterator : iterators)
                iterator.markWriteFinished();

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
            Partition.RowIterator[] iterators = new Partition.RowIterator[partitions.size()];
            for (int i = 0 ; i < iterators.length ; i++)
                iterators[i] = partitions.get(i).iterator(selectChance.next(), true);
            partitionCount = partitions.size();

            for (Partition.RowIterator iterator : iterators)
            {
                if (iterator.done())
                    continue;

                for (Row row : iterator.next())
                {
                    validate(client.execute_prepared_cql3_query(thriftId, iterator.partition().getToken(), thriftRowArgs(row), settings.command.consistencyLevel));
                    rowCount += 1;
                }
            }

            for (Partition.RowIterator iterator : iterators)
                iterator.markWriteFinished();

            return true;
        }
    }

    @Override
    public void run(JavaDriverClient client) throws IOException
    {
        timeWithRetry(new JavaDriverRun(client));
    }

    public boolean isWrite()
    {
        return true;
    }

    @Override
    public void run(ThriftClient client) throws IOException
    {
        timeWithRetry(new ThriftRun(client));
    }

}
