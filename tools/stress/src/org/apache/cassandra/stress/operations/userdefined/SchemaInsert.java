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
import java.util.stream.Collectors;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.sstable.StressCQLSSTableWriter;
import org.apache.cassandra.stress.WorkManager;
import org.apache.cassandra.stress.generate.*;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;

public class SchemaInsert extends SchemaStatement
{

    private final String tableSchema;
    private final String insertStatement;
    private final BatchStatement.Type batchType;

    public SchemaInsert(Timer timer, StressSettings settings, PartitionGenerator generator, SeedManager seedManager, Distribution batchSize, RatioDistribution useRatio, RatioDistribution rowPopulation, Integer thriftId, PreparedStatement statement, ConsistencyLevel cl, BatchStatement.Type batchType)
    {
        super(timer, settings, new DataSpec(generator, seedManager, batchSize, useRatio, rowPopulation), statement, statement.getVariables().asList().stream().map(d -> d.getName()).collect(Collectors.toList()), thriftId, cl);
        this.batchType = batchType;
        this.insertStatement = null;
        this.tableSchema = null;
    }

    /**
     * Special constructor for offline use
     */
    public SchemaInsert(Timer timer, StressSettings settings, PartitionGenerator generator, SeedManager seedManager, RatioDistribution useRatio, RatioDistribution rowPopulation, Integer thriftId, String statement, String tableSchema)
    {
        super(timer, settings, new DataSpec(generator, seedManager, new DistributionFixed(1), useRatio, rowPopulation), null, generator.getColumnNames(), thriftId, ConsistencyLevel.ONE);
        this.batchType = BatchStatement.Type.UNLOGGED;
        this.insertStatement = statement;
        this.tableSchema = tableSchema;
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
            List<BoundStatement> stmts = new ArrayList<>();
            partitionCount = partitions.size();

            for (PartitionIterator iterator : partitions)
                while (iterator.hasNext())
                    stmts.add(bindRow(iterator.next()));

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

                client.getSession().execute(stmt);
            }
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
            for (PartitionIterator iterator : partitions)
            {
                while (iterator.hasNext())
                {
                    client.execute_prepared_cql3_query(thriftId, iterator.getToken(), thriftRowArgs(iterator.next()), settings.command.consistencyLevel);
                    rowCount += 1;
                }
            }
            return true;
        }
    }

    private class OfflineRun extends Runner
    {
        final StressCQLSSTableWriter writer;

        OfflineRun(StressCQLSSTableWriter writer)
        {
            this.writer = writer;
        }

        public boolean run() throws Exception
        {
            for (PartitionIterator iterator : partitions)
            {
                while (iterator.hasNext())
                {
                    Row row = iterator.next();
                    writer.rawAddRow(thriftRowArgs(row));
                    rowCount += 1;
                }
            }

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

    public StressCQLSSTableWriter createWriter(ColumnFamilyStore cfs, int bufferSize, boolean makeRangeAware)
    {
        return StressCQLSSTableWriter.builder()
                               .withCfs(cfs)
                               .withBufferSizeInMB(bufferSize)
                               .forTable(tableSchema)
                               .using(insertStatement)
                               .rangeAware(makeRangeAware)
                               .build();
    }

    public void runOffline(StressCQLSSTableWriter writer, WorkManager workManager) throws Exception
    {
        OfflineRun offline = new OfflineRun(writer);

        while (true)
        {
            if (ready(workManager) == 0)
                break;

            offline.run();
        }
    }
}
