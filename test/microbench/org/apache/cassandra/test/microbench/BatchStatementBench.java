/*
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

package org.apache.cassandra.test.microbench;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;


@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class BatchStatementBench
{
    static
    {

        DatabaseDescriptor.toolInitialization();
        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    static String keyspace = "keyspace1";
    String table = "tbl";

    long nowInSec = FBUtilities.nowInSeconds();
    long queryStartTime = nanoTime();
    BatchStatement bs;
    BatchQueryOptions bqo;

    @Param({"true", "false"})
    boolean uniquePartition;

    @Param({"10000"})
    int batchSize;

    @Setup
    public void setup() throws Throwable
    {
        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1)), false);
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        TableMetadata metadata = CreateTableStatement.parse(String.format("CREATE TABLE %s (id int, ck int, v int, primary key (id, ck))", table), keyspace).build();

        SchemaTestUtil.addOrUpdateKeyspace(ksm.withSwapped(ksm.tables.with(metadata)), false);

        List<ModificationStatement> modifications = new ArrayList<>(batchSize);
        List<List<ByteBuffer>> parameters = new ArrayList<>(batchSize);
        List<Object> queryOrIdList = new ArrayList<>(batchSize);
        QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(String.format("INSERT INTO %s.%s (id, ck, v) VALUES (?,?,?)", keyspace, table));

        for (int i = 0; i < batchSize; i++)
        {
            modifications.add((ModificationStatement) prepared.statement);
            parameters.add(Lists.newArrayList(bytes(uniquePartition ? i : 1), bytes(i), bytes(i)));
            queryOrIdList.add(prepared.rawCQLStatement);
        }
        bs = new BatchStatement(BatchStatement.Type.UNLOGGED, VariableSpecifications.empty(), modifications, Attributes.none());
        bqo = BatchQueryOptions.withPerStatementVariables(QueryOptions.DEFAULT, parameters, queryOrIdList);
    }

    @Benchmark
    public void bench()
    {
        bs.getMutations(ClientState.forInternalCalls(), bqo, false, nowInSec, nowInSec, queryStartTime);
    }


    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
                       .include(".*"+BatchStatementBench.class.getSimpleName()+".*")
                       .jvmArgs("-server")
                       .forks(1)
                       .mode(Mode.Throughput)
                       .addProfiler(GCProfiler.class)
                       .build();

        Collection<RunResult> records = new Runner(opts).run();
        for ( RunResult result : records) {
            Result r = result.getPrimaryResult();
            System.out.println("API replied benchmark score: "
                               + r.getScore() + " "
                               + r.getScoreUnit() + " over "
                               + r.getStatistics().getN() + " iterations");
        }
    }
}
