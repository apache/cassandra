/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

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

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;

/** Measures creation of a cached Mutation serialization, including its backing array allocation. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = { "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor" })
@Threads(1)
@State(Scope.Benchmark)
public class MutationSerializationCachingBench
{
    static
    {
        CQLTester.setUpClass();
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Param({ "128", "16384", "262144", "786432" })
    public int valueSize;

    private Mutation mutation;
    private DataOutputBufferFixed output;

    @Setup
    public void setup()
    {
        String keyspace = "mutation_cache_bench";
        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1)), false);
        KeyspaceMetadata keyspaceMetadata = Schema.instance.getKeyspaceMetadata(keyspace);
        TableMetadata metadata = CreateTableStatement.parse("CREATE TABLE mutation_cache (pk bigint PRIMARY KEY, value blob)", keyspace)
                                                     .build();
        SchemaTestUtil.addOrUpdateKeyspace(keyspaceMetadata.withSwapped(keyspaceMetadata.tables.with(metadata)), false);

        mutation = (Mutation) UpdateBuilder.create(metadata, 1L)
                                            .newRow()
                                            .add("value", ByteBuffer.wrap(new byte[valueSize]))
                                            .makeMutation();
        output = new DataOutputBufferFixed(Math.toIntExact(mutation.serializedSize(MessagingService.current_version)));
    }

    @Benchmark
    public void cacheSerialization() throws IOException
    {
        mutation.clearCachedSerializationsForRetry();
        output.clear();
        Mutation.serializer.serialize(mutation, output, MessagingService.current_version);
    }
}
