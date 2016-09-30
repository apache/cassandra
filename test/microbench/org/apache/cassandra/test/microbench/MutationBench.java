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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.*;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1
       , jvmArgsAppend = {"-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"
       //,"-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder","-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints",
       // "-XX:StartFlightRecording=duration=60s,filename=./profiling-data.jfr,name=profile,settings=profile",
       // "-XX:FlightRecorderOptions=settings=/home/jake/workspace/cassandra/profiling-advanced.jfc,samplethreads=true"
     }
)
@Threads(1)
@State(Scope.Benchmark)
public class MutationBench
{
    static
    {
        DatabaseDescriptor.clientInitialization(false);
        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    static String keyspace = "keyspace1";

    private Mutation mutation;
    private MessageOut<Mutation> messageOut;

    private ByteBuffer buffer;
    private DataOutputBuffer outputBuffer;
    private DataInputBuffer inputBuffer;


    @State(Scope.Thread)
    public static class ThreadState
    {
        MessageIn<Mutation> in;
        int counter = 0;
    }

    @Setup
    public void setup() throws IOException
    {
        Schema.instance.load(KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1)));
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspace);
        CFMetaData metadata = CFMetaData.compile("CREATE TABLE userpics " +
                                                   "( userid bigint," +
                                                   "picid bigint," +
                                                   "commentid bigint, " +
                                                   "PRIMARY KEY(userid, picid))", keyspace);

        Schema.instance.load(metadata);
        Schema.instance.setKeyspaceMetadata(ksm.withSwapped(ksm.tables.with(metadata)));


        mutation = (Mutation)UpdateBuilder.create(metadata, 1L).newRow(1L).add("commentid", 32L).makeMutation();
        messageOut = mutation.createMessage();
        buffer = ByteBuffer.allocate(messageOut.serializedSize(MessagingService.current_version));
        outputBuffer = new DataOutputBufferFixed(buffer);
        inputBuffer = new DataInputBuffer(buffer, false);

        messageOut.serialize(outputBuffer, MessagingService.current_version);
    }

    @Benchmark
    public void serialize(ThreadState state) throws IOException
    {
        buffer.rewind();
        messageOut.serialize(outputBuffer, MessagingService.current_version);
        state.counter++;
    }

    @Benchmark
    public void deserialize(ThreadState state) throws IOException
    {
        buffer.rewind();
        state.in = MessageIn.read(inputBuffer, MessagingService.current_version, 0);
        state.counter++;
    }

    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
                       .include(".*"+MutationBench.class.getSimpleName()+".*")
                       .jvmArgs("-server")
                       .forks(1)
                       .mode(Mode.Throughput)
                       .addProfiler(StackProfiler.class)
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
