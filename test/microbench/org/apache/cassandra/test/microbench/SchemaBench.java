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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.tcm.Epoch;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class SchemaBench
{
    public static class BenchmarkingDistributedSchema extends DistributedSchema
    {
        public BenchmarkingDistributedSchema(Keyspaces keyspaces, Epoch epoch, boolean validate)
        {
            super(keyspaces, epoch, validate);
        }

        @Override
        public void validate()
        {
            super.validate();
        }
    }

    @Param({"1", "1000", "10000"})
    int numKeyspaces;

    private BenchmarkingDistributedSchema schema;

    @Setup(Level.Trial)
    public void setUp() throws IOException
    {
        createSchema();
    }

    private void createSchema()
    {
        List<KeyspaceMetadata> keyspaceMetadatas = new ArrayList<>(numKeyspaces);
        for (int i = 0; i < numKeyspaces; i++)
        {
            String keyspaceName = String.format("ks_%d", i);
            keyspaceMetadatas.add(KeyspaceMetadata.create(keyspaceName, KeyspaceParams.simple(3)));
        }

        Keyspaces keyspaces = Keyspaces.of(keyspaceMetadatas.toArray(new KeyspaceMetadata[numKeyspaces]));
        schema = new BenchmarkingDistributedSchema(keyspaces, Epoch.EMPTY, false);
    }

    @Benchmark
    public void validate()
    {
        schema.validate();
    }
}