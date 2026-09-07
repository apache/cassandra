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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.Epoch;

/**
 * The per-statement cost of a schema change, as a function of how many tables the cluster already holds.
 *
 * <p>Every DDL statement runs {@link Keyspaces#diff} several times, mutates a {@link Tables} collection, and builds a
 * {@link DistributedSchema} at least twice. If any of those costs grows with the size of the schema, bulk table
 * creation is O(n^2) and the cluster hits a GC wall rather than a gradual slowdown.
 *
 * <p>The interesting number here is {@code gc.alloc.rate.norm} (bytes/op, reported by {@code -prof gc}), not elapsed
 * time: allocation is what accumulates into the wall. Run with:
 *
 * <pre>
 * ant microbench -Dbenchmark.name=SchemaChangeBench -Djmh.args="-prof gc -f 1 -wi 3 -i 5 -r 1"
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class SchemaChangeBench
{
    /** Tables held by the single keyspace under test before the change being measured is applied. */
    @Param({ "400", "3200" })
    int tableCount;

    /** {@code before} and {@code after} of a CREATE TABLE against a keyspace already holding {@link #tableCount}. */
    private Keyspaces diffBefore;
    private Keyspaces diffAfter;

    /** The table collection of that keyspace, and the three tables the mutations operate on. */
    private Tables tables;
    private TableMetadata addedTable;
    private TableMetadata swappedTable;
    private String removedTableName;

    /** A schema over the same keyspace, and the epoch a DDL transformation would stamp onto it. */
    private DistributedSchema schema;
    private Epoch nextEpoch;

    @Setup(Level.Trial)
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();

        List<TableMetadata> existing = new ArrayList<>(tableCount);
        for (int i = 0; i < tableCount; i++)
            existing.add(table("ks", i));

        tables = Tables.of(existing);
        addedTable = table("ks", tableCount);
        swappedTable = existing.get(7).unbuild().comment("altered").build();
        removedTableName = existing.get(7).name;

        diffBefore = Keyspaces.of(KeyspaceMetadata.create("ks", KeyspaceParams.simple(1), tables));
        diffAfter = Keyspaces.of(KeyspaceMetadata.create("ks", KeyspaceParams.simple(1), tables.with(addedTable)));

        schema = new DistributedSchema(diffBefore, Epoch.FIRST);
        nextEpoch = Epoch.FIRST.nextEpoch();
    }

    /** CASSANDRA-21659: diffing two schemas that differ by exactly one table. */
    @Benchmark
    public Keyspaces.KeyspacesDiff keyspacesDiff()
    {
        return Keyspaces.diff(diffBefore, diffAfter);
    }

    /** CASSANDRA-21660: CREATE TABLE. */
    @Benchmark
    public Tables tablesWith()
    {
        return tables.with(addedTable);
    }

    /** CASSANDRA-21660: ALTER TABLE. */
    @Benchmark
    public Tables tablesWithSwapped()
    {
        return tables.withSwapped(swappedTable);
    }

    /** CASSANDRA-21660: DROP TABLE. */
    @Benchmark
    public Tables tablesWithout()
    {
        return tables.without(removedTableName);
    }

    /** CASSANDRA-21661: stamping the new epoch onto the schema a DDL transformation produced. */
    @Benchmark
    public DistributedSchema distributedSchemaWithLastModified()
    {
        return schema.withLastModified(nextEpoch);
    }

    private static TableMetadata table(String keyspace, int i)
    {
        return TableMetadata.builder(keyspace, "table_" + i)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addClusteringColumn("ck", Int32Type.instance)
                            .addRegularColumn("v", Int32Type.instance)
                            .build();
    }
}
