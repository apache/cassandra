/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.test.microbench.index.sai;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks SAI queries with different selectivities.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 2) // seconds
@Measurement(iterations = 5, time = 2) // seconds
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class QuerySelectivityBench extends CQLTester
{
    static final Random RANDOM = new Random();

    /** The SAI index format version, {@code none} for no index. */
    @Param({ "aa", "ec", "ga", "none" })
    public String version;

    /** The number of partitions to be inserted. */
    @Param({ "100" })
    public int partitions;

    /** The number of rows per partition to be inserted. */
    @Param({ "1000" })
    public int rowsPerPartition;

    /** The size of payload added to the clustering key, in bytes. */
    @Param({ "100" })
    public int clusteringSizeInBytes;

    /** The size of payload added to the row's regular columns, in bytes. */
    @Param({ "1000" })
    public int payloadSizeInBytes;

    /** The selectivity of the query, defined as the number of rows matching the query divided by the total number of rows. */
    @Param({ "0.01", "0.03", "0.05", "0.1", "0.2", "0.3", "0.4", "0.5", "0.7", "0.9", "1" })
    public float selectivity;

    /** The LIMIT of the query. */
    @Param({ "5000" })
    public int limit;

    private String partitionQuery;
    private String rangeQuery;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        if (!version.equals("none"))
            CassandraRelevantProperties.SAI_CURRENT_VERSION.setString(version);

        CQLTester.setUpClass();
        CQLTester.prepareServer();
        beforeTest();
        DatabaseDescriptor.setAutoSnapshot(false);

        // create the schema
        String table = createTable("CREATE TABLE %s (k int, c1 int, c2 blob, v int, p blob, PRIMARY KEY (k, c1, c2))");
        if (!version.equals("none"))
            createIndex("CREATE CUSTOM INDEX idx ON %s(v) USING 'StorageAttachedIndex'");

        // disable autocompaction so it doesn't interfere
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // insert data
        for (int k = 0; k < partitions; k++)
        {
            for (int c1 = 0; c1 < rowsPerPartition; c1++)
            {
                ByteBuffer c2 = randomBuffer(clusteringSizeInBytes);
                int value = RANDOM.nextInt((int) (1.0f / selectivity));
                ByteBuffer payload = randomBuffer(payloadSizeInBytes);
                execute("INSERT INTO %s (k, c1, c2, v, p) VALUES (?, ?, ?, ?, ?)", k, c1, c2, value, payload);
            }
        }
        flush();

        // prepare queries
        partitionQuery = String.format("SELECT * FROM %s.%s WHERE k = ? AND v = 0 LIMIT %s ALLOW FILTERING", KEYSPACE, table, limit);
        rangeQuery = String.format("SELECT * FROM %s.%s WHERE v = 0 LIMIT %s ALLOW FILTERING", KEYSPACE, table, limit);
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CommitLog.instance.shutdownBlocking();
        CQLTester.cleanup();
    }

    @Benchmark
    public Object partitionQuery()
    {
        int k = RANDOM.nextInt(partitions);
        return execute(partitionQuery, k).size();
    }

    @Benchmark
    public Object rangeQuery()
    {
        return execute(rangeQuery).size();
    }

    private ByteBuffer randomBuffer(int size)
    {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        RANDOM.nextBytes(buffer.array());
        return buffer;
    }
}
