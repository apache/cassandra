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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
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

import static org.apache.cassandra.utils.JVMStabilityInspector.removeShutdownHooks;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public abstract class BaseCompactionBenchmark extends CQLTester
{
    @Param( {"DEFAULT"} )
    DataBuilderHelper dataBuilder;

    @Param( {"1", "2", "4"} )
    int compactors;

    @Param( {"1", "2", "4"} )
    int size;

    @Param( {"2"} )
    int sstableCount;

    @Param( {"0"} )
    int compactionMbSecThrottle;

    @Param("false")
    boolean compression;

    @Param("1.0")
    double overlapRatio;

    ColumnFamilyStore cfs;

    protected long rowsPerSSTable;
    protected long mergedRows;

    @Setup(Level.Trial)
    public final void setup() throws Throwable
    {
        if (sstableCount != 2)
            throw new IllegalArgumentException("Not implemented yet");
        if (compactors < 1)
            throw new IllegalArgumentException();
        if (size < 1)
            throw new IllegalArgumentException();

        prepareServer();

        String keyspaceName = createKeyspace("CREATE KEYSPACE %s with replication = " +
                "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }" +
                " and durable_writes = false");

        String createTableStatement =
                String.format("%s with compaction = { 'class' : '%s' } and compression = %s",
                              dataBuilder.createTableStatement(),
                              compactionClass(),
                              (compression ? "{ 'sstable_compression' : 'LZ4Compressor' }" : "{ 'enabled' : false }")
                              );
        String tableName = createTable(keyspaceName, createTableStatement);

        execute("use " + keyspaceName + ";");
        String writeStatement = dataBuilder.writeStatement(tableName);

        // we need this to use prepared statements
        requireNetwork();
        try (Cluster cluster = clusterBuilder().build();
             Session session = cluster.newSession())
        {
            session.execute("use " + keyspaceName + ";");

            PreparedStatement write = session.prepare(writeStatement);
            Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

            cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);
            cfs.disableAutoCompaction();

            List<ResultSetFuture> futures = new ArrayList<>(256);
            rowsPerSSTable = this.size * 100000;

            System.err.print("\nWriting " + this.size + "00k");
            cfs.unsafeRunWithoutFlushing(()-> writeSSTable(session, write, tableName, futures, rowsPerSSTable, 0));
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

            long start = (long) ((1.0 - overlapRatio) * rowsPerSSTable);

            System.err.print("Writing " + this.size + "00k with overlapRatio=" + overlapRatio + " start=" + start);
            cfs.unsafeRunWithoutFlushing(()-> writeSSTable(session, write, tableName, futures, rowsPerSSTable, start));
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            mergedRows = start + rowsPerSSTable - dataBuilder.deletions(start + rowsPerSSTable);
            if (cfs.getLiveSSTables().size() != sstableCount)
                throw new IllegalStateException("Should have " + sstableCount + " SSTables");
            StorageService.instance.setConcurrentCompactors(compactors);
            DatabaseDescriptor.setCompactionThroughputMbPerSec(compactionMbSecThrottle);
            DatabaseDescriptor.setAutoSnapshot(false);
        }
    }

    // overridden by CompactionBenchmark
    public String compactionClass()
    {
        return SizeTieredCompactionStrategy.class.getSimpleName();
    }

    protected void writeSSTable(
            Session session,
            PreparedStatement write,
            String tableName,
            List<ResultSetFuture> futures,
            long size,
            long offset)
    {
        long startNs = System.nanoTime();
        long percent = size / 100;
        for (long i = offset; i < offset + size; i++)
        {
            futures.add(session.executeAsync(dataBuilder.bindWrite(session, tableName, write, i)));
            if (futures.size() == 256)
            {
                FBUtilities.waitOnFutures(futures);
                futures.clear();
            }
            if ((i - offset) % percent == 0)
            {
                System.err.print(".");
            }

        }
        FBUtilities.waitOnFutures(futures);
        futures.clear();
        long doneWriting = System.nanoTime();
        System.err.printf(" - done in %.3f seconds\n", (doneWriting - startNs) / 1_000_000_000.0);
    }

    @TearDown(Level.Invocation)
    public final void teardownInvocation() throws IOException
    {
        // Tests are currently done without clearing cache to better show processing performance differences.
        // ChunkCache.instance.enable(true);// drops the cache
    }


    @TearDown(Level.Trial)
    public void teardown() throws Throwable
    {
        removeShutdownHooks();
        tearDownClass();
        cleanup();
    }

    static final ByteBuffer BLOB = ByteBuffer.allocate(Integer.getInteger("benchmark.blobSize", 100));
    static final int FIELD_COUNT = Integer.getInteger("benchmark.fieldCount", 10);
    static final int ROWS_PER_PARTITION = Integer.getInteger("benchmark.rowsPerPartition", 100);
    static final int TOMBSTONE_FREQUENCY = Integer.getInteger("benchmark.fieldCount", 10);

    public enum DataBuilderHelper
    {
        DEFAULT,
        BLOB_CLUSTER_KEY {
            @Override
            protected String createTableStatement()
            {
                return "CREATE TABLE %s ( userid bigint, picid blob, commentid bigint, PRIMARY KEY(userid, picid))";
            }

            @Override
            protected BoundStatement bindWrite(Session session, String tableName, PreparedStatement write, long i)
            {
                return write.bind(i, BLOB.duplicate(), i);
            }
        },
        BLOB_VALUE{
            @Override
            protected String createTableStatement()
            {
                return "CREATE TABLE %s ( userid bigint, picid bigint, commentid blob, PRIMARY KEY(userid, picid))";
            }
            @Override
            protected BoundStatement bindWrite(Session session, String tableName, PreparedStatement write, long i)
            {
                return write.bind(i, i, BLOB.duplicate());
            }

        },
        MANY_CLUSTER_KEYS{
            @Override
            protected String createTableStatement()
            {
                String table = "CREATE TABLE %s ( userid bigint, ";
                for (int i= 0; i < FIELD_COUNT; i++)
                    table += "ck" + i +" bigint, ";
                table += " commentid bigint, PRIMARY KEY(userid";
                for (int i= 0; i < FIELD_COUNT; i++)
                    table += ", ck" + i;
                table += "))";
                return table;
            }

            @Override
            protected String writeStatement(String tableName)
            {
                String write = "INSERT INTO " + tableName + " (userid";
                for (int i= 0; i < FIELD_COUNT; i++)
                    write += ", ck" + i;

                write += ", commentid) VALUES (?, ?";
                for (int i= 0; i < FIELD_COUNT; i++)
                    write += ", ?";
                write += ")";
                return write;
            }

            @Override
            protected BoundStatement bindWrite(Session session, String tableName, PreparedStatement write, long i)
            {
                Long[] values = new Long[FIELD_COUNT +2];
                Arrays.fill(values, new Long(i));
                return write.bind((Object[])values);
            }
        },
        MANY_FIELDS {
            @Override
            protected String createTableStatement()
            {
                String table = "CREATE TABLE %s ( userid bigint, picid bigint, ";
                for (int i= 0; i < FIELD_COUNT; i++)
                    table += "v" + i +" bigint, ";
                table += " PRIMARY KEY(userid, picid))";
                return table;
            }

            @Override
            protected String writeStatement(String tableName)
            {
                String write = "INSERT INTO " + tableName + " (userid, picid";
                for (int i= 0; i < FIELD_COUNT; i++)
                    write += ", v" + i;

                write += ") VALUES (?, ?";
                for (int i= 0; i < FIELD_COUNT; i++)
                    write += ", ?";
                write += ")";
                return write;
            }

            @Override
            protected BoundStatement bindWrite(Session session, String tableName, PreparedStatement write, long i)
            {
                Long[] values = new Long[FIELD_COUNT +2];
                Arrays.fill(values, new Long(i));
                return write.bind((Object[]) values);
            }
        },
        WIDE_PARTITIONS {
            protected BoundStatement bindWrite(Session session, String tableName, PreparedStatement write, long i)
            {
                return write.bind(i / ROWS_PER_PARTITION, i % ROWS_PER_PARTITION, i);
            }

        },
        COMPLEX_COLUMNS_INSERT {
            Random rand = new Random(1);

            @Override
            protected String createTableStatement()
            {
                return "CREATE TABLE %s ( userid bigint, picid bigint, comments set<bigint>,  PRIMARY KEY(userid, picid))";
            }

            @Override
            protected String writeStatement(String tableName)
            {
                return "INSERT INTO " + tableName + " (userid, picid, comments) VALUES (?, ?, ?)";
            }

            @Override
            protected BoundStatement bindWrite(Session session, String tableName, PreparedStatement write, long i)
            {
                return write.bind(i, i, ImmutableSet.of(i + rand.nextInt(2), i + 2 + rand.nextInt(3)));
            }
        },
        COMPLEX_COLUMNS_UPDATE_SET {
            Random rand = new Random(1);

            @Override
            protected String createTableStatement()
            {
                return "CREATE TABLE %s ( userid bigint, picid bigint, comments set<bigint>,  PRIMARY KEY(userid, picid))";
            }

            @Override
            protected String writeStatement(String tableName)
            {
                return "UPDATE " + tableName + " SET comments = ? WHERE userid = ? AND picid = ?";
            }

            @Override
            protected BoundStatement bindWrite(Session session, String tableName, PreparedStatement write, long i)
            {
                return write.bind(ImmutableSet.of(i + rand.nextInt(2), i + 2 + rand.nextInt(3)), i, i);
            }
        },
        COMPLEX_COLUMNS_UPDATE_ADD {
            Random rand = new Random(1);

            @Override
            protected String createTableStatement()
            {
                return "CREATE TABLE %s ( userid bigint, picid bigint, comments set<bigint>,  PRIMARY KEY(userid, picid))";
            }

            @Override
            protected String writeStatement(String tableName)
            {
                return "UPDATE " + tableName + " SET comments = comments + ? WHERE userid = ? AND picid = ?";
            }

            @Override
            protected BoundStatement bindWrite(Session session, String tableName, PreparedStatement write, long i)
            {
                return write.bind(ImmutableSet.of(i + rand.nextInt(2), i + 2 + rand.nextInt(3)), i, i);
            }
        },
        TOMBSTONES {
            BitSet deletions = new BitSet();
            Random rand = new Random(1);

            @Override
            protected BoundStatement bindWrite(Session session, String tableName, PreparedStatement write, long i)
            {
                switch (rand.nextInt(TOMBSTONE_FREQUENCY * 3))
                {
                    case 0: // partition tombstone
                        deletions.set((int) i);
                        return session.prepare("DELETE FROM " + tableName + " WHERE userid = ?").bind(i);
                    case 1: // row tombstone
                        deletions.clear((int) i);   // this still gets reported as a row (empty, with tombstone)
                        return session.prepare("DELETE FROM " + tableName + " WHERE userid = ? AND picid = ?").bind(i, i);
                    case 2: // range tombstone
                        deletions.set((int) i);
                        return session.prepare("DELETE FROM " + tableName + " WHERE userid = ? AND picid >= ? AND picid < ?").bind(i, i - 2, i + 2);
                    default:
                        deletions.clear((int) i);
                        return super.bindWrite(session, tableName, write, i);
                }
            }

            @Override
            protected long deletions(long max)
            {
                return deletions.cardinality();
            }
        },
        TOMBSTONES_WIDE {
            BitSet deletions = new BitSet();
            Random rand = new Random(1);

            @Override
            protected BoundStatement bindWrite(Session session, String tableName, PreparedStatement write, long i)
            {
                switch (rand.nextInt(TOMBSTONE_FREQUENCY * 2))
                {
                    case 0: // row tombstone
                        deletions.clear((int) i);   // this still gets reported as a row (empty, with tombstone)
                        return session.prepare("DELETE FROM " + tableName + " WHERE userid = ? AND picid = ?").bind(1L, i);
                    case 1: // range tombstone
                        deletions.set(Math.max(0, (int) i - 2), (int) i + 2);
                        return session.prepare("DELETE FROM " + tableName + " WHERE userid = ? AND picid >= ? AND picid < ?").bind(1L, i - 2, i + 2);
                    default:
                        deletions.clear((int) i);
                        return write.bind(1L, i, i);
                }
            }

            @Override
            protected long deletions(long max)
            {
                deletions.clear((int) max, (int) max + 2);
                return deletions.cardinality();
            }
        }
        ;

        /** override the following to benchmark different schema or data distribution */
        protected String writeStatement(String tableName)
        {
            return "INSERT INTO " + tableName + " (userid, picid, commentid) VALUES (?, ?, ?)";
        }

        protected BoundStatement bindWrite(Session session, String tableName, PreparedStatement write, long i)
        {
            return write.bind(i, i, i);
        }

        protected String createTableStatement()
        {
            return "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))";
        }

        protected long deletions(long max)
        {
            return 0;
        }
    }
}
