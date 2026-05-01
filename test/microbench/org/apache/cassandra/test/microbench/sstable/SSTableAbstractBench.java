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

package org.apache.cassandra.test.microbench.sstable;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tcm.ClusterMetadataService;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 1)
@State(Scope.Benchmark)
public class SSTableAbstractBench extends CQLTester
{
    ColumnFamilyStore cfs;
    String keyspace;

    @Param("50000")
    int rowCount = 50000;
    private String table;
    protected String writeStatement;

    // TODO: elaborate data setup with multiple schemas
    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        prepareServer();
        beforeTest();

        setupTable();
        setupData();
    }

    protected void setupTable()
    {
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid1 bigint, picid2 bigint, commentid bigint, " +
                                             "PRIMARY KEY(userid, picid1, picid2))");
        execute("use "+keyspace+";");

        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
    }

    protected void setupData()
    {
        writeStatement = "INSERT INTO " + table + "(userid,picid1,picid2,commentid)VALUES(?,?,?,?)";
        for (long i = 0; i < rowCount; i++)
            insertForIndex(writeStatement, i);

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
    }

    protected UntypedResultSet insertForIndex(String writeStatement, long i)
    {
        return execute(writeStatement, i, i, i, i);
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CommitLog.instance.shutdownBlocking();
        ClusterMetadataService.instance().log().close();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }

    public SSTableReader getReader() throws IOException
    {
        return cfs.getLiveSSTables().stream().filter(s -> s.getKeyspaceName().equals(keyspace)).findFirst().orElse(null);
    }
}
