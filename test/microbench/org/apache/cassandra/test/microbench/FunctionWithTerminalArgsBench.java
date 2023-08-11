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


import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks the performance of CQL functions calls with terminal arguments.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 15, time = 2) // seconds
@Measurement(iterations = 10, time = 2) // seconds
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class FunctionWithTerminalArgsBench extends CQLTester
{
    private static final int NUM_PARTITIONS = 10;
    private static final int NUM_CLUSTERINGS = 100;

    // We repeat the number of times that the function call appears on the query to amplify its effect on the benchmark,
    // since its impact is relatively small in the context of a full query.
    private static final int NUM_FUNCTION_CALLS = 10;

    private String udf;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        // we disable UDF threads to avoid the overhead and better see the impact of the terminal arguments
        OverrideConfigurationLoader.override((config) -> {
            config.allow_insecure_udfs = true;
            config.user_defined_functions_threads_enabled = false;
        });
        DatabaseDescriptor.daemonInitialization();

        CQLTester.setUpClass();
        beforeTest();

        udf = createFunction(KEYSPACE,
                             "int, text",
                             " CREATE FUNCTION %s (a1 text, a2 text, a3 text, a4 text, a5 text)" +
                             " CALLED ON NULL INPUT" +
                             " RETURNS text" +
                             " LANGUAGE java" +
                             " AS 'return a1 + a2 + a3 + a4 + a5;'");

        String table = createTable(KEYSPACE, "CREATE TABLE %s (k int, c int, v text, PRIMARY KEY(k, c))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        System.out.println("Writing " + (NUM_PARTITIONS * NUM_CLUSTERINGS) + " rows...");
        int n = 0;
        for (int k = 0; k < NUM_PARTITIONS; k++)
        {
            for (int c = 0; c < NUM_CLUSTERINGS; c++)
            {
                execute("INSERT INTO %s(k, c, v) VALUES (?, ?, ?)", k, c, String.valueOf(n++));
            }
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws InterruptedException
    {
        CommitLog.instance.shutdownBlocking();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }

    @Benchmark
    public Object none()
    {
        return execute("v");
    }

    @Benchmark
    public Object add()
    {
        return execute("v + 'abc'");
    }

    @Benchmark
    public Object udf()
    {
        return execute(udf + "(v, 'a1', 'a2', 'a3', 'a4')");
    }

    @Benchmark
    public Object maskReplace()
    {
        return execute("mask_replace(v, '***')");
    }

    @Benchmark
    public Object maskPartial()
    {
        return execute("mask_inner(v, 1, 1, '#')");
    }

    @Benchmark
    public Object maskHash()
    {
        return execute("mask_hash(v, 'MD5')");
    }

    private UntypedResultSet execute(String functionCall)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (int i = 0; i < NUM_FUNCTION_CALLS; i++)
        {
            if (i > 0)
                sb.append(", ");
            sb.append(functionCall);
        }
        sb.append(" FROM %s");
        return super.execute(sb.toString());
    }
}
