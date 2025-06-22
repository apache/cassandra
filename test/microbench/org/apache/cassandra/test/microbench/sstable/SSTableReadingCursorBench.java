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

import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class SSTableReadingCursorBench extends SSTableAbstractBench
{
    private SSTableReader ssTableReader;
    private SSTableCursorReader cursor;

    @Setup(Level.Invocation)
    public void prepareReader() throws IOException
    {
        ssTableReader = super.getReader();
        cursor = new SSTableCursorReader(ssTableReader);
    }

    @TearDown(Level.Invocation)
    public void closeReader() throws Exception
    {
        cursor.close();
        ssTableReader.ref().close();
    }

    long[] counters = new long[4];


    @Benchmark
    public void readPartitionAndUnfiltered() throws IOException
    {
        SSTableReadingFileCursorBench.readPartitionAndUnfiltered(counters, cursor);
    }

    @Benchmark
    public void readPartitionSkipUnfiltered() throws IOException
    {
        SSTableReadingFileCursorBench.readPartitionSkipUnfiltered(counters, cursor);
    }

    @Benchmark
    public void skipPartition() throws IOException
    {
        SSTableReadingFileCursorBench.skipPartition(counters, cursor);
    }

}
