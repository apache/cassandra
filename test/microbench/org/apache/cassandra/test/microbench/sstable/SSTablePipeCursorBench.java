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

import java.io.File;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.SSTableCursorWriter;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;


@State(Scope.Benchmark)
public class SSTablePipeCursorBench extends SSTableAbstractPipeBench
{
    @TearDown(Level.Invocation)
    public void closeReaderAndDeleteOutput() throws Exception
    {
        for (File file : tmpDir.listFiles())
        {
            file.delete();
        }
    }

    @Benchmark
    public void readAndWrite() throws Throwable
    {
        try(SSTableCursorReader cursorReader = SSTableCursorReader.fromDescriptor(desc);
            SortedTableWriter ssTableWriter = (SortedTableWriter) CompactionManager.createWriter(cfs, new org.apache.cassandra.io.util.File(tmpDir), 0, 0, null, false, cursorReader.ssTableReader(), LifecycleTransaction.offline(OperationType.COMPACTION));
            SSTableCursorWriter cursorWriter = new SSTableCursorWriter(ssTableWriter);){
            SSTableCursorPipeUtil.copySSTable(cursorReader, cursorWriter);
        }
    }
}
