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
import java.util.stream.Stream;

import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tools.Util;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;


@State(Scope.Benchmark)
public class SSTablePipeBench extends SSTableAbstractPipeBench
{
    @TearDown(Level.Invocation)
    public void closeReaderAndDeleteOutput()
    {
        for (File file : tmpDir.listFiles())
        {
            file.delete();
        }
    }

    @Benchmark
    public void readAndWrite() throws Throwable
    {
        SSTableReader ssTableReader = SSTableReader.openNoValidation(null, desc, TableMetadataRef.forOfflineTools(metadata));
        try (SSTableWriter ssTableWriter = CompactionManager.createWriter(cfs, new org.apache.cassandra.io.util.File(tmpDir), -1, -1, null, false, ssTableReader, LifecycleTransaction.offline(OperationType.COMPACTION));)
        {
            final ISSTableScanner currentScanner = ssTableReader.getScanner();
            Stream<UnfilteredRowIterator> partitions = Util.iterToStream(currentScanner);
            partitions.forEach(unfilteredRowIterator -> {
                ssTableWriter.append(unfilteredRowIterator);
            });
            ssTableWriter.finish(false);
        }
        ssTableReader.ref().close();
    }
}
