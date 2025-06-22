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
import java.nio.file.Files;
import java.util.List;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.tools.Util;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;


@State(Scope.Benchmark)
public class SSTableAbstractPipeBench extends SSTableAbstractBench
{
    Descriptor desc;
    TableMetadata metadata;
    File tmpDir;
    List<org.apache.cassandra.io.util.File> snapshotFiles;

    @Setup(Level.Trial)
    public void setupSnapshots() throws Throwable
    {
        SnapshotManager.instance.takeSnapshot("originals", cfs.getKeyspaceTableName());

        snapshotFiles = cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots("originals").listFiles();

        desc = Descriptor.fromFileWithComponent(snapshotFiles.get(0), false).left;
        metadata = Util.metadataFromSSTable(desc);
        tmpDir = Files.createTempDirectory("sstable-copy").toFile();
        System.err.println("Writing to : " + tmpDir);
    }
}
