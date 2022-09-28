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

package org.apache.cassandra.db.memtable.pmem;

//import java.io.FilenameFilter;

import java.io.IOError;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import com.intel.pmem.llpl.TransactionalHeap;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.memtable.PersistentMemoryMemtable;
import org.apache.cassandra.schema.TableMetadataRef;

public class MemtableImporter
{
    private java.util.stream.Collectors Collectors;

    public synchronized List<String> importMemtables(Set<String> srcPath, TableMetadataRef metadata, ColumnFamilyStore cfs)
    {
        String dest = System.getProperty("pmem_path") + "/" + (metadata.get().id.toString()).replaceAll("\\-", "");
        PersistentMemoryMemtable.removeFromTableMetadatMap(metadata);
        TransactionalHeap heap = TransactionalHeap.openHeap(dest);
        heap.close();
        PersistentMemoryMemtable.removePmemFilesFormDestination(dest, false);
        List<String> failedTasks = this.restoreSnapshotData(srcPath,dest);
        new PersistentMemoryMemtable(metadata, cfs);
        return failedTasks;
    }

    private List<String> restoreSnapshotData(Set<String> srcPath, String dest)
    {
        List<String> failedSnapshotTasks = new ArrayList<String>();
        for (String src : srcPath)
        {
            try
            {
                Stream<Path> walk = Files.walk(Paths.get(src));
                walk
                .filter(p -> !Files.isDirectory(p) && (p.toString().endsWith(".pmem") || p.toString().endsWith(".set")  ))
                .forEach(f -> {
                    try {
                        Files.copy(f, Paths.get(dest).resolve(Paths.get(src).relativize(f)), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    }
                    catch (IOException e)
                    {
                        failedSnapshotTasks.add(src);
                    }
                });        // collect all matched to a List
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
        return failedSnapshotTasks;
    }
}
